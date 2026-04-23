"""
Complex PySpark transformation: Customer Lifetime Value (CLV) + RFM scoring.

Given three source DataFrames (customers, orders, order_items enriched with
product price), this module produces a gold-layer CLV table that demonstrates:

* Multi-way joins with broadcast hints
* Aggregations (sum, count, countDistinct, max)
* Window functions (running totals, lag, dense_rank, ntile for RFM quintiles)
* Conditional bucketing (customer tier, churn risk)
* Date arithmetic (recency in days, tenure in months)
* Array construction / explode for "top categories" flattening
* Reuse of shared utilities (currency & name normalization)

The public entry point `build_customer_lifetime_value` is a pure function of
DataFrames in / DataFrame out so that it can be unit-tested without a pipeline
runtime.
"""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.transformations import normalize_currency_code, normalize_name


@dataclass(frozen=True)
class CLVConfig:
    """Configuration for the CLV / RFM transformation."""

    as_of_date: date
    churn_threshold_days: int = 180
    top_n_categories: int = 3
    rfm_bucket_count: int = 5


def _normalize_customers(customers: DataFrame) -> DataFrame:
    return (
        customers
        .withColumn("full_name", normalize_name(F.col("full_name")))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("signup_date", F.to_date("signup_date"))
    )


def _normalize_orders(orders: DataFrame) -> DataFrame:
    return (
        orders
        .withColumn("currency", normalize_currency_code(F.col("currency")))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("order_date", F.to_date("order_ts"))
    )


def _order_level_metrics(orders: DataFrame, items: DataFrame) -> DataFrame:
    """Compute per-order revenue and item counts from line items."""
    line_totals = items.withColumn(
        "line_total",
        F.col("quantity").cast("decimal(18,2)") * F.col("unit_price").cast("decimal(18,2)"),
    )

    per_order = (
        line_totals.groupBy("order_id")
        .agg(
            F.sum("line_total").alias("order_revenue"),
            F.sum("quantity").alias("order_items"),
            F.countDistinct("product_id").alias("distinct_products"),
        )
    )

    return orders.join(F.broadcast(per_order), on="order_id", how="left")


def _with_order_sequence(orders_enriched: DataFrame) -> DataFrame:
    """Add per-customer order sequencing & inter-order gaps via window functions."""
    w = Window.partitionBy("customer_id").orderBy("order_ts")
    return (
        orders_enriched
        .withColumn("order_seq", F.row_number().over(w))
        .withColumn(
            "prev_order_ts",
            F.lag("order_ts").over(w),
        )
        .withColumn(
            "days_since_prev_order",
            F.when(
                F.col("prev_order_ts").isNotNull(),
                F.datediff(F.col("order_date"), F.to_date("prev_order_ts")),
            ),
        )
        .withColumn(
            "running_revenue",
            F.sum("order_revenue").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
    )


def _top_categories_per_customer(
    items: DataFrame,
    orders: DataFrame,
    top_n: int,
) -> DataFrame:
    """Rank product categories by revenue per customer and keep the top N."""
    items_with_customer = items.join(
        orders.select("order_id", "customer_id"),
        on="order_id",
        how="inner",
    )
    cat_rev = (
        items_with_customer
        .groupBy("customer_id", "category")
        .agg(
            (F.sum(F.col("quantity") * F.col("unit_price"))).alias("category_revenue"),
        )
    )

    ranked = cat_rev.withColumn(
        "rnk",
        F.dense_rank().over(
            Window.partitionBy("customer_id").orderBy(F.col("category_revenue").desc())
        ),
    ).filter(F.col("rnk") <= top_n)

    return (
        ranked.groupBy("customer_id")
        .agg(
            F.collect_list(
                F.struct("rnk", "category", "category_revenue")
            ).alias("top_categories")
        )
    )


def _customer_aggregates(orders_seq: DataFrame, as_of: date) -> DataFrame:
    as_of_lit = F.lit(as_of)

    return orders_seq.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("order_revenue").alias("lifetime_revenue"),
        F.avg("order_revenue").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
        F.avg("days_since_prev_order").alias("avg_days_between_orders"),
    ).withColumn(
        "recency_days",
        F.datediff(as_of_lit, F.col("last_order_date")),
    ).withColumn(
        "tenure_months",
        F.months_between(as_of_lit, F.col("first_order_date")).cast("int"),
    )


def _with_rfm_scores(agg: DataFrame, buckets: int) -> DataFrame:
    """Assign RFM quintiles using ntile. Higher score = better customer."""
    w_all = Window.orderBy(F.col("recency_days").asc())
    w_freq = Window.orderBy(F.col("order_count").desc())
    w_mon = Window.orderBy(F.col("lifetime_revenue").desc())

    return (
        agg
        .withColumn("r_score", F.ntile(buckets).over(w_all))
        .withColumn("f_score", F.ntile(buckets).over(w_freq))
        .withColumn("m_score", F.ntile(buckets).over(w_mon))
        # ntile produces 1..N where 1 is best for our orderings; invert so 5 = best.
        .withColumn("r_score", F.lit(buckets + 1) - F.col("r_score"))
        .withColumn("f_score", F.lit(buckets + 1) - F.col("f_score"))
        .withColumn("m_score", F.lit(buckets + 1) - F.col("m_score"))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )


def _with_tier_and_churn(agg: DataFrame, churn_threshold_days: int) -> DataFrame:
    tier = (
        F.when(F.col("rfm_score") >= 13, F.lit("PLATINUM"))
        .when(F.col("rfm_score") >= 10, F.lit("GOLD"))
        .when(F.col("rfm_score") >= 7, F.lit("SILVER"))
        .otherwise(F.lit("BRONZE"))
    )
    churn = F.col("recency_days") > F.lit(churn_threshold_days)
    return (
        agg
        .withColumn("customer_tier", tier)
        .withColumn("is_at_risk", churn)
    )


def build_customer_lifetime_value(
    customers: DataFrame,
    orders: DataFrame,
    order_items: DataFrame,
    config: CLVConfig,
) -> DataFrame:
    """Build the gold CLV DataFrame.

    Parameters
    ----------
    customers : DataFrame[customer_id, full_name, country, signup_date]
    orders : DataFrame[order_id, customer_id, order_ts, currency]
    order_items : DataFrame[order_id, product_id, category, quantity, unit_price]
    config : CLVConfig
    """
    customers_n = _normalize_customers(customers)
    orders_n = _normalize_orders(orders)

    orders_enriched = _order_level_metrics(orders_n, order_items)
    orders_seq = _with_order_sequence(orders_enriched)

    agg = _customer_aggregates(orders_seq, config.as_of_date)
    scored = _with_rfm_scores(agg, config.rfm_bucket_count)
    scored = _with_tier_and_churn(scored, config.churn_threshold_days)

    top_cats = _top_categories_per_customer(order_items, orders_n, config.top_n_categories)

    result = (
        customers_n
        .join(scored, on="customer_id", how="left")
        .join(top_cats, on="customer_id", how="left")
        .withColumn("as_of_date", F.lit(config.as_of_date))
        .withColumn(
            "lifetime_revenue",
            F.coalesce(F.col("lifetime_revenue"), F.lit(0).cast("decimal(18,2)")),
        )
        .withColumn("order_count", F.coalesce(F.col("order_count"), F.lit(0)))
    )

    return result.select(
        "customer_id",
        "full_name",
        "country",
        "signup_date",
        "as_of_date",
        "first_order_date",
        "last_order_date",
        "tenure_months",
        "recency_days",
        "order_count",
        "avg_days_between_orders",
        "lifetime_revenue",
        "avg_order_value",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "customer_tier",
        "is_at_risk",
        "top_categories",
    )

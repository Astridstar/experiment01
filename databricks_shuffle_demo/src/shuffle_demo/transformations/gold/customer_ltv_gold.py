"""
Gold Layer — Customer Lifetime Value
======================================
SHUFFLE DEMO: GROUPBY + SORT-MERGE JOIN + THREE WINDOW FUNCTIONS

This table surfaces per-customer spending metrics and multi-dimensional
rankings.  Each aggregation or window with a different partition key
generates its own Exchange (shuffle) node.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #1 — GroupBy customer_id  (~200 K groups, 200 shuffle partitions)
──────────────────────────────────────────────────────────────────────────────
  groupBy("customer_id") over completed transactions.
  Each of the 200 K customer_ids is assigned to partition
  hash(customer_id) % spark.sql.shuffle.partitions.
  With 200 shuffle partitions, each partition holds ~1 K customer groups.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #2 — Sort-Merge Join: customer metrics ⋈ customers  (on customer_id)
──────────────────────────────────────────────────────────────────────────────
  Both sides already partitioned by customer_id (from the groupBy above).
  Spark's AQE can detect this and skip one Exchange — but with AQE disabled
  (spark.sql.adaptive.enabled=false in pipeline YAML), a full sort-merge
  shuffle is executed.  Good for demo; remove the AQE override in production.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #3 — Window  partitionBy("segment")  (5 partitions)
──────────────────────────────────────────────────────────────────────────────
  Rank customers by LTV within their loyalty segment.
  5 segments → 5 reducer partitions.  Very skewed if Diamond is rare
  (few rows) vs Bronze (many rows).  Good example of data skew impact.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #4 — Window  partitionBy("country_code", "segment")  (≤100 partitions)
──────────────────────────────────────────────────────────────────────────────
  Rank customers within their country + segment combination.
  Finer granularity → more, smaller partitions.
  Contrast with shuffle #3: same data, different shuffle granularity.

──────────────────────────────────────────────────────────────────────────────
OUTPUT PARTITIONING
──────────────────────────────────────────────────────────────────────────────
  repartition(50, "segment") before returning:
    • Full shuffle — all ~200 K rows redistributed.
    • Groups customers by segment for efficient downstream queries
      that filter or aggregate by segment.
    • Creates 50 evenly-sized output files (vs 5 unbalanced segment buckets).

  Difference from coalesce: repartition produces balanced files regardless
  of the current data layout; coalesce only merges, never rebalances.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

catalog = spark.conf.get("catalog", "dev")
schema  = spark.conf.get("schema",  "shuffle_demo")


@dp.materialized_view(
    name="customer_ltv_gold",
    comment=(
        "Gold: customer lifetime value with multi-dimensional ranking. "
        "Shuffle #1: groupBy(customer_id) — 200 K groups. "
        "Shuffle #2: sort-merge join customer metrics ⋈ attributes. "
        "Shuffle #3: window rank partitionBy(segment) — 5 partitions. "
        "Shuffle #4: window rank partitionBy(country_code, segment) — ≤100 partitions. "
        "Output repartition(50, 'segment') for balanced downstream reads."
    ),
)
def customer_ltv_gold():

    txn_silver   = spark.read.table(f"`{catalog}`.`{schema}`.txn_silver")
    customers_df = spark.read.table(f"`{catalog}`.`{schema}`.customers_bronze")

    # ── SHUFFLE #1: GroupBy customer_id ───────────────────────────────────
    customer_metrics = (
        txn_silver
        .filter(F.col("order_status") == "completed")
        .groupBy("customer_id")
        .agg(
            F.count("txn_id")                                  .alias("total_orders"),
            F.sum("total_amount")                              .alias("lifetime_value"),
            F.avg("total_amount")                              .alias("avg_order_value"),
            F.sum("quantity")                                  .alias("total_units"),
            F.avg("discount_pct")                              .alias("avg_discount_pct"),
            F.countDistinct("category")                        .alias("category_breadth"),
            F.countDistinct("txn_month")                       .alias("active_months"),
            F.min("txn_date")                                  .alias("first_purchase_date"),
            F.max("txn_date")                                  .alias("last_purchase_date"),
            F.sum("gross_margin_amount")                       .alias("total_margin_contributed"),
            F.sum(F.when(F.col("is_high_value_txn"), 1).otherwise(0)).alias("high_value_order_count"),
        )
        .withColumn(
            "avg_monthly_spend",
            F.round(F.col("lifetime_value") / F.col("active_months"), 2),
        )
        .withColumn(
            "days_between_first_last",
            F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")),
        )
    )

    # ── SHUFFLE #2: Sort-Merge Join — metrics ⋈ customer attributes ───────
    # Both sides partitioned by customer_id after the groupBy shuffle.
    # With AQE disabled this triggers a second sort-merge Exchange.
    # With AQE enabled Spark may skip the Exchange (sort-merge on already-
    # partitioned data — "sort merge without exchange" optimisation).
    customer_ltv = customer_metrics.join(
        customers_df.select(
            "customer_id", "segment", "country_code",
            "age", "gender", "credit_limit", "preferred_payment",
        ),
        on="customer_id",
        how="inner",   # only customers who made at least one purchase
    )

    # ── SHUFFLE #3: Window — LTV Rank within Segment ──────────────────────
    # 5 distinct segment values → 5 reducer partitions.
    # If Diamond (5 % of customers = 10 K rows) vs Bronze (30 % = 60 K rows)
    # the partitions are SKEWED — visible in the Spark UI task duration chart.
    segment_window = (
        Window
        .partitionBy("segment")
        .orderBy(F.col("lifetime_value").desc())
    )

    customer_ltv = (
        customer_ltv
        .withColumn("rank_in_segment",         F.rank()        .over(segment_window))
        .withColumn("pct_rank_in_segment",      F.round(F.percent_rank().over(segment_window) * 100, 1))
        .withColumn("dense_rank_in_segment",    F.dense_rank()  .over(segment_window))
    )

    # ── SHUFFLE #4: Window — LTV Rank within Country × Segment ───────────
    # 20 countries × 5 segments = up to 100 distinct (country, segment) groups.
    # Finer granularity than shuffle #3 → more, smaller partitions.
    # Demonstrates how partition key cardinality affects shuffle partition count.
    country_segment_window = (
        Window
        .partitionBy("country_code", "segment")
        .orderBy(F.col("lifetime_value").desc())
    )

    customer_ltv = customer_ltv.withColumn(
        "rank_in_country_segment",
        F.rank().over(country_segment_window),
    )

    # ── Derived classification fields (no shuffle) ────────────────────────
    customer_ltv = (
        customer_ltv
        .withColumn(
            "ltv_tier",
            F.when(F.col("lifetime_value") >= 10_000, F.lit("HIGH"))
            .when(F.col("lifetime_value") >=  2_000,  F.lit("MEDIUM"))
            .otherwise(                                F.lit("LOW")),
        )
        .withColumn(
            "days_since_last_purchase",
            F.datediff(F.current_date(), F.col("last_purchase_date")),
        )
        .withColumn(
            "is_at_risk",
            # No purchase in the last 90 days
            F.datediff(F.current_date(), F.col("last_purchase_date")) > 90,
        )
        .withColumn("gold_refreshed_at", F.current_timestamp())
    )

    # ── OUTPUT: repartition(50, "segment") — full shuffle ─────────────────
    # Unlike coalesce, repartition performs a full shuffle to produce
    # exactly 50 evenly-sized output files, one for each hash(segment) bucket.
    # 200 K rows / 50 partitions = ~4 K rows per file — manageable file size.
    # Downstream queries filtering by segment benefit from partition pruning.
    return customer_ltv.repartition(50, "segment")

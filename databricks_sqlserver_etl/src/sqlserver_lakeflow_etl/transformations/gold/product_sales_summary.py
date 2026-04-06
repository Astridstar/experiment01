"""
Gold Layer: Product sales summary for business analytics.

Aggregates silver_orders into product-level sales metrics
with monthly trends.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_product_sales_summary",
    comment="Gold: Product-level sales metrics with monthly trends",
)
def gold_product_sales_summary():
    orders = spark.read.table("silver_orders")

    df = (
        orders
        .groupBy("product_id", "product_name", "category", "sub_category")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("line_total").alias("total_revenue"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.avg("discount").alias("avg_discount_rate"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("order_id").alias("total_orders"),
            F.min("order_date").alias("first_sale_date"),
            F.max("order_date").alias("last_sale_date"),
        )
    )

    # Revenue per customer
    df = df.withColumn(
        "revenue_per_customer",
        F.round(F.col("total_revenue") / F.col("unique_customers"), 2),
    )

    df = df.withColumn("gold_processed_ts", F.current_timestamp())

    return df


@dp.materialized_view(
    name="gold_monthly_sales_trend",
    comment="Gold: Monthly sales trend by category",
)
def gold_monthly_sales_trend():
    orders = spark.read.table("silver_orders")

    df = (
        orders
        .withColumn("order_month", F.date_trunc("month", "order_date"))
        .groupBy("order_month", "category")
        .agg(
            F.sum("line_total").alias("monthly_revenue"),
            F.sum("quantity").alias("monthly_units"),
            F.countDistinct("order_id").alias("monthly_orders"),
            F.countDistinct("customer_id").alias("monthly_customers"),
        )
        .orderBy("order_month", "category")
    )

    df = df.withColumn("gold_processed_ts", F.current_timestamp())

    return df

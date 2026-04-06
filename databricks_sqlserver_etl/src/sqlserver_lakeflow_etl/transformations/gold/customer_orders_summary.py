"""
Gold Layer: Customer orders summary for business analytics.

Aggregates silver_orders and silver_customers into a single
customer-level summary with key business metrics.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_customer_orders_summary",
    comment="Gold: Customer-level order summary with lifetime value and segmentation",
)
def gold_customer_orders_summary():
    customers = spark.read.table("silver_customers")
    orders = spark.read.table("silver_orders")

    # Aggregate order metrics per customer
    order_metrics = (
        orders
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("order_id").alias("distinct_orders"),
            F.sum("line_total").alias("lifetime_value"),
            F.avg("line_total").alias("avg_order_line_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("category").alias("distinct_categories"),
        )
    )

    # Join with customer details
    df = customers.join(order_metrics, on="customer_id", how="left")

    # Calculate days since last order
    df = df.withColumn(
        "days_since_last_order",
        F.datediff(F.current_date(), F.col("last_order_date")),
    )

    # Customer segmentation based on RFM-style logic
    df = df.withColumn(
        "customer_segment",
        F.when(F.col("lifetime_value").isNull(), "prospect")
        .when(
            (F.col("lifetime_value") >= 10000) & (F.col("days_since_last_order") <= 90),
            "vip",
        )
        .when(
            (F.col("lifetime_value") >= 5000) & (F.col("days_since_last_order") <= 180),
            "loyal",
        )
        .when(F.col("days_since_last_order") <= 365, "active")
        .when(F.col("days_since_last_order") <= 730, "at_risk")
        .otherwise("churned"),
    )

    df = df.withColumn("gold_processed_ts", F.current_timestamp())

    return df.select(
        "customer_id",
        "full_name",
        "email",
        "city",
        "state",
        "country",
        "total_orders",
        "distinct_orders",
        "lifetime_value",
        "avg_order_line_value",
        "first_order_date",
        "last_order_date",
        "days_since_last_order",
        "distinct_categories",
        "customer_segment",
        "quality_score",
        "gold_processed_ts",
    )

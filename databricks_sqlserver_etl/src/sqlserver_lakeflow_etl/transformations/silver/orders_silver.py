"""
Silver Layer: Cleansed and enriched orders with line-item details.

Joins bronze_orders with bronze_order_items and bronze_products to
produce a denormalised order-line view ready for gold aggregations.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.quality_checks import add_quality_flags, add_quality_score


@dp.table(
    name="silver_orders",
    comment="Silver: Enriched orders with line-item and product details from SQL Server",
)
@dp.expect_all_or_drop(
    {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "positive_quantity": "quantity > 0",
        "positive_unit_price": "unit_price > 0",
    }
)
def silver_orders():
    orders_df = spark.read.table("bronze_orders")
    items_df = spark.read.table("bronze_order_items")
    products_df = spark.read.table("bronze_products")

    # Standardise column names
    orders = (
        orders_df
        .withColumnRenamed("OrderID", "order_id")
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("OrderDate", "order_date")
        .withColumnRenamed("ShipDate", "ship_date")
        .withColumnRenamed("Status", "order_status")
        .withColumnRenamed("TotalAmount", "order_total")
        .withColumnRenamed("ModifiedDate", "order_modified_date")
    )

    items = (
        items_df
        .withColumnRenamed("OrderItemID", "order_item_id")
        .withColumnRenamed("OrderID", "order_id")
        .withColumnRenamed("ProductID", "product_id")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("UnitPrice", "unit_price")
        .withColumnRenamed("Discount", "discount")
        .withColumnRenamed("ModifiedDate", "item_modified_date")
    )

    products = (
        products_df
        .withColumnRenamed("ProductID", "product_id")
        .withColumnRenamed("ProductName", "product_name")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("SubCategory", "sub_category")
        .withColumnRenamed("ModifiedDate", "product_modified_date")
        .select("product_id", "product_name", "category", "sub_category")
    )

    # Join orders -> items -> products
    df = (
        orders
        .join(items, on="order_id", how="inner")
        .join(products, on="product_id", how="left")
    )

    # Compute line total
    df = df.withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.coalesce("discount", F.lit(0))), 2),
    )

    # Cast dates
    df = (
        df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("ship_date", F.to_date("ship_date"))
    )

    # Quality rules
    quality_rules = {
        "missing_product": "product_name IS NOT NULL",
        "future_order_date": "order_date <= current_date()",
        "ship_before_order": "ship_date IS NULL OR ship_date >= order_date",
        "negative_discount": "discount IS NULL OR discount >= 0",
    }
    df = add_quality_flags(df, quality_rules)
    df = add_quality_score(df)

    df = df.withColumn("silver_processed_ts", F.current_timestamp())

    return df

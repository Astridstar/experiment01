"""
Bronze Layer — Products
========================
SHUFFLE CONCEPT: SMALL TABLE — broadcast vs sort-merge join

raw_products is partitioned by category (8 directories, 5 K rows total).

At ~5 K rows this table is small enough to be broadcast by Spark's default
autoBroadcastJoinThreshold (10 MB).  A broadcast join:
  • Collects the small table to the driver.
  • Sends (broadcasts) a copy to EVERY executor.
  • Each executor joins its local partition of the large table in-memory.
  • NO SHUFFLE is needed for the large table.

For the shuffle demo the silver layer uses the MERGE hint to force a
sort-merge join on product_id, making the shuffle boundary visible.
In a production pipeline you would remove that hint and let Spark
optimise automatically (likely choosing broadcast for this table).

Broadcast join visible in Spark UI as: BroadcastHashJoin (no Exchange node).
Sort-merge join visible as:            SortMergeJoin     (two Exchange nodes).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog", "dev")
schema  = spark.conf.get("schema",  "shuffle_demo")


@dp.materialized_view(
    name="products_bronze",
    comment=(
        "Bronze: product catalogue (5 K rows, 8 category partitions). "
        "Small enough for broadcast join, but silver forces sort-merge "
        "via MERGE hint to demonstrate full shuffle on product_id."
    ),
)
def products_bronze():
    return (
        spark.read
        .table(f"`{catalog}`.`{schema}`.raw_products")
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )

"""
Bronze Layer — Customers
=========================
SHUFFLE CONCEPT: MULTI-PARTITION READ + BROADCAST candidate

raw_customers is partitioned by region (5 directories).
Each region partition is read in a separate Spark task (parallel I/O, no shuffle).

Join strategy preview
---------------------
200 K customer rows is typically too large to broadcast (> autoBroadcastJoinThreshold
default of 10 MB in serialised form).  Spark therefore chooses a sort-merge join
when transactions are joined to customers in the silver layer:

  SORT-MERGE JOIN requires TWO shuffle stages:
    ① Shuffle transactions  by hash(customer_id) → 200 partitions
    ② Shuffle customers     by hash(customer_id) → 200 partitions
    ③ Sort each partition by customer_id
    ④ Merge-join matching partition pairs on the same executor

The silver transformation forces this strategy explicitly so the shuffle
is always visible in the Spark UI regardless of table statistics.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog", "dev")
schema  = spark.conf.get("schema",  "shuffle_demo")


@dp.materialized_view(
    name="customers_bronze",
    comment=(
        "Bronze: customer reference data (200 K rows). "
        "Source partitioned by region (5 parallel read tasks). "
        "Join with transactions in silver triggers sort-merge shuffle on customer_id."
    ),
)
def customers_bronze():
    return (
        spark.read
        .table(f"`{catalog}`.`{schema}`.raw_customers")
        .filter(F.col("is_active") == True)
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )

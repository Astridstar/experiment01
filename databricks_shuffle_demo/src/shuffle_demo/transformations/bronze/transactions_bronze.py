"""
Bronze Layer — Transactions
============================
SHUFFLE CONCEPT: MULTI-PARTITION READ (no shuffle here)

raw_transactions is partitioned by (region, txn_month) — up to 60 directories.

What Spark does when reading a partitioned Delta table
------------------------------------------------------
1. The driver enumerates partition directories and their files.
2. Each file is assigned to one Spark task (one CPU core).
3. Tasks execute IN PARALLEL across all available executors.
   → This is parallel I/O, NOT shuffle.  No data crosses the network yet.

Partition pruning (bonus)
--------------------------
If a downstream query or filter references the partition columns, Databricks
pushes the predicate down to the scan and skips irrelevant directories.
Example:  .filter(F.col("region") == "APAC")
          → Spark reads only 12 of the 60 partition directories.
Visible in Spark UI → SQL → scan node: "numFiles read" vs "numFiles total".

No shuffle happens at this bronze stage.
The first Exchange (shuffle) node appears in the silver join stage.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog", "dev")
schema  = spark.conf.get("schema",  "shuffle_demo")


@dp.materialized_view(
    name="txn_bronze",
    comment=(
        "Bronze: raw e-commerce transactions. "
        "Source partitioned by (region, txn_month) → up to 60 parallel read tasks. "
        "No shuffle at this layer; first shuffle occurs in silver (sort-merge joins)."
    ),
)
def transactions_bronze():
    return (
        spark.read
        .table(f"`{catalog}`.`{schema}`.raw_transactions")
        # Lightweight type-safe cast — no shuffle required
        .withColumn("txn_date",      F.col("txn_date").cast("date"))
        .withColumn("txn_timestamp", F.col("txn_timestamp").cast("timestamp"))
        .withColumn("total_amount",  F.col("total_amount").cast("double"))
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )

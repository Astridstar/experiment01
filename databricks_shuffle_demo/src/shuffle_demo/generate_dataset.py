"""
Databricks Shuffle Demo — Dataset Generator
============================================
Generates three partitioned Delta tables to serve as the source for the
shuffle demo DLT pipeline.

Tables created
--------------
  raw_products     — 5 K rows, partitioned by category (8 dirs)
  raw_customers    — 200 K rows, partitioned by region  (5 dirs)
  raw_transactions — 2 M rows,  partitioned by (region, txn_month) (≤60 dirs)

WHY partitioning matters for shuffle demos
------------------------------------------
Partitioned source tables let Spark demonstrate two complementary behaviours:

  1. MULTI-PARTITION READS (parallel I/O)
     Spark assigns partition directories to tasks and reads them in parallel.
     Visible in the Spark UI → SQL → scan: "numFiles", "numPartitions".

  2. PARTITION PRUNING (skipping irrelevant data)
     If a downstream query filters on a partition column (e.g. region='APAC'),
     Spark skips the other four region directories entirely.
     Visible in Spark UI: "numFiles read" < "numFiles total".

Run as a Databricks Job Python wheel task (entry-point: generate-dataset).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate shuffle demo dataset")
    parser.add_argument("--catalog", default="dev")
    parser.add_argument("--schema", default="shuffle_demo")
    parser.add_argument("--num-transactions", type=int, default=2_000_000)
    parser.add_argument("--num-customers",    type=int, default=200_000)
    parser.add_argument("--num-products",     type=int, default=5_000)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    catalog   = args.catalog
    schema    = args.schema
    num_txn   = args.num_transactions
    num_cust  = args.num_customers
    num_prod  = args.num_products

    sep = "=" * 64
    print(f"\n{sep}")
    print("DATABRICKS SHUFFLE DEMO — DATASET GENERATOR")
    print(sep)
    print(f"  Target   : {catalog}.{schema}")
    print(f"  Products : {num_prod:>12,}")
    print(f"  Customers: {num_cust:>12,}")
    print(f"  Txns     : {num_txn:>12,}")
    print(sep)

    # Control how many reducers Spark uses when generating the dataset itself.
    # (The DLT pipeline sets its own value via pipeline configuration.)
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    _generate_products(spark, catalog, schema, num_prod)
    _generate_customers(spark, catalog, schema, num_cust)
    _generate_transactions(spark, catalog, schema, num_txn, num_cust, num_prod)

    print(f"\n{sep}")
    print("DATASET GENERATION COMPLETE")
    print(sep)
    print(f"  {catalog}.{schema}.raw_products     — {num_prod:,} rows  | partitioned by category (8)")
    print(f"  {catalog}.{schema}.raw_customers    — {num_cust:,} rows  | partitioned by region   (5)")
    print(f"  {catalog}.{schema}.raw_transactions — {num_txn:,} rows  | partitioned by (region, txn_month) (≤60)")
    print("\nNext step: run the shuffle_demo DLT pipeline.")


# ---------------------------------------------------------------------------
# Products — 5 K rows, partitioned by category (8 partitions)
# ---------------------------------------------------------------------------

CATEGORIES = [
    "Electronics", "Clothing", "Food & Beverage", "Books",
    "Sports",      "Home & Garden", "Beauty",      "Toys",
]

BRANDS = [
    "AlphaMax", "BetaCorp", "GammaTech", "DeltaWave", "EpsilonCo",
    "ZetaBrand", "EtaLine",  "ThetaWorks", "IotaGroup", "KappaStore",
]


def _generate_products(spark, catalog: str, schema: str, num_prod: int) -> None:
    print(f"\n[1/3] Generating raw_products ({num_prod:,} rows) …")

    cat_arr   = F.array(*[F.lit(c) for c in CATEGORIES])
    brand_arr = F.array(*[F.lit(b) for b in BRANDS])
    type_arr  = F.array(F.lit("Type-A"), F.lit("Type-B"), F.lit("Type-C"),
                        F.lit("Type-D"), F.lit("Type-E"))

    products_df = (
        spark.range(num_prod)
        .withColumnRenamed("id", "_idx")
        .withColumn(
            "product_id",
            F.concat(F.lit("PROD"), F.lpad(F.col("_idx").cast("string"), 6, "0")),
        )
        # Round-robin assignment ensures each category gets an equal share.
        .withColumn(
            "category",
            F.element_at(cat_arr, (F.col("_idx") % len(CATEGORIES)).cast("int") + 1),
        )
        .withColumn(
            "subcategory",
            F.concat(
                F.col("category"),
                F.lit(" — "),
                F.element_at(type_arr, (F.rand(seed=11) * 5).cast("int") + 1),
            ),
        )
        .withColumn(
            "brand",
            F.element_at(brand_arr, (F.rand(seed=12) * len(BRANDS)).cast("int") + 1),
        )
        .withColumn("unit_cost",  F.round(F.rand(seed=13) * 490 + 10,  2))
        .withColumn("list_price", F.round(F.col("unit_cost") * (1 + F.rand(seed=14) * 0.8 + 0.2), 2))
        .withColumn(
            "margin_pct",
            F.round(
                (F.col("list_price") - F.col("unit_cost")) / F.col("list_price") * 100,
                1,
            ),
        )
        .withColumn("weight_kg",    F.round(F.rand(seed=15) * 9.5 + 0.5, 2))
        # Food & Beverage (index 2) is perishable
        .withColumn("is_perishable", (F.col("_idx") % len(CATEGORIES)) == 2)
        .withColumn(
            "launch_date",
            F.date_sub(F.current_date(), (F.rand(seed=16) * 1825).cast("int")),
        )
        .drop("_idx")
    )

    (
        products_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("category")           # 8 partition directories
        .saveAsTable(f"`{catalog}`.`{schema}`.raw_products")
    )
    print(f"  → Written: {catalog}.{schema}.raw_products  (8 category partitions)")


# ---------------------------------------------------------------------------
# Customers — 200 K rows, partitioned by region (5 partitions)
# ---------------------------------------------------------------------------

REGIONS = ["APAC", "EMEA", "AMER", "LATAM", "MEA"]

COUNTRY_BY_REGION = {
    "APAC":  ["SG", "JP", "AU", "IN", "KR"],
    "EMEA":  ["GB", "DE", "FR", "NL", "SE"],
    "AMER":  ["US", "CA", "MX", "US", "US"],   # US weighted higher
    "LATAM": ["BR", "AR", "CO", "CL", "PE"],
    "MEA":   ["AE", "SA", "ZA", "NG", "KE"],
}

SEGMENTS        = ["Bronze", "Silver", "Gold", "Platinum", "Diamond"]
GENDERS         = ["M", "F", "Non-Binary"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Crypto", "Gift Card"]


def _generate_customers(spark, catalog: str, schema: str, num_cust: int) -> None:
    print(f"\n[2/3] Generating raw_customers ({num_cust:,} rows) …")

    reg_arr = F.array(*[F.lit(r) for r in REGIONS])
    gen_arr = F.array(*[F.lit(g) for g in GENDERS])
    pay_arr = F.array(*[F.lit(p) for p in PAYMENT_METHODS])

    customers_df = (
        spark.range(num_cust)
        .withColumnRenamed("id", "_idx")
        .withColumn(
            "customer_id",
            F.concat(F.lit("CUST"), F.lpad(F.col("_idx").cast("string"), 8, "0")),
        )
        .withColumn(
            "region",
            F.element_at(reg_arr, (F.rand(seed=20) * len(REGIONS)).cast("int") + 1),
        )
        # Country derived from region (adds a second partition column if needed later)
        .withColumn(
            "country_code",
            F.when(F.col("region") == "APAC",  F.element_at(F.array(*[F.lit(c) for c in COUNTRY_BY_REGION["APAC"]]),  (F.rand(seed=21)*5).cast("int")+1))
            .when(F.col("region") == "EMEA",   F.element_at(F.array(*[F.lit(c) for c in COUNTRY_BY_REGION["EMEA"]]),  (F.rand(seed=21)*5).cast("int")+1))
            .when(F.col("region") == "AMER",   F.element_at(F.array(*[F.lit(c) for c in COUNTRY_BY_REGION["AMER"]]),  (F.rand(seed=21)*5).cast("int")+1))
            .when(F.col("region") == "LATAM",  F.element_at(F.array(*[F.lit(c) for c in COUNTRY_BY_REGION["LATAM"]]), (F.rand(seed=21)*5).cast("int")+1))
            .otherwise(                         F.element_at(F.array(*[F.lit(c) for c in COUNTRY_BY_REGION["MEA"]]),   (F.rand(seed=21)*5).cast("int")+1))
        )
        # Weighted segment distribution: more Bronze/Silver than Platinum/Diamond
        .withColumn(
            "segment",
            F.when(F.rand(seed=22) < 0.30, F.lit("Bronze"))
            .when(F.rand(seed=22) < 0.55,  F.lit("Silver"))
            .when(F.rand(seed=22) < 0.75,  F.lit("Gold"))
            .when(F.rand(seed=22) < 0.90,  F.lit("Platinum"))
            .otherwise(                     F.lit("Diamond")),
        )
        .withColumn("age",    (F.rand(seed=23) * 52 + 18).cast("int"))
        .withColumn("gender", F.element_at(gen_arr, (F.rand(seed=24) * len(GENDERS)).cast("int") + 1))
        .withColumn(
            "signup_date",
            F.date_sub(F.current_date(), (F.rand(seed=25) * 1825).cast("int")),
        )
        # Credit limit scales with segment
        .withColumn(
            "credit_limit",
            F.round(
                F.when(F.col("segment") == "Diamond",  F.rand(seed=26) * 45_000 + 5_000)
                .when(F.col("segment") == "Platinum",  F.rand(seed=26) * 25_000 + 5_000)
                .when(F.col("segment") == "Gold",      F.rand(seed=26) * 15_000 + 2_000)
                .when(F.col("segment") == "Silver",    F.rand(seed=26) *  8_000 + 1_000)
                .otherwise(                             F.rand(seed=26) *  3_000 +   500),
                2,
            ),
        )
        .withColumn(
            "preferred_payment",
            F.element_at(pay_arr, (F.rand(seed=27) * len(PAYMENT_METHODS)).cast("int") + 1),
        )
        .withColumn("is_active", F.rand(seed=28) > 0.05)   # 95 % active
        .drop("_idx")
    )

    (
        customers_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("region")             # 5 partition directories
        .saveAsTable(f"`{catalog}`.`{schema}`.raw_customers")
    )
    print(f"  → Written: {catalog}.{schema}.raw_customers  (5 region partitions)")


# ---------------------------------------------------------------------------
# Transactions — 2 M rows, partitioned by (region, txn_month) (≤60 partitions)
# ---------------------------------------------------------------------------

ORDER_STATUSES = ["completed", "pending", "cancelled", "refunded", "processing"]
CHANNELS       = ["mobile_app", "web", "in_store", "call_center", "partner_api"]


def _generate_transactions(
    spark,
    catalog: str,
    schema: str,
    num_txn: int,
    num_cust: int,
    num_prod: int,
) -> None:
    print(f"\n[3/3] Generating raw_transactions ({num_txn:,} rows) …")

    reg_arr  = F.array(*[F.lit(r) for r in REGIONS])
    pay_arr  = F.array(*[F.lit(p) for p in PAYMENT_METHODS])
    chan_arr = F.array(*[F.lit(c) for c in CHANNELS])

    transactions_df = (
        spark.range(num_txn)
        .withColumnRenamed("id", "_idx")
        .withColumn(
            "txn_id",
            F.concat(F.lit("TXN"), F.lpad(F.col("_idx").cast("string"), 10, "0")),
        )
        # Random FK into customers table (CUST00000000 … CUST{num_cust-1})
        .withColumn(
            "customer_id",
            F.concat(
                F.lit("CUST"),
                F.lpad((F.rand(seed=30) * num_cust).cast("long").cast("string"), 8, "0"),
            ),
        )
        # Random FK into products table (PROD000000 … PROD{num_prod-1})
        .withColumn(
            "product_id",
            F.concat(
                F.lit("PROD"),
                F.lpad((F.rand(seed=31) * num_prod).cast("long").cast("string"), 6, "0"),
            ),
        )
        # Transaction region is independent of the customer's home region,
        # ensuring that the silver join cannot rely on co-location and MUST shuffle.
        .withColumn(
            "region",
            F.element_at(reg_arr, (F.rand(seed=32) * len(REGIONS)).cast("int") + 1),
        )
        # Date spread across the last 12 months
        .withColumn("txn_date", F.date_sub(F.current_date(), (F.rand(seed=33) * 365).cast("int")))
        .withColumn("txn_month", F.date_format(F.col("txn_date"), "yyyy-MM"))
        # Reconstruct a realistic intra-day timestamp from the date
        .withColumn(
            "txn_timestamp",
            (
                F.unix_timestamp(F.col("txn_date").cast("timestamp"))
                + (F.rand(seed=34) * 86_400).cast("long")
            ).cast("timestamp"),
        )
        .withColumn("quantity",     (F.rand(seed=35) * 9 + 1).cast("int"))
        .withColumn("unit_price",   F.round(F.rand(seed=36) * 499 + 1, 2))
        .withColumn(
            "discount_pct",
            F.round(
                F.when(F.rand(seed=37) > 0.70, F.rand(seed=38) * 0.30).otherwise(F.lit(0.0)),
                2,
            ),
        )
        .withColumn(
            "total_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2),
        )
        .withColumn(
            "payment_method",
            F.element_at(pay_arr, (F.rand(seed=39) * len(PAYMENT_METHODS)).cast("int") + 1),
        )
        # Realistic order-status distribution: ~75 % completed
        .withColumn(
            "order_status",
            F.when(F.rand(seed=40) < 0.75, F.lit("completed"))
            .when(F.rand(seed=40) < 0.85,  F.lit("pending"))
            .when(F.rand(seed=40) < 0.92,  F.lit("cancelled"))
            .when(F.rand(seed=40) < 0.97,  F.lit("refunded"))
            .otherwise(                     F.lit("processing")),
        )
        .withColumn(
            "channel",
            F.element_at(chan_arr, (F.rand(seed=41) * len(CHANNELS)).cast("int") + 1),
        )
        .withColumn(
            "store_id",
            F.concat(
                F.col("region"),
                F.lit("_STORE_"),
                F.lpad((F.rand(seed=42) * 99 + 1).cast("int").cast("string"), 3, "0"),
            ),
        )
        .withColumn("_generated_at", F.current_timestamp())
        .drop("_idx")
    )

    # ── Explicit repartition before write ────────────────────────────────────
    # repartition(n, col) triggers a FULL SHUFFLE — all data is redistributed
    # by hash(region, txn_month) into exactly n partitions before writing.
    #
    # This ensures that each output partition file covers exactly one
    # (region, txn_month) combination, giving the DLT pipeline clean
    # partition pruning opportunities.
    #
    # Without this step Spark might write many small files per partition
    # directory (one per original task), causing the "small files problem".
    num_write_partitions = len(REGIONS) * 12   # 5 regions × 12 months = 60
    print(f"  repartition({num_write_partitions}, 'region', 'txn_month') before write …  ← explicit shuffle")

    (
        transactions_df
        .repartition(num_write_partitions, "region", "txn_month")
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("region", "txn_month")    # ≤60 partition directories
        .saveAsTable(f"`{catalog}`.`{schema}`.raw_transactions")
    )
    print(f"  → Written: {catalog}.{schema}.raw_transactions  (≤60 region×month partitions)")


if __name__ == "__main__":
    main()

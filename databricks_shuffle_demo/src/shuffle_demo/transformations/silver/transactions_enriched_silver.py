"""
Silver Layer — Transactions Enriched
======================================
PRIMARY SHUFFLE DEMO: SORT-MERGE JOIN × 2  +  EXPLICIT REPARTITION

This file is the centrepiece of the shuffle demo.  Three distinct shuffle
operations are triggered, each visible as a separate Exchange node in the
Spark UI SQL → DAG view.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #1 — Sort-Merge Join: transactions ⋈ customers  (on customer_id)
──────────────────────────────────────────────────────────────────────────────
  Input sizes (approx):
    transactions : 2 M rows  × ~300 B = ~600 MB (before compression)
    customers    : 200 K rows × ~200 B = ~40 MB

  Why sort-merge (not broadcast)?
    The MERGE hint forces sort-merge even though customers (40 MB) is close
    to the default broadcast threshold (10 MB serialised).  In production,
    remove the hint and let Spark choose — it would broadcast customers.

  Shuffle steps executed by Spark:
    ① Map-side: each task reads its local partition of transactions/customers.
    ② Shuffle-write: each row is sent to bucket  hash(customer_id) % 200.
    ③ Shuffle-read : each reducer receives one bucket from ALL map tasks.
    ④ Sort each reducer's bucket by customer_id.
    ⑤ Merge-join: walk both sorted streams simultaneously.

  Spark UI — what to look for:
    • Two "Exchange" nodes before "SortMergeJoin" in the SQL DAG.
    • Stage metrics: "Shuffle Write" on map tasks, "Shuffle Read" on reducers.
    • "Sort Merge Join" operator with join type "LeftOuter".

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #2 — Sort-Merge Join: enriched ⋈ products  (on product_id)
──────────────────────────────────────────────────────────────────────────────
  products is only 5 K rows — normally broadcast.
  MERGE hint forces sort-merge to demonstrate the shuffle boundary.
  In production: remove the hint; Spark broadcasts products, no shuffle needed.

  Spark UI: another pair of Exchange nodes → SortMergeJoin.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #3 — Explicit repartition(200, "region")
──────────────────────────────────────────────────────────────────────────────
  repartition(n, col) triggers a FULL SHUFFLE — every row moves over the
  network regardless of its current location.

  Purpose here:
    • Co-locate all rows for the same region on the same set of partitions.
    • Downstream gold aggregations on region therefore read local data only
      (within a single batch stage), avoiding an extra shuffle.
    • Creates exactly 200 evenly-sized output partition files.

  Spark UI: a third Exchange node at the end of the silver plan.

──────────────────────────────────────────────────────────────────────────────
HOW TO OBSERVE SHUFFLE IN THE SPARK UI
──────────────────────────────────────────────────────────────────────────────
  1. Open Databricks workspace → Compute → (your cluster) → Spark UI.
  2. Go to SQL tab → find the query for this materialized view refresh.
  3. Click on the query → expand the DAG visualisation.
  4. Exchange nodes are the shuffle boundaries — hover for byte/row counts.
  5. Go to Stages tab → each stage separated by an Exchange is a shuffle stage.
  6. Click a shuffle stage → see "Shuffle Read" and "Shuffle Write" totals.
  7. Tasks tab within a stage → see per-task shuffle bytes to spot data skew.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog", "dev")
schema  = spark.conf.get("schema",  "shuffle_demo")

# 200 shuffle partitions balances parallelism and overhead for ~2 M rows.
# Rule of thumb: target ~128 MB per partition after shuffle.
# Adjust spark.sql.shuffle.partitions in the pipeline YAML for your cluster.
SHUFFLE_PARTITIONS = 200


@dp.materialized_view(
    name="txn_silver",
    comment=(
        "Silver: transactions enriched with customer and product attributes. "
        "Demonstrates 3 shuffle operations: "
        "(1) sort-merge join on customer_id, "
        "(2) sort-merge join on product_id (forced via MERGE hint), "
        "(3) explicit repartition(200, 'region') for downstream locality. "
        "Inspect Spark UI SQL DAG to count Exchange nodes."
    ),
)
def transactions_enriched_silver():

    # ── Read bronze tables (multi-partition reads, no shuffle) ─────────────
    txn_df  = spark.read.table(f"`{catalog}`.`{schema}`.txn_bronze")
    cust_df = spark.read.table(f"`{catalog}`.`{schema}`.customers_bronze")
    prod_df = spark.read.table(f"`{catalog}`.`{schema}`.products_bronze")

    # ── SHUFFLE #1: Sort-Merge Join — transactions × customers ────────────
    # The MERGE join hint overrides Spark's cost-based join selection and
    # forces a sort-merge join.  Remove it in production to enable broadcast.
    enriched_df = txn_df.join(
        cust_df.hint("MERGE").select(
            "customer_id",
            "segment",
            "age",
            "gender",
            "country_code",
            "credit_limit",
            "preferred_payment",
        ),
        on="customer_id",
        how="left",   # keep all transactions, null-fill unknown customers
    )

    # ── SHUFFLE #2: Sort-Merge Join — enriched × products ─────────────────
    # products is 5 K rows — Spark would normally broadcast it.
    # MERGE hint forces sort-merge to make the shuffle visible in the demo.
    enriched_df = enriched_df.join(
        prod_df.hint("MERGE").select(
            "product_id",
            "category",
            "subcategory",
            "brand",
            "unit_cost",
            "list_price",
            "margin_pct",
            "weight_kg",
            "is_perishable",
        ),
        on="product_id",
        how="left",
    )

    # ── Derived metrics (computed after joins, no additional shuffle) ──────
    enriched_df = (
        enriched_df
        .withColumn(
            "gross_margin_amount",
            F.round(
                F.col("total_amount") - (F.col("unit_cost") * F.col("quantity")),
                2,
            ),
        )
        .withColumn(
            "gross_margin_pct",
            F.round(
                F.when(
                    F.col("total_amount") > 0,
                    (F.col("total_amount") - F.col("unit_cost") * F.col("quantity"))
                    / F.col("total_amount") * 100,
                ).otherwise(F.lit(0.0)),
                1,
            ),
        )
        .withColumn("is_high_value_txn", F.col("total_amount") > 500)
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    # ── SHUFFLE #3: Explicit repartition by region ─────────────────────────
    # repartition(n, col) is a FULL SHUFFLE — every row moves to a new
    # partition determined by hash(region) % 200.
    #
    # After this shuffle all rows for "APAC" live in the same set of
    # partitions.  The gold aggregation on (region, txn_month) can then
    # often perform its groupBy within a single Spark stage without an
    # extra shuffle (if AQE detects it is already partitioned correctly).
    #
    # Set spark.sql.adaptive.enabled=false in pipeline YAML to always
    # see the full shuffle chain without AQE collapsing stages.
    return enriched_df.repartition(SHUFFLE_PARTITIONS, "region")

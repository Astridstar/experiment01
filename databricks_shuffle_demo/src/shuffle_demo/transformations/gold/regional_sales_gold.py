"""
Gold Layer — Regional Sales Summary
=====================================
SHUFFLE DEMO: GROUP-BY AGGREGATION  +  MULTIPLE WINDOW FUNCTIONS

Three distinct shuffle patterns are demonstrated in this file:

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #1 — GroupBy Aggregation  (HashAggregate → Exchange → HashAggregate)
──────────────────────────────────────────────────────────────────────────────
  groupBy("region", "txn_month", "category")

  Spark two-phase aggregation:
    Phase 1 (map-side): each task computes PARTIAL aggregates locally.
                        No network I/O yet; pure CPU work.
    Exchange:           rows are shuffled by hash(region, txn_month, category).
                        All rows with the same key go to the same reducer.
    Phase 2 (reduce-side): final merge of partial aggregates.

  Result: at most 5 regions × 12 months × 8 categories = 480 output rows.
  (A tiny result from 2 M input rows — classic "wide → narrow" aggregation.)

  Spark UI: look for "HashAggregate → Exchange → HashAggregate" in the DAG.
  The Exchange node shows shuffle bytes; the two HashAggregate nodes show
  the partial (map) and final (reduce) aggregation phases.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #2 — Window  partitionBy("region")  (5 output partitions)
──────────────────────────────────────────────────────────────────────────────
  Cumulative revenue per region, ordered by txn_month.

  A Window function with partitionBy triggers its own shuffle:
    Exchange: data redistributed by hash(region) → 5 distinct buckets.
    Sort:     within each partition, rows sorted by txn_month.
    Frame:    unboundedPreceding → currentRow  (running sum).

  Spark UI: "Window → Sort → Exchange" chain after the groupBy stage.

──────────────────────────────────────────────────────────────────────────────
SHUFFLE #3 — Window  partitionBy("region", "txn_month")  (≤60 partitions)
──────────────────────────────────────────────────────────────────────────────
  Rank categories by revenue within each (region, month) group.

  Different partition key → DIFFERENT shuffle from #2.
  Both windows appear as separate Exchange nodes in the DAG.

  Spark UI: a second Window plan with its own Exchange → Sort.

──────────────────────────────────────────────────────────────────────────────
COALESCE vs REPARTITION for output
──────────────────────────────────────────────────────────────────────────────
  The 480-row result needs far fewer than 200 shuffle partitions.

  coalesce(8):
    • Merges ADJACENT partitions on the same executor — no network transfer.
    • Avoids the cost of a full shuffle (unlike repartition).
    • Preferred when REDUCING partition count.

  repartition(8):
    • Full shuffle — all data moves over the network.
    • Produces perfectly balanced partitions.
    • Preferred when INCREASING partition count or needing even distribution.

  For 480 rows coalesce(8) is correct: overhead of repartition is not justified.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

catalog = spark.conf.get("catalog", "dev")
schema  = spark.conf.get("schema",  "shuffle_demo")


@dp.materialized_view(
    name="regional_sales_gold",
    comment=(
        "Gold: regional sales aggregated by (region, txn_month, category). "
        "Shuffle #1: groupBy aggregation (HashAggregate → Exchange → HashAggregate). "
        "Shuffle #2: window cumulative revenue partitionBy(region) — 5 partitions. "
        "Shuffle #3: window rank partitionBy(region, txn_month) — ≤60 partitions. "
        "Output coalesced to 8 files (no shuffle — adjacent partition merge)."
    ),
)
def regional_sales_gold():

    txn_silver = spark.read.table(f"`{catalog}`.`{schema}`.txn_silver")

    # Filter to revenue-generating statuses only (no shuffle — local predicate)
    revenue_txn = txn_silver.filter(
        F.col("order_status").isin(["completed", "refunded"])
    )

    # ── SHUFFLE #1: GroupBy Aggregation ───────────────────────────────────
    # Each row's destination partition = hash(region, txn_month, category) % 200.
    # spark.sql.shuffle.partitions=200 is set at pipeline level.
    regional_agg = (
        revenue_txn
        .groupBy("region", "txn_month", "category")
        .agg(
            F.count("txn_id")                    .alias("num_transactions"),
            F.countDistinct("customer_id")        .alias("unique_customers"),
            F.countDistinct("product_id")         .alias("unique_products"),
            F.sum("total_amount")                 .alias("gross_revenue"),
            F.sum("gross_margin_amount")          .alias("total_margin"),
            F.avg("total_amount")                 .alias("avg_order_value"),
            F.sum("quantity")                     .alias("total_units_sold"),
            F.avg("discount_pct")                 .alias("avg_discount_pct"),
            F.sum(
                F.when(F.col("is_high_value_txn"), 1).otherwise(0)
            )                                     .alias("high_value_txn_count"),
            F.sum(
                F.when(F.col("order_status") == "refunded", F.col("total_amount"))
                .otherwise(0)
            )                                     .alias("refund_amount"),
        )
        .withColumn(
            "margin_pct",
            F.round(F.col("total_margin") / F.col("gross_revenue") * 100, 1),
        )
        .withColumn(
            "refund_rate",
            F.round(F.col("refund_amount") / F.col("gross_revenue") * 100, 2),
        )
    )

    # ── SHUFFLE #2: Window — Cumulative Revenue per Region ────────────────
    # partitionBy("region") → Exchange redistributes all 480 rows into 5 buckets.
    # orderBy("txn_month") → Sort within each bucket (no extra shuffle).
    # rowsBetween(unboundedPreceding, 0) → running sum up to current row.
    region_running_window = (
        Window
        .partitionBy("region")
        .orderBy("txn_month")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    regional_agg = regional_agg.withColumn(
        "cumulative_revenue_in_region",
        F.round(F.sum("gross_revenue").over(region_running_window), 2),
    )

    # ── SHUFFLE #3: Window — Category Revenue Rank within Region-Month ────
    # Different partitionBy key → SEPARATE Exchange node in the DAG.
    # hash(region, txn_month) ≠ hash(region) so Spark cannot reuse the
    # data layout from shuffle #2.
    region_month_rank_window = (
        Window
        .partitionBy("region", "txn_month")
        .orderBy(F.col("gross_revenue").desc())
    )

    regional_agg = regional_agg.withColumn(
        "category_revenue_rank",        # 1 = top-grossing category this month
        F.rank().over(region_month_rank_window),
    ).withColumn(
        "is_top_category",
        F.col("category_revenue_rank") == 1,
    )

    regional_agg = regional_agg.withColumn("gold_refreshed_at", F.current_timestamp())

    # ── OUTPUT: coalesce (no shuffle) ────────────────────────────────────
    # 480 rows fit comfortably in 8 small files.
    # coalesce merges adjacent partitions locally — no network movement.
    return regional_agg.coalesce(8)

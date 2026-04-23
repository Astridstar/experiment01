"""
Unit & integration tests for `transformations.gold.customer_lifetime_value`.

This module demonstrates two complementary testing styles:

1. **Offline unit tests** — run against a local SparkSession provided by the
   `local_spark` fixture in `conftest.py`. These are fast, deterministic and
   don't need a Databricks workspace.

2. **Integration tests** — use fixtures from `databricks-labs-pytester`
   (`ws`, `spark`, `make_random`, `make_schema`, `make_table`) to materialise
   input tables in a real Unity Catalog schema and verify the transformation
   end-to-end. They are marked `@pytest.mark.integration` and auto-skip when
   DATABRICKS_HOST is not configured.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations.gold.customer_lifetime_value import (
    CLVConfig,
    build_customer_lifetime_value,
)


# ---------------------------------------------------------------------------
# Test data builders
# ---------------------------------------------------------------------------

_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("full_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
])

_ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("currency", StringType(), True),
])

_ITEMS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(18, 2), False),
])


def _build_fixture_dfs(spark):
    """Return (customers, orders, items) DataFrames that cover all code paths.

    The scenario:
      * c=1 Alice (SG) — 4 orders spanning 18 months, diverse categories
      * c=2 Bob   (US) — 1 recent small order
      * c=3 Carol (SG) — 2 orders, last one > 6 months ago (at-risk)
      * c=4 Dan   (UK) — signed up but never ordered (null metrics)
    """
    customers = spark.createDataFrame(
        [
            (1, "  alice tan ", "sg", "2022-01-10"),
            (2, "BOB LEE", "USA", "2023-06-01"),
            (3, "Carol Lim", " SG ", "2021-11-30"),
            (4, "Dan None", "uk", "2024-02-15"),
        ],
        schema=_CUSTOMERS_SCHEMA,
    )

    orders = spark.createDataFrame(
        [
            # Alice — 4 orders
            (100, 1, datetime(2023, 1, 15, 10, 0, 0), "sgd"),
            (101, 1, datetime(2023, 6, 20, 11, 0, 0), "SGD"),
            (102, 1, datetime(2023, 11, 5, 9, 30, 0), "sgd"),
            (103, 1, datetime(2024, 4, 1, 14, 0, 0), "SGD"),
            # Bob — 1 order
            (200, 2, datetime(2024, 4, 10, 12, 0, 0), "usd"),
            # Carol — 2 orders, last one > 180 days before as_of_date (2024-04-23)
            (300, 3, datetime(2023, 6, 1, 10, 0, 0), "sgd"),
            (301, 3, datetime(2023, 10, 1, 10, 0, 0), "sgd"),
            # c=4 has no orders
        ],
        schema=_ORDERS_SCHEMA,
    )

    items = spark.createDataFrame(
        [
            # Alice's orders — multiple categories, enough to exercise top-3
            (100, 11, "electronics", 1, Decimal("500.00")),
            (100, 12, "books", 2, Decimal("20.00")),
            (101, 13, "home", 3, Decimal("50.00")),
            (101, 11, "electronics", 1, Decimal("300.00")),
            (102, 14, "fashion", 2, Decimal("75.00")),
            (103, 11, "electronics", 1, Decimal("700.00")),
            (103, 15, "toys", 4, Decimal("25.00")),
            # Bob
            (200, 20, "books", 1, Decimal("15.00")),
            # Carol
            (300, 21, "fashion", 2, Decimal("40.00")),
            (301, 22, "home", 1, Decimal("120.00")),
        ],
        schema=_ITEMS_SCHEMA,
    )
    return customers, orders, items


@pytest.fixture
def clv_config() -> CLVConfig:
    return CLVConfig(
        as_of_date=date(2024, 4, 23),
        churn_threshold_days=180,
        top_n_categories=3,
        rfm_bucket_count=5,
    )


@pytest.fixture
def clv_result(local_spark, clv_config):
    customers, orders, items = _build_fixture_dfs(local_spark)
    return (
        build_customer_lifetime_value(customers, orders, items, clv_config)
        .orderBy("customer_id")
        .cache()
    )


# ---------------------------------------------------------------------------
# Offline unit tests
# ---------------------------------------------------------------------------

def test_schema_contains_expected_columns(clv_result):
    expected = {
        "customer_id", "full_name", "country", "signup_date",
        "as_of_date", "first_order_date", "last_order_date",
        "tenure_months", "recency_days", "order_count",
        "avg_days_between_orders", "lifetime_revenue", "avg_order_value",
        "r_score", "f_score", "m_score", "rfm_score",
        "customer_tier", "is_at_risk", "top_categories",
    }
    assert expected.issubset(set(clv_result.columns))


def test_customer_row_count_preserved(clv_result):
    # Left join on customers must preserve customer cardinality, including
    # the customer with zero orders.
    assert clv_result.count() == 4


def test_name_and_country_are_normalized(clv_result):
    rows = {r.customer_id: r for r in clv_result.collect()}
    assert rows[1].full_name == "ALICE TAN"
    assert rows[1].country == "SG"
    assert rows[2].country == "USA"  # upper+trim only; no country mapping applied
    assert rows[3].full_name == "CAROL LIM"


def test_lifetime_revenue_matches_line_totals(clv_result):
    rows = {r.customer_id: r for r in clv_result.collect()}
    # Alice: 500 + 40 + 150 + 300 + 150 + 700 + 100 = 1940
    assert rows[1].lifetime_revenue == Decimal("1940.00")
    # Bob: 15
    assert rows[2].lifetime_revenue == Decimal("15.00")
    # Carol: 80 + 120 = 200
    assert rows[3].lifetime_revenue == Decimal("200.00")


def test_customer_with_no_orders_has_zero_defaults(clv_result):
    dan = clv_result.filter(F.col("customer_id") == 4).collect()[0]
    assert dan.order_count == 0
    assert dan.lifetime_revenue == Decimal("0.00")
    assert dan.last_order_date is None
    assert dan.top_categories is None


def test_order_count_and_recency(clv_result, clv_config):
    rows = {r.customer_id: r for r in clv_result.collect()}
    assert rows[1].order_count == 4
    assert rows[2].order_count == 1
    assert rows[3].order_count == 2

    # Recency = as_of_date - last_order_date (days)
    assert rows[1].recency_days == (clv_config.as_of_date - date(2024, 4, 1)).days
    assert rows[3].recency_days == (clv_config.as_of_date - date(2023, 10, 1)).days


def test_churn_flag_respects_threshold(clv_result, clv_config):
    rows = {r.customer_id: r for r in clv_result.collect()}
    # Alice is recent, Carol is older than threshold, Bob is recent.
    assert rows[1].is_at_risk is False
    assert rows[2].is_at_risk is False
    assert rows[3].is_at_risk is True
    for r in rows.values():
        if r.recency_days is None:
            continue
        assert r.is_at_risk == (r.recency_days > clv_config.churn_threshold_days)


def test_rfm_score_is_in_expected_range(clv_result, clv_config):
    for r in clv_result.filter(F.col("order_count") > 0).collect():
        assert 3 <= r.rfm_score <= 3 * clv_config.rfm_bucket_count
        assert r.customer_tier in {"BRONZE", "SILVER", "GOLD", "PLATINUM"}


def test_top_categories_are_ranked_and_limited(clv_result, clv_config):
    alice = clv_result.filter(F.col("customer_id") == 1).collect()[0]
    cats = alice.top_categories
    assert cats is not None
    assert len(cats) <= clv_config.top_n_categories
    ranks = [c["rnk"] for c in cats]
    assert ranks == sorted(ranks)
    # Electronics should be Alice's #1 category (500 + 300 + 700 = 1500).
    top = min(cats, key=lambda c: c["rnk"])
    assert top["category"] == "electronics"


def test_running_revenue_is_monotonic_per_customer(local_spark, clv_config):
    """Smoke-test the internal window ordering by re-deriving it."""
    from transformations.gold.customer_lifetime_value import (
        _normalize_orders,
        _order_level_metrics,
        _with_order_sequence,
    )

    customers, orders, items = _build_fixture_dfs(local_spark)
    orders_n = _normalize_orders(orders)
    enriched = _order_level_metrics(orders_n, items)
    sequenced = _with_order_sequence(enriched).orderBy("customer_id", "order_seq")

    last_running = {}
    for row in sequenced.collect():
        prev = last_running.get(row.customer_id, Decimal("-1"))
        assert row.running_revenue >= prev
        last_running[row.customer_id] = row.running_revenue


# ---------------------------------------------------------------------------
# Integration tests (databricks-labs-pytester)
# ---------------------------------------------------------------------------

# Skip the whole integration block unless workspace creds are available.
_HAS_WS = bool(os.environ.get("DATABRICKS_HOST")) or os.path.exists(
    os.path.expanduser("~/.databrickscfg")
)

integration = pytest.mark.skipif(
    not _HAS_WS,
    reason="Set DATABRICKS_HOST or configure ~/.databrickscfg to run integration tests.",
)


@integration
@pytest.mark.integration
def test_clv_roundtrip_on_uc(ws, spark, make_schema, make_random, clv_config):
    """End-to-end integration test.

    Uses pytester's `make_schema` fixture to create an ephemeral UC schema,
    writes input tables with `spark` (Databricks Connect), runs the
    transformation, and asserts on the materialised gold table. The schema
    is automatically torn down by pytester at the end of the test.
    """
    schema = make_schema(catalog_name="main")
    suffix = make_random(6)

    customers_tbl = f"{schema.full_name}.customers_{suffix}"
    orders_tbl = f"{schema.full_name}.orders_{suffix}"
    items_tbl = f"{schema.full_name}.order_items_{suffix}"
    gold_tbl = f"{schema.full_name}.customer_lifetime_value_{suffix}"

    customers, orders, items = _build_fixture_dfs(spark)
    customers.write.mode("overwrite").saveAsTable(customers_tbl)
    orders.write.mode("overwrite").saveAsTable(orders_tbl)
    items.write.mode("overwrite").saveAsTable(items_tbl)

    clv = build_customer_lifetime_value(
        spark.table(customers_tbl),
        spark.table(orders_tbl),
        spark.table(items_tbl),
        clv_config,
    )
    clv.write.mode("overwrite").saveAsTable(gold_tbl)

    persisted = spark.table(gold_tbl)
    assert persisted.count() == 4
    alice = persisted.filter("customer_id = 1").select("lifetime_revenue").collect()[0]
    assert alice.lifetime_revenue == Decimal("1940.00")

    # Sanity: WorkspaceClient is wired up and targets the same host we wrote to.
    assert ws.config.host.startswith("https://")

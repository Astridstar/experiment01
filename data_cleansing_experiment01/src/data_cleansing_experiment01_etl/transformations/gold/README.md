# Customer Lifetime Value (CLV) + RFM Sample

A worked example of a non-trivial PySpark transformation plus two complementary
styles of test using [`databricks-labs-pytester`](https://github.com/databrickslabs/pytester).

The goal is to show, in one place:

1. What a real gold-layer transformation looks like when it uses joins,
   window functions, aggregations, date math and conditional logic.
2. How to test such code **offline** on your laptop (no Databricks account).
3. How to test it **end-to-end** against a real Databricks workspace using
   pytester fixtures.
4. How the same code can be dropped into a Databricks notebook or a Lakeflow
   pipeline without changes.

---

## 1. What the sample computes

Given three inputs:

| DataFrame     | Columns                                                                   |
|---------------|---------------------------------------------------------------------------|
| `customers`   | `customer_id`, `full_name`, `country`, `signup_date`                      |
| `orders`      | `order_id`, `customer_id`, `order_ts`, `currency`                         |
| `order_items` | `order_id`, `product_id`, `category`, `quantity`, `unit_price`            |

`build_customer_lifetime_value(...)` produces one row per customer with:

- Lifetime revenue, average order value, order count, first/last order dates
- **Recency** in days and **tenure** in months, both relative to a configurable
  `as_of_date`
- **R/F/M scores** (1–5) assigned with `ntile` quintiles, summed into an
  `rfm_score`
- **Customer tier** (`BRONZE` / `SILVER` / `GOLD` / `PLATINUM`) derived from the
  RFM score with `CASE` logic
- **`is_at_risk`** churn flag when recency exceeds a configurable threshold
- **`top_categories`** — an array of structs (`rnk`, `category`, `category_revenue`)
  listing the customer's top N product categories, built with `dense_rank`

Techniques demonstrated:

- Multi-way joins with `F.broadcast` for the small aggregated side
- Window functions: `row_number`, `lag`, `dense_rank`, `ntile`, running `sum`
  over `rowsBetween(unboundedPreceding, currentRow)`
- Aggregations: `sum`, `avg`, `count`, `countDistinct`, `max`, `min`
- Date arithmetic: `datediff`, `months_between`
- Conditional bucketing via chained `F.when(...).otherwise(...)`
- Array-of-struct output via `collect_list(struct(...))`
- Reuse of shared utility functions (`normalize_name`, `normalize_currency_code`)

The function is **pure** — DataFrames in, DataFrame out — which is what makes it
unit-testable without a pipeline runtime.

---

## 2. Where the files live

```
data_cleansing_experiment01/
├── pyproject.toml                                             # dev deps + pytest config
└── src/
    └── data_cleansing_experiment01_etl/
        ├── transformations/
        │   └── gold/
        │       ├── customer_lifetime_value.py                 # THE SAMPLE
        │       └── README.md                                  # (this file)
        ├── tests/
        │   ├── conftest.py                                    # pytester plugin + local_spark
        │   └── test_customer_lifetime_value.py                # unit + integration tests
        └── utils/
            └── transformations.py                             # reused helpers
```

---

## 3. Prerequisites

| Requirement           | Why                                                                    |
|-----------------------|------------------------------------------------------------------------|
| Python 3.10 – 3.12    | Matches `requires-python` in `pyproject.toml`                          |
| Java 17 (or 11)       | Required by local PySpark (`JAVA_HOME` must be set)                    |
| `uv` (recommended)    | Resolves the `dev` dependency group declared in `pyproject.toml`       |
| Git                   | You already have this                                                  |
| Databricks account    | **Optional** — only needed for the integration test and workspace runs |

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verify Java:

```bash
java -version       # should print 11.x or 17.x
echo $JAVA_HOME
```

On macOS, `brew install openjdk@17` then `export JAVA_HOME="$(/usr/libexec/java_home -v 17)"`.

---

## 4. Do I need a Databricks account?

Short answer: **no, not for the unit tests**.

| Scenario                                          | Databricks account? | Cluster running? |
|---------------------------------------------------|---------------------|------------------|
| Run the offline unit tests                        | No                  | No               |
| Run the `@pytest.mark.integration` test           | Yes                 | Serverless or classic, optional |
| Execute the transformation inside a notebook      | Yes                 | Yes              |
| Deploy the Lakeflow pipeline (`databricks bundle deploy`) | Yes         | Yes (Databricks starts it) |

The unit tests spin up a local `SparkSession` and do everything in-process. The
integration test talks to a real workspace through Databricks Connect and
materialises tables in Unity Catalog.

---

## 5. Option A — Run everything locally (no Databricks account)

From the project root (`data_cleansing_experiment01/`):

```bash
# 1. Install deps into a throwaway venv managed by uv
uv sync --group dev

# 2. Run only the offline unit tests
uv run pytest -m "not integration" -v
```

Expected output (eight tests, all passing):

```
test_customer_lifetime_value.py::test_schema_contains_expected_columns PASSED
test_customer_lifetime_value.py::test_customer_row_count_preserved PASSED
test_customer_lifetime_value.py::test_name_and_country_are_normalized PASSED
test_customer_lifetime_value.py::test_lifetime_revenue_matches_line_totals PASSED
test_customer_lifetime_value.py::test_customer_with_no_orders_has_zero_defaults PASSED
test_customer_lifetime_value.py::test_order_count_and_recency PASSED
test_customer_lifetime_value.py::test_churn_flag_respects_threshold PASSED
test_customer_lifetime_value.py::test_rfm_score_is_in_expected_range PASSED
test_customer_lifetime_value.py::test_top_categories_are_ranked_and_limited PASSED
test_customer_lifetime_value.py::test_running_revenue_is_monotonic_per_customer PASSED
```

The first run takes ~15–30 seconds because `local_spark` has to boot a JVM;
subsequent runs in the same session reuse it.

Run a single test:

```bash
uv run pytest -v -k lifetime_revenue_matches
```

Show print/log output:

```bash
uv run pytest -s
```

### What is `local_spark`?

Defined in `tests/conftest.py`. It builds a session-scoped PySpark session
configured for tests:

```python
SparkSession.builder
    .master("local[2]")
    .appName("data_cleansing_experiment01_unit_tests")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
```

It is deliberately independent of Databricks Connect so tests can run in CI
without any workspace credentials.

---

## 6. Option B — Integration test against a real Databricks workspace

This runs `test_clv_roundtrip_on_uc`, which:

1. Creates an ephemeral Unity Catalog schema (`make_schema`).
2. Writes the three input tables into that schema via Databricks Connect.
3. Runs `build_customer_lifetime_value` against the tables.
4. Writes the gold table back and reads it to assert on the result.
5. Lets pytester tear everything down afterwards.

### Prerequisites

- A Databricks workspace with **Unity Catalog enabled**.
- A catalog you can create schemas in (the test uses `main` by default — edit
  the fixture if your workspace uses something else).
- Either
  - A running interactive cluster (DBR 14.x or 15.x) with Databricks Connect
    enabled, **or**
  - Serverless compute for Databricks Connect.

### One-time setup

Install the Databricks CLI and authenticate:

```bash
pip install databricks-cli         # or: brew install databricks/tap/databricks
databricks configure                # follow prompts; stores in ~/.databrickscfg
```

Alternatively set environment variables:

```bash
export DATABRICKS_HOST=https://<your-workspace-host>
export DATABRICKS_TOKEN=<personal-access-token>
# If using interactive compute:
export DATABRICKS_CLUSTER_ID=<cluster-id>
# If using serverless:
export DATABRICKS_SERVERLESS_COMPUTE_ID=auto
```

### Run the test

```bash
uv run pytest -m integration -v
```

The test is auto-skipped if neither `DATABRICKS_HOST` nor `~/.databrickscfg` is
present, so it is safe to leave on in CI pipelines that don't have workspace
credentials.

### What the pytester fixtures give you

Loaded via `pytest_plugins = ["databricks.labs.pytester.fixtures.plugin"]` in
`conftest.py`:

| Fixture         | What it provides                                                |
|-----------------|-----------------------------------------------------------------|
| `ws`            | An authenticated `WorkspaceClient`                              |
| `spark`         | A `SparkSession` backed by Databricks Connect                   |
| `make_random`   | Callable returning unique random strings for table name suffixes|
| `make_catalog`  | Creates and cleans up a UC catalog                              |
| `make_schema`   | Creates and cleans up a UC schema                               |
| `make_table`    | Creates and cleans up a UC table                                |
| `make_volume`   | Creates and cleans up a UC volume                               |
| `env_or_skip`   | Reads an env var or skips the test if missing                   |
| `debug_env`     | Loads `~/.databricks/debug-env.json` entries for IDE runs       |

Each `make_*` fixture registers a teardown so artifacts are deleted when the
test finishes, even on failure.

### Running only from an IDE (PyCharm / VS Code)

Pytester reads a per-user file `~/.databricks/debug-env.json` when the
`debug_env_name` fixture returns a key that exists in it. `conftest.py`
defaults that key to `ws` (overridable with `DEBUG_ENV_NAME`). An example:

```json
{
  "ws": {
    "DATABRICKS_HOST": "https://adb-1234567890.12.azuredatabricks.net",
    "DATABRICKS_TOKEN": "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "DATABRICKS_CLUSTER_ID": "0000-000000-abc123"
  }
}
```

Put a breakpoint in the integration test, hit Debug, and you get a live
workspace session in your debugger.

---

## 7. Option C — Run inside a Databricks workspace notebook

The transformation is plain PySpark, so it works unmodified in any Databricks
notebook or job. Example notebook cell:

```python
# In a notebook whose working directory is the repo root
%pip install -q -e data_cleansing_experiment01
dbutils.library.restartPython()
```

```python
from datetime import date
from transformations.gold.customer_lifetime_value import (
    CLVConfig, build_customer_lifetime_value
)

customers = spark.table("dev.experiment01.customers_silver").filter("__END_AT IS NULL")
orders    = spark.table("dev.experiment01.orders_silver").filter("__END_AT IS NULL")
items     = spark.table("dev.experiment01.order_items_silver").filter("__END_AT IS NULL")

clv = build_customer_lifetime_value(
    customers, orders, items,
    CLVConfig(as_of_date=date.today(), churn_threshold_days=180),
)

(clv.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.experiment01.customer_lifetime_value_gold"))
```

### Or plug it into the Lakeflow pipeline

Create a gold file next to the existing silver tables:

```python
# transformations/gold/customer_lifetime_value_gold.py
from datetime import date
from pyspark import pipelines as dp
from transformations.gold.customer_lifetime_value import (
    CLVConfig, build_customer_lifetime_value,
)

@dp.table(name="customer_lifetime_value_gold", comment="CLV + RFM scoring")
def customer_lifetime_value_gold():
    customers = spark.read.table("dev.experiment01.customers_silver") \
                         .filter("__END_AT IS NULL")
    orders = spark.read.table("dev.experiment01.orders_silver") \
                       .filter("__END_AT IS NULL")
    items = spark.read.table("dev.experiment01.order_items_silver") \
                      .filter("__END_AT IS NULL")
    return build_customer_lifetime_value(
        customers, orders, items,
        CLVConfig(as_of_date=date.today()),
    )
```

Deploy as usual:

```bash
databricks bundle deploy --target dev
databricks bundle run
```

---

## 8. How the tests are structured

`tests/test_customer_lifetime_value.py` contains:

- **`_build_fixture_dfs(spark)`** — four customers exercising every branch:
  Alice (active big spender), Bob (single recent order), Carol (at-risk, last
  order > 180 days ago), Dan (signed up but no orders → null metrics).
- **Ten offline unit tests** covering schema, row preservation, revenue math,
  name/country normalization, zero-order defaults, recency, churn flag, RFM
  bounds, top-N category ranking, and monotonicity of the running-revenue
  window.
- **One integration test** (`test_clv_roundtrip_on_uc`) that round-trips the
  sample through Unity Catalog.

The private helpers in `customer_lifetime_value.py` (prefixed with `_`) are
imported directly by `test_running_revenue_is_monotonic_per_customer` to test
one stage in isolation — a useful pattern when you want to pin down behaviour
of a specific window or join without materialising the whole pipeline.

---

## 9. Extending the sample

- **Add a new metric**: put it in a new helper in `customer_lifetime_value.py`,
  append a column in the final `select`, and add a unit test against
  `clv_result`.
- **Change the tier thresholds**: edit `_with_tier_and_churn`; the existing
  `test_rfm_score_is_in_expected_range` will still pass as long as tiers stay
  within the allowed set.
- **Use the sample with your own silver tables**: point the three arguments
  at whatever DataFrames you already have — as long as the column names match
  the input contract above, it will work.

---

## 10. Troubleshooting

| Symptom                                                 | Fix                                                                 |
|---------------------------------------------------------|---------------------------------------------------------------------|
| `JAVA_HOME is not set`                                  | Install Java 17, export `JAVA_HOME`                                 |
| `ModuleNotFoundError: transformations`                  | Run pytest from `data_cleansing_experiment01/` so `conftest.py` can insert `src/...etl/` into `sys.path` |
| Integration test reports `catalog 'main' not found`     | Edit `make_schema(catalog_name="main")` in the test to use your catalog |
| `databricks.sdk.errors.platform.PermissionDenied`       | Your token needs `USE CATALOG` + `CREATE SCHEMA` on the target catalog |
| `Py4JError: ... SparkContext was shut down`             | Another test killed the session; the `local_spark` fixture is session-scoped — don't call `.stop()` yourself |
| Integration test is silently skipped                    | Either `DATABRICKS_HOST` env var or `~/.databrickscfg` must be present |

---

## 11. Further reading

- Databricks Labs pytester: <https://github.com/databrickslabs/pytester>
- Databricks Connect: <https://docs.databricks.com/dev-tools/databricks-connect.html>
- Lakeflow Declarative Pipelines: <https://docs.databricks.com/aws/en/dlt>
- PySpark window functions: <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html>

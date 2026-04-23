"""
Shared pytest fixtures for the data_cleansing_experiment01_etl test suite.

This conftest wires in `databricks-labs-pytester` so its fixtures (e.g.
`make_random`, `ws`, `make_schema`, `make_table`, `debug_env`, `env_or_skip`)
are available in every test module.

It also provides a `local_spark` fixture that spins up a local SparkSession
for pure, offline unit tests of transformation logic — no Databricks workspace
required. Integration-style tests that need a real workspace can use the
pytester `spark` fixture (which is backed by Databricks Connect).

Run unit tests locally:

    uv run --group dev pytest src/data_cleansing_experiment01_etl/tests -m "not integration"

Run integration tests (needs DATABRICKS_HOST / DATABRICKS_TOKEN or a .databrickscfg):

    uv run --group dev pytest src/data_cleansing_experiment01_etl/tests -m integration
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Make `utils` and `transformations` importable as top-level packages during tests.
_SRC_PKG = Path(__file__).resolve().parents[1]
if str(_SRC_PKG) not in sys.path:
    sys.path.insert(0, str(_SRC_PKG))


# Load databricks-labs-pytester fixtures. When the library is installed this
# exposes fixtures such as `ws`, `spark`, `make_random`, `make_schema`,
# `make_catalog`, `make_table`, `make_volume`, `env_or_skip`, `debug_env`, etc.
pytest_plugins = ["databricks.labs.pytester.fixtures.plugin"]


@pytest.fixture(scope="session")
def local_spark():
    """Session-scoped local SparkSession for offline unit tests.

    Uses a single executor and disables the UI so it is cheap to start up in
    CI. Intentionally independent of databricks-connect so tests can run
    without workspace credentials.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("data_cleansing_experiment01_unit_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture
def debug_env_name() -> str:
    """Name of the debug environment entry in ~/.databricks/debug-env.json.

    Required by databricks-labs-pytester's `debug_env` fixture so that
    engineers can run integration tests from their IDE. CI should rely on
    DATABRICKS_HOST / DATABRICKS_TOKEN env vars instead.
    """
    return os.environ.get("DEBUG_ENV_NAME", "ws")


@pytest.fixture
def product_info() -> tuple[str, str]:
    """Product (name, version) tuple consumed by pytester for User-Agent tags."""
    return ("data_cleansing_experiment01", "0.0.1")

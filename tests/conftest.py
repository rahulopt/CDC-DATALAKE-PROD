"""Shared PyTest fixtures for the CDC pipeline unit tests.

Provides a single, session-scoped local SparkSession (Spark is expensive to
start) and puts ``glue/`` on ``sys.path`` so ``import cdc_lib...`` works without
installing the package.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make `cdc_lib` (and the job modules) importable from the repo layout.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO_ROOT, "glue"))
sys.path.insert(0, os.path.join(_REPO_ROOT, "glue", "jobs"))

# Bind Spark to localhost to avoid host-resolution issues on laptops/CI.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Force Spark's Python workers (and driver) to use the *same* interpreter that
# is running the tests. Without this, Spark may launch workers with a different
# system/conda Python (e.g. 3.12) than the driver (venv 3.11), which fails with
# [PYTHON_VERSION_MISMATCH] because PySpark requires matching minor versions.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Force SPARK_HOME to the installed pyspark so a system Spark on PATH (e.g. a
# newer Spark 4.x) cannot shadow the pinned pyspark and cause Py4J constructor
# mismatches. This mirrors the `unset SPARK_HOME` trick used in the prototype.
import pyspark as _pyspark  # noqa: E402

os.environ["SPARK_HOME"] = os.path.dirname(_pyspark.__file__)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    # Iceberg runtime matching the local test Spark (3.5). Configuring it on the
    # single session fixture means the whole JVM shares one consistent set of
    # jars/catalogs (Spark cannot change spark.jars.packages after JVM start),
    # which lets the Iceberg e2e tests reuse this session without conflicts.
    iceberg_pkg = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
    warehouse = str(tmp_path_factory.mktemp("iceberg_wh"))

    session = (
        SparkSession.builder.appName("cdc-lib-tests")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", iceberg_pkg)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def iceberg_ready(spark):
    """Skip Iceberg e2e tests if the Iceberg runtime jar didn't load."""
    try:
        spark.sql("CREATE TABLE local.db.__probe (id int) USING iceberg")
        spark.sql("DROP TABLE local.db.__probe")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Iceberg runtime unavailable: {exc}")
    return spark

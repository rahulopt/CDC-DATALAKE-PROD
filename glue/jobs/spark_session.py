"""Shared Glue/Spark bootstrap for all CDC jobs.

Centralises the SparkSession + Iceberg + Glue Data Catalog configuration so the
individual job scripts stay focused on business logic. On real Glue 4.0 the
Iceberg runtime is enabled via the job parameter ``--datalake-formats iceberg``;
the extra catalog ``--conf`` lines below make Iceberg the default catalog
``glue_catalog`` backed by S3 + the Glue metastore.

This module is imported by the job entry-points in ``glue/jobs/``. It is kept
separate from ``cdc_lib`` because it *does* assume a Spark/Glue runtime.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str, silver_warehouse: str) -> SparkSession:
    """Create a SparkSession wired for Iceberg on the Glue Data Catalog.

    ``silver_warehouse`` is the S3 root for Iceberg data, e.g.
    ``s3://my-bucket/silver``.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg SQL extensions (MERGE, CALL procedures, etc.)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Register the Glue-backed Iceberg catalog as `glue_catalog`.
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", silver_warehouse)
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # Cost/perf: adaptive execution + reasonable shuffle default.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    return builder.getOrCreate()


def env(name: str, default: str = "") -> str:
    """Read a pipeline environment variable with a default."""
    return os.environ.get(name, default)

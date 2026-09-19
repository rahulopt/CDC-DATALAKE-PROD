"""End-to-end CDC transformation test on a real (local) Iceberg table.

Exercises the full INSERT / UPDATE / DELETE MERGE semantics plus idempotency
using an in-process Iceberg Hadoop catalog. The Iceberg runtime is configured
once on the shared session fixture (see ``conftest.py``); if it did not load the
``iceberg_ready`` fixture skips these tests rather than failing the suite. The
pure-Python and DataFrame-only tests still cover the transformation logic.
"""

from __future__ import annotations

import pytest
from cdc_lib import ordering
from pyspark.sql import functions as F


def _merge(spark, target):
    spark.sql(f"""
        MERGE INTO {target} t
        USING cdc_src s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s.__op = 'D' THEN DELETE
        WHEN MATCHED AND s.__op = 'U' THEN UPDATE SET t.status = s.status
        WHEN NOT MATCHED AND s.__op IN ('I','U') THEN
            INSERT (order_id, status) VALUES (s.order_id, s.status)
    """)


@pytest.mark.iceberg
def test_insert_update_delete_merge(iceberg_ready):
    spark = iceberg_ready
    spark.sql("DROP TABLE IF EXISTS local.db.orders")
    spark.sql("CREATE TABLE local.db.orders (order_id int, status string) USING iceberg")
    spark.sql("INSERT INTO local.db.orders VALUES (1,'PLACED'),(2,'PLACED'),(3,'PLACED')")

    batch = (
        spark.createDataFrame(
            [
                (1, "U", "2026-09-19T10:00:00Z", "t1", 1, "SHIPPED"),
                (1, "U", "2026-09-19T12:00:00Z", "t3", 3, "DELIVERED"),  # newest wins
                (2, "D", "2026-09-19T11:00:00Z", "t2", 2, "PLACED"),  # delete
                (4, "I", "2026-09-19T09:00:00Z", "t0", 0, "PLACED"),  # insert
            ],
            ["order_id", "__op", "commit_ts", "__txn_id", "__seq", "status"],
        )
        .withColumn("__commit_ts", F.to_timestamp("commit_ts"))
        .drop("commit_ts")
    )

    deduped = ordering.latest_per_key(batch, ["order_id"])
    deduped.createOrReplaceTempView("cdc_src")
    _merge(spark, "local.db.orders")

    result = {
        r["order_id"]: r["status"] for r in spark.sql("SELECT * FROM local.db.orders").collect()
    }
    assert result == {1: "DELIVERED", 3: "PLACED", 4: "PLACED"}  # 2 deleted, 1 updated, 4 inserted


@pytest.mark.iceberg
def test_idempotent_reapply(iceberg_ready):
    spark = iceberg_ready
    spark.sql("DROP TABLE IF EXISTS local.db.orders2")
    spark.sql("CREATE TABLE local.db.orders2 (order_id int, status string) USING iceberg")
    spark.sql("INSERT INTO local.db.orders2 VALUES (1,'PLACED')")

    batch = (
        spark.createDataFrame(
            [(1, "U", "2026-09-19T12:00:00Z", "t3", 3, "DELIVERED")],
            ["order_id", "__op", "commit_ts", "__txn_id", "__seq", "status"],
        )
        .withColumn("__commit_ts", F.to_timestamp("commit_ts"))
        .drop("commit_ts")
    )
    batch = ordering.latest_per_key(batch, ["order_id"])
    batch.createOrReplaceTempView("cdc_src")

    # Apply the same batch twice; state must be identical (idempotent MERGE).
    _merge(spark, "local.db.orders2")
    first = spark.sql("SELECT * FROM local.db.orders2").collect()
    _merge(spark, "local.db.orders2")
    second = spark.sql("SELECT * FROM local.db.orders2").collect()

    assert len(first) == len(second) == 1
    assert first[0]["status"] == second[0]["status"] == "DELIVERED"

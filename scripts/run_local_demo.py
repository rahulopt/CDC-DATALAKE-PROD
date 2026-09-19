#!/usr/bin/env python3
"""LOCAL end-to-end demo of the CDC pipeline — no AWS required.

Runs the *real* cdc_lib building blocks (the same code the Glue Silver job uses)
against a local Iceberg table so you can SEE the full flow:

    generate DMS-style CDC  (I/U/D + duplicates + out-of-order + invalid + spike)
        -> parse (canonical)
        -> data-quality split  (valid  vs  quarantine)
        -> dedup latest-wins   (ordering by commit-ts / txn / seq)
        -> Iceberg MERGE       (INSERT / UPDATE / DELETE, ACID)
        -> show Silver current state + snapshot history + audit counts

Run:
    scripts/run_local_demo.sh          # wrapper that sets Java/Spark env
  or directly (with the right env):
    python scripts/run_local_demo.py
"""

from __future__ import annotations

import os
import sys
import tempfile

# Make cdc_lib importable from the repo layout.
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO, "glue"))

# Force SPARK_HOME to the installed pyspark (avoid a shadowing system Spark).
import pyspark  # noqa: E402

os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from cdc_lib import cdc_parser, ordering, validation  # noqa: E402
from cdc_lib.config import DQRule  # noqa: E402

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

DQ_RULES = [
    DQRule("order_id", "not_null"),
    DQRule("customer_id", "not_null"),
    DQRule("order_amount", "non_negative"),
    DQRule("status", "allowed_values", ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]),
]


def banner(msg: str) -> None:
    print("\n" + "=" * 68)
    print(f" {msg}")
    print("=" * 68)


def build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("cdc-local-demo")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_PKG)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .getOrCreate()
    )


def sample_cdc():
    """A small, readable CDC feed showing every tricky case."""
    # (Op, order_id, customer_id, order_amount, status, commit_ts, txn, seq)
    base = "2026-09-19T17:0"
    return [
        # order 101: placed -> shipped -> delivered  (out of order arrival!)
        ("I", 101, 1, 500.0, "PLACED", f"{base}0:00Z", "tx1", 1),
        ("U", 101, 1, 500.0, "DELIVERED", f"{base}2:00Z", "tx3", 3),  # newest
        ("U", 101, 1, 500.0, "SHIPPED", f"{base}1:00Z", "tx2", 2),  # older, arrives late
        # order 102: placed then DELETED
        ("I", 102, 2, 250.0, "PLACED", f"{base}0:00Z", "tx4", 4),
        ("D", 102, 2, 250.0, "PLACED", f"{base}3:00Z", "tx5", 5),
        # order 103: duplicate delivery of the same insert
        ("I", 103, 3, 999.0, "PLACED", f"{base}0:00Z", "tx6", 6),
        ("I", 103, 3, 999.0, "PLACED", f"{base}0:00Z", "tx6", 6),  # exact duplicate
        # order 104: invalid status -> must be quarantined
        ("I", 104, 4, 100.0, "TELEPORTED", f"{base}0:00Z", "tx7", 7),
        # order 105: negative amount -> quarantined
        ("I", 105, 5, -5.0, "PLACED", f"{base}0:00Z", "tx8", 8),
    ]


def main() -> int:
    warehouse = tempfile.mkdtemp(prefix="cdc_demo_")
    spark = build_spark(warehouse)
    spark.sparkContext.setLogLevel("ERROR")

    banner("STEP 0: Raw CDC feed (as DMS would emit it)")
    cols = [
        "Op",
        "order_id",
        "customer_id",
        "order_amount",
        "status",
        "commit_timestamp",
        "transaction_id",
        "seq",
    ]
    raw = spark.createDataFrame(sample_cdc(), cols)
    raw.show(truncate=False)
    print(f"Total raw events: {raw.count()}  "
          "(includes out-of-order, duplicate, 2 invalid)")

    banner("STEP 1: Parse DMS envelope -> canonical CDC rows")
    parsed = cdc_parser.parse(raw, "orders")
    parsed.select("__op", "order_id", "status", "__commit_ts", "__txn_id", "__seq").show(
        truncate=False
    )

    banner("STEP 2: Data-quality split (valid vs quarantine)")
    valid, invalid = validation.split_valid_invalid(parsed, DQ_RULES)
    print(f"valid rows   : {valid.count()}")
    print(f"rejected rows: {invalid.count()}  -> S3 reject/quarantine layer")
    quarantine = validation.to_quarantine_records(invalid, "orders", "demo_batch")
    quarantine.select("table_name", "batch_id", "error_reason").show(truncate=False)

    banner("STEP 3: Dedup latest-wins (ordering by commit-ts / txn / seq)")
    dup_count = ordering.count_duplicates(valid, ["order_id"])
    deduped = ordering.latest_per_key(valid, ["order_id"])
    print(f"duplicates removed: {dup_count}")
    deduped.select("__op", "order_id", "status", "__commit_ts").orderBy("order_id").show(
        truncate=False
    )
    print("Note: order 101 keeps DELIVERED (newest) even though SHIPPED arrived later.")

    banner("STEP 4: Iceberg MERGE into Silver (INSERT / UPDATE / DELETE)")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.orders")
    spark.sql(
        "CREATE TABLE local.db.orders "
        "(order_id int, customer_id int, order_amount double, status string) "
        "USING iceberg"
    )
    # Seed an existing state so we see a real UPDATE + DELETE, not just inserts.
    spark.sql(
        "INSERT INTO local.db.orders VALUES "
        "(101, 1, 500.0, 'PLACED'), (102, 2, 250.0, 'PLACED')"
    )
    print("Silver BEFORE merge:")
    spark.sql("SELECT * FROM local.db.orders ORDER BY order_id").show()

    deduped.createOrReplaceTempView("cdc_src")
    spark.sql(
        """
        MERGE INTO local.db.orders t
        USING cdc_src s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s.__op = 'D' THEN DELETE
        WHEN MATCHED AND s.__op = 'U' THEN UPDATE SET
            t.status = s.status, t.order_amount = s.order_amount
        WHEN NOT MATCHED AND s.__op IN ('I','U') THEN
            INSERT (order_id, customer_id, order_amount, status)
            VALUES (s.order_id, s.customer_id, s.order_amount, s.status)
        """
    )
    print("Silver AFTER merge (current state):")
    spark.sql("SELECT * FROM local.db.orders ORDER BY order_id").show()

    banner("STEP 5: Iceberg snapshot history (audit / time-travel)")
    spark.sql(
        "SELECT snapshot_id, committed_at, operation "
        "FROM local.db.orders.snapshots ORDER BY committed_at"
    ).show(truncate=False)

    banner("STEP 6: Audit counts for this batch")
    op_counts = {
        r["__op"]: r["c"]
        for r in deduped.groupBy("__op").agg(F.count("*").alias("c")).collect()
    }
    print(f"  source_records : {raw.count()}")
    print(f"  inserted       : {op_counts.get('I', 0)}")
    print(f"  updated        : {op_counts.get('U', 0)}")
    print(f"  deleted        : {op_counts.get('D', 0)}")
    print(f"  duplicates     : {dup_count}")
    print(f"  rejected       : {invalid.count()}")

    print("\nExpected final Silver:")
    print("  101 DELIVERED  (updated, newest wins over late SHIPPED)")
    print("  102 GONE       (deleted)")
    print("  103 PLACED     (inserted, duplicate collapsed)")
    print("  104 / 105      NOT present (quarantined: bad status / negative amount)")

    spark.stop()
    print(f"\n(demo warehouse: {warehouse})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

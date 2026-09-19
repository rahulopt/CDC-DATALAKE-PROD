"""Unit tests for cdc_lib.ordering — dedup, ordering, stale-event guard."""

from __future__ import annotations

from cdc_lib import ordering
from pyspark.sql import functions as F


def _batch(spark, rows):
    """rows: list of (pk, op, commit_ts_iso, txn, seq, payload)"""
    df = spark.createDataFrame(
        rows, ["order_id", "__op", "commit_ts", "__txn_id", "__seq", "status"]
    )
    return df.withColumn("__commit_ts", F.to_timestamp("commit_ts")).drop("commit_ts")


def test_latest_wins_within_batch(spark):
    rows = [
        (1, "U", "2026-09-19T10:00:00Z", "t1", 1, "PLACED"),
        (1, "U", "2026-09-19T12:00:00Z", "t3", 3, "DELIVERED"),  # newest
        (1, "U", "2026-09-19T11:00:00Z", "t2", 2, "SHIPPED"),
    ]
    out = ordering.latest_per_key(_batch(spark, rows), ["order_id"]).collect()
    assert len(out) == 1
    assert out[0]["status"] == "DELIVERED"


def test_out_of_order_uses_commit_ts_not_arrival(spark):
    # Rows appear in arrival order PLACED, DELIVERED, SHIPPED but commit ts wins.
    rows = [
        (5, "U", "2026-09-19T10:00:00Z", "t1", 1, "PLACED"),
        (5, "U", "2026-09-19T12:00:00Z", "t3", 3, "DELIVERED"),
        (5, "U", "2026-09-19T11:00:00Z", "t2", 2, "SHIPPED"),
    ]
    out = ordering.latest_per_key(_batch(spark, rows), ["order_id"]).collect()
    assert out[0]["status"] == "DELIVERED"


def test_count_duplicates(spark):
    rows = [
        (1, "U", "2026-09-19T10:00:00Z", "t1", 1, "A"),
        (1, "U", "2026-09-19T11:00:00Z", "t2", 2, "B"),
        (2, "I", "2026-09-19T10:00:00Z", "t3", 1, "C"),
    ]
    assert ordering.count_duplicates(_batch(spark, rows), ["order_id"]) == 1


def test_guard_stale_updates_drops_older(spark):
    incoming = _batch(
        spark,
        [
            (1, "U", "2026-09-19T09:00:00Z", "t0", 0, "OLD"),  # older than silver
            (2, "U", "2026-09-19T15:00:00Z", "t9", 9, "NEW"),  # newer -> keep
            (3, "I", "2026-09-19T09:00:00Z", "t1", 1, "BRANDNEW"),  # new key -> keep
        ],
    )
    current = (
        spark.createDataFrame(
            [(1, "2026-09-19T10:00:00Z"), (2, "2026-09-19T10:00:00Z")],
            ["order_id", "silver_ts"],
        )
        .withColumn("__silver_commit_ts", F.to_timestamp("silver_ts"))
        .drop("silver_ts")
    )

    out = ordering.guard_stale_updates(incoming, current, ["order_id"]).collect()
    kept = {r["order_id"] for r in out}
    assert kept == {2, 3}  # stale id=1 dropped

"""Unit tests for cdc_lib.cdc_parser — DMS envelope normalisation."""

from __future__ import annotations

from cdc_lib import cdc_parser


def test_parse_dms_s3_flat_ops(spark):
    rows = [
        ("I", "1", "PLACED", "100", "2026-09-19T10:00:00Z", "tx1", 1),
        ("U", "1", "SHIPPED", "100", "2026-09-19T11:00:00Z", "tx2", 2),
        ("D", "2", "CANCELLED", "50", "2026-09-19T12:00:00Z", "tx3", 3),
    ]
    df = spark.createDataFrame(
        rows,
        ["Op", "order_id", "status", "order_amount", "commit_timestamp", "transaction_id", "seq"],
    )
    out = cdc_parser.parse(df, "orders")
    assert "__op" in out.columns
    assert "__commit_ts" in out.columns
    ops = {r["order_id"]: r["__op"] for r in out.collect()}
    assert ops["1"] in ("I", "U")  # two rows for id=1
    got_ops = sorted(r["__op"] for r in out.collect())
    assert got_ops == ["D", "I", "U"]


def test_op_normalisation_variants(spark):
    rows = [("load", "1"), ("insert", "2"), ("update", "3"), ("delete", "4"), ("weird", "5")]
    df = spark.createDataFrame(rows, ["Op", "id"])
    out = cdc_parser.parse(df, "t")
    mapping = {r["id"]: r["__op"] for r in out.collect()}
    assert mapping["1"] == "I"  # load -> insert
    assert mapping["2"] == "I"
    assert mapping["3"] == "U"
    assert mapping["4"] == "D"
    assert mapping["5"] is None  # unknown op -> NULL (will be quarantined)


def test_parse_dms_kinesis_nested(spark):
    # Build a nested metadata/data envelope like DMS -> Kinesis.
    json_rows = [
        '{"metadata":{"operation":"insert","record-type":"data","table-name":"orders",'
        '"transaction-id":10,"commit-timestamp":"2026-09-19T10:00:00Z","transaction-record-id":1},'
        '"data":{"order_id":1,"status":"PLACED","order_amount":100}}',
        # a control record that must be dropped
        '{"metadata":{"operation":"update","record-type":"control","table-name":"orders",'
        '"transaction-id":11,"commit-timestamp":"2026-09-19T10:05:00Z","transaction-record-id":2},'
        '"data":{"order_id":1,"status":"SHIPPED","order_amount":100}}',
    ]
    rdd = spark.sparkContext.parallelize(json_rows)
    df = spark.read.json(rdd)
    out = cdc_parser.parse(df, "orders")
    collected = out.collect()
    assert len(collected) == 1  # control record dropped
    row = collected[0]
    assert row["__op"] == "I"
    assert row["order_id"] == 1
    assert row["__table"] == "orders"
    assert row["__txn_id"] == "10"

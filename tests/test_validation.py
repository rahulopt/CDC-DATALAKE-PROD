"""Unit tests for cdc_lib.validation — DQ rules and quarantine shaping."""

from __future__ import annotations

from cdc_lib import validation
from cdc_lib.config import DQRule
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RULES = [
    DQRule("order_id", "not_null"),
    DQRule("customer_id", "not_null"),
    DQRule("order_amount", "non_negative"),
    DQRule("status", "allowed_values", ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]),
]

_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("__op", StringType(), True),
    ]
)


def _df(spark, rows):
    return spark.createDataFrame(rows, _SCHEMA)


def test_valid_rows_pass(spark):
    rows = [(1, 10, 100.0, "PLACED", "I"), (2, 11, 0.0, "DELIVERED", "U")]
    valid, invalid = validation.split_valid_invalid(_df(spark, rows), RULES)
    assert valid.count() == 2
    assert invalid.count() == 0


def test_null_pk_rejected(spark):
    rows = [(None, 10, 100.0, "PLACED", "I")]
    valid, invalid = validation.split_valid_invalid(_df(spark, rows), RULES)
    assert valid.count() == 0
    reason = invalid.collect()[0]["__error_reason"]
    assert "order_id IS NULL" in reason


def test_negative_amount_and_bad_status(spark):
    rows = [(1, 10, -5.0, "SHIPPING", "I")]
    valid, invalid = validation.split_valid_invalid(_df(spark, rows), RULES)
    assert invalid.count() == 1
    reason = invalid.collect()[0]["__error_reason"]
    assert "order_amount < 0" in reason
    assert "status NOT IN" in reason


def test_invalid_op_rejected(spark):
    rows = [(1, 10, 100.0, "PLACED", "X")]  # X is not I/U/D
    valid, invalid = validation.split_valid_invalid(_df(spark, rows), RULES)
    assert invalid.count() == 1
    assert "invalid op" in invalid.collect()[0]["__error_reason"]


def test_delete_exempt_from_payload_rules(spark):
    # A delete may carry only the key; negative amount / bad status ignored.
    rows = [(1, 10, -1.0, "NONSENSE", "D")]
    valid, invalid = validation.split_valid_invalid(_df(spark, rows), RULES)
    assert valid.count() == 1
    assert invalid.count() == 0


def test_quarantine_record_shape(spark):
    rows = [(None, 10, 100.0, "PLACED", "I")]
    _, invalid = validation.split_valid_invalid(_df(spark, rows), RULES)
    q = validation.to_quarantine_records(invalid, "orders", "batch_1")
    assert set(q.columns) == {
        "original_event",
        "error_reason",
        "table_name",
        "processing_time",
        "batch_id",
    }
    row = q.collect()[0]
    assert row["table_name"] == "orders"
    assert row["batch_id"] == "batch_1"
    # to_json drops null fields, so assert on a populated column from the row.
    assert "customer_id" in row["original_event"]  # JSON of the original row

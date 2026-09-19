"""CDC envelope parsing.

AWS DMS, when targeting Kinesis / S3, emits change records with a control
section and a data section. A typical DMS Kinesis JSON record looks like::

    {
      "metadata": {
        "operation": "update",           # load | insert | update | delete
        "record-type": "data",           # data | control
        "schema-name": "public",
        "table-name": "orders",
        "transaction-id": 123456789,
        "commit-timestamp": "2026-09-19T17:04:11.123456Z",
        "transaction-record-id": 42
      },
      "data":       { ...current column values... },
      "before":     { ...previous values (updates/deletes if enabled)... }
    }

Some DMS S3 targets instead inline the columns and add an ``Op`` column
(``I`` / ``U`` / ``D``) plus ``transact_id`` / ``timestamp``. We support both.

This module produces a **canonical** DataFrame with these columns:

    __op            STRING   normalised to I / U / D
    __table         STRING   source table name
    __commit_ts     TIMESTAMP commit timestamp (for ordering / lag)
    __txn_id        STRING   transaction id (nullable)
    __seq           LONG     monotonic ordering value within a txn (nullable)
    __source_pos    STRING   LSN / source position when available
    <payload cols>  ...      the actual data columns

Keeping parsing separate from ordering/merge makes each piece independently
testable and lets us plug a different source (Debezium/Kafka) later by only
swapping this module.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import OP_DELETE, OP_INSERT, OP_UPDATE

# Map every DMS spelling of an operation onto our canonical codes.
# DMS "load" (full-load rows) are treated as inserts.
_OP_NORMALISE = {
    "load": OP_INSERT,
    "insert": OP_INSERT,
    "i": OP_INSERT,
    "update": OP_UPDATE,
    "u": OP_UPDATE,
    "delete": OP_DELETE,
    "d": OP_DELETE,
}


def normalise_op_expr(col):
    """Return a Spark Column that maps a raw op string to canonical I/U/D.

    Unknown / null operations map to NULL so validation can quarantine them
    rather than silently dropping or misclassifying.
    """
    lowered = F.lower(F.trim(col.cast("string")))
    mapping = F.create_map([F.lit(x) for kv in _OP_NORMALISE.items() for x in kv])
    return mapping[lowered]


def _has_column(df: DataFrame, name: str) -> bool:
    return name in df.columns


def parse_dms_kinesis(df: DataFrame, table_name: str) -> DataFrame:
    """Parse the nested DMS Kinesis envelope (metadata/data) into canonical form.

    ``df`` is expected to already have ``metadata`` and ``data`` struct columns
    (Spark infers these from JSON). Control records are filtered out.
    """
    if not (_has_column(df, "metadata") and _has_column(df, "data")):
        raise ValueError(
            "parse_dms_kinesis expects 'metadata' and 'data' columns; "
            f"got {df.columns}. Use parse_dms_s3 for flat records."
        )

    # Drop control records (DDL / heartbeat); keep only data changes.
    data_only = df.filter(F.col("metadata.record-type") == F.lit("data"))

    meta = F.col("metadata")
    parsed = (
        data_only.withColumn("__op", normalise_op_expr(meta["operation"]))
        .withColumn("__table", F.coalesce(meta["table-name"], F.lit(table_name)))
        .withColumn("__commit_ts", F.to_timestamp(meta["commit-timestamp"]))
        .withColumn("__txn_id", meta["transaction-id"].cast("string"))
        .withColumn("__seq", meta["transaction-record-id"].cast("long"))
        .withColumn("__source_pos", meta["transaction-id"].cast("string"))
    )

    # Flatten the data struct: promote every data.* field to a top-level column.
    data_fields = [f.name for f in parsed.schema["data"].dataType.fields]  # type: ignore[attr-defined]
    for fld in data_fields:
        parsed = parsed.withColumn(fld, F.col("data")[fld])

    keep = ["__op", "__table", "__commit_ts", "__txn_id", "__seq", "__source_pos"] + data_fields
    return parsed.select(*keep)


def parse_dms_s3(df: DataFrame, table_name: str) -> DataFrame:
    """Parse the flat DMS S3 style record (``Op`` column + inline columns).

    Column names for control fields vary by DMS version; we accept the common
    ones and fall back to nulls when a field is absent.
    """
    op_col = None
    for candidate in ("Op", "op", "__op"):
        if _has_column(df, candidate):
            op_col = candidate
            break
    if op_col is None:
        raise ValueError(f"No operation column found among Op/op/__op in {df.columns}")

    out = df.withColumn("__op", normalise_op_expr(F.col(op_col)))
    out = out.withColumn("__table", F.lit(table_name))

    # Commit timestamp: DMS S3 uses transaction commit ts if configured.
    ts_col = next(
        (c for c in ("commit_timestamp", "timestamp", "__commit_ts") if _has_column(df, c)), None
    )
    out = out.withColumn(
        "__commit_ts", F.to_timestamp(F.col(ts_col)) if ts_col else F.lit(None).cast("timestamp")
    )

    txn_col = next(
        (c for c in ("transaction_id", "transact_id", "__txn_id") if _has_column(df, c)), None
    )
    out = out.withColumn(
        "__txn_id", F.col(txn_col).cast("string") if txn_col else F.lit(None).cast("string")
    )

    seq_col = next(
        (c for c in ("seq", "transaction_record_id", "__seq") if _has_column(df, c)), None
    )
    out = out.withColumn(
        "__seq", F.col(seq_col).cast("long") if seq_col else F.lit(None).cast("long")
    )

    pos_col = next((c for c in ("source_pos", "lsn", "__source_pos") if _has_column(df, c)), None)
    out = out.withColumn(
        "__source_pos", F.col(pos_col).cast("string") if pos_col else F.col("__txn_id")
    )

    # Drop the raw op column if it differs from canonical name to avoid dupes.
    if op_col not in ("__op",):
        out = out.drop(op_col)
    return out


def parse(df: DataFrame, table_name: str) -> DataFrame:
    """Auto-detect the DMS record shape and return canonical CDC rows."""
    if _has_column(df, "metadata") and _has_column(df, "data"):
        return parse_dms_kinesis(df, table_name)
    return parse_dms_s3(df, table_name)

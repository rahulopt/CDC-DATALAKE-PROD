"""Glue job: Bronze -> Silver CDC MERGE.

This is the heart of the pipeline. For one source table and one batch it:

  1. Reads the batch's raw CDC records from Bronze (S3, partitioned by date/hour).
  2. Parses the DMS envelope into canonical CDC rows (cdc_lib.cdc_parser).
  3. Detects schema evolution vs the current Silver table:
        - additive  -> ALTER TABLE ADD COLUMNS and continue
        - breaking  -> STOP this table, write error, SNS alert, exit non-zero
  4. Runs data-quality checks; invalid rows go to the S3 reject/quarantine layer.
  5. Deduplicates to latest-wins per primary key (in-batch ordering).
  6. Guards against stale/late events regressing newer Silver state.
  7. Applies a single Iceberg MERGE (INSERT / UPDATE / DELETE) — idempotent.
  8. Writes a DynamoDB audit record with all counts.

Idempotency: an ``_applied_batches`` Iceberg table records processed batch ids;
re-running the same batch is a no-op MERGE-guarded skip, and the stale-guard plus
latest-wins ensure re-delivery cannot corrupt state.

Invoked by Glue with job parameters:
  --table_name        orders
  --batch_id          20260919_1800
  --bronze_path       s3://.../bronze/orders/year=2026/month=09/day=19/hour=18/
  --config_path       (packaged) config/pipeline.yaml   [optional]
"""

from __future__ import annotations

import sys

# Glue provides getResolvedOptions; guard import so the file is importable in
# non-Glue environments (e.g. static analysis / packaging).
try:
    from awsglue.utils import getResolvedOptions  # type: ignore

    _GLUE = True
except Exception:  # pragma: no cover - only true off-Glue
    _GLUE = False

from cdc_lib import OP_DELETE, OP_INSERT, OP_UPDATE, cdc_parser, ordering, validation
from cdc_lib import audit as audit_mod
from cdc_lib import schema as schema_mod
from cdc_lib.config import load_config
from cdc_lib.notifications import ALERT_SCHEMA, build_failure_message, publish
from pyspark.sql import functions as F
from spark_session import build_spark


def _args() -> dict:
    if _GLUE:
        return getResolvedOptions(
            sys.argv,
            ["table_name", "batch_id", "bronze_path", "config_path"],
        )
    # Local fallback: parse "--k v" pairs from argv for testing/dry runs.
    it = iter(sys.argv[1:])
    out = {}
    for tok in it:
        if tok.startswith("--"):
            out[tok[2:]] = next(it, "")
    out.setdefault("config_path", "config/pipeline.yaml")
    return out


def run() -> int:
    args = _args()
    table_name = args["table_name"]
    batch_id = args["batch_id"]
    bronze_path = args["bronze_path"]
    config_path = args.get("config_path", "config/pipeline.yaml")

    cfg = load_config(config_path)
    tcfg = cfg.table(table_name)

    spark = build_spark(f"cdc-silver-{table_name}-{batch_id}", cfg.silver_warehouse)
    spark.sparkContext.setLogLevel("WARN")

    import boto3

    dynamodb = boto3.resource("dynamodb")
    sns = boto3.client("sns")

    record = audit_mod.AuditRecord(batch_id=batch_id, table_name=table_name)

    catalog = "glue_catalog"
    silver = f"{catalog}.{tcfg.silver_table}"
    applied = f"{catalog}.{cfg.glue_database}._applied_batches"

    try:
        # --- 1. read raw CDC from Bronze -----------------------------------
        raw = spark.read.option("recursiveFileLookup", "true").json(bronze_path)
        record.source_records = raw.count()

        # --- 2. parse DMS envelope -> canonical ----------------------------
        parsed = cdc_parser.parse(raw, table_name)

        # --- ensure Silver + bookkeeping tables exist ----------------------
        _ensure_tables(spark, silver, applied, parsed)

        # --- idempotency: skip already-applied batch -----------------------
        already = spark.sql(
            f"SELECT COUNT(*) c FROM {applied} WHERE batch_id = '{batch_id}' "
            f"AND table_name = '{table_name}'"
        ).collect()[0]["c"]
        if already > 0:
            print(f"[IDEMPOTENT] batch {batch_id}/{table_name} already applied — skipping")
            record.mark_success()
            audit_mod.write(dynamodb, cfg.audit_table, record)
            return 0

        # --- 3. schema evolution detection ---------------------------------
        cur_struct = spark.table(silver).schema
        diff = schema_mod.diff_dataframe_schema(cur_struct, parsed.schema)
        if diff.is_breaking:
            msg = build_failure_message(
                pipeline=f"{table_name}-cdc",
                environment=cfg.environment,
                stage="Silver Schema Check",
                records_received=record.source_records,
                error=f"Breaking schema change: {diff.summary()}",
                action="Review source DDL; evolve Silver schema manually then replay.",
            )
            publish(sns, cfg.sns_topic_arn, f"{ALERT_SCHEMA}: {table_name}", msg)
            record.mark_failed(f"breaking schema change: {diff.summary()}")
            audit_mod.write(dynamodb, cfg.audit_table, record)
            print(f"[SCHEMA-BREAK] {diff.summary()}")
            return 2
        if diff.is_additive:
            _add_columns(spark, silver, diff.added)
            print(f"[SCHEMA-ADD] {diff.summary()}")

        # --- 4. data quality + quarantine ----------------------------------
        valid, invalid = validation.split_valid_invalid(parsed, tcfg.dq_rules)
        record.reject_count = invalid.count()
        if record.reject_count > 0:
            q = validation.to_quarantine_records(invalid, table_name, batch_id)
            (q.write.mode("append").json(f"{cfg.reject_prefix}/{table_name}/batch_id={batch_id}"))

        # --- 5. dedup latest-wins ------------------------------------------
        record.duplicate_count = ordering.count_duplicates(valid, tcfg.primary_keys)
        deduped = ordering.latest_per_key(valid, tcfg.primary_keys)

        # --- 6. guard against stale/late events ----------------------------
        current = spark.table(silver)
        guarded = ordering.guard_stale_updates(deduped, current, tcfg.primary_keys)

        # attach the Silver watermark before merge
        merge_src = guarded.withColumn("__silver_commit_ts", F.col("__commit_ts"))
        merge_src.createOrReplaceTempView("cdc_src")

        # per-op counts for audit
        op_counts = {
            r["__op"]: r["c"]
            for r in merge_src.groupBy("__op").agg(F.count("*").alias("c")).collect()
        }
        record.insert_count = int(op_counts.get(OP_INSERT, 0))
        record.update_count = int(op_counts.get(OP_UPDATE, 0))
        record.delete_count = int(op_counts.get(OP_DELETE, 0))
        record.processed_records = record.insert_count + record.update_count + record.delete_count

        # --- 7. Iceberg MERGE ----------------------------------------------
        _merge(spark, silver, tcfg.primary_keys, merge_src.columns)

        # mark batch applied (idempotency ledger)
        spark.sql(
            f"INSERT INTO {applied} VALUES "
            f"('{batch_id}', '{table_name}', current_timestamp(), "
            f"{record.processed_records})"
        )

        # --- 8. audit success ----------------------------------------------
        record.mark_success()
        audit_mod.write(dynamodb, cfg.audit_table, record)
        print(
            f"[OK] {table_name}/{batch_id}: I={record.insert_count} "
            f"U={record.update_count} D={record.delete_count} "
            f"dup={record.duplicate_count} rej={record.reject_count}"
        )
        return 0

    except Exception as exc:  # noqa: BLE001 - top-level job guard
        record.mark_failed(str(exc))
        try:
            audit_mod.write(dynamodb, cfg.audit_table, record)
        finally:
            msg = build_failure_message(
                pipeline=f"{table_name}-cdc",
                environment=cfg.environment,
                stage="Silver Processing",
                records_received=record.source_records,
                processed=record.processed_records,
                rejected=record.reject_count,
                error=str(exc),
            )
            publish(sns, cfg.sns_topic_arn, f"Glue Job Failure: {table_name}", msg)
        raise
    finally:
        spark.stop()


def _ensure_tables(spark, silver, applied, parsed) -> None:
    """Create the Silver table (from the incoming schema) and the ledger."""
    db = silver.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    # Silver: use the parsed payload columns + a stored watermark.
    cols = [c for c in parsed.columns]
    if "__silver_commit_ts" not in cols:
        cols_ddl = ", ".join(f"{c} {parsed.schema[c].dataType.simpleString()}" for c in cols)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {silver} ({cols_ddl}, "
            f"__silver_commit_ts timestamp) USING iceberg"
        )
    ledger_db = applied.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ledger_db}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {applied} ("
        f"batch_id string, table_name string, applied_at timestamp, "
        f"row_count bigint) USING iceberg"
    )


def _add_columns(spark, silver, added) -> None:
    for name, simple_type in added:
        spark.sql(f"ALTER TABLE {silver} ADD COLUMN {name} {simple_type}")


def _merge(spark, silver, primary_keys, src_cols) -> None:
    on = " AND ".join(f"t.{k} = s.{k}" for k in primary_keys)
    payload = [c for c in src_cols if not (c.startswith("__") and c != "__silver_commit_ts")]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in payload)
    insert_cols = ", ".join(payload)
    insert_vals = ", ".join(f"s.{c}" for c in payload)
    spark.sql(f"""
        MERGE INTO {silver} t
        USING cdc_src s
        ON {on}
        WHEN MATCHED AND s.__op = 'D' THEN DELETE
        WHEN MATCHED AND s.__op = 'U' THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND s.__op IN ('I','U') THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


if __name__ == "__main__":
    sys.exit(run())

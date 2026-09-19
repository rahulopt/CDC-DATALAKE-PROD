"""Glue job: Iceberg table maintenance (requirement 27 & 28).

Keeps Silver/Gold Iceberg tables healthy and cheap by running, per table:

  * rewrite_data_files  — compaction of the many small files produced by
                          frequent CDC MERGEs (fixes the "small files problem").
  * expire_snapshots    — snapshot cleanup beyond the retention window
                          (bounds metadata + storage growth).
  * remove_orphan_files — delete files no longer referenced by any snapshot.

Run on a schedule (e.g. nightly via EventBridge). Retention is configurable.

Job parameters:
  --config_path        config/pipeline.yaml
  --retention_days     7
"""

from __future__ import annotations

import sys

try:
    from awsglue.utils import getResolvedOptions  # type: ignore

    _GLUE = True
except Exception:  # pragma: no cover
    _GLUE = False

from cdc_lib.config import load_config
from spark_session import build_spark


def _args() -> dict:
    keys = ["config_path", "retention_days"]
    if _GLUE:
        return getResolvedOptions(sys.argv, keys)
    it = iter(sys.argv[1:])
    out = {}
    for tok in it:
        if tok.startswith("--"):
            out[tok[2:]] = next(it, "")
    out.setdefault("config_path", "config/pipeline.yaml")
    out.setdefault("retention_days", "7")
    return out


def run() -> None:
    args = _args()
    cfg = load_config(args["config_path"])
    retention_days = int(args["retention_days"])
    spark = build_spark("cdc-iceberg-maintenance", cfg.silver_warehouse)
    spark.sparkContext.setLogLevel("WARN")

    cat = "glue_catalog"
    db = cfg.glue_database

    # Maintain every Silver table plus the Gold tables.
    silver_tables = [t.silver_table.split(".")[-1] for t in cfg.tables.values()]
    gold_tables = [
        "gold_daily_sales",
        "gold_order_status",
        "gold_customer_metrics",
        "gold_product_metrics",
    ]

    for tbl in silver_tables + gold_tables:
        fq = f"{db}.{tbl}"
        try:
            print(f"[maint] compacting {fq}")
            spark.sql(f"""
                CALL {cat}.system.rewrite_data_files(
                    table => '{fq}',
                    options => map('min-input-files','5','target-file-size-bytes','134217728')
                )
            """).show(truncate=False)

            print(f"[maint] expiring snapshots on {fq} (> {retention_days}d)")
            spark.sql(f"""
                CALL {cat}.system.expire_snapshots(
                    table => '{fq}',
                    older_than => TIMESTAMP '{_cutoff(retention_days)}',
                    retain_last => 5
                )
            """).show(truncate=False)

            print(f"[maint] removing orphan files on {fq}")
            spark.sql(f"""
                CALL {cat}.system.remove_orphan_files(
                    table => '{fq}',
                    older_than => TIMESTAMP '{_cutoff(retention_days)}'
                )
            """).show(truncate=False)
        except Exception as exc:  # noqa: BLE001 - maintenance is best-effort per table
            print(f"[maint][WARN] {fq}: {exc}")

    spark.stop()


def _cutoff(days: int) -> str:
    import datetime as dt

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    run()

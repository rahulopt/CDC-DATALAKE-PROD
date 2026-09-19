"""Glue job: Silver -> Gold analytical tables (requirement 18).

Builds business-oriented, query-optimised Iceberg tables from the current-state
Silver tables. Gold tables are fully recomputed (idempotent overwrite) from
Silver each run — simple, correct, and cheap at these cardinalities. For very
large volumes this can be switched to incremental MERGE on the daily grain.

Produces:
  gold_daily_sales     : per-day order + revenue metrics
  gold_customer_metrics: per-customer lifetime metrics
  gold_product_metrics : per-product sales metrics
  gold_order_status    : status distribution per day

Job parameters:
  --config_path   config/pipeline.yaml
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
    if _GLUE:
        return getResolvedOptions(sys.argv, ["config_path"])
    it = iter(sys.argv[1:])
    out = {}
    for tok in it:
        if tok.startswith("--"):
            out[tok[2:]] = next(it, "")
    out.setdefault("config_path", "config/pipeline.yaml")
    return out


def run() -> None:
    args = _args()
    cfg = load_config(args["config_path"])
    spark = build_spark("cdc-gold", cfg.silver_warehouse)
    spark.sparkContext.setLogLevel("WARN")

    cat = "glue_catalog"
    db = cfg.glue_database
    orders = f"{cat}.{db}.orders"
    customers = f"{cat}.{db}.customers"
    order_items = f"{cat}.{db}.order_items"
    products = f"{cat}.{db}.products"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cat}.{db}")

    # gold_daily_sales -----------------------------------------------------
    spark.sql(f"""
        CREATE OR REPLACE TABLE {cat}.{db}.gold_daily_sales USING iceberg AS
        SELECT
            CAST(order_date AS DATE)                                   AS date,
            COUNT(*)                                                   AS total_orders,
            SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END)      AS delivered_orders,
            SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)      AS cancelled_orders,
            SUM(order_amount)                                          AS total_revenue,
            ROUND(AVG(order_amount), 2)                                AS average_order_value
        FROM {orders}
        GROUP BY CAST(order_date AS DATE)
    """)

    # gold_order_status ----------------------------------------------------
    spark.sql(f"""
        CREATE OR REPLACE TABLE {cat}.{db}.gold_order_status USING iceberg AS
        SELECT CAST(order_date AS DATE) AS date, status, COUNT(*) AS order_count
        FROM {orders}
        GROUP BY CAST(order_date AS DATE), status
    """)

    # gold_customer_metrics ------------------------------------------------
    spark.sql(f"""
        CREATE OR REPLACE TABLE {cat}.{db}.gold_customer_metrics USING iceberg AS
        SELECT
            c.customer_id,
            COUNT(o.order_id)                                          AS total_orders,
            COALESCE(SUM(o.order_amount), 0)                           AS lifetime_value,
            MAX(o.order_date)                                          AS last_order_date
        FROM {customers} c
        LEFT JOIN {orders} o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
    """)

    # gold_product_metrics -------------------------------------------------
    spark.sql(f"""
        CREATE OR REPLACE TABLE {cat}.{db}.gold_product_metrics USING iceberg AS
        SELECT
            p.product_id,
            COALESCE(SUM(oi.quantity), 0)                              AS units_sold,
            COUNT(DISTINCT oi.order_id)                                AS orders_containing
        FROM {products} p
        LEFT JOIN {order_items} oi ON p.product_id = oi.product_id
        GROUP BY p.product_id
    """)

    print("[OK] gold tables rebuilt")
    spark.stop()


if __name__ == "__main__":
    run()

"""CDC ordering and deduplication.

The core correctness guarantee of the pipeline: when several changes touch the
same primary key inside one micro-batch, we must apply only the **newest** one,
and we must never let an older event overwrite a newer Silver state.

Ordering key precedence (highest priority first):
    1. commit timestamp (__commit_ts)      -- source transaction commit time
    2. transaction id     (__txn_id)        -- monotonic within source
    3. in-transaction seq (__seq)           -- order of ops inside a txn

All three are used as a composite ordering so that ties are broken
deterministically. Nulls sort *lowest* so a record carrying full metadata
always beats one missing it.

These are pure DataFrame transforms — no I/O — so they unit-test cleanly.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Canonical ordering columns produced by cdc_parser.
_ORDER_COLS = ["__commit_ts", "__txn_id", "__seq"]


def _order_expressions() -> list:
    """Descending ordering with nulls-last so richer metadata wins ties."""
    return [F.col(c).desc_nulls_last() for c in _ORDER_COLS]


def add_rank(df: DataFrame, primary_keys: List[str]) -> DataFrame:
    """Add a ``__rn`` column ranking rows newest-first within each PK group."""
    if not primary_keys:
        raise ValueError("primary_keys must not be empty for CDC ranking")
    window = Window.partitionBy(*primary_keys).orderBy(*_order_expressions())
    return df.withColumn("__rn", F.row_number().over(window))


def latest_per_key(df: DataFrame, primary_keys: List[str]) -> DataFrame:
    """Collapse a batch to exactly one (newest) row per primary key.

    This is the "latest-wins" dedup that makes an Iceberg MERGE safe: MERGE
    fails if the source has multiple rows matching the same target key, and it
    also protects against duplicate/out-of-order delivery within the batch.
    """
    ranked = add_rank(df, primary_keys)
    return ranked.filter(F.col("__rn") == 1).drop("__rn")


def count_duplicates(df: DataFrame, primary_keys: List[str]) -> int:
    """Number of *extra* rows removed by dedup (for audit ``duplicate_count``)."""
    total = df.count()
    distinct_keys = df.select(*primary_keys).distinct().count()
    return max(total - distinct_keys, 0)


def guard_stale_updates(
    incoming: DataFrame,
    current: DataFrame,
    primary_keys: List[str],
    watermark_col: str = "__commit_ts",
) -> DataFrame:
    """Drop incoming rows that are older than what Silver already holds.

    Even after in-batch dedup, a *late* event can arrive in a later batch
    carrying an older commit timestamp than the row already merged. Applying it
    would regress the Silver state (e.g. DELIVERED -> SHIPPED). We left-join the
    incoming batch against the current Silver watermark and keep an incoming row
    only when it is strictly newer (or the key is brand new).

    ``current`` must expose the primary keys and a ``__commit_ts`` watermark
    column (Silver stores this as ``__silver_commit_ts``).
    """
    if watermark_col not in incoming.columns:
        raise ValueError(f"incoming batch missing watermark column {watermark_col!r}")

    cur = current.select(
        *[F.col(k).alias(f"__cur_{k}") for k in primary_keys],
        F.col("__silver_commit_ts").alias("__cur_ts"),
    )

    join_cond = [incoming[k] == cur[f"__cur_{k}"] for k in primary_keys]
    joined = incoming.join(cur, join_cond, "left")

    # Keep if the key is new (no current row) OR incoming is strictly newer.
    keep = joined["__cur_ts"].isNull() | (joined[watermark_col] > joined["__cur_ts"])
    result = joined.filter(keep)

    drop_cols = [f"__cur_{k}" for k in primary_keys] + ["__cur_ts"]
    return result.drop(*drop_cols)

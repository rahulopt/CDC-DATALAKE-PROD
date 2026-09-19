"""Data-quality validation and quarantine.

Given a batch of canonical CDC rows and a table's declarative DQ rules
(:class:`cdc_lib.config.DQRule`), split the batch into:

    * **valid**   rows that pass every rule
    * **rejected**rows that fail at least one rule, annotated with the reason

Rejected rows are shaped for the S3 reject/quarantine layer with the required
fields (original_event, error_reason, table_name, processing_time, batch_id) so
that no invalid record silently disappears (requirement 12).
"""

from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import VALID_OPS
from .config import DQRule


def _rule_failure_expr(rule: DQRule):
    """Return a boolean Column that is True when ``rule`` is VIOLATED."""
    col = F.col(rule.column)
    if rule.kind == "not_null":
        return col.isNull()
    if rule.kind == "non_negative":
        return col.isNotNull() & (col.cast("double") < F.lit(0))
    if rule.kind == "allowed_values":
        return col.isNotNull() & (~col.isin(list(rule.values)))
    if rule.kind == "valid_timestamp":
        return col.isNotNull() & F.to_timestamp(col).isNull()
    raise ValueError(f"Unsupported DQ rule kind: {rule.kind}")


def _rule_reason(rule: DQRule) -> str:
    if rule.kind == "not_null":
        return f"{rule.column} IS NULL"
    if rule.kind == "non_negative":
        return f"{rule.column} < 0"
    if rule.kind == "allowed_values":
        return f"{rule.column} NOT IN {tuple(rule.values)}"
    if rule.kind == "valid_timestamp":
        return f"{rule.column} is not a valid timestamp"
    return f"{rule.column} failed {rule.kind}"


def build_error_reason(rules: List[DQRule]) -> "F.Column":
    """A Column concatenating all violated-rule reasons for a row (or NULL)."""
    parts = []
    for rule in rules:
        parts.append(F.when(_rule_failure_expr(rule), F.lit(_rule_reason(rule))))
    # Always include the structural op-validity check.
    parts.append(
        F.when(
            ~F.col("__op").isin(list(VALID_OPS)),
            F.concat(F.lit("invalid op: "), F.coalesce(F.col("__op"), F.lit("<null>"))),
        )
    )
    # array of non-null reasons -> comma joined; empty array -> NULL
    reasons = F.array_except(F.array(*parts), F.array(F.lit(None).cast("string")))
    return F.when(F.size(reasons) > 0, F.array_join(reasons, "; "))


def split_valid_invalid(
    df: DataFrame,
    rules: List[DQRule],
) -> Tuple[DataFrame, DataFrame]:
    """Return ``(valid_df, invalid_df)``.

    ``invalid_df`` carries an extra ``__error_reason`` string column. DELETE
    operations are exempt from payload rules (a delete may legitimately carry
    only the key), except for the structural op check and primary-key checks,
    which the caller expresses as ``not_null`` rules on the key columns.
    """
    reason = build_error_reason(rules)
    annotated = df.withColumn("__error_reason", reason)

    # For DELETE ops, ignore payload-value failures but still enforce op + PK.
    # We approximate this by re-evaluating only not_null rules for deletes.
    pk_only_reason = build_error_reason([r for r in rules if r.kind == "not_null"])
    annotated = annotated.withColumn(
        "__error_reason",
        F.when(F.col("__op") == F.lit("D"), pk_only_reason).otherwise(F.col("__error_reason")),
    )

    invalid = annotated.filter(F.col("__error_reason").isNotNull())
    valid = annotated.filter(F.col("__error_reason").isNull()).drop("__error_reason")
    return valid, invalid


def to_quarantine_records(
    invalid: DataFrame,
    table_name: str,
    batch_id: str,
) -> DataFrame:
    """Shape invalid rows for the S3 reject layer (requirement 12).

    Produces columns: original_event (JSON string of the whole row),
    error_reason, table_name, processing_time, batch_id.
    """
    payload_cols = [c for c in invalid.columns if c != "__error_reason"]
    return invalid.select(
        F.to_json(F.struct(*payload_cols)).alias("original_event"),
        F.col("__error_reason").alias("error_reason"),
        F.lit(table_name).alias("table_name"),
        F.current_timestamp().alias("processing_time"),
        F.lit(batch_id).alias("batch_id"),
    )

"""Configuration loading for the CDC pipeline.

Configuration is expressed as YAML (see ``config/pipeline.yaml``) and describes,
per source table, the primary keys, ordering columns, data-quality rules and
target Iceberg table name. Keeping this data-driven means adding a new table is
a config change, not a code change.

Design notes
------------
* We avoid a hard dependency on a YAML parser at import time being present in
  every runtime by falling back gracefully; PyYAML is in requirements.txt and
  is available on Glue via ``--additional-python-modules`` if needed. For unit
  tests PyYAML is installed.
* ``PipelineConfig`` / ``TableConfig`` are plain dataclasses so they are trivial
  to construct in tests without any files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DQRule:
    """A single declarative data-quality rule.

    kind:
        not_null      -> column must not be null
        non_negative  -> numeric column must be >= 0
        allowed_values-> column value must be in ``values``
        valid_timestamp -> column must parse as a timestamp (non-null after cast)
    """

    column: str
    kind: str
    values: Optional[List[Any]] = None

    def __post_init__(self) -> None:
        allowed = {"not_null", "non_negative", "allowed_values", "valid_timestamp"}
        if self.kind not in allowed:
            raise ValueError(f"Unknown DQ rule kind: {self.kind!r} (allowed: {sorted(allowed)})")
        if self.kind == "allowed_values" and not self.values:
            raise ValueError(f"allowed_values rule on {self.column!r} requires non-empty `values`")


@dataclass(frozen=True)
class TableConfig:
    """Per-table processing configuration."""

    name: str  # logical source table name, e.g. "orders"
    primary_keys: List[str]  # business key columns
    order_by: List[str]  # ordering cols (LSN/commit ts) newest-wins
    silver_table: str  # e.g. "silver.orders"
    dq_rules: List[DQRule] = field(default_factory=list)
    # columns that may legitimately appear over time (additive schema evolution)
    partition_by: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.primary_keys:
            raise ValueError(f"TableConfig {self.name!r} must declare at least one primary key")
        if not self.order_by:
            raise ValueError(f"TableConfig {self.name!r} must declare ordering columns")


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level pipeline configuration."""

    environment: str
    bronze_bucket: str
    silver_warehouse: str  # s3://.../silver (Iceberg warehouse root)
    reject_prefix: str  # s3://.../reject
    glue_database: str
    audit_table: str  # DynamoDB table name
    sns_topic_arn: str
    tables: Dict[str, TableConfig]

    def table(self, name: str) -> TableConfig:
        try:
            return self.tables[name]
        except KeyError as exc:
            raise KeyError(
                f"No configuration for table {name!r}. Known: {sorted(self.tables)}"
            ) from exc


def _apply_env_overrides(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Allow selected top-level values to be overridden by environment variables.

    This lets the same YAML be reused across environments while Terraform / the
    Glue job supplies concrete ARNs and bucket names via ``--conf`` style env.
    """
    env_map = {
        "environment": "CDC_ENVIRONMENT",
        "bronze_bucket": "CDC_BRONZE_BUCKET",
        "silver_warehouse": "CDC_SILVER_WAREHOUSE",
        "reject_prefix": "CDC_REJECT_PREFIX",
        "glue_database": "CDC_GLUE_DATABASE",
        "audit_table": "CDC_AUDIT_TABLE",
        "sns_topic_arn": "CDC_SNS_TOPIC_ARN",
    }
    for key, env_name in env_map.items():
        if os.environ.get(env_name):
            raw[key] = os.environ[env_name]
    return raw


def parse_config(raw: Dict[str, Any]) -> PipelineConfig:
    """Build a :class:`PipelineConfig` from a plain dict (already parsed YAML)."""
    raw = _apply_env_overrides(dict(raw))

    tables: Dict[str, TableConfig] = {}
    for tname, tcfg in (raw.get("tables") or {}).items():
        rules = [
            DQRule(column=r["column"], kind=r["kind"], values=r.get("values"))
            for r in (tcfg.get("dq_rules") or [])
        ]
        tables[tname] = TableConfig(
            name=tname,
            primary_keys=tcfg["primary_keys"],
            order_by=tcfg["order_by"],
            silver_table=tcfg["silver_table"],
            dq_rules=rules,
            partition_by=tcfg.get("partition_by", []),
        )

    required = [
        "environment",
        "bronze_bucket",
        "silver_warehouse",
        "reject_prefix",
        "glue_database",
        "audit_table",
        "sns_topic_arn",
    ]
    missing = [k for k in required if not raw.get(k)]
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    return PipelineConfig(
        environment=raw["environment"],
        bronze_bucket=raw["bronze_bucket"],
        silver_warehouse=raw["silver_warehouse"],
        reject_prefix=raw["reject_prefix"],
        glue_database=raw["glue_database"],
        audit_table=raw["audit_table"],
        sns_topic_arn=raw["sns_topic_arn"],
        tables=tables,
    )


def load_config(path: str) -> PipelineConfig:
    """Load and parse a YAML config file into a :class:`PipelineConfig`."""
    import yaml  # local import so the module is importable without PyYAML

    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return parse_config(raw)

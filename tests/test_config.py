"""Unit tests for cdc_lib.config — parsing, validation, env overrides."""

from __future__ import annotations

import os

import pytest
from cdc_lib import config

BASE = {
    "environment": "dev",
    "bronze_bucket": "s3://b",
    "silver_warehouse": "s3://s",
    "reject_prefix": "s3://r",
    "glue_database": "cdc_silver",
    "audit_table": "audit",
    "sns_topic_arn": "arn:aws:sns:x",
    "tables": {
        "orders": {
            "primary_keys": ["order_id"],
            "order_by": ["__commit_ts"],
            "silver_table": "cdc_silver.orders",
            "dq_rules": [
                {"column": "order_id", "kind": "not_null"},
                {"column": "status", "kind": "allowed_values", "values": ["PLACED"]},
            ],
        }
    },
}


def test_parse_config_ok():
    cfg = config.parse_config(dict(BASE))
    assert cfg.environment == "dev"
    t = cfg.table("orders")
    assert t.primary_keys == ["order_id"]
    assert len(t.dq_rules) == 2


def test_unknown_table_raises():
    cfg = config.parse_config(dict(BASE))
    with pytest.raises(KeyError):
        cfg.table("nope")


def test_missing_required_key_raises():
    bad = dict(BASE)
    del bad["audit_table"]
    with pytest.raises(ValueError):
        config.parse_config(bad)


def test_env_override(monkeypatch):
    monkeypatch.setenv("CDC_AUDIT_TABLE", "prod_audit")
    monkeypatch.setenv("CDC_ENVIRONMENT", "prod")
    cfg = config.parse_config(dict(BASE))
    assert cfg.audit_table == "prod_audit"
    assert cfg.environment == "prod"


def test_bad_dq_rule_kind():
    with pytest.raises(ValueError):
        config.DQRule("c", "not_a_kind")


def test_allowed_values_requires_values():
    with pytest.raises(ValueError):
        config.DQRule("c", "allowed_values", None)


def test_load_yaml_file():
    # Load the actual repo config to ensure it parses.
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg = config.load_config(os.path.join(repo, "config", "pipeline.yaml"))
    assert "orders" in cfg.tables
    assert "payments" in cfg.tables

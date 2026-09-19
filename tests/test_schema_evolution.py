"""Unit tests for cdc_lib.schema — schema-evolution classification."""

from __future__ import annotations

from cdc_lib import schema


def test_no_change():
    cur = [("order_id", "int"), ("status", "string")]
    inc = [("order_id", "int"), ("status", "string")]
    d = schema.diff_schema(cur, inc)
    assert d.is_empty
    assert not d.is_breaking
    assert not d.is_additive


def test_additive_new_column():
    cur = [("order_id", "int"), ("status", "string")]
    inc = [("order_id", "int"), ("status", "string"), ("coupon_code", "string")]
    d = schema.diff_schema(cur, inc)
    assert d.is_additive
    assert not d.is_breaking
    assert d.added == [("coupon_code", "string")]


def test_breaking_dropped_column():
    cur = [("order_id", "int"), ("status", "string")]
    inc = [("order_id", "int")]
    d = schema.diff_schema(cur, inc)
    assert d.is_breaking
    assert "status" in d.removed


def test_breaking_incompatible_type_change():
    cur = [("order_id", "int"), ("amount", "string")]
    inc = [("order_id", "int"), ("amount", "int")]  # string -> int is incompatible
    d = schema.diff_schema(cur, inc)
    assert d.is_breaking
    assert d.incompatible == [("amount", "string", "int")]


def test_compatible_widening_is_not_breaking():
    cur = [("order_id", "int"), ("amount", "float")]
    inc = [("order_id", "long"), ("amount", "double")]  # widening promotions
    d = schema.diff_schema(cur, inc)
    assert not d.is_breaking
    assert d.type_changed  # recorded but compatible
    assert d.incompatible == []


def test_control_columns_ignored():
    cur = [("order_id", "int")]
    inc = [("order_id", "int"), ("__op", "string"), ("__commit_ts", "timestamp")]
    d = schema.diff_schema(cur, inc)
    assert d.is_empty  # __-prefixed columns are pipeline-managed

"""Unit tests for cdc_lib.audit and cdc_lib.notifications (mocked AWS)."""

from __future__ import annotations

import boto3
from cdc_lib import audit as audit_mod
from cdc_lib import notifications as notif
from moto import mock_aws


def test_audit_record_lifecycle():
    rec = audit_mod.AuditRecord(batch_id="b1", table_name="orders")
    assert rec.status == "RUNNING"
    rec.insert_count = 8200
    rec.update_count = 42100
    rec.mark_success()
    assert rec.status == "SUCCESS"
    assert rec.end_time is not None
    item = rec.to_dynamodb_item()
    # ints become Decimal, None omitted, composite key added
    assert item["batch_table"] == "b1#orders"
    assert "error_message" not in item  # None dropped
    from decimal import Decimal

    assert item["insert_count"] == Decimal(8200)


def test_audit_mark_failed():
    rec = audit_mod.AuditRecord(batch_id="b2", table_name="orders")
    rec.mark_failed("boom")
    assert rec.status == "FAILED"
    assert rec.error_message == "boom"


@mock_aws
def test_audit_write_to_dynamodb():
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="audit",
        KeySchema=[{"AttributeName": "batch_table", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "batch_table", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    rec = audit_mod.AuditRecord(batch_id="b1", table_name="orders")
    rec.source_records = 100
    rec.mark_success()
    audit_mod.write(ddb, "audit", rec)

    got = ddb.Table("audit").get_item(Key={"batch_table": "b1#orders"})["Item"]
    assert got["status"] == "SUCCESS"
    assert int(got["source_records"]) == 100


def test_build_failure_message_contains_fields():
    msg = notif.build_failure_message(
        pipeline="orders-cdc",
        environment="PROD",
        stage="Silver Processing",
        records_received=52481,
        processed=51920,
        rejected=561,
        error="Iceberg MERGE failed",
        retry_attempts=3,
    )
    assert "orders-cdc" in msg
    assert "Silver Processing" in msg
    assert "52,481" in msg
    assert "Retry attempts: 3" in msg


@mock_aws
def test_publish_to_sns():
    sns = boto3.client("sns", region_name="us-east-1")
    topic = sns.create_topic(Name="alerts")["TopicArn"]
    mid = notif.publish(sns, topic, "Glue Job Failure: orders", "body")
    assert mid is not None


def test_publish_noop_without_topic():
    assert notif.publish(None, "", "s", "m") is None

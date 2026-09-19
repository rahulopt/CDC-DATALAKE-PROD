"""SNS notification helper (requirement 21).

Sends *critical-only* operational alerts (failures, high lag, DQ failure,
schema-breaking change, excessive rejects, SLA breach). Successful runs are
intentionally NOT notified here — they are tracked via CloudWatch/DynamoDB — to
avoid alert fatigue.

``build_failure_message`` is a pure function (unit-tested); ``publish`` performs
the SNS call and is injected with a boto3 client so it can be mocked.
"""

from __future__ import annotations

from typing import Optional

# Alert categories -> subject prefixes.
ALERT_DMS = "DMS Failure"
ALERT_KINESIS = "Kinesis Processing Failure"
ALERT_GLUE = "Glue Job Failure"
ALERT_STEPFN = "Step Functions Failure"
ALERT_LAG = "High CDC Lag"
ALERT_DQ = "Data Quality Failure"
ALERT_SCHEMA = "Schema-Breaking Change"
ALERT_REJECTS = "Excessive Rejected Records"
ALERT_SLA = "SLA Breach"


def build_failure_message(
    *,
    pipeline: str,
    environment: str,
    stage: str,
    records_received: int = 0,
    processed: int = 0,
    rejected: int = 0,
    error: str = "",
    retry_attempts: int = 0,
    action: str = "Manual investigation required",
) -> str:
    """Render the standard operational alert body (matches requirement 21)."""
    return (
        "CDC Pipeline Failure\n\n"
        f"Pipeline: {pipeline}\n"
        f"Environment: {environment}\n"
        f"Stage: {stage}\n\n"
        f"Records received: {records_received:,}\n"
        f"Processed: {processed:,}\n"
        f"Rejected: {rejected:,}\n\n"
        f"Error:\n{error}\n\n"
        f"Retry attempts: {retry_attempts}\n\n"
        f"Action:\n{action}\n"
    )


def publish(
    sns_client,
    topic_arn: str,
    subject: str,
    message: str,
) -> Optional[str]:
    """Publish an alert to SNS. Returns the MessageId (or None on empty topic).

    ``sns_client`` is a ``boto3.client('sns')`` (injected for testability).
    SNS subjects are limited to 100 chars, so we truncate defensively.
    """
    if not topic_arn:
        return None
    resp = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message=message,
    )
    return resp.get("MessageId")

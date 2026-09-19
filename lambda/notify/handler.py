"""Lambda: notify — publishes a critical SNS alert from a Step Functions catch.

Wired to the state machine's Catch block so any unrecovered failure (after
retries) produces a single, structured operational alert (requirement 21).
Successful runs do NOT invoke this — they are tracked via CloudWatch/DynamoDB.

Event (from the Catch):
    {
      "table_name": "orders", "batch_id": "...", "stage": "Silver Processing",
      "error": {"Error": "...", "Cause": "..."}
    }
"""

from __future__ import annotations

import os

import boto3

sns = boto3.client("sns")

TOPIC_ARN = os.environ["CDC_SNS_TOPIC_ARN"]
ENVIRONMENT = os.environ.get("CDC_ENVIRONMENT", "dev")


def handler(event, _context):
    table = event.get("table_name", "unknown")
    batch = event.get("batch_id", "unknown")
    stage = event.get("stage", "Pipeline")
    err = event.get("error", {})
    cause = err.get("Cause", "")
    error_name = err.get("Error", "ExecutionError")

    message = (
        "CDC Pipeline Failure\n\n"
        f"Pipeline: {table}-cdc\n"
        f"Environment: {ENVIRONMENT.upper()}\n"
        f"Stage: {stage}\n"
        f"Batch: {batch}\n\n"
        f"Error: {error_name}\n"
        f"Cause: {cause[:1500]}\n\n"
        "Action:\nRetries exhausted — manual investigation required.\n"
    )

    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=f"CDC Pipeline Failure: {table} [{ENVIRONMENT.upper()}]"[:100],
        Message=message,
    )
    return {"notified": True, "table_name": table, "batch_id": batch}

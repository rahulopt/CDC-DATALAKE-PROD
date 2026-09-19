"""Lambda: check_input — validates a batch before the pipeline processes it.

Invoked as the first Step Functions state. Given a table + Bronze prefix, it
confirms there is at least one object to process. If there is nothing, the state
machine can short-circuit to a benign "no data" success rather than launching an
(expensive) empty Glue run.

Event:
    {"table_name": "orders", "batch_id": "20260919_1800", "bronze_path": "s3://.../"}

Returns the event enriched with {"has_data": bool, "object_count": int}.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3

s3 = boto3.client("s3")


def handler(event, _context):
    bronze_path = event["bronze_path"]
    parsed = urlparse(bronze_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
    count = resp.get("KeyCount", 0)

    event["has_data"] = count > 0
    event["object_count"] = count
    event["environment"] = os.environ.get("CDC_ENVIRONMENT", "dev")
    return event

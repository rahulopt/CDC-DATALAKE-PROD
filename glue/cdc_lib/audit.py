"""DynamoDB audit records (requirement 19).

Every batch execution writes one audit item capturing counts and status so the
pipeline is fully observable and each batch's fate is queryable. The item shape
matches the requirement exactly.

The :class:`AuditRecord` dataclass is pure Python (unit-testable). :func:`write`
performs the actual DynamoDB ``put_item`` and is exercised with ``moto`` in
tests so no real AWS calls are needed.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Optional


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


@dataclass
class AuditRecord:
    batch_id: str
    table_name: str
    start_time: str = field(default_factory=_now_iso)
    end_time: Optional[str] = None
    source_records: int = 0
    processed_records: int = 0
    insert_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    duplicate_count: int = 0
    reject_count: int = 0
    status: str = "RUNNING"  # RUNNING | SUCCESS | FAILED
    error_message: Optional[str] = None

    def mark_success(self) -> "AuditRecord":
        self.status = "SUCCESS"
        self.end_time = _now_iso()
        return self

    def mark_failed(self, error_message: str) -> "AuditRecord":
        self.status = "FAILED"
        self.error_message = error_message[:2000]  # keep item small
        self.end_time = _now_iso()
        return self

    def to_dynamodb_item(self) -> dict:
        """Serialise to a DynamoDB-friendly dict (ints -> Decimal, drop Nones).

        DynamoDB's resource API rejects floats and ``None`` values, so integer
        counters become ``Decimal`` and empty optionals are omitted.
        """
        item = {}
        for key, value in asdict(self).items():
            if value is None:
                continue
            item[key] = Decimal(value) if isinstance(value, int) else value
        # composite-friendly sort key: table#start allows per-table history query
        item["batch_table"] = f"{self.batch_id}#{self.table_name}"
        return item


def write(dynamodb_resource, table_name: str, record: AuditRecord) -> dict:
    """Persist ``record`` to the DynamoDB audit table.

    ``dynamodb_resource`` is a ``boto3.resource('dynamodb')`` object (injected so
    tests can pass a moto-backed resource).
    """
    table = dynamodb_resource.Table(table_name)
    item = record.to_dynamodb_item()
    table.put_item(Item=item)
    return item

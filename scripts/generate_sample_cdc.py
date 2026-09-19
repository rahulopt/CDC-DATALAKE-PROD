#!/usr/bin/env python3
"""Generate sample DMS-style CDC records for local testing and Bronze seeding.

Produces newline-delimited JSON in the DMS *flat S3* shape (Op column + inline
data + control fields), which the pipeline's ``cdc_parser.parse`` auto-detects.
Deliberately includes the tricky scenarios the pipeline must handle:

  * INSERT / UPDATE / DELETE
  * duplicate events (same key + same commit ts delivered twice)
  * out-of-order events (later commit ts emitted before an earlier one)
  * an invalid record (bad status) that must be quarantined
  * a "festival spike" burst of many orders

Usage:
    python scripts/generate_sample_cdc.py --table orders --count 500 \
        --out ./sample_out/orders

Then upload to Bronze:
    aws s3 cp ./sample_out/orders s3://<bronze>/bronze/orders/... --recursive
"""

from __future__ import annotations

import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone

STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


def _ts(base: datetime, seconds: int) -> str:
    return (base + timedelta(seconds=seconds)).astimezone(timezone.utc).isoformat()


def gen_orders(count: int) -> list[dict]:
    base = datetime.now(timezone.utc) - timedelta(hours=1)
    records: list[dict] = []
    seq = 0

    for oid in range(1, count + 1):
        seq += 1
        cust = random.randint(1, max(1, count // 5))
        amount = round(random.uniform(10, 2000), 2)
        # INSERT (PLACED)
        records.append(_rec("I", oid, cust, amount, "PLACED", base, seq))

        # Some orders progress with UPDATEs.
        if oid % 2 == 0:
            seq += 1
            records.append(_rec("U", oid, cust, amount, "SHIPPED", base, seq))
            seq += 1
            records.append(_rec("U", oid, cust, amount, "DELIVERED", base, seq))

        # A few get cancelled + deleted.
        if oid % 17 == 0:
            seq += 1
            records.append(_rec("U", oid, cust, amount, "CANCELLED", base, seq))
            seq += 1
            records.append(_rec("D", oid, cust, amount, "CANCELLED", base, seq))

    # --- edge cases ---------------------------------------------------------
    if records:
        # duplicate delivery of the first record
        records.append(dict(records[0]))

        # out-of-order: an older UPDATE emitted after a newer one for a key
        seq += 1
        records.append(_rec("U", 1, 1, 99.0, "SHIPPED", base, 2))  # low seq/ts
        seq += 1
        records.append(_rec("U", 1, 1, 99.0, "DELIVERED", base, 999))  # newest

        # invalid record (bad status) -> must be quarantined
        records.append(_rec("I", count + 1, 1, 50.0, "TELEPORTED", base, seq + 1))

    # --- festival spike -----------------------------------------------------
    spike_base = base + timedelta(minutes=30)
    for i in range(count):
        seq += 1
        oid = count + 100 + i
        records.append(
            _rec(
                "I",
                oid,
                random.randint(1, 50),
                round(random.uniform(10, 500), 2),
                "PLACED",
                spike_base,
                seq,
            )
        )

    random.shuffle(records)  # simulate arrival disorder
    return records


def _rec(op, oid, cust, amount, status, base, seq) -> dict:
    return {
        "Op": op,
        "order_id": oid,
        "customer_id": cust,
        "order_amount": amount,
        "status": status,
        "order_date": _ts(base, seq),
        "commit_timestamp": _ts(base, seq),
        "transaction_id": f"tx{seq}",
        "seq": seq,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="orders", choices=["orders"])
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--out", default="./sample_out/orders")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    records = gen_orders(args.count)

    path = os.path.join(args.out, "part-000.json")
    with open(path, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")

    print(f"wrote {len(records)} CDC records -> {path}")
    print("includes: I/U/D, duplicates, out-of-order, 1 invalid, festival spike")


if __name__ == "__main__":
    main()

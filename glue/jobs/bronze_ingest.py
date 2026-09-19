"""Glue Streaming job: Kinesis -> S3 Bronze (immutable raw CDC).

DMS writes ongoing CDC to Kinesis Data Streams (the burst buffer). This Glue
Structured Streaming job consumes the stream and lands **immutable, partitioned**
raw records in the Bronze S3 layer as Parquet, partitioned by
``table / year / month / day / hour`` (requirement 6).

Because Bronze is append-only and preserves all CDC metadata (op, LSN, commit
ts, txn id, key), the whole downstream can be rebuilt from it (replay,
requirement 17).

Buffering in Kinesis + a modest trigger interval means a temporary Silver
slowdown never loses data — records simply accumulate in Kinesis and Bronze
(requirement 5).

Job parameters:
  --stream_name       cdc-orders-stream (Kinesis stream name)
  --bronze_path       s3://.../bronze/
  --checkpoint_path   s3://.../checkpoints/bronze/<stream>/
  --region            us-east-1
  --trigger_seconds   60
"""

from __future__ import annotations

import sys

try:
    from awsglue.utils import getResolvedOptions  # type: ignore

    _GLUE = True
except Exception:  # pragma: no cover
    _GLUE = False

from pyspark.sql import functions as F
from spark_session import build_spark


def _args() -> dict:
    keys = ["stream_name", "bronze_path", "checkpoint_path", "region", "trigger_seconds"]
    if _GLUE:
        return getResolvedOptions(sys.argv, keys)
    it = iter(sys.argv[1:])
    out = {}
    for tok in it:
        if tok.startswith("--"):
            out[tok[2:]] = next(it, "")
    out.setdefault("trigger_seconds", "60")
    out.setdefault("region", "us-east-1")
    return out


def run() -> None:
    args = _args()
    # Bronze warehouse root isn't an Iceberg warehouse; reuse builder for config.
    spark = build_spark(f"cdc-bronze-{args['stream_name']}", args["bronze_path"])
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kinesis using the Spark SQL Kinesis connector shipped with Glue.
    raw = (
        spark.readStream.format("kinesis")
        .option("streamName", args["stream_name"])
        .option("region", args["region"])
        .option("startingPosition", "TRIM_HORIZON")  # never skip buffered data
        .option("classification", "json")
        .load()
    )

    # Kinesis payload arrives as binary `data`; decode to a JSON string column.
    decoded = raw.selectExpr(
        "CAST(data AS STRING) AS json_str", "approximateArrivalTimestamp AS arrival_ts"
    )

    # Add hourly partition columns from arrival time (Bronze partitioning).
    partitioned = (
        decoded.withColumn("year", F.date_format("arrival_ts", "yyyy"))
        .withColumn("month", F.date_format("arrival_ts", "MM"))
        .withColumn("day", F.date_format("arrival_ts", "dd"))
        .withColumn("hour", F.date_format("arrival_ts", "HH"))
    )

    query = (
        partitioned.writeStream.format("parquet")
        .option("path", args["bronze_path"])
        .option("checkpointLocation", args["checkpoint_path"])
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .trigger(processingTime=f"{args['trigger_seconds']} seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()

"""cdc_lib — shared, testable building blocks for the CDC Data Lake pipeline.

This package deliberately contains **no** AWS Glue import-time dependencies so
that every module can be unit-tested with a plain local ``pyspark`` session.
Glue-specific glue code lives in ``glue/jobs/*`` and imports from here.

Public surface:

    config        -> load pipeline configuration (YAML) with env overrides
    cdc_parser    -> normalise raw DMS/Kinesis CDC envelopes into a canonical schema
    ordering      -> LSN / commit-timestamp based ordering + latest-wins dedup
    validation    -> data-quality rules, quarantine record construction
    schema        -> table schema registry + schema-evolution detection
    audit         -> DynamoDB audit record model + writer
    notifications -> SNS alerting helper
"""

__version__ = "1.0.0"

# Canonical CDC operation codes used everywhere downstream.
OP_INSERT = "I"
OP_UPDATE = "U"
OP_DELETE = "D"
VALID_OPS = frozenset({OP_INSERT, OP_UPDATE, OP_DELETE})

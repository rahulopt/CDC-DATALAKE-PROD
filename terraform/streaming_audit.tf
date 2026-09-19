# ---------------------------------------------------------------------------
# Kinesis Data Streams — burst buffer between DMS and processing (req 5, 26).
# PROVISIONED with configurable shards so capacity is planned (req 28) yet can
# be scaled up for festival traffic; retention gives a replay/slowdown buffer.
# ---------------------------------------------------------------------------
resource "aws_kinesis_stream" "cdc" {
  name             = "${local.name_prefix}-cdc-stream"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.cdc.arn

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  shard_level_metrics = [
    "IncomingRecords",
    "OutgoingRecords",
    "IteratorAgeMilliseconds",
    "ReadProvisionedThroughputExceeded",
    "WriteProvisionedThroughputExceeded",
  ]
}

# ---------------------------------------------------------------------------
# DynamoDB — pipeline execution audit (requirement 19). On-demand billing so we
# pay only for batches processed (cost, req 28). PK = batch_table (batch#table).
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "audit" {
  name         = "${local.name_prefix}-audit"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "batch_table"

  attribute {
    name = "batch_table"
    type = "S"
  }

  # GSI to query all batches for a given table by recency.
  attribute {
    name = "table_name"
    type = "S"
  }
  attribute {
    name = "start_time"
    type = "S"
  }
  global_secondary_index {
    name            = "by_table"
    hash_key        = "table_name"
    range_key       = "start_time"
    projection_type = "ALL"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.cdc.arn
  }

  point_in_time_recovery {
    enabled = true
  }
}

# ---------------------------------------------------------------------------
# SNS — critical operational alerts only (requirement 21).
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name              = "${local.name_prefix}-alerts"
  kms_master_key_id = aws_kms_key.cdc.id
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---------------------------------------------------------------------------
# Secrets Manager — Aurora credentials (requirement 22). The secret VALUE is
# never stored in git; Terraform only creates the container. Populate via:
#   aws secretsmanager put-secret-value --secret-id <name> --secret-string ...
# ---------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "aurora" {
  name       = var.aurora_secret_name
  kms_key_id = aws_kms_key.cdc.arn
}

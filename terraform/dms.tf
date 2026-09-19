# ---------------------------------------------------------------------------
# AWS DMS — full load to S3 (Bronze) + ongoing CDC to Kinesis (requirement 4).
#
# NOTE: DMS requires networking (subnet group, security group) and the source
# Aurora cluster to exist. Those are environment-specific; supply the Aurora
# connection details via the Secrets Manager secret and the variables below.
# The replication *instance* here is modest and can be resized for festival
# load; DMS itself does not need to run at peak capacity permanently (req 26).
# ---------------------------------------------------------------------------

variable "dms_subnet_ids" {
  description = "Subnet IDs (>=2 AZs) for the DMS replication subnet group. Empty disables DMS."
  type        = list(string)
  default     = []
}

variable "dms_security_group_ids" {
  description = "Security group IDs for the DMS replication instance."
  type        = list(string)
  default     = []
}

variable "aurora_host" {
  description = "Aurora PostgreSQL writer endpoint (source)."
  type        = string
  default     = ""
}

variable "aurora_port" {
  description = "Aurora PostgreSQL port."
  type        = number
  default     = 5432
}

variable "aurora_db_name" {
  description = "Aurora source database name."
  type        = string
  default     = "ecommerce"
}

locals {
  dms_enabled = length(var.dms_subnet_ids) > 0 && var.aurora_host != ""
}

resource "aws_dms_replication_subnet_group" "this" {
  count                                = local.dms_enabled ? 1 : 0
  replication_subnet_group_id          = "${local.name_prefix}-dms-subnets"
  replication_subnet_group_description = "CDC DMS subnet group"
  subnet_ids                           = var.dms_subnet_ids
}

resource "aws_dms_replication_instance" "this" {
  count                       = local.dms_enabled ? 1 : 0
  replication_instance_id     = "${local.name_prefix}-dms"
  replication_instance_class  = "dms.t3.medium"
  allocated_storage           = 50
  engine_version              = "3.5.2"
  publicly_accessible         = false
  multi_az                    = var.environment == "prod"
  kms_key_arn                 = aws_kms_key.cdc.arn
  replication_subnet_group_id = aws_dms_replication_subnet_group.this[0].id
  vpc_security_group_ids      = var.dms_security_group_ids
}

# Source endpoint: Aurora PostgreSQL. Credentials pulled from Secrets Manager.
resource "aws_dms_endpoint" "source" {
  count                           = local.dms_enabled ? 1 : 0
  endpoint_id                     = "${local.name_prefix}-aurora-src"
  endpoint_type                   = "source"
  engine_name                     = "aurora-postgresql"
  server_name                     = var.aurora_host
  port                            = var.aurora_port
  database_name                   = var.aurora_db_name
  secrets_manager_arn             = aws_secretsmanager_secret.aurora.arn
  secrets_manager_access_role_arn = aws_iam_role.dms_target.arn
  kms_key_arn                     = aws_kms_key.cdc.arn
}

# Target endpoint: S3 (full load, Bronze).
resource "aws_dms_s3_endpoint" "bronze" {
  count                             = local.dms_enabled ? 1 : 0
  endpoint_id                       = "${local.name_prefix}-s3-bronze"
  endpoint_type                     = "target"
  service_access_role_arn           = aws_iam_role.dms_target.arn
  bucket_name                       = aws_s3_bucket.this["bronze"].id
  bucket_folder                     = "fullload"
  data_format                       = "parquet"
  compression_type                  = "GZIP"
  include_op_for_full_load          = true
  timestamp_column_name             = "commit_timestamp"
  cdc_inserts_and_updates           = true
  add_column_name                   = true
  encryption_mode                   = "SSE_KMS"
  server_side_encryption_kms_key_id = aws_kms_key.cdc.arn
}

# Target endpoint: Kinesis (ongoing CDC).
resource "aws_dms_endpoint" "kinesis" {
  count         = local.dms_enabled ? 1 : 0
  endpoint_id   = "${local.name_prefix}-kinesis-tgt"
  endpoint_type = "target"
  engine_name   = "kinesis"

  kinesis_settings {
    stream_arn                     = aws_kinesis_stream.cdc.arn
    message_format                 = "json"
    service_access_role_arn        = aws_iam_role.dms_target.arn
    include_transaction_details    = true
    include_partition_value        = true
    partition_include_schema_table = true
    include_control_details        = true
    include_null_and_empty         = true
  }
}

# Table mapping: capture all five e-commerce tables from the public schema.
locals {
  dms_table_mappings = jsonencode({
    rules = [
      {
        rule-type = "selection"
        rule-id   = "1"
        rule-name = "select-ecommerce"
        object-locator = {
          schema-name = "public"
          table-name  = "%"
        }
        rule-action = "include"
      }
    ]
  })
}

# Full-load task -> S3 Bronze.
resource "aws_dms_replication_task" "fullload" {
  count                    = local.dms_enabled ? 1 : 0
  replication_task_id      = "${local.name_prefix}-fullload"
  migration_type           = "full-load"
  replication_instance_arn = aws_dms_replication_instance.this[0].replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.source[0].endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.bronze[0].endpoint_arn
  table_mappings           = local.dms_table_mappings
}

# Ongoing CDC task -> Kinesis.
resource "aws_dms_replication_task" "cdc" {
  count                    = local.dms_enabled ? 1 : 0
  replication_task_id      = "${local.name_prefix}-cdc"
  migration_type           = "cdc"
  replication_instance_arn = aws_dms_replication_instance.this[0].replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.source[0].endpoint_arn
  target_endpoint_arn      = aws_dms_endpoint.kinesis[0].endpoint_arn
  table_mappings           = local.dms_table_mappings
}

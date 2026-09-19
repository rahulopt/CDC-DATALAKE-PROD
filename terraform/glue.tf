# ---------------------------------------------------------------------------
# Glue Data Catalog database for Silver/Gold Iceberg tables.
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "silver" {
  name = replace("${local.name_prefix}_silver", "-", "_")
}

# ---------------------------------------------------------------------------
# Upload Glue scripts + shared library + config to the scripts bucket so jobs
# can reference them. The cdc_lib package is zipped and passed via
# --extra-py-files.
# ---------------------------------------------------------------------------
resource "aws_s3_object" "job_silver" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "jobs/silver_cdc_merge.py"
  source = "${path.module}/../glue/jobs/silver_cdc_merge.py"
  etag   = filemd5("${path.module}/../glue/jobs/silver_cdc_merge.py")
}

resource "aws_s3_object" "job_bronze" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "jobs/bronze_ingest.py"
  source = "${path.module}/../glue/jobs/bronze_ingest.py"
  etag   = filemd5("${path.module}/../glue/jobs/bronze_ingest.py")
}

resource "aws_s3_object" "job_gold" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "jobs/gold_aggregates.py"
  source = "${path.module}/../glue/jobs/gold_aggregates.py"
  etag   = filemd5("${path.module}/../glue/jobs/gold_aggregates.py")
}

resource "aws_s3_object" "job_maint" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "jobs/iceberg_maintenance.py"
  source = "${path.module}/../glue/jobs/iceberg_maintenance.py"
  etag   = filemd5("${path.module}/../glue/jobs/iceberg_maintenance.py")
}

resource "aws_s3_object" "job_spark_session" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "jobs/spark_session.py"
  source = "${path.module}/../glue/jobs/spark_session.py"
  etag   = filemd5("${path.module}/../glue/jobs/spark_session.py")
}

resource "aws_s3_object" "config" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "config/pipeline.yaml"
  source = "${path.module}/../config/pipeline.yaml"
  etag   = filemd5("${path.module}/../config/pipeline.yaml")
}

# The shared library packaged as a zip (built by the Makefile / CI before apply).
resource "aws_s3_object" "cdc_lib" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "lib/cdc_lib.zip"
  source = "${path.module}/../dist/cdc_lib.zip"
  etag   = filemd5("${path.module}/../dist/cdc_lib.zip")
}

locals {
  scripts_bucket = aws_s3_bucket.this["scripts"].id
  extra_py_files = "s3://${local.scripts_bucket}/lib/cdc_lib.zip,s3://${local.scripts_bucket}/jobs/spark_session.py"

  # Common env vars supplied to jobs; the config loader reads these to override
  # the placeholder YAML values (config.py::_apply_env_overrides).
  common_conf_env = {
    "--CDC_ENVIRONMENT"      = var.environment
    "--CDC_BRONZE_BUCKET"    = "s3://${aws_s3_bucket.this["bronze"].id}"
    "--CDC_SILVER_WAREHOUSE" = "s3://${aws_s3_bucket.this["silver"].id}/silver"
    "--CDC_REJECT_PREFIX"    = "s3://${aws_s3_bucket.this["reject"].id}/reject"
    "--CDC_GLUE_DATABASE"    = aws_glue_catalog_database.silver.name
    "--CDC_AUDIT_TABLE"      = aws_dynamodb_table.audit.name
    "--CDC_SNS_TOPIC_ARN"    = aws_sns_topic.alerts.arn
  }
}

# ---------------------------------------------------------------------------
# Silver CDC MERGE job (per-batch, per-table). Iceberg enabled via
# --datalake-formats. Bookmarks off (idempotency handled in-job).
# ---------------------------------------------------------------------------
resource "aws_glue_job" "silver" {
  name              = "${local.name_prefix}-silver-cdc-merge"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${local.scripts_bucket}/jobs/silver_cdc_merge.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_conf_env, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--extra-py-files"                   = local.extra_py_files
    "--config_path"                      = "s3://${local.scripts_bucket}/config/pipeline.yaml"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-observability-metrics"     = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = "s3://${local.scripts_bucket}/tmp/"
  })
}

# ---------------------------------------------------------------------------
# Bronze streaming job (Kinesis -> S3). Long-running streaming job.
# ---------------------------------------------------------------------------
resource "aws_glue_job" "bronze" {
  name              = "${local.name_prefix}-bronze-ingest"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  command {
    name            = "gluestreaming"
    script_location = "s3://${local.scripts_bucket}/jobs/bronze_ingest.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_conf_env, {
    "--job-language"                     = "python"
    "--extra-py-files"                   = local.extra_py_files
    "--stream_name"                      = aws_kinesis_stream.cdc.name
    "--bronze_path"                      = "s3://${aws_s3_bucket.this["bronze"].id}/bronze/"
    "--checkpoint_path"                  = "s3://${aws_s3_bucket.this["bronze"].id}/_checkpoints/bronze/"
    "--region"                           = var.aws_region
    "--trigger_seconds"                  = "60"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  })
}

# ---------------------------------------------------------------------------
# Gold aggregates job.
# ---------------------------------------------------------------------------
resource "aws_glue_job" "gold" {
  name              = "${local.name_prefix}-gold-aggregates"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${local.scripts_bucket}/jobs/gold_aggregates.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_conf_env, {
    "--job-language"        = "python"
    "--datalake-formats"    = "iceberg"
    "--extra-py-files"      = local.extra_py_files
    "--config_path"         = "s3://${local.scripts_bucket}/config/pipeline.yaml"
    "--enable-metrics"      = "true"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--TempDir"             = "s3://${local.scripts_bucket}/tmp/"
  })
}

# ---------------------------------------------------------------------------
# Iceberg maintenance job (scheduled nightly by EventBridge).
# ---------------------------------------------------------------------------
resource "aws_glue_job" "maintenance" {
  name              = "${local.name_prefix}-iceberg-maintenance"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${local.scripts_bucket}/jobs/iceberg_maintenance.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_conf_env, {
    "--job-language"        = "python"
    "--datalake-formats"    = "iceberg"
    "--extra-py-files"      = local.extra_py_files
    "--config_path"         = "s3://${local.scripts_bucket}/config/pipeline.yaml"
    "--retention_days"      = "7"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--TempDir"             = "s3://${local.scripts_bucket}/tmp/"
  })
}

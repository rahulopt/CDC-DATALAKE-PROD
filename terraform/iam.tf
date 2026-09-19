# ---------------------------------------------------------------------------
# IAM — least-privilege roles for Glue, DMS, Step Functions, EventBridge
# (requirement 22). Each role is scoped to the specific ARNs it needs.
# ---------------------------------------------------------------------------

locals {
  bucket_arns = { for k, b in aws_s3_bucket.this : k => b.arn }
}

# ===========================================================================
# Glue job role
# ===========================================================================
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue" {
  # S3: read Bronze + scripts, read/write Silver + Reject.
  statement {
    sid     = "S3ReadBronzeScripts"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      local.bucket_arns["bronze"], "${local.bucket_arns["bronze"]}/*",
      local.bucket_arns["scripts"], "${local.bucket_arns["scripts"]}/*",
    ]
  }
  statement {
    sid = "S3ReadWriteSilverReject"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
    ]
    resources = [
      local.bucket_arns["silver"], "${local.bucket_arns["silver"]}/*",
      local.bucket_arns["reject"], "${local.bucket_arns["reject"]}/*",
    ]
  }
  # Kinesis: read the CDC stream (bronze streaming job).
  statement {
    sid = "KinesisRead"
    actions = [
      "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:SubscribeToShard",
    ]
    resources = [aws_kinesis_stream.cdc.arn]
  }
  # Glue Data Catalog for Iceberg metadata.
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
      "glue:DeleteTable", "glue:BatchCreatePartition", "glue:GetPartition",
      "glue:GetPartitions", "glue:BatchGetPartition", "glue:UpdatePartition",
      "glue:CreatePartition",
    ]
    resources = ["*"]
  }
  # DynamoDB audit writes.
  statement {
    sid       = "DynamoAudit"
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.audit.arn, "${aws_dynamodb_table.audit.arn}/index/*"]
  }
  # SNS publish for alerts.
  statement {
    sid       = "SnsPublish"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  # KMS use for all the above encrypted resources.
  statement {
    sid       = "KmsUse"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [aws_kms_key.cdc.arn]
  }
  # CloudWatch Logs + custom metrics.
  statement {
    sid = "Observability"
    actions = [
      "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
      "cloudwatch:PutMetricData",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "${local.name_prefix}-glue-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

# ===========================================================================
# DMS role (VPC + S3 target + Kinesis target). DMS also needs the standard
# service roles dms-vpc-role / dms-cloudwatch-logs-role at the account level.
# ===========================================================================
data "aws_iam_policy_document" "dms_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dms.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dms_target" {
  name               = "${local.name_prefix}-dms-target-role"
  assume_role_policy = data.aws_iam_policy_document.dms_assume.json
}

data "aws_iam_policy_document" "dms_target" {
  statement {
    sid = "S3FullLoadTarget"
    actions = [
      "s3:PutObject", "s3:DeleteObject", "s3:GetObject", "s3:ListBucket",
      "s3:PutObjectTagging",
    ]
    resources = [
      local.bucket_arns["bronze"], "${local.bucket_arns["bronze"]}/*",
    ]
  }
  statement {
    sid = "KinesisTarget"
    actions = [
      "kinesis:PutRecord", "kinesis:PutRecords", "kinesis:DescribeStream",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.cdc.arn]
  }
  statement {
    sid       = "KmsUse"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [aws_kms_key.cdc.arn]
  }
  statement {
    sid       = "SecretsRead"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.aurora.arn]
  }
}

resource "aws_iam_role_policy" "dms_target" {
  name   = "${local.name_prefix}-dms-target-policy"
  role   = aws_iam_role.dms_target.id
  policy = data.aws_iam_policy_document.dms_target.json
}

# ===========================================================================
# Step Functions role — start/monitor Glue jobs, invoke Lambda, publish SNS,
# write DynamoDB.
# ===========================================================================
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${local.name_prefix}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid = "GlueControl"
    actions = [
      "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }
  statement {
    sid       = "LambdaInvoke"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.check_input.arn, aws_lambda_function.notify.arn]
  }
  statement {
    sid       = "SnsPublish"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    sid       = "DynamoAudit"
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem"]
    resources = [aws_dynamodb_table.audit.arn]
  }
  statement {
    sid       = "KmsUse"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.cdc.arn]
  }
  # Managed-rule permissions Step Functions needs for .sync Glue integration.
  statement {
    sid       = "Events"
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.aws_region}:${local.account_id}:rule/StepFunctionsGetEventsForGlueJobRule"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "${local.name_prefix}-sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

# ===========================================================================
# EventBridge role — start the Step Functions state machine on schedule.
# ===========================================================================
data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "${local.name_prefix}-events-role"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

resource "aws_iam_role_policy" "events" {
  name = "${local.name_prefix}-events-policy"
  role = aws_iam_role.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.cdc.arn
    }]
  })
}

# ===========================================================================
# Lambda execution role (check_input + notify helpers).
# ===========================================================================
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.name_prefix}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetObject"]
    resources = [local.bucket_arns["bronze"], "${local.bucket_arns["bronze"]}/*"]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.cdc.arn]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${local.name_prefix}-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

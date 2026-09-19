# ---------------------------------------------------------------------------
# Lambda functions: check_input + notify (requirement 14, 21).
# ---------------------------------------------------------------------------
data "archive_file" "check_input" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/check_input"
  output_path = "${path.module}/../dist/check_input.zip"
}

data "archive_file" "notify" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/notify"
  output_path = "${path.module}/../dist/notify.zip"
}

resource "aws_lambda_function" "check_input" {
  function_name    = "${local.name_prefix}-check-input"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.handler"
  runtime          = "python3.11"
  timeout          = 30
  filename         = data.archive_file.check_input.output_path
  source_code_hash = data.archive_file.check_input.output_base64sha256

  environment {
    variables = {
      CDC_ENVIRONMENT = var.environment
    }
  }
}

resource "aws_lambda_function" "notify" {
  function_name    = "${local.name_prefix}-notify"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.handler"
  runtime          = "python3.11"
  timeout          = 30
  filename         = data.archive_file.notify.output_path
  source_code_hash = data.archive_file.notify.output_base64sha256

  environment {
    variables = {
      CDC_ENVIRONMENT   = var.environment
      CDC_SNS_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }
}

# ---------------------------------------------------------------------------
# CloudWatch Logs group for the state machine (retention = cost control).
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-cdc"
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.cdc.arn
}

# ---------------------------------------------------------------------------
# Step Functions state machine (requirement 14).
# ---------------------------------------------------------------------------
resource "aws_sfn_state_machine" "cdc" {
  name     = "${local.name_prefix}-cdc"
  role_arn = aws_iam_role.sfn.arn

  definition = templatefile("${path.module}/../orchestration/state_machine.asl.json", {
    check_input_arn = aws_lambda_function.check_input.arn
    notify_arn      = aws_lambda_function.notify.arn
    silver_job_name = aws_glue_job.silver.name
    gold_job_name   = aws_glue_job.gold.name
    bronze_bucket   = aws_s3_bucket.this["bronze"].id
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }
}

# ---------------------------------------------------------------------------
# EventBridge — trigger the pipeline on a schedule (requirement 14). The rule
# passes the list of tables + a batch_id derived from the schedule time.
# Every 15 minutes aligns with the ~15-min Silver SLA (requirement 2).
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "schedule" {
  name                = "${local.name_prefix}-cdc-schedule"
  description         = "Trigger CDC pipeline for the Silver SLA window"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "sfn" {
  rule     = aws_cloudwatch_event_rule.schedule.name
  arn      = aws_sfn_state_machine.cdc.arn
  role_arn = aws_iam_role.events.arn

  input = jsonencode({
    tables   = var.source_tables
    batch_id = "scheduled"
  })
}

# ---------------------------------------------------------------------------
# EventBridge — nightly Iceberg maintenance (requirement 27, 28).
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "maintenance" {
  name                = "${local.name_prefix}-maintenance-schedule"
  description         = "Nightly Iceberg compaction / snapshot expiry / orphan cleanup"
  schedule_expression = "cron(0 3 * * ? *)"
}

resource "aws_cloudwatch_event_target" "maintenance" {
  rule     = aws_cloudwatch_event_rule.maintenance.name
  arn      = "arn:aws:glue:${var.aws_region}:${local.account_id}:job/${aws_glue_job.maintenance.name}"
  role_arn = aws_iam_role.events.arn
}

# Allow EventBridge to start the maintenance Glue job.
resource "aws_iam_role_policy" "events_glue" {
  name = "${local.name_prefix}-events-glue-policy"
  role = aws_iam_role.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "glue:StartJobRun"
      Resource = "arn:aws:glue:${var.aws_region}:${local.account_id}:job/${aws_glue_job.maintenance.name}"
    }]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch monitoring + alarms (requirement 20, 21). Alarms notify SNS.
# ---------------------------------------------------------------------------

# High Kinesis consumer lag -> processing can't keep up with festival traffic.
resource "aws_cloudwatch_metric_alarm" "kinesis_lag" {
  alarm_name          = "${local.name_prefix}-kinesis-high-lag"
  alarm_description   = "Kinesis GetRecords iterator age is high — consumer lag / risk of SLA breach."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 900000 # 15 minutes in ms == Silver SLA
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions    = { StreamName = aws_kinesis_stream.cdc.name }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# Kinesis write throttling -> shard capacity too low for the burst.
resource "aws_cloudwatch_metric_alarm" "kinesis_write_throttle" {
  alarm_name          = "${local.name_prefix}-kinesis-write-throttle"
  alarm_description   = "Kinesis write throughput exceeded — consider adding shards."
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions    = { StreamName = aws_kinesis_stream.cdc.name }
  alarm_actions = [aws_sns_topic.alerts.arn]
}

# Silver Glue job failures.
resource "aws_cloudwatch_metric_alarm" "glue_silver_failed" {
  alarm_name          = "${local.name_prefix}-silver-job-failed"
  alarm_description   = "Silver CDC MERGE Glue job failed."
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = aws_glue_job.silver.name
    Type    = "gauge"
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
}

# Step Functions execution failures.
resource "aws_cloudwatch_metric_alarm" "sfn_failed" {
  alarm_name          = "${local.name_prefix}-sfn-failed"
  alarm_description   = "CDC Step Functions execution failed."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions    = { StateMachineArn = aws_sfn_state_machine.cdc.arn }
  alarm_actions = [aws_sns_topic.alerts.arn]
}

# ---------------------------------------------------------------------------
# Dashboard — infra + pipeline health at a glance (requirement 20).
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_dashboard" "cdc" {
  dashboard_name = "${local.name_prefix}-cdc"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          title  = "Kinesis throughput & lag"
          region = var.aws_region
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", aws_kinesis_stream.cdc.name],
            [".", "GetRecords.IteratorAgeMilliseconds", ".", "."]
          ]
          view = "timeSeries"
        }
      },
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          title  = "Step Functions executions"
          region = var.aws_region
          metrics = [
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.cdc.arn],
            [".", "ExecutionsFailed", ".", "."],
            [".", "ExecutionTime", ".", "."]
          ]
          view = "timeSeries"
        }
      }
    ]
  })
}

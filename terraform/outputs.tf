output "kms_key_arn" {
  description = "CDC data lake KMS key ARN."
  value       = aws_kms_key.cdc.arn
}

output "bronze_bucket" {
  value = aws_s3_bucket.this["bronze"].id
}

output "silver_bucket" {
  value = aws_s3_bucket.this["silver"].id
}

output "reject_bucket" {
  value = aws_s3_bucket.this["reject"].id
}

output "scripts_bucket" {
  value = aws_s3_bucket.this["scripts"].id
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.cdc.name
}

output "audit_table_name" {
  value = aws_dynamodb_table.audit.name
}

output "sns_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "glue_database" {
  value = aws_glue_catalog_database.silver.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.cdc.arn
}

output "silver_job_name" {
  value = aws_glue_job.silver.name
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.cdc.dashboard_name
}

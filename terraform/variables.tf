variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name, used as a resource name prefix."
  type        = string
  default     = "cdc-lake"
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of dev, staging, prod."
  }
}

variable "alert_email" {
  description = "Email address subscribed to the SNS alerts topic. Leave empty to skip subscription."
  type        = string
  default     = ""
}

variable "kinesis_shard_count" {
  description = "Initial Kinesis shard count (scale independently of processing)."
  type        = number
  default     = 2
}

variable "kinesis_retention_hours" {
  description = "Kinesis stream retention (hours). Larger = more replay buffer during slowdowns."
  type        = number
  default     = 48
}

variable "glue_worker_type" {
  description = "Glue worker type for CDC jobs."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers (Spark scaling)."
  type        = number
  default     = 3
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention (cost control)."
  type        = number
  default     = 30
}

variable "bronze_expire_days" {
  description = "S3 lifecycle: transition/expire Bronze raw data after N days (replay window)."
  type        = number
  default     = 90
}

variable "source_tables" {
  description = "Logical source tables processed by the pipeline."
  type        = list(string)
  default     = ["orders", "customers", "products", "order_items", "payments"]
}

variable "aurora_secret_name" {
  description = "Secrets Manager secret name holding Aurora credentials (created outside or here)."
  type        = string
  default     = "cdc/aurora/credentials"
}

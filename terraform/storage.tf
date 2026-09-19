# ---------------------------------------------------------------------------
# KMS — single customer-managed key encrypting S3, Kinesis, DynamoDB, SNS,
# CloudWatch Logs and Secrets Manager (requirement 22).
# ---------------------------------------------------------------------------
resource "aws_kms_key" "cdc" {
  description             = "${local.name_prefix} CDC data lake encryption key"
  deletion_window_in_days = 14
  enable_key_rotation     = true

  # Allow the account root (IAM) to manage, and AWS services used by the
  # pipeline to use the key for encrypt/decrypt via service principals.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableRootAccountAdmin"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid    = "AllowServiceUse"
        Effect = "Allow"
        Principal = {
          Service = [
            "logs.${var.aws_region}.amazonaws.com",
            "s3.amazonaws.com",
            "kinesis.amazonaws.com",
            "dynamodb.amazonaws.com",
            "sns.amazonaws.com",
            "secretsmanager.amazonaws.com",
            "dms.amazonaws.com",
            "glue.amazonaws.com"
          ]
        }
        Action = [
          "kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*",
          "kms:GenerateDataKey*", "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "cdc" {
  name          = "alias/${local.name_prefix}-cdc"
  target_key_id = aws_kms_key.cdc.key_id
}

# ---------------------------------------------------------------------------
# S3 buckets: bronze (raw CDC), silver (Iceberg warehouse), reject (quarantine),
# scripts (Glue code + assets). All encrypted with KMS, all public access
# blocked, versioning on (requirement 6, 22).
# ---------------------------------------------------------------------------
locals {
  buckets = {
    bronze  = "${local.name_prefix}-bronze"
    silver  = "${local.name_prefix}-silver"
    reject  = "${local.name_prefix}-reject"
    scripts = "${local.name_prefix}-scripts"
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets
  bucket   = "${each.value}-${local.account_id}"
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.cdc.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Bronze lifecycle: keep a replay window, then transition to cheaper storage
# and expire (requirement 28 cost control; requirement 17 replay window).
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.this["bronze"].id

  rule {
    id     = "bronze-tiering"
    status = "Enabled"

    # Apply to all objects in the bucket.
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 60
      storage_class = "GLACIER"
    }
    expiration {
      days = var.bronze_expire_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 14
    }
  }
}

# Reject bucket: keep quarantined records 30 days for triage then expire.
resource "aws_s3_bucket_lifecycle_configuration" "reject" {
  bucket = aws_s3_bucket.this["reject"].id
  rule {
    id     = "reject-expire"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
  }
}

# Example Terraform variables. Copy to terraform.tfvars and edit.
# NEVER put secrets (DB passwords, keys) here — those live in Secrets Manager.

aws_region  = "us-east-1"
project     = "cdc-lake"
environment = "dev"

# Operational alerts destination (confirm the SNS subscription email after apply).
alert_email = "data-oncall@example.com"

# Kinesis capacity planning (scale for festival traffic).
kinesis_shard_count     = 2
kinesis_retention_hours = 48

# Glue sizing.
glue_worker_type       = "G.1X"
glue_number_of_workers = 3

# Cost controls.
log_retention_days = 30
bronze_expire_days = 90

# ---- DMS / Aurora (leave empty to skip DMS resources on first bring-up) ----
# dms_subnet_ids         = ["subnet-aaa", "subnet-bbb"]
# dms_security_group_ids = ["sg-xxxx"]
# aurora_host            = "ecommerce.cluster-xxxx.us-east-1.rds.amazonaws.com"
# aurora_port            = 5432
# aurora_db_name         = "ecommerce"
# aurora_secret_name     = "cdc/aurora/credentials"

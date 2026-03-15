# =============================================================================
# Variables for AWS Glue Catalog Federation
# =============================================================================

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "ap-northeast-1"
}

variable "snowflake_account" {
  description = "Snowflake account identifier (format: account.region.cloud)"
  type        = string
}

variable "snowflake_database" {
  description = "Snowflake database name containing Iceberg tables"
  type        = string
  default     = "ICEBERG_DEMO_DB"
}

variable "s3_bucket" {
  description = "S3 bucket containing Iceberg data"
  type        = string
}

variable "s3_prefix" {
  description = "S3 prefix for Iceberg data"
  type        = string
  default     = "iceberg-demo"
}

variable "oauth_token" {
  description = "OAuth token for Snowflake Horizon API (sensitive)"
  type        = string
  sensitive   = true
}

variable "secret_name" {
  description = "Name for the Secrets Manager secret"
  type        = string
  default     = "horizon-catalog-token"
}

variable "connection_name" {
  description = "Name for the Glue connection"
  type        = string
  default     = "snowflake-horizon-connection"
}

variable "catalog_name" {
  description = "Name for the federated catalog"
  type        = string
  default     = "snowflake_iceberg_catalog"
}

# =============================================================================
# Terraform Configuration for AWS Glue Catalog Federation with Snowflake
# =============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# -----------------------------------------------------------------------------
# Data Sources
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Secrets Manager - Store OAuth Token
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "horizon_token" {
  name        = var.secret_name
  description = "Snowflake Horizon Catalog OAuth Token for Glue Federation"
}

resource "aws_secretsmanager_secret_version" "horizon_token" {
  secret_id = aws_secretsmanager_secret.horizon_token.id
  secret_string = jsonencode({
    BEARER_TOKEN = var.oauth_token
  })
}

# -----------------------------------------------------------------------------
# IAM Role for Lake Formation Data Access
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lf_data_access" {
  name = "LFDataAccessRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com",
            "lakeformation.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lf_s3_access" {
  name = "S3AccessPolicy"
  role = aws_iam_role.lf_data_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/${var.s3_prefix}/*"
        ]
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# IAM Role for Glue Connection
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_connection" {
  name = "GlueSnowflakeFederationRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_secrets_access" {
  name = "SecretsManagerAccessPolicy"
  role = aws_iam_role.glue_connection.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = aws_secretsmanager_secret.horizon_token.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_connection.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# -----------------------------------------------------------------------------
# Lake Formation - Register S3 Location
# -----------------------------------------------------------------------------

resource "aws_lakeformation_resource" "iceberg_data" {
  arn      = "arn:aws:s3:::${var.s3_bucket}"
  role_arn = aws_iam_role.lf_data_access.arn

  use_service_linked_role = false
}

# -----------------------------------------------------------------------------
# Glue Connection
# -----------------------------------------------------------------------------

resource "aws_glue_connection" "snowflake_horizon" {
  name = var.connection_name

  connection_type = "CUSTOM"

  connection_properties = {
    CONNECTOR_CLASS_NAME = "SNOWFLAKEICEBERGRESTCATALOG"
    INSTANCE_URL         = "https://${var.snowflake_account}.snowflakecomputing.com"
    ROLE_ARN            = aws_iam_role.lf_data_access.arn
  }

  physical_connection_requirements {
    availability_zone      = null
    security_group_id_list = []
    subnet_id              = null
  }
}

# -----------------------------------------------------------------------------
# Glue Catalog (Federated)
# Note: As of early 2024, aws_glue_catalog resource for federated catalogs
# may require aws provider updates. Use AWS CLI as fallback if needed.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Athena Workgroup
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "federation_demo" {
  name = "snowflake-federation-demo"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.s3_bucket}/athena-results/"
    }
  }
}

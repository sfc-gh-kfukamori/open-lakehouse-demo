# =============================================================================
# Outputs for AWS Glue Catalog Federation
# =============================================================================

output "lf_role_arn" {
  description = "Lake Formation data access role ARN"
  value       = aws_iam_role.lf_data_access.arn
}

output "glue_role_arn" {
  description = "Glue connection role ARN"
  value       = aws_iam_role.glue_connection.arn
}

output "secret_arn" {
  description = "Secrets Manager secret ARN"
  value       = aws_secretsmanager_secret.horizon_token.arn
}

output "connection_name" {
  description = "Glue connection name"
  value       = aws_glue_connection.snowflake_horizon.name
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.federation_demo.name
}

output "sample_athena_query" {
  description = "Sample Athena query to test the federation"
  value       = "SELECT * FROM ${var.catalog_name}.PUBLIC.SALES_DATA LIMIT 10;"
}

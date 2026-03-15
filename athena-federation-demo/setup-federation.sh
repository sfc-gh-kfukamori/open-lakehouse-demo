#!/bin/bash
# =============================================================================
# AWS Glue Catalog Federation Setup for Snowflake Horizon
# =============================================================================

set -e

# Configuration - Update these values
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-YOUR_AWS_ACCOUNT_ID}"
AWS_REGION="${AWS_REGION:-ap-northeast-1}"
SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-YOUR_ACCOUNT.YOUR_REGION.aws}"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-ICEBERG_DEMO_DB}"
S3_BUCKET="${S3_BUCKET:-YOUR_S3_BUCKET}"
S3_PREFIX="${S3_PREFIX:-iceberg-demo}"

# Resource names
SECRET_NAME="horizon-catalog-token"
CONNECTION_NAME="snowflake-horizon-connection"
CATALOG_NAME="snowflake_iceberg_catalog"
LF_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/LFDataAccessRole"

echo "=============================================="
echo "Setting up Glue Catalog Federation"
echo "=============================================="
echo "Snowflake Account: $SNOWFLAKE_ACCOUNT"
echo "Snowflake Database: $SNOWFLAKE_DATABASE"
echo "AWS Region: $AWS_REGION"
echo ""

# -----------------------------------------------------------------------------
# 1. Verify Secret exists
# -----------------------------------------------------------------------------
echo "Step 1: Verifying Secrets Manager secret..."

if aws secretsmanager describe-secret --secret-id $SECRET_NAME --region $AWS_REGION > /dev/null 2>&1; then
    echo "  ✅ Secret '$SECRET_NAME' exists"
    SECRET_ARN=$(aws secretsmanager describe-secret --secret-id $SECRET_NAME --region $AWS_REGION --query 'ARN' --output text)
else
    echo "  ❌ Secret '$SECRET_NAME' not found!"
    echo ""
    echo "  Create the secret first:"
    echo "  aws secretsmanager create-secret \\"
    echo "    --name $SECRET_NAME \\"
    echo "    --secret-string '{\"BEARER_TOKEN\": \"<YOUR_OAUTH_TOKEN>\"}' \\"
    echo "    --region $AWS_REGION"
    exit 1
fi

# -----------------------------------------------------------------------------
# 2. Register S3 location with Lake Formation
# -----------------------------------------------------------------------------
echo ""
echo "Step 2: Registering S3 location with Lake Formation..."

S3_ARN="arn:aws:s3:::${S3_BUCKET}/${S3_PREFIX}"

aws lakeformation register-resource \
    --resource-arn "arn:aws:s3:::${S3_BUCKET}" \
    --role-arn $LF_ROLE_ARN \
    --with-federation \
    --region $AWS_REGION 2>/dev/null && echo "  ✅ S3 location registered" || echo "  ℹ️ S3 location already registered or error occurred"

# -----------------------------------------------------------------------------
# 3. Create Glue Connection
# -----------------------------------------------------------------------------
echo ""
echo "Step 3: Creating Glue Connection..."

INSTANCE_URL="https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"

CONNECTION_INPUT=$(cat << EOF
{
  "Name": "${CONNECTION_NAME}",
  "ConnectionType": "SNOWFLAKEICEBERGRESTCATALOG",
  "ConnectionProperties": {
    "INSTANCE_URL": "${INSTANCE_URL}",
    "ROLE_ARN": "${LF_ROLE_ARN}",
    "CATALOG_CASING_FILTER": "UPPERCASE_ONLY"
  },
  "AuthenticationConfiguration": {
    "AuthenticationType": "CUSTOM",
    "SecretArn": "${SECRET_ARN}"
  }
}
EOF
)

aws glue create-connection \
    --connection-input "$CONNECTION_INPUT" \
    --region $AWS_REGION 2>/dev/null && echo "  ✅ Glue connection created" || echo "  ℹ️ Connection already exists or error occurred"

# -----------------------------------------------------------------------------
# 4. Create Federated Catalog
# -----------------------------------------------------------------------------
echo ""
echo "Step 4: Creating Federated Catalog..."

CATALOG_INPUT=$(cat << EOF
{
  "FederatedCatalog": {
    "Identifier": "${SNOWFLAKE_DATABASE}",
    "ConnectionName": "${CONNECTION_NAME}"
  },
  "CreateTableDefaultPermissions": [],
  "CreateDatabaseDefaultPermissions": []
}
EOF
)

aws glue create-catalog \
    --name $CATALOG_NAME \
    --catalog-input "$CATALOG_INPUT" \
    --region $AWS_REGION 2>/dev/null && echo "  ✅ Federated catalog created" || echo "  ℹ️ Catalog already exists or error occurred"

# -----------------------------------------------------------------------------
# 5. Verify setup
# -----------------------------------------------------------------------------
echo ""
echo "Step 5: Verifying setup..."

echo ""
echo "  Checking connection..."
aws glue get-connection --name $CONNECTION_NAME --region $AWS_REGION > /dev/null 2>&1 && \
    echo "  ✅ Connection verified" || echo "  ❌ Connection not found"

echo ""
echo "  Checking catalog..."
aws glue get-catalog --name $CATALOG_NAME --region $AWS_REGION > /dev/null 2>&1 && \
    echo "  ✅ Catalog verified" || echo "  ❌ Catalog not found"

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Open Amazon Athena console"
echo "  2. Select the '$CATALOG_NAME' data source"
echo "  3. Run: SHOW TABLES IN ${CATALOG_NAME}.PUBLIC"
echo "  4. Query: SELECT * FROM ${CATALOG_NAME}.PUBLIC.SALES_DATA"
echo ""
echo "Sample Athena queries are in: queries/sample-queries.sql"

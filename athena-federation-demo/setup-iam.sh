#!/bin/bash
# =============================================================================
# IAM Role Setup for AWS Glue Catalog Federation with Snowflake Horizon
# =============================================================================

set -e

# Configuration - Update these values
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-YOUR_AWS_ACCOUNT_ID}"
AWS_REGION="${AWS_REGION:-ap-northeast-1}"
S3_BUCKET="${S3_BUCKET:-YOUR_S3_BUCKET}"
S3_PREFIX="${S3_PREFIX:-iceberg-demo}"

# Role names
LF_DATA_ACCESS_ROLE="LFDataAccessRole"
GLUE_CONNECTION_ROLE="GlueSnowflakeFederationRole"

echo "=============================================="
echo "Setting up IAM roles for Catalog Federation"
echo "=============================================="
echo "AWS Account: $AWS_ACCOUNT_ID"
echo "Region: $AWS_REGION"
echo "S3 Bucket: $S3_BUCKET"
echo ""

# -----------------------------------------------------------------------------
# 1. Create Lake Formation Data Access Role
# -----------------------------------------------------------------------------
echo "Creating Lake Formation Data Access Role..."

cat > /tmp/lf-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "glue.amazonaws.com",
          "lakeformation.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
  --role-name $LF_DATA_ACCESS_ROLE \
  --assume-role-policy-document file:///tmp/lf-trust-policy.json \
  --description "Lake Formation data access role for Snowflake federation" \
  --region $AWS_REGION 2>/dev/null || echo "Role already exists"

# -----------------------------------------------------------------------------
# 2. Create S3 Access Policy
# -----------------------------------------------------------------------------
echo "Creating S3 access policy..."

cat > /tmp/s3-access-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${S3_BUCKET}",
        "arn:aws:s3:::${S3_BUCKET}/${S3_PREFIX}/*"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name $LF_DATA_ACCESS_ROLE \
  --policy-name S3AccessPolicy \
  --policy-document file:///tmp/s3-access-policy.json \
  --region $AWS_REGION

# -----------------------------------------------------------------------------
# 3. Create Glue Connection Role
# -----------------------------------------------------------------------------
echo "Creating Glue Connection Role..."

cat > /tmp/glue-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
  --role-name $GLUE_CONNECTION_ROLE \
  --assume-role-policy-document file:///tmp/glue-trust-policy.json \
  --description "Glue connection role for Snowflake Horizon federation" \
  --region $AWS_REGION 2>/dev/null || echo "Role already exists"

# -----------------------------------------------------------------------------
# 4. Create Secrets Manager Access Policy
# -----------------------------------------------------------------------------
echo "Creating Secrets Manager access policy..."

cat > /tmp/secrets-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:horizon-catalog-token*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name $GLUE_CONNECTION_ROLE \
  --policy-name SecretsManagerAccessPolicy \
  --policy-document file:///tmp/secrets-policy.json \
  --region $AWS_REGION

# -----------------------------------------------------------------------------
# 5. Attach AWS managed policies
# -----------------------------------------------------------------------------
echo "Attaching managed policies..."

aws iam attach-role-policy \
  --role-name $GLUE_CONNECTION_ROLE \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole \
  --region $AWS_REGION 2>/dev/null || echo "Policy already attached"

# -----------------------------------------------------------------------------
# Cleanup temp files
# -----------------------------------------------------------------------------
rm -f /tmp/lf-trust-policy.json /tmp/s3-access-policy.json \
      /tmp/glue-trust-policy.json /tmp/secrets-policy.json

echo ""
echo "=============================================="
echo "IAM Setup Complete!"
echo "=============================================="
echo ""
echo "Created roles:"
echo "  - $LF_DATA_ACCESS_ROLE"
echo "  - $GLUE_CONNECTION_ROLE"
echo ""
echo "Next steps:"
echo "  1. Run setup-federation.sh to create the Glue connection"
echo "  2. Register S3 location with Lake Formation"
echo "  3. Create the federated catalog"

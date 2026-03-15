# 🔗 AWS Glue Catalog Federation Demo

## Athena から Snowflake Managed Iceberg テーブルにアクセス

このデモでは、**AWS Glue Catalog Federation** を使用して、Amazon Athena から Snowflake Horizon Catalog 経由で Snowflake Managed Iceberg テーブルにクエリを実行します。

---

## 📖 概要

### Catalog Federation とは？

AWS Glue Data Catalog の **Catalog Federation** 機能により、外部カタログ（Snowflake Horizon Catalog）のメタデータを AWS Glue Data Catalog 内でフェデレート（連携）できます。

これにより：
- **データの移動・複製なし** で Snowflake のデータに Athena からアクセス
- **Lake Formation** による細粒度アクセス制御
- **統一されたカタログ** からの複数データソースへのアクセス

---

## 🏗️ アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              AWS                                        │
│  ┌─────────────┐    ┌─────────────────┐    ┌───────────────────────┐   │
│  │   Athena    │───►│  Glue Data      │───►│  Federated Catalog    │   │
│  │  (Query)    │    │  Catalog        │    │  (Snowflake連携)       │   │
│  └─────────────┘    └─────────────────┘    └───────────┬───────────┘   │
│                                                        │               │
│  ┌─────────────────────────────────────────────────────┼───────────┐   │
│  │                    Lake Formation                   │           │   │
│  │              (Fine-grained Access Control)          │           │   │
│  └─────────────────────────────────────────────────────┼───────────┘   │
│                                                        │               │
│  ┌─────────────┐                                       │               │
│  │  Secrets    │◄──────────────────────────────────────┘               │
│  │  Manager    │  (OAuth Token)                                        │
│  └─────────────┘                                                       │
└────────────────────────────────────────┬────────────────────────────────┘
                                         │
                                         │ Iceberg REST Protocol
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Snowflake                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  Horizon REST Catalog API                        │   │
│  │              /polaris/api/catalog                                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              Snowflake Managed Iceberg Table                     │   │
│  │                    (ICEBERG_DEMO_DB.PUBLIC.SALES_DATA)           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────┬────────────────────────────────┘
                                         │
                                         │ Vended Credentials
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Amazon S3                                     │
│                    (Iceberg データファイル)                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 前提条件

- Snowflake Managed Iceberg テーブルが作成済み（親デモの `setup.sql` を実行済み）
- AWS アカウントへのアクセス
- Lake Formation データレイク管理者権限を持つ IAM ロール
- AWS CLI がインストール済み

---

## 🚀 セットアップ手順

### Step 1: OAuth トークンの取得

Snowflake の PAT を使って OAuth トークンを取得します。

```bash
# OAuth トークンを取得
curl -X POST "https://<ACCOUNT>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "scope=session:role:ICEBERG_SPARK_ROLE" \
  -d "client_secret=<YOUR_PAT_TOKEN>"
```

レスポンスの `access_token` を控えておきます。

### Step 2: AWS Secrets Manager にトークンを保存

```bash
# シークレットを作成
aws secretsmanager create-secret \
  --name horizon-catalog-token \
  --description "Snowflake Horizon Catalog OAuth Token" \
  --secret-string '{"BEARER_TOKEN": "<YOUR_ACCESS_TOKEN>"}' \
  --region ap-northeast-1
```

### Step 3: IAM ロールの作成

`setup-iam.sh` スクリプトを実行するか、手動で以下を設定します。

```bash
# IAMロール作成スクリプトを実行
./setup-iam.sh
```

### Step 4: Lake Formation にS3ロケーションを登録

```bash
# S3ロケーションを登録
aws lakeformation register-resource \
  --resource-arn "arn:aws:s3:::<YOUR_BUCKET>/iceberg-demo/" \
  --role-arn "arn:aws:iam::<ACCOUNT_ID>:role/LFDataAccessRole" \
  --with-federation \
  --region ap-northeast-1
```

### Step 5: Glue Connection の作成

```bash
# Glue Connection を作成
aws glue create-connection \
  --connection-input '{
    "Name": "snowflake-horizon-connection",
    "ConnectionType": "SNOWFLAKEICEBERGRESTCATALOG",
    "ConnectionProperties": {
      "INSTANCE_URL": "https://<ACCOUNT>.snowflakecomputing.com"
    },
    "AuthenticationConfiguration": {
      "AuthenticationType": "CUSTOM",
      "SecretArn": "arn:aws:secretsmanager:ap-northeast-1:<ACCOUNT_ID>:secret:horizon-catalog-token"
    }
  }' \
  --region ap-northeast-1
```

### Step 6: Federated Catalog の作成

```bash
# Federated Catalog を作成
aws glue create-catalog \
  --name snowflake_iceberg_catalog \
  --catalog-input '{
    "FederatedCatalog": {
      "Identifier": "ICEBERG_DEMO_DB",
      "ConnectionName": "snowflake-horizon-connection"
    },
    "CreateTableDefaultPermissions": [],
    "CreateDatabaseDefaultPermissions": []
  }' \
  --region ap-northeast-1
```

### Step 7: Athena からクエリ実行

AWS コンソールで Athena を開き、以下のクエリを実行：

```sql
-- Federated Catalog のテーブル一覧を確認
SHOW TABLES IN snowflake_iceberg_catalog.PUBLIC;

-- データをクエリ
SELECT * FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA LIMIT 10;

-- 集計クエリ
SELECT 
    REGION,
    COUNT(*) as order_count,
    SUM(AMOUNT) as total_sales
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY REGION
ORDER BY total_sales DESC;
```

---

## 📁 ファイル構成

```
athena-federation-demo/
├── README.md              # このファイル
├── setup-iam.sh           # IAM ロール作成スクリプト
├── setup-federation.sh    # Federation セットアップスクリプト
├── terraform/             # Terraform による自動化（オプション）
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
└── queries/               # サンプル Athena クエリ
    └── sample-queries.sql
```

---

## ⚠️ トラブルシューティング

### "Access Denied" エラー

Lake Formation の権限を確認：
```bash
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::<ACCOUNT_ID>:role/<YOUR_ROLE>"}' \
  --resource '{"Table": {"CatalogId": "<ACCOUNT_ID>", "DatabaseName": "PUBLIC", "Name": "SALES_DATA"}}' \
  --permissions "SELECT" \
  --region ap-northeast-1
```

### Connection エラー

Secrets Manager のトークンが有効か確認：
```bash
aws secretsmanager get-secret-value \
  --secret-id horizon-catalog-token \
  --region ap-northeast-1
```

---

## 📚 参考リンク

- [AWS Blog: Access Snowflake Horizon Catalog data using catalog federation](https://aws.amazon.com/blogs/big-data/access-snowflake-horizon-catalog-data-using-catalog-federation-in-the-aws-glue-data-catalog/)
- [AWS Glue Catalog Federation Documentation](https://docs.aws.amazon.com/glue/latest/dg/catalog-federation.html)
- [Snowflake Horizon REST Catalog API](https://docs.snowflake.com/en/user-guide/tables-iceberg-rest-api)

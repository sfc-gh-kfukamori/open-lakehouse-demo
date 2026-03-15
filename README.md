# 🏔️ Open Lakehouse Demo

**ローカルSparkからSnowflake Managed Icebergテーブルを読み取るデモ**

[![Snowflake](https://img.shields.io/badge/Snowflake-Horizon%20REST%20API-29B5E8?style=flat&logo=snowflake)](https://www.snowflake.com/)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue?style=flat)](https://iceberg.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache-Spark%203.5-E25A1C?style=flat&logo=apachespark)](https://spark.apache.org/)

---

## 📖 概要

このデモでは、**Snowflake Horizon REST Catalog API** を使用して、ローカル環境で動作する Apache Spark から Snowflake 上の **Managed Iceberg テーブル** に直接アクセスします。

### Open Lakehouse とは？

Open Lakehouse は、データレイクの柔軟性とデータウェアハウスの信頼性を兼ね備えた次世代データアーキテクチャです。

- **オープンフォーマット**: Apache Iceberg によるベンダーロックインの回避
- **マルチエンジン**: Snowflake、Spark、Trino、Flink など様々なエンジンから同一データにアクセス
- **統一カタログ**: Snowflake Horizon がカタログとガバナンスを一元管理

---

## 🏗️ アーキテクチャ

```
┌─────────────────────┐                    ┌─────────────────────────────┐
│                     │   REST API         │                             │
│   Local Spark       │◄──────────────────►│  Snowflake Horizon          │
│   (PySpark)         │   (Iceberg REST    │  REST Catalog API           │
│                     │    Protocol)       │  /polaris/api/catalog       │
└──────────┬──────────┘                    └──────────────┬──────────────┘
           │                                              │
           │ Vended Credentials                           │ メタデータ管理
           │ (一時的なS3認証情報)                           │ (テーブル定義、
           │                                              │  スキーマ、統計)
           ▼                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│                           Amazon S3                                     │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  Iceberg Table Data                                             │   │
│   │  ├── metadata/                                                  │   │
│   │  │   ├── v1.metadata.json                                       │   │
│   │  │   └── snap-xxx.avro (manifest list)                          │   │
│   │  └── data/                                                      │   │
│   │      └── *.parquet (実データファイル)                             │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### データフロー

1. **認証**: PAT → OAuth トークン交換 → Horizon API アクセス
2. **メタデータ取得**: Spark が Horizon API からテーブルメタデータを取得
3. **認証情報発行**: Snowflake が Vended Credentials（一時S3認証）を発行
4. **データ読み取り**: Spark が S3 から直接 Parquet ファイルを読み取り

> **ポイント**: データは Snowflake を経由せず、Spark が S3 から直接読み取ります。
> Snowflake は「カタログ」として機能し、メタデータと認証情報を提供します。

---

## 🔑 主要な技術要素

### 1. Horizon REST Catalog API

Snowflake が提供する **Apache Iceberg REST Catalog** 仕様準拠のエンドポイント。

```
https://<account>.snowflakecomputing.com/polaris/api/catalog
```

- **Iceberg REST Protocol** に完全準拠
- 外部エンジン（Spark、Trino、Flink等）からアクセス可能
- 読み取り専用（現時点）

### 2. Vended Credentials

Snowflake が発行する**一時的なクラウドストレージ認証情報**。

| 項目 | 説明 |
|------|------|
| 有効期間 | 約60分（自動更新） |
| 対象 | テーブルデータファイルへのアクセスのみ |
| メリット | Spark側でのAWS認証設定が不要 |

### 3. Programmatic Access Token (PAT)

Snowflake への API アクセス用長期トークン。

- Snowsight UI から生成
- ロールにスコープ付け可能
- OAuth トークンに交換して使用

### 4. Snowflake Managed Iceberg Table

Snowflake が完全に管理する Iceberg テーブル。

```sql
CREATE ICEBERG TABLE my_table (...)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = my_ext_vol
BASE_LOCATION = 'path/';
```

---

## 📋 前提条件

### Snowflake 側

- [ ] Snowflake アカウント（Enterprise Edition 以上推奨）
- [ ] ACCOUNTADMIN または必要な権限を持つロール
- [ ] External Volume 用の AWS S3 バケット
- [ ] IAM ロール（Snowflake からの S3 アクセス用）

### ローカル環境

- [ ] macOS / Linux（Windows WSL2 も可）
- [ ] Anaconda または Miniconda
- [ ] Java 11 以上
- [ ] Git

---

## 🚀 セットアップ手順

### Step 1: リポジトリのクローン

```bash
git clone https://github.com/<your-username>/open-lakehouse-demo.git
cd open-lakehouse-demo
```

### Step 2: AWS IAM ロールの作成

Snowflake が S3 にアクセスするための IAM ロールを作成します。

1. AWS Console → IAM → Roles → Create role
2. **Trusted entity**: Another AWS account
   - Account ID: `<Snowflake AWS Account ID>`（setup.sql実行後に確認可能）
   - External ID: `iceberg_demo_ext_id`（任意の文字列）
3. **Permissions**: 以下のポリシーをアタッチ

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<your-bucket>",
                "arn:aws:s3:::<your-bucket>/*"
            ]
        }
    ]
}
```

### Step 3: Snowflake リソースの作成

`setup.sql` を編集して、以下の値を環境に合わせて変更：

```sql
-- setup.sql 内の以下を変更
STORAGE_BASE_URL = 's3://<your-bucket>/iceberg-demo/'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account>:role/<your-role>'
STORAGE_AWS_EXTERNAL_ID = '<your-external-id>'
```

Snowflake で実行：

```bash
# Snowsight または SnowSQL で実行
snowsql -a <account> -u <user> -f setup.sql
```

### Step 4: IAM Trust Policy の更新

`setup.sql` 実行後、External Volume の情報を確認：

```sql
DESCRIBE EXTERNAL VOLUME iceberg_demo_ext_vol;
```

出力の `STORAGE_AWS_IAM_USER_ARN` を IAM ロールの Trust Policy に追加：

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<your-external-id>"
                }
            }
        }
    ]
}
```

### Step 5: PAT (Programmatic Access Token) の生成

1. Snowsight にログイン
2. 右上のユーザーメニュー → **My Profile**
3. **Programmatic Access Tokens** セクション
4. **Generate New Token**
   - Name: `ICEBERG_SPARK_PAT`
   - Role: `ICEBERG_SPARK_ROLE`
   - Expiry: 30 days
5. トークンをコピーして保存

### Step 6: Conda 環境の作成

```bash
# 環境作成
conda env create -f environment.yml

# 環境の有効化
conda activate spark-iceberg

# 追加パッケージのインストール
pip install requests pyspark==3.5.3
```

### Step 7: Jupyter カーネルの登録

```bash
python -m ipykernel install --user --name=spark-iceberg --display-name="Spark Iceberg (Python 3.11)"
```

### Step 8: 設定ファイルの編集

`config.env.example` をコピーして `config.env` を作成：

```bash
cp config.env.example config.env
```

`config.env` を編集：

```bash
# Snowflake Account (account locator format recommended)
SNOWFLAKE_ACCOUNT=<account_locator>.<region>.<cloud>

# PAT Token
PAT_TOKEN=<your-pat-token>
```

### Step 9: ノートブックの実行

```bash
# Jupyter Lab を起動
jupyter lab

# または VS Code でノートブックを開く
code spark_iceberg_demo.ipynb
```

---

## 📁 ファイル構成

```
open-lakehouse-demo/
├── README.md                 # このファイル
├── environment.yml           # Conda 環境定義
├── setup.sql                 # Snowflake セットアップ SQL
├── config.env.example        # 設定ファイルテンプレート
├── config.env                # 設定ファイル（gitignore）
├── spark_iceberg_demo.ipynb  # メインデモノートブック
├── spark_iceberg_demo.py     # Python スクリプト版
└── .gitignore
```

---

## ⚠️ トラブルシューティング

### アカウント名にアンダースコアが含まれる場合

Java の SSL ホスト名検証でエラーが発生します。

**解決策**: アカウントロケーター形式を使用

```python
# NG: SFSEAPAC-K_FUKAMORI
# OK: zx48016.ap-northeast-1.aws
SNOWFLAKE_ACCOUNT = "<locator>.<region>.<cloud>"
```

### SHOW TABLES でテーブルが表示されない

ロールに適切な権限がない可能性があります。

```sql
GRANT SELECT ON TABLE <table> TO ROLE <role>;
GRANT USAGE ON DATABASE <db> TO ROLE <role>;
GRANT USAGE ON SCHEMA <db>.<schema> TO ROLE <role>;
```

### OAuth トークン取得エラー

PAT の有効期限が切れているか、ロールのスコープが正しくない可能性があります。

```bash
# エンドポイントのテスト
curl -X POST "https://<account>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "scope=session:role:<ROLE>" \
  -d "client_secret=<PAT>"
```

### Java バージョンエラー

Iceberg 1.9.x は Java 11 以上が必要です。

```bash
java -version
# openjdk version "11.x.x" であることを確認
```

---

## 📚 参考リンク

- [Snowflake Horizon REST Catalog API ドキュメント](https://docs.snowflake.com/en/user-guide/tables-iceberg-rest-api)
- [Apache Iceberg REST Catalog Spec](https://iceberg.apache.org/concepts/catalog/#rest-catalog)
- [Snowflake Managed Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [Apache Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

---

## 📄 ライセンス

MIT License

---

## 🤝 コントリビューション

Issue や Pull Request を歓迎します！

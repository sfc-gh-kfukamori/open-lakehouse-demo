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

## 💡 Horizon REST Catalog API の意義

### なぜ Horizon REST Catalog API が重要なのか？

#### 従来の課題：Managed Iceberg への外部アクセス不可

Horizon REST Catalog API 登場前、Snowflake Managed Iceberg テーブルは **Snowflake 内からしかアクセスできませんでした**。

| アクセス方法 | Managed Iceberg | Externally Managed Iceberg |
|------------|-----------------|---------------------------|
| **Snowflake内から** | ✅ 可能 | ✅ 可能 |
| **外部エンジンから** | ❌ **不可能** | ✅ 可能（外部カタログ経由） |

外部エンジンから Iceberg にアクセスするには、以下のいずれかが必要でした：

```
【従来パターン1: Externally Managed】
外部カタログ（Glue/Polaris）がマスター、Snowflakeは読み取り側

  Spark ──► AWS Glue Catalog ◄── Snowflake
                   │
                   ▼
                  S3

【従来パターン2: Snowflake Open Catalog経由】
Managed IcebergをOpen Catalogに同期して外部公開（追加コスト・運用負荷）

  Snowflake ──(sync)──► Open Catalog ◄── Spark
      │                      │
      └──────► S3 ◄──────────┘
```

#### Horizon REST API による解決

```
【現在: Horizon REST Catalog API】
Snowflakeがカタログとして直接機能 - 追加インフラ不要！

  Spark ──► Snowflake Horizon REST API
                   │
                   │ Vended Credentials
                   ▼
                  S3 ◄── Snowflake (read/write)
```

| 観点 | 従来 | Horizon REST API |
|------|------|------------------|
| **外部からManaged Icebergにアクセス** | ❌ 不可 | ✅ 可能 |
| **追加インフラ** | Polaris/Glue等が必要 | 不要 |
| **カタログの一元管理** | 分散（複数カタログ） | Snowflakeに統合 |
| **ガバナンス** | カタログごとに設定 | Snowflake RBACで統一 |
| **認証情報管理** | 各エンジンでS3認証設定 | Vended Credentialsで自動 |

---

## ⚖️ Managed Iceberg vs Native Snowflake Table

### 重要な区別

**Snowflake Managed Iceberg は Native Snowflake Table とは異なります。**

| 特性 | Native Snowflake Table | Snowflake Managed Iceberg |
|------|----------------------|---------------------------|
| **ストレージ形式** | Snowflake独自形式 | Apache Iceberg（Parquet） |
| **マイクロパーティション** | ✅ あり | ❌ なし |
| **Time Travel** | Snowflake方式（最大90日） | Iceberg Snapshot方式 |
| **Zero-copy Clone** | ✅ あり | ❌ なし（Iceberg方式） |
| **Search Optimization** | ✅ あり | ❌ なし |
| **外部エンジンからのアクセス** | ❌ 不可 | ✅ 可能 |

### Iceberg が提供する同等機能

| Snowflake Native機能 | Iceberg での代替 |
|---------------------|-----------------|
| Time Travel | **Snapshot** - 各コミットがスナップショットとして保存 |
| マイクロパーティション | **Hidden Partitioning** - パーティション変換 |
| Clone | **Branching/Tagging** (Iceberg v2) |
| メタデータ最適化 | **Manifest files** によるファイルプルーニング |

### Spark からの Time Travel（Iceberg Snapshot）

```python
# 特定のスナップショットを読む
spark.read.option("snapshot-id", 123456789).table("my_table")

# タイムスタンプ指定
spark.read.option("as-of-timestamp", "2024-01-15 10:00:00").table("my_table")
```

> **結論**: Managed Iceberg は「Snowflake独自機能を外部に公開する」のではなく、
> 「オープンフォーマットのテーブルを Snowflake が管理・ガバナンスしつつ外部にも開放する」アプローチです。

---

## 🚀 Managed Iceberg のパフォーマンス優位性

### なぜ Managed Iceberg は外部カタログ管理より高性能なのか？

Snowflake Managed Iceberg は、外部で管理される Iceberg テーブルより高いパフォーマンスを発揮します。

#### 1. 自動コンパクション（小ファイル問題の解消）

```
【外部カタログ管理の場合】
頻繁な INSERT/UPDATE で小ファイルが大量発生 → クエリ性能劣化

data/
├── part-00001.parquet (1MB)
├── part-00002.parquet (500KB)
├── part-00003.parquet (2MB)
├── ... (数千ファイル)
└── part-09999.parquet (100KB)

【Snowflake Managed の場合】
自動的に最適なサイズにコンパクション

data/
├── data-00001.parquet (128MB) ← 最適化済み
├── data-00002.parquet (128MB)
└── data-00003.parquet (128MB)
```

#### 2. 統計情報の自動収集・最適化

Snowflake は書き込み時に詳細な統計情報を収集：

| 統計情報 | 用途 |
|---------|------|
| Min/Max値 | ファイルプルーニング（不要ファイルのスキップ） |
| Null count | クエリ最適化 |
| Distinct count | カーディナリティ推定 |

#### 3. データレイアウトの最適化

- 自動ソート/クラスタリング
- 列の物理配置の最適化
- 圧縮アルゴリズムの自動選択（Zstd等）

#### 4. メタデータの効率的な管理

- メタデータファイルの自動整理
- 古いスナップショットの自動クリーンアップ
- マニフェストファイルの最適な分割

### 外部エンジンにも恩恵はあるか？

**はい、データ層の最適化は外部エンジンにも恩恵があります：**

| 最適化 | Snowflake内 | 外部エンジン（Spark等） |
|--------|------------|---------------------|
| コンパクション（少ないファイル数） | ✅ 恩恵あり | ✅ **恩恵あり** |
| 統計情報によるプルーニング | ✅ 恩恵あり | ✅ **恩恵あり** |
| データレイアウト最適化 | ✅ 恩恵あり | ✅ **恩恵あり** |
| Snowflakeクエリオプティマイザ | ✅ 恩恵あり | ❌ 関係なし |
| Snowflakeキャッシュ | ✅ 恩恵あり | ❌ 関係なし |

### 具体例：ファイルプルーニングの効果

```python
# Spark からのクエリ
spark.sql("SELECT * FROM sales_data WHERE sale_date = '2024-01-15'")
```

**Snowflake Managed Iceberg の場合：**
```
読み取りファイル: 3/100 (統計情報により97%スキップ)
スキャンデータ量: 50MB / 5GB
```

**最適化されていない外部管理 Iceberg：**
```
読み取りファイル: 100/100 (全ファイルスキャン)
スキャンデータ量: 5GB / 5GB
```

> **結論**: Snowflake Managed Iceberg は「書き込み時の最適化」が優れており、
> その恩恵は **外部エンジンからアクセスしても享受できます**。

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

### Step 9: ノートブックを開く

```bash
# Jupyter Lab を起動
jupyter lab

# または VS Code でノートブックを開く
code spark_iceberg_demo.ipynb
```

---

## ▶️ デモの実行方法

ノートブック `spark_iceberg_demo.ipynb` を開いたら、以下の順序でセルを実行します。

### 実行手順

#### 1. 環境設定（セル 1-2）
```
[セル 1] Java 環境の設定
  → JAVA_HOME を conda 環境の Java 11 に設定
  → 出力: "openjdk version 11.x.x" を確認
```

#### 2. Snowflake 接続設定（セル 3-4）
```
[セル 2] 接続パラメータの設定
  → SNOWFLAKE_ACCOUNT: あなたのアカウントロケーター（例: zx48016.ap-northeast-1.aws）
  → PAT_TOKEN: Step 5 で生成した PAT トークン
  → 出力: 設定内容の確認表示
```

> ⚠️ **重要**: `SNOWFLAKE_ACCOUNT` と `PAT_TOKEN` を必ず自分の値に変更してください

#### 3. OAuth トークン取得（セル 5-6）
```
[セル 3] PAT → OAuth トークン交換
  → Horizon REST API の認証トークンを取得
  → 出力: "✅ OAuth トークンを取得しました"
```

#### 4. Spark Session 作成（セル 7-8）
```
[セル 4] Spark Session の構築
  → Iceberg REST Catalog として Horizon API を設定
  → 初回実行時は JAR ダウンロードのため数分かかる場合あり
  → 出力: "✅ Spark Session を作成しました"
```

#### 5. カタログ探索（セル 9-14）
```
[セル 5] SHOW NAMESPACES
  → 利用可能なスキーマ一覧を表示
  → 出力: PUBLIC スキーマが表示される

[セル 6] SHOW TABLES
  → PUBLIC スキーマ内のテーブル一覧
  → 出力: SALES_DATA テーブルが表示される

[セル 7] DESCRIBE TABLE
  → テーブルのカラム定義を表示
```

#### 6. データ読み取り（セル 15-20）
```
[セル 8] SELECT * クエリ
  → Iceberg テーブルの全データを取得
  → 🎯 ここで Spark が S3 から直接データを読み取る！

[セル 9] 集計クエリ
  → 地域別売上サマリーを計算
  → Spark のローカルエンジンで集計処理

[セル 10] DataFrame API
  → プログラマティックなデータ操作
  → レコード数、スキーマ、Top 3 を表示
```

#### 7. クリーンアップ（セル 21-22）
```
[セル 11] Spark Session 停止
  → リソースを解放
  → 出力: "✅ Spark Session を停止しました"
```

### クイック実行（全セル一括実行）

VS Code または Jupyter Lab で全セルを一括実行することもできます：

- **VS Code**: `Ctrl+Alt+Enter` (Windows) / `Cmd+Alt+Enter` (Mac)
- **Jupyter Lab**: メニュー → Run → Run All Cells

> ⚠️ 一括実行前に、セル 2 の `SNOWFLAKE_ACCOUNT` と `PAT_TOKEN` を必ず設定してください

### 期待される出力例

```
📁 ICEBERG_DEMO_DB 内の Namespace 一覧:
+---------+
|namespace|
+---------+
|PUBLIC   |
+---------+

📋 ICEBERG_DEMO_DB.PUBLIC 内のテーブル一覧:
+---------+----------+-----------+
|namespace|tableName |type       |
+---------+----------+-----------+
|PUBLIC   |SALES_DATA|ICEBERG    |
+---------+----------+-----------+

📊 ICEBERG_DEMO_DB.PUBLIC.SALES_DATA の全データ:
+-------+------------------------+--------+----------+---------+
|SALE_ID|PRODUCT_NAME            |AMOUNT  |SALE_DATE |REGION   |
+-------+------------------------+--------+----------+---------+
|1      |ノートPC                 |128000.00|2024-01-15|Tokyo    |
|2      |モニター                 |45000.50|2024-01-16|Osaka    |
|3      |キーボード               |12500.00|2024-01-17|Tokyo    |
...
+-------+------------------------+--------+----------+---------+

📈 地域別売上サマリー:
+------+-----------+-----------+--------+
|REGION|order_count|total_sales|avg_sale|
+------+-----------+-----------+--------+
|Tokyo |4          |184600.00  |46150.00|
|Osaka |2          |48200.50   |24100.25|
...
+------+-----------+-----------+--------+
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

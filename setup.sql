-- =============================================================================
-- Open Lakehouse Demo: Snowflake Setup SQL
-- =============================================================================
-- Run this script in Snowflake to set up the demo environment
-- Account: SFSEAPAC-K_FUKAMORI
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Create External Volume (S3 Storage)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL VOLUME iceberg_demo_ext_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'iceberg_s3'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://kfukamori-tech-fast-track/iceberg-demo/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/kfukamori_iceberg_bucket_role'
      STORAGE_AWS_EXTERNAL_ID = 'iceberg_demo_ext_id'
    )
  );

-- Verify External Volume
DESCRIBE EXTERNAL VOLUME iceberg_demo_ext_vol;

-- -----------------------------------------------------------------------------
-- 2. Create Database & Schema
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ICEBERG_DEMO_DB;
ALTER DATABASE ICEBERG_DEMO_DB SET EXTERNAL_VOLUME = iceberg_demo_ext_vol;

-- -----------------------------------------------------------------------------
-- 3. Create Managed Iceberg Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.PUBLIC.SALES_DATA (
  sale_id INT,
  product_name STRING,
  amount DECIMAL(10,2),
  sale_date DATE,
  region STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = iceberg_demo_ext_vol
BASE_LOCATION = 'sales_data';

-- -----------------------------------------------------------------------------
-- 4. Insert Sample Data
-- -----------------------------------------------------------------------------
INSERT INTO ICEBERG_DEMO_DB.PUBLIC.SALES_DATA VALUES
  (1, 'ノートPC', 128000.00, '2024-01-15', 'Tokyo'),
  (2, 'モニター', 45000.50, '2024-01-16', 'Osaka'),
  (3, 'キーボード', 12500.00, '2024-01-17', 'Tokyo'),
  (4, 'マウス', 5800.00, '2024-01-18', 'Nagoya'),
  (5, 'Webカメラ', 8900.00, '2024-01-19', 'Fukuoka'),
  (6, 'ヘッドセット', 15600.00, '2024-01-20', 'Tokyo'),
  (7, 'USBハブ', 3200.00, '2024-01-21', 'Osaka'),
  (8, 'SSD 1TB', 12000.00, '2024-01-22', 'Sapporo'),
  (9, 'ドッキングステーション', 28500.00, '2024-01-23', 'Tokyo'),
  (10, 'スピーカー', 9800.00, '2024-01-24', 'Hiroshima');

-- Verify data
SELECT * FROM ICEBERG_DEMO_DB.PUBLIC.SALES_DATA ORDER BY sale_id;

-- -----------------------------------------------------------------------------
-- 5. Create Service Role for Spark Access
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ROLE ICEBERG_SPARK_ROLE;

GRANT USAGE ON DATABASE ICEBERG_DEMO_DB TO ROLE ICEBERG_SPARK_ROLE;
GRANT USAGE ON SCHEMA ICEBERG_DEMO_DB.PUBLIC TO ROLE ICEBERG_SPARK_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ICEBERG_DEMO_DB.PUBLIC TO ROLE ICEBERG_SPARK_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ICEBERG_DEMO_DB.PUBLIC TO ROLE ICEBERG_SPARK_ROLE;

-- Grant to your user (for PAT generation)
GRANT ROLE ICEBERG_SPARK_ROLE TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 6. (Optional) Create Service User
-- -----------------------------------------------------------------------------
CREATE OR REPLACE USER ICEBERG_SPARK_USER 
  TYPE = SERVICE 
  DEFAULT_ROLE = ICEBERG_SPARK_ROLE;
GRANT ROLE ICEBERG_SPARK_ROLE TO USER ICEBERG_SPARK_USER;

-- -----------------------------------------------------------------------------
-- 7. Generate PAT (Run this manually in Snowsight UI)
-- -----------------------------------------------------------------------------
-- Go to: Snowsight → User Menu → My Profile → Programmatic Access Tokens
-- Generate new token with:
--   - Name: ICEBERG_SPARK_PAT
--   - Role: ICEBERG_SPARK_ROLE
--   - Expiry: 30 days

-- -----------------------------------------------------------------------------
-- Verification Queries
-- -----------------------------------------------------------------------------
-- Check Iceberg table properties
SHOW ICEBERG TABLES IN SCHEMA ICEBERG_DEMO_DB.PUBLIC;

-- Check table metadata location
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_DEMO_DB.PUBLIC.SALES_DATA');

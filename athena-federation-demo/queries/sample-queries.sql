-- =============================================================================
-- Sample Athena Queries for Snowflake Horizon Catalog Federation
-- =============================================================================
-- 
-- Prerequisites:
--   1. Glue Catalog Federation is configured
--   2. Federated catalog 'snowflake_iceberg_catalog' is created
--   3. Lake Formation permissions are granted
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Catalog Discovery
-- -----------------------------------------------------------------------------

-- List all schemas (namespaces) in the federated catalog
SHOW DATABASES IN snowflake_iceberg_catalog;

-- List all tables in the PUBLIC schema
SHOW TABLES IN snowflake_iceberg_catalog.PUBLIC;

-- Get table metadata
DESCRIBE snowflake_iceberg_catalog.PUBLIC.SALES_DATA;

-- Get detailed table information
SHOW CREATE TABLE snowflake_iceberg_catalog.PUBLIC.SALES_DATA;


-- -----------------------------------------------------------------------------
-- 2. Basic Queries
-- -----------------------------------------------------------------------------

-- Preview data
SELECT * 
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA 
LIMIT 10;

-- Count total records
SELECT COUNT(*) as total_records
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA;

-- Sample with specific columns
SELECT 
    TRANSACTION_ID,
    CUSTOMER_NAME,
    PRODUCT,
    AMOUNT,
    TRANSACTION_DATE
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 3. Analytical Queries
-- -----------------------------------------------------------------------------

-- Sales by Region
SELECT 
    REGION,
    COUNT(*) as order_count,
    SUM(AMOUNT) as total_sales,
    AVG(AMOUNT) as avg_order_value,
    MIN(AMOUNT) as min_sale,
    MAX(AMOUNT) as max_sale
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY REGION
ORDER BY total_sales DESC;

-- Sales by Product
SELECT 
    PRODUCT,
    COUNT(*) as units_sold,
    SUM(AMOUNT) as revenue,
    ROUND(AVG(AMOUNT), 2) as avg_price
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY PRODUCT
ORDER BY revenue DESC;

-- Monthly Sales Trend
SELECT 
    DATE_TRUNC('month', TRANSACTION_DATE) as month,
    COUNT(*) as transactions,
    SUM(AMOUNT) as monthly_revenue
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY DATE_TRUNC('month', TRANSACTION_DATE)
ORDER BY month;

-- Top Customers
SELECT 
    CUSTOMER_NAME,
    COUNT(*) as order_count,
    SUM(AMOUNT) as total_spent,
    AVG(AMOUNT) as avg_order_value
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY CUSTOMER_NAME
ORDER BY total_spent DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 4. Advanced Analytics
-- -----------------------------------------------------------------------------

-- Regional Product Analysis
SELECT 
    REGION,
    PRODUCT,
    COUNT(*) as sales_count,
    SUM(AMOUNT) as total_revenue
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY REGION, PRODUCT
ORDER BY REGION, total_revenue DESC;

-- Running Total by Date
SELECT 
    TRANSACTION_DATE,
    AMOUNT,
    SUM(AMOUNT) OVER (ORDER BY TRANSACTION_DATE) as running_total
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
ORDER BY TRANSACTION_DATE;

-- Percentile Analysis
SELECT 
    PRODUCT,
    APPROX_PERCENTILE(AMOUNT, 0.5) as median_sale,
    APPROX_PERCENTILE(AMOUNT, 0.9) as p90_sale
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY PRODUCT;


-- -----------------------------------------------------------------------------
-- 5. Comparison with Native Athena Data (Cross-catalog query example)
-- -----------------------------------------------------------------------------

-- If you have local Glue catalog data, you can join with federated data:
-- 
-- SELECT 
--     sf.REGION,
--     sf.total_sales,
--     local.additional_metrics
-- FROM (
--     SELECT REGION, SUM(AMOUNT) as total_sales
--     FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
--     GROUP BY REGION
-- ) sf
-- LEFT JOIN aws_glue_datacatalog.my_database.region_metrics local
--     ON sf.REGION = local.region_name;


-- -----------------------------------------------------------------------------
-- 6. Data Quality Checks
-- -----------------------------------------------------------------------------

-- Check for NULL values
SELECT 
    COUNT(*) as total_rows,
    COUNT(TRANSACTION_ID) as non_null_tx_id,
    COUNT(CUSTOMER_NAME) as non_null_customer,
    COUNT(AMOUNT) as non_null_amount
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA;

-- Check value distributions
SELECT 
    REGION,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM snowflake_iceberg_catalog.PUBLIC.SALES_DATA
GROUP BY REGION
ORDER BY count DESC;

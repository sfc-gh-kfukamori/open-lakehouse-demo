"""
Open Lakehouse Demo: Spark → Snowflake Managed Iceberg via Horizon REST Catalog API

This script demonstrates how to query Snowflake-managed Iceberg tables
from a local PySpark environment using the Horizon REST Catalog API.

Architecture:
  [Local Spark] → [Horizon REST API] → [S3 (Parquet/Iceberg)]
                         ↑
               [Snowflake Managed Iceberg Table]

Requirements:
  - conda env create -f environment.yml
  - conda activate spark-iceberg-demo
  - cp config.env.example config.env  # Then edit with your PAT
"""

import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv('config.env')

# =============================================================================
# Configuration
# =============================================================================
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "SFSEAPAC-K_FUKAMORI")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ICEBERG_SPARK_ROLE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ICEBERG_DEMO_DB")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
PAT_TOKEN = os.getenv("SNOWFLAKE_PAT")

HORIZON_CATALOG_URI = f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/polaris/api/catalog"
HORIZON_SESSION_ROLE = f"session:role:{SNOWFLAKE_ROLE}"
CATALOG_NAME = SNOWFLAKE_DATABASE

ICEBERG_VERSION = "1.9.1"

# =============================================================================
# Spark Session Setup
# =============================================================================
def create_spark_session():
    """Create and configure Spark session for Snowflake Iceberg access."""
    
    if not PAT_TOKEN:
        raise ValueError("SNOWFLAKE_PAT not set in config.env")
    
    spark = (
        SparkSession.builder
        .appName("SnowflakeIcebergDemo")
        .master("local[*]")
        
        # JAR Dependencies for Iceberg and AWS
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
        )
        
        # Iceberg SQL Extensions
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        
        # Horizon REST Catalog Configuration
        .config(f"spark.sql.catalog.{CATALOG_NAME}", 
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", HORIZON_CATALOG_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CATALOG_NAME)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.credential", PAT_TOKEN)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.scope", HORIZON_SESSION_ROLE)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.client.region", AWS_REGION)
        
        # Required for vended credentials (Snowflake provides temporary S3 access)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.header.X-Iceberg-Access-Delegation", 
                "vended-credentials")
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# =============================================================================
# Demo Functions
# =============================================================================
def demo_list_namespaces(spark):
    """Demo 1: List available namespaces (schemas)"""
    print("\n" + "=" * 60)
    print("Demo 1: List Namespaces (Schemas)")
    print("=" * 60)
    spark.sql("SHOW NAMESPACES").show(truncate=False)


def demo_list_tables(spark):
    """Demo 2: List tables in the PUBLIC schema"""
    print("\n" + "=" * 60)
    print("Demo 2: List Tables in PUBLIC Schema")
    print("=" * 60)
    spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.PUBLIC").show(truncate=False)


def demo_describe_table(spark):
    """Demo 3: Describe table schema"""
    print("\n" + "=" * 60)
    print("Demo 3: Describe Table Schema")
    print("=" * 60)
    spark.sql(f"DESCRIBE TABLE {CATALOG_NAME}.PUBLIC.SALES_DATA").show(truncate=False)


def demo_query_data(spark):
    """Demo 4: Query data from Iceberg table"""
    print("\n" + "=" * 60)
    print("Demo 4: Query Iceberg Table (SELECT *)")
    print("=" * 60)
    spark.sql(f"SELECT * FROM {CATALOG_NAME}.PUBLIC.SALES_DATA").show(truncate=False)


def demo_aggregation(spark):
    """Demo 5: Aggregation query"""
    print("\n" + "=" * 60)
    print("Demo 5: Aggregation Query (Sales by Region)")
    print("=" * 60)
    spark.sql(f"""
        SELECT 
            region,
            COUNT(*) as order_count,
            SUM(amount) as total_sales,
            AVG(amount) as avg_sale
        FROM {CATALOG_NAME}.PUBLIC.SALES_DATA 
        GROUP BY region
        ORDER BY total_sales DESC
    """).show(truncate=False)


def demo_dataframe_api(spark):
    """Demo 6: Using DataFrame API"""
    print("\n" + "=" * 60)
    print("Demo 6: DataFrame API Example")
    print("=" * 60)
    
    df = spark.table(f"{CATALOG_NAME}.PUBLIC.SALES_DATA")
    
    print(f"Total records: {df.count()}")
    print(f"\nSchema:")
    df.printSchema()
    
    print("\nTop 3 sales by amount:")
    df.orderBy(df.amount.desc()).limit(3).show(truncate=False)


def demo_time_travel(spark):
    """Demo 7: Iceberg Time Travel (if snapshots exist)"""
    print("\n" + "=" * 60)
    print("Demo 7: Iceberg Metadata - Snapshots")
    print("=" * 60)
    try:
        spark.sql(f"SELECT * FROM {CATALOG_NAME}.PUBLIC.SALES_DATA.snapshots").show(truncate=False)
    except Exception as e:
        print(f"Time travel info not available: {e}")


# =============================================================================
# Main
# =============================================================================
def main():
    print("=" * 60)
    print("Open Lakehouse Demo")
    print("Spark → Snowflake Managed Iceberg via Horizon REST API")
    print("=" * 60)
    print(f"\nAccount: {SNOWFLAKE_ACCOUNT}")
    print(f"Database: {SNOWFLAKE_DATABASE}")
    print(f"Role: {SNOWFLAKE_ROLE}")
    print(f"Region: {AWS_REGION}")
    
    print("\nCreating Spark session...")
    spark = create_spark_session()
    print("Spark session created successfully!")
    
    try:
        demo_list_namespaces(spark)
        demo_list_tables(spark)
        demo_describe_table(spark)
        demo_query_data(spark)
        demo_aggregation(spark)
        demo_dataframe_api(spark)
        demo_time_travel(spark)
        
        print("\n" + "=" * 60)
        print("Demo completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demo: {e}")
        raise
    finally:
        print("\nStopping Spark session...")
        spark.stop()


if __name__ == "__main__":
    main()

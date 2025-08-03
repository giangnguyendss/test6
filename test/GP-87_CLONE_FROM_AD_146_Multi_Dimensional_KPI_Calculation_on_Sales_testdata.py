# PySpark script for comprehensive test data generation for 'sales_window' table in Unity Catalog
# Purpose: Generate diverse test data for multi-dimensional sales KPI analysis (region, product, quarter, etc.)
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script creates a PySpark DataFrame with 30 diverse test records for the 'sales_window' table,
# covering happy path, edge, error, NULL, and special character scenarios. It uses Databricks-compatible data types,
# ensures schema consistency, and includes inline comments for each test scenario.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType  
from pyspark.sql import Row  
from datetime import date  

# Define schema for 'sales_window' table as per requirements
sales_window_schema = StructType([
    StructField("Sale_ID", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Sales_Amount", DecimalType(18,2), True),
    StructField("Sale_Date", DateType(), True)
])

# Prepare test data records
sales_window_test_data = [
    # Happy path: Valid sales in different regions, products, and quarters
    Row("S001", "East", "Widget", 100.00, date(2024, 1, 15)),    # 2024-Q1
    Row("S002", "East", "Widget", 150.00, date(2024, 4, 10)),    # 2024-Q2
    Row("S003", "East", "Widget", 200.00, date(2024, 7, 5)),     # 2024-Q3
    Row("S004", "West", "Gadget", 200.00, date(2024, 5, 20)),    # 2024-Q2
    Row("S005", "West", "Gadget", 180.00, date(2024, 8, 1)),     # 2024-Q3
    Row("S006", "North", "Widget", 300.00, date(2024, 2, 28)),   # 2024-Q1
    Row("S007", "South", "Gizmo", 250.00, date(2024, 3, 10)),    # 2024-Q1
    Row("S008", "South", "Gizmo", 275.00, date(2024, 6, 15)),    # 2024-Q2
    Row("S009", "East", "Gizmo", 120.00, date(2024, 9, 30)),     # 2024-Q3
    Row("S010", "West", "Widget", 90.00, date(2024, 10, 5)),     # 2024-Q4

    # Edge case: Sales_Amount = 0, boundary dates (start/end of quarter)
    Row("S011", "East", "Widget", 0.00, date(2024, 3, 31)),      # 2024-Q1 end
    Row("S012", "West", "Gadget", 0.00, date(2024, 4, 1)),       # 2024-Q2 start
    Row("S013", "North", "Widget", 99999999.99, date(2024, 12, 31)), # Large value, 2024-Q4 end

    # Error case: Negative Sales_Amount (should be excluded by validation)
    Row("S014", "East", "Widget", -50.00, date(2024, 5, 15)),

    # Error case: Invalid Sale_Date (should be excluded by validation)
    Row("S015", "West", "Gadget", 120.00, None),

    # NULL handling: NULL Sales_Amount
    Row("S016", "South", "Gizmo", None, date(2024, 7, 1)),

    # NULL handling: NULL Region
    Row("S017", None, "Widget", 80.00, date(2024, 8, 15)),

    # NULL handling: NULL Product
    Row("S018", "East", None, 60.00, date(2024, 9, 10)),

    # Special characters: Region and Product with special/multibyte chars
    Row("S019", "Nørth", "Widgét", 110.00, date(2024, 2, 14)),
    Row("S020", "南", "ガジェット", 130.00, date(2024, 5, 22)),  # Chinese/Japanese

    # Edge: Same product in multiple regions
    Row("S021", "East", "Gizmo", 140.00, date(2024, 10, 10)),
    Row("S022", "West", "Gizmo", 160.00, date(2024, 11, 11)),

    # Edge: Same region, different products, same quarter
    Row("S023", "North", "Gadget", 210.00, date(2024, 1, 5)),
    Row("S024", "North", "Gizmo", 220.00, date(2024, 1, 6)),

    # Edge: Multiple sales for same product/region/quarter
    Row("S025", "East", "Widget", 80.00, date(2024, 1, 20)),
    Row("S026", "East", "Widget", 90.00, date(2024, 1, 25)),

    # Error: Sale_ID is NULL
    Row(None, "West", "Gadget", 100.00, date(2024, 6, 30)),

    # Edge: Sale_Date far in the past/future
    Row("S027", "East", "Widget", 70.00, date(1999, 12, 31)),
    Row("S028", "West", "Gadget", 85.00, date(2050, 1, 1)),

    # Edge: All fields NULL (should be excluded)
    Row(None, None, None, None, None),

    # Happy path: Q4 sales
    Row("S029", "South", "Gizmo", 300.00, date(2024, 12, 15)),
    Row("S030", "East", "Widget", 110.00, date(2024, 11, 20)),
]

def create_sales_window_test_df(spark):
    """
    Create a PySpark DataFrame with comprehensive test data for the 'sales_window' table.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: PySpark DataFrame with test data and correct schema.
    """
    # Create DataFrame from test data and schema
    df = spark.createDataFrame(sales_window_test_data, schema=sales_window_schema)
    return df

def write_test_data_to_unity_catalog(df):
    """
    Write the test DataFrame to Unity Catalog as 'purgo_databricks.purgo_playground.sales_window' in Delta format.

    Args:
        df (DataFrame): The test data DataFrame.

    Returns:
        None
    """
    # Write to Unity Catalog, overwrite mode, Delta format
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("purgo_databricks.purgo_playground.sales_window")

def validate_sales_window_test_data(df):
    """
    Validate the test data for schema consistency and data type correctness.

    Args:
        df (DataFrame): The test data DataFrame.

    Returns:
        None
    """
    # Check schema matches expected
    assert df.schema == sales_window_schema, "Schema mismatch in test data"
    # Check data types for each column
    for field in sales_window_schema.fields:
        assert field.name in df.columns, f"Missing column: {field.name}"
    # Check for column count
    assert len(df.columns) == len(sales_window_schema.fields), "Column count mismatch"

# Main execution block
try:
    # Create test DataFrame
    sales_window_df = create_sales_window_test_df(spark)
    # Validate test data
    validate_sales_window_test_data(sales_window_df)
    # Write test data to Unity Catalog
    write_test_data_to_unity_catalog(sales_window_df)
except Exception as e:
    # Gracefully handle errors (e.g., missing/invalid data)
    print(f"Error generating or writing test data: {e}")

# spark.stop()  # Do not stop SparkSession in Databricks

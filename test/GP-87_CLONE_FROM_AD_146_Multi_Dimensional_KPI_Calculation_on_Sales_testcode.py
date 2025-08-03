# PySpark script: Comprehensive Test Suite for Advanced Sales KPI Calculation on Databricks
# Purpose: Validate multi-dimensional KPI calculations (Total Sales, Sales Trends, Top Selling Products, Final KPI) on 'purgo_playground.sales_window'
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script implements unit, integration, data quality, and error handling tests for advanced sales KPI calculations using PySpark DataFrame APIs. It covers schema validation, data type checks, null handling, window functions, ranking, and join logic, ensuring robust analytics for sales performance.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import Row  
from pyspark.sql.types import (StructType, StructField, LongType, StringType, DoubleType)  
from pyspark.sql.functions import (col, sum as _sum, avg, round as _round, lag, dense_rank, isnan, when, count, expr)  
from pyspark.sql.window import Window  

# -------------------------------
# Test Setup and Test Data Section
# -------------------------------

def generate_sales_window_test_data():
    """
    Generate comprehensive test data for the 'sales_window' table, covering:
    - Happy path (valid data)
    - Edge cases (boundary values)
    - Error cases (out-of-range, invalid combinations)
    - NULL handling
    - Special/multi-byte characters

    Returns:
        list[Row]: List of Row objects for DataFrame creation
    """
    data = [
        Row(Sales_ID=1, Product="WidgetA", Region="North", Year=2023, Quarter=1, Sales_Amount=10000),
        Row(Sales_ID=2, Product="WidgetA", Region="North", Year=2023, Quarter=2, Sales_Amount=12000),
        Row(Sales_ID=3, Product="WidgetA", Region="North", Year=2023, Quarter=3, Sales_Amount=11000),
        Row(Sales_ID=4, Product="WidgetA", Region="North", Year=2023, Quarter=4, Sales_Amount=13000),
        Row(Sales_ID=5, Product="WidgetB", Region="North", Year=2023, Quarter=1, Sales_Amount=9000),
        Row(Sales_ID=6, Product="WidgetB", Region="North", Year=2023, Quarter=2, Sales_Amount=9500),
        Row(Sales_ID=7, Product="WidgetB", Region="North", Year=2023, Quarter=3, Sales_Amount=9700),
        Row(Sales_ID=8, Product="WidgetB", Region="North", Year=2023, Quarter=4, Sales_Amount=9800),
        Row(Sales_ID=9, Product="WidgetC", Region="South", Year=2023, Quarter=1, Sales_Amount=15000),
        Row(Sales_ID=10, Product="WidgetC", Region="South", Year=2023, Quarter=2, Sales_Amount=15500),
        Row(Sales_ID=11, Product="WidgetC", Region="South", Year=2023, Quarter=3, Sales_Amount=16000),
        Row(Sales_ID=12, Product="WidgetC", Region="South", Year=2023, Quarter=4, Sales_Amount=16500),
        Row(Sales_ID=13, Product="WidgetD", Region="South", Year=2023, Quarter=1, Sales_Amount=14000),
        Row(Sales_ID=14, Product="WidgetD", Region="South", Year=2023, Quarter=2, Sales_Amount=14200),
        Row(Sales_ID=15, Product="WidgetD", Region="South", Year=2023, Quarter=3, Sales_Amount=14500),
        Row(Sales_ID=16, Product="WidgetD", Region="South", Year=2023, Quarter=4, Sales_Amount=14800),
        Row(Sales_ID=17, Product="WidgetE", Region="East", Year=2023, Quarter=1, Sales_Amount=8000),
        Row(Sales_ID=18, Product="WidgetE", Region="East", Year=2023, Quarter=2, Sales_Amount=8500),
        Row(Sales_ID=19, Product="WidgetE", Region="East", Year=2023, Quarter=3, Sales_Amount=8700),
        Row(Sales_ID=20, Product="WidgetE", Region="East", Year=2023, Quarter=4, Sales_Amount=9000),
        Row(Sales_ID=21, Product="WidgetF", Region="West", Year=2023, Quarter=1, Sales_Amount=0),
        Row(Sales_ID=22, Product="WidgetF", Region="West", Year=2023, Quarter=2, Sales_Amount=999999999),
        Row(Sales_ID=23, Product="WidgetF", Region="West", Year=2023, Quarter=3, Sales_Amount=-500),
        Row(Sales_ID=24, Product="WidgetG", Region="North", Year=2023, Quarter=1, Sales_Amount=None),
        Row(Sales_ID=25, Product=None, Region="North", Year=2023, Quarter=2, Sales_Amount=5000),
        Row(Sales_ID=26, Product="WidgetH", Region=None, Year=2023, Quarter=3, Sales_Amount=6000),
        Row(Sales_ID=27, Product="WidgetI", Region="South", Year=None, Quarter=4, Sales_Amount=7000),
        Row(Sales_ID=28, Product="WidgetJ", Region="East", Year=2023, Quarter=None, Sales_Amount=8000),
        Row(Sales_ID=29, Product="WidgétΩ", Region="Nørth", Year=2023, Quarter=1, Sales_Amount=12345),
        Row(Sales_ID=30, Product="ウィジェットK", Region="南", Year=2023, Quarter=2, Sales_Amount=54321),
        Row(Sales_ID=31, Product="WidgetL", Region="South", Year=2023, Quarter=5, Sales_Amount=1000),
        Row(Sales_ID=32, Product="WidgetM", Region="East", Year=1899, Quarter=1, Sales_Amount=2000),
        Row(Sales_ID=33, Product="WidgetN", Region="West", Year=2023, Quarter=2, Sales_Amount=None),
    ]
    return data

sales_window_schema = StructType([
    StructField("Sales_ID", LongType(), True),
    StructField("Product", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Year", LongType(), True),
    StructField("Quarter", LongType(), True),
    StructField("Sales_Amount", LongType(), True)
])

try:
    sales_window_test_data = generate_sales_window_test_data()
    sales_window_df = spark.createDataFrame(sales_window_test_data, schema=sales_window_schema)
except Exception as e:
    raise RuntimeError("Failed to generate or load test data for sales_window: {}".format(str(e)))

# -------------------------------
# Schema Validation Tests Section
# -------------------------------

def test_sales_window_schema(df):
    """
    Validates that the DataFrame schema matches the expected schema for 'sales_window'.

    Args:
        df (DataFrame): DataFrame to validate

    Returns:
        None
    """
    expected_fields = [
        ("Sales_ID", LongType()),
        ("Product", StringType()),
        ("Region", StringType()),
        ("Year", LongType()),
        ("Quarter", LongType()),
        ("Sales_Amount", LongType())
    ]
    actual_fields = [(f.name, type(f.dataType)()) for f in df.schema.fields]
    assert len(actual_fields) == len(expected_fields), "Column count mismatch in sales_window schema"
    for (exp_name, exp_type), (act_name, act_type) in zip(expected_fields, actual_fields):
        assert exp_name == act_name, f"Expected column {exp_name}, got {act_name}"
        assert isinstance(act_type, type(exp_type)), f"Expected type {exp_type} for {exp_name}, got {act_type}"

test_sales_window_schema(sales_window_df)

# -------------------------------
# Data Cleansing Section
# -------------------------------

def cleanse_sales_window_df(df):
    """
    Cleanses the sales_window DataFrame by:
    - Removing rows with nulls in critical columns (Product, Region, Year, Quarter, Sales_Amount)
    - Ensuring correct data types for numeric columns
    - Filtering out-of-range values for Year and Quarter

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: Cleansed DataFrame
    """
    cleansed_df = df.filter(
        col("Product").isNotNull() &
        col("Region").isNotNull() &
        col("Year").isNotNull() &
        col("Quarter").isNotNull() &
        col("Sales_Amount").isNotNull()
    )
    cleansed_df = cleansed_df.filter(
        (col("Year") >= 1900) & (col("Year") <= 2100) &
        (col("Quarter") >= 1) & (col("Quarter") <= 4)
    )
    return cleansed_df

sales_window_clean_df = cleanse_sales_window_df(sales_window_df)

# -------------------------------
# Data Quality Validation Tests Section
# -------------------------------

def test_no_nulls_in_critical_columns(df):
    """
    Asserts that there are no nulls in Product, Region, Year, Quarter, Sales_Amount.

    Args:
        df (DataFrame): DataFrame to check

    Returns:
        None
    """
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in ["Product", "Region", "Year", "Quarter", "Sales_Amount"]
    ]).collect()[0].asDict()
    for col_name, null_count in null_counts.items():
        assert null_count == 0, f"Nulls found in column {col_name}: {null_count}"

test_no_nulls_in_critical_columns(sales_window_clean_df)

def test_numeric_types(df):
    """
    Asserts that Year, Quarter, Sales_Amount are numeric types.

    Args:
        df (DataFrame): DataFrame to check

    Returns:
        None
    """
    for field in ["Year", "Quarter", "Sales_Amount"]:
        dtype = dict(df.dtypes)[field]
        assert dtype in ("bigint", "int", "long"), f"Column {field} is not numeric: {dtype}"

test_numeric_types(sales_window_clean_df)

# -------------------------------
# Total_Sales Calculation Section
# -------------------------------

def calculate_total_sales(df):
    """
    Calculates total and average Sales_Amount by Region and Product.

    Args:
        df (DataFrame): Cleansed sales_window DataFrame

    Returns:
        DataFrame: Total_Sales DataFrame with columns: Region, Product, Total_Sales_Amount, Average_Sales_Amount
    """
    total_sales_df = (
        df.groupBy("Region", "Product")
        .agg(
            _sum("Sales_Amount").alias("Total_Sales_Amount"),
            _round(avg("Sales_Amount"), 2).alias("Average_Sales_Amount")
        )
        .orderBy("Region", "Product")
    )
    return total_sales_df

total_sales_df = calculate_total_sales(sales_window_clean_df)

def test_total_sales_schema(df):
    """
    Validates the schema of the Total_Sales DataFrame.

    Args:
        df (DataFrame): Total_Sales DataFrame

    Returns:
        None
    """
    expected_fields = [
        ("Region", StringType()),
        ("Product", StringType()),
        ("Total_Sales_Amount", LongType()),
        ("Average_Sales_Amount", DoubleType())
    ]
    actual_fields = [(f.name, type(f.dataType)()) for f in df.schema.fields]
    assert len(actual_fields) == len(expected_fields), "Column count mismatch in Total_Sales"
    for (exp_name, exp_type), (act_name, act_type) in zip(expected_fields, actual_fields):
        assert exp_name == act_name, f"Expected column {exp_name}, got {act_name}"
        assert isinstance(act_type, type(exp_type)), f"Expected type {exp_type} for {exp_name}, got {act_type}"

test_total_sales_schema(total_sales_df)
test_no_nulls_in_critical_columns(total_sales_df.select("Region", "Product", "Total_Sales_Amount", "Average_Sales_Amount"))

# -------------------------------
# Sales_Trends Calculation Section
# -------------------------------

def calculate_sales_trends(df):
    """
    Calculates sales trends by Region, Product, Year, Quarter:
    - Previous_Sales_Amount (lag)
    - Sales_Change (difference from previous quarter)

    Args:
        df (DataFrame): Cleansed sales_window DataFrame

    Returns:
        DataFrame: Sales_Trends DataFrame with columns: Region, Product, Year, Quarter, Sales_Amount, Previous_Sales_Amount, Sales_Change
    """
    window_spec = Window.partitionBy("Region", "Product").orderBy("Year", "Quarter")
    sales_trends_df = (
        df
        .withColumn("Previous_Sales_Amount", lag("Sales_Amount").over(window_spec))
        .withColumn("Sales_Change", col("Sales_Amount") - col("Previous_Sales_Amount"))
        .select(
            "Region", "Product", "Year", "Quarter", "Sales_Amount", "Previous_Sales_Amount", "Sales_Change"
        )
        .orderBy("Region", "Product", "Year", "Quarter")
    )
    return sales_trends_df

sales_trends_df = calculate_sales_trends(sales_window_clean_df)

def test_sales_trends_schema(df):
    """
    Validates the schema of the Sales_Trends DataFrame.

    Args:
        df (DataFrame): Sales_Trends DataFrame

    Returns:
        None
    """
    expected_fields = [
        ("Region", StringType()),
        ("Product", StringType()),
        ("Year", LongType()),
        ("Quarter", LongType()),
        ("Sales_Amount", LongType()),
        ("Previous_Sales_Amount", LongType()),
        ("Sales_Change", LongType())
    ]
    actual_fields = [(f.name, type(f.dataType)()) for f in df.schema.fields]
    assert len(actual_fields) == len(expected_fields), "Column count mismatch in Sales_Trends"
    for (exp_name, exp_type), (act_name, act_type) in zip(expected_fields, actual_fields):
        assert exp_name == act_name, f"Expected column {exp_name}, got {act_name}"
        assert isinstance(act_type, type(exp_type)), f"Expected type {exp_type} for {exp_name}, got {act_type}"

test_sales_trends_schema(sales_trends_df)

def test_sales_trends_nulls(df):
    """
    Validates that Previous_Sales_Amount and Sales_Change are null only for the first quarter of each Region/Product.

    Args:
        df (DataFrame): Sales_Trends DataFrame

    Returns:
        None
    """
    from pyspark.sql.functions import row_number
    window_spec = Window.partitionBy("Region", "Product").orderBy("Year", "Quarter")
    df_with_rownum = df.withColumn("rownum", row_number().over(window_spec))
    first_rows = df_with_rownum.filter(col("rownum") == 1)
    for row in first_rows.collect():
        assert row["Previous_Sales_Amount"] is None, "Previous_Sales_Amount should be null for first quarter"
        assert row["Sales_Change"] is None, "Sales_Change should be null for first quarter"
    non_first_rows = df_with_rownum.filter(col("rownum") > 1)
    for row in non_first_rows.collect():
        assert row["Previous_Sales_Amount"] is not None, "Previous_Sales_Amount should not be null after first quarter"

test_sales_trends_nulls(sales_trends_df)

# -------------------------------
# Top_Selling_Products Calculation Section
# -------------------------------

def calculate_top_selling_products(total_sales_df, top_n=3):
    """
    Identifies top N selling products by Total_Sales_Amount per Region.

    Args:
        total_sales_df (DataFrame): DataFrame with Region, Product, Total_Sales_Amount
        top_n (int): Number of top products to select per region

    Returns:
        DataFrame: Top_Selling_Products DataFrame with columns: Region, Product, Total_Sales_Amount, Product_Rank
    """
    window_spec = Window.partitionBy("Region").orderBy(col("Total_Sales_Amount").desc(), col("Product"))
    ranked_df = (
        total_sales_df
        .withColumn("Product_Rank", dense_rank().over(window_spec))
        .filter(col("Product_Rank") <= top_n)
        .orderBy("Region", "Product_Rank", "Product")
    )
    return ranked_df

top_selling_products_df = calculate_top_selling_products(total_sales_df, top_n=3)

def test_top_selling_products_schema(df):
    """
    Validates the schema of the Top_Selling_Products DataFrame.

    Args:
        df (DataFrame): Top_Selling_Products DataFrame

    Returns:
        None
    """
    expected_fields = [
        ("Region", StringType()),
        ("Product", StringType()),
        ("Total_Sales_Amount", LongType()),
        ("Product_Rank", LongType())
    ]
    actual_fields = [(f.name, type(f.dataType)()) for f in df.schema.fields]
    assert len(actual_fields) == len(expected_fields), "Column count mismatch in Top_Selling_Products"
    for (exp_name, exp_type), (act_name, act_type) in zip(expected_fields, actual_fields):
        assert exp_name == act_name, f"Expected column {exp_name}, got {act_name}"
        assert isinstance(act_type, type(exp_type)), f"Expected type {exp_type} for {exp_name}, got {act_type}"

test_top_selling_products_schema(top_selling_products_df)

def test_top_selling_products_ranking(df):
    """
    Validates that Product_Rank is 1 to N per Region and handles ties correctly.

    Args:
        df (DataFrame): Top_Selling_Products DataFrame

    Returns:
        None
    """
    from pyspark.sql.functions import countDistinct
    for region in [r.Region for r in df.select("Region").distinct().collect()]:
        region_df = df.filter(col("Region") == region)
        ranks = [r.Product_Rank for r in region_df.select("Product_Rank").collect()]
        assert all(1 <= rank <= 3 for rank in ranks), f"Product_Rank out of range in region {region}"
        # Check for ties: if two products have same Total_Sales_Amount, they should have same rank
        sales_to_rank = region_df.select("Total_Sales_Amount", "Product_Rank").distinct().groupBy("Total_Sales_Amount").agg(countDistinct("Product_Rank").alias("rank_count"))
        for row in sales_to_rank.collect():
            assert row["rank_count"] == 1, f"Tie in Total_Sales_Amount not handled correctly in region {region}"

test_top_selling_products_ranking(top_selling_products_df)

# -------------------------------
# Final_KPI Aggregation Section
# -------------------------------

def aggregate_final_kpi(total_sales_df, top_selling_products_df, sales_trends_df):
    """
    Aggregates Total_Sales, Top_Selling_Products, and Sales_Trends into Final_KPI DataFrame.

    Args:
        total_sales_df (DataFrame): Total_Sales DataFrame
        top_selling_products_df (DataFrame): Top_Selling_Products DataFrame
        sales_trends_df (DataFrame): Sales_Trends DataFrame

    Returns:
        DataFrame: Final_KPI DataFrame with all required columns, only for products in Top_Selling_Products
    """
    kpi_df = (
        top_selling_products_df
        .join(total_sales_df, on=["Region", "Product"], how="inner")
        .join(sales_trends_df, on=["Region", "Product"], how="inner")
        .select(
            top_selling_products_df.Region,
            top_selling_products_df.Product,
            total_sales_df.Total_Sales_Amount,
            total_sales_df.Average_Sales_Amount,
            top_selling_products_df.Product_Rank,
            sales_trends_df.Year,
            sales_trends_df.Quarter,
            sales_trends_df.Sales_Amount,
            sales_trends_df.Previous_Sales_Amount,
            sales_trends_df.Sales_Change
        )
        .orderBy("Region", "Product", "Year", "Quarter")
    )
    return kpi_df

final_kpi_df = aggregate_final_kpi(total_sales_df, top_selling_products_df, sales_trends_df)

def test_final_kpi_schema(df):
    """
    Validates the schema of the Final_KPI DataFrame.

    Args:
        df (DataFrame): Final_KPI DataFrame

    Returns:
        None
    """
    expected_fields = [
        ("Region", StringType()),
        ("Product", StringType()),
        ("Total_Sales_Amount", LongType()),
        ("Average_Sales_Amount", DoubleType()),
        ("Product_Rank", LongType()),
        ("Year", LongType()),
        ("Quarter", LongType()),
        ("Sales_Amount", LongType()),
        ("Previous_Sales_Amount", LongType()),
        ("Sales_Change", LongType())
    ]
    actual_fields = [(f.name, type(f.dataType)()) for f in df.schema.fields]
    assert len(actual_fields) == len(expected_fields), "Column count mismatch in Final_KPI"
    for (exp_name, exp_type), (act_name, act_type) in zip(expected_fields, actual_fields):
        assert exp_name == act_name, f"Expected column {exp_name}, got {act_name}"
        assert isinstance(act_type, type(exp_type)), f"Expected type {exp_type} for {exp_name}, got {act_type}"

test_final_kpi_schema(final_kpi_df)

def test_final_kpi_join_keys(df):
    """
    Validates that there are no nulls in join keys (Region, Product) in Final_KPI.

    Args:
        df (DataFrame): Final_KPI DataFrame

    Returns:
        None
    """
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in ["Region", "Product"]
    ]).collect()[0].asDict()
    for col_name, null_count in null_counts.items():
        assert null_count == 0, f"Nulls found in join key {col_name} in Final_KPI"

test_final_kpi_join_keys(final_kpi_df)

# -------------------------------
# Error Handling Tests Section
# -------------------------------

def test_empty_sales_window():
    """
    Tests error handling when the sales_window table is empty.

    Returns:
        None
    """
    empty_df = spark.createDataFrame([], sales_window_schema)
    try:
        cleanse_sales_window_df(empty_df)
        assert empty_df.count() == 0, "Empty DataFrame should have zero rows"
    except Exception as e:
        assert False, f"Unexpected error for empty DataFrame: {e}"

test_empty_sales_window()

def test_invalid_data_types():
    """
    Tests error handling for invalid data types in Sales_Amount, Year, or Quarter.

    Returns:
        None
    """
    invalid_data = [
        Row(Sales_ID=100, Product="WidgetX", Region="North", Year="2023", Quarter=1, Sales_Amount=1000),  # Year as string
        Row(Sales_ID=101, Product="WidgetY", Region="South", Year=2023, Quarter="Q1", Sales_Amount=2000),  # Quarter as string
        Row(Sales_ID=102, Product="WidgetZ", Region="East", Year=2023, Quarter=2, Sales_Amount="TenThousand"),  # Sales_Amount as string
    ]
    try:
        df = spark.createDataFrame(invalid_data, schema=sales_window_schema)
        cleanse_sales_window_df(df)
        assert False, "Invalid data types should raise an error"
    except Exception as e:
        assert "field" in str(e) or "type" in str(e) or "cannot resolve" in str(e), f"Unexpected error message: {e}"

test_invalid_data_types()

# -------------------------------
# Performance Test Section
# -------------------------------

def test_performance_large_dataset():
    """
    Performance test: Ensures calculations complete within reasonable time for large datasets.

    Returns:
        None
    """
    import time  
    # Generate large dataset (100,000 rows)
    large_data = [
        Row(Sales_ID=i, Product=f"Product{i%10}", Region=f"Region{i%5}", Year=2023, Quarter=(i%4)+1, Sales_Amount=1000+i%100)
        for i in range(1, 100001)
    ]
    large_df = spark.createDataFrame(large_data, schema=sales_window_schema)
    cleansed_large_df = cleanse_sales_window_df(large_df)
    start = time.time()
    total_sales_large = calculate_total_sales(cleansed_large_df)
    sales_trends_large = calculate_sales_trends(cleansed_large_df)
    top_selling_large = calculate_top_selling_products(total_sales_large, top_n=3)
    final_kpi_large = aggregate_final_kpi(total_sales_large, top_selling_large, sales_trends_large)
    duration = time.time() - start
    assert duration < 30, f"Performance test failed: took {duration} seconds"
    assert final_kpi_large.count() > 0, "Final_KPI on large dataset should not be empty"

test_performance_large_dataset()

# -------------------------------
# Output Section
# -------------------------------

# Display Total_Sales DataFrame
# Test scenario: Output of Total_Sales calculation
print("=== Total_Sales DataFrame ===")
total_sales_df.show(truncate=False)

# Display Sales_Trends DataFrame
# Test scenario: Output of Sales_Trends calculation
print("=== Sales_Trends DataFrame ===")
sales_trends_df.show(truncate=False)

# Display Final_KPI DataFrame
# Test scenario: Output of Final_KPI aggregation
print("=== Final_KPI DataFrame ===")
final_kpi_df.show(truncate=False)

# spark.stop()  # Do not stop SparkSession in Databricks

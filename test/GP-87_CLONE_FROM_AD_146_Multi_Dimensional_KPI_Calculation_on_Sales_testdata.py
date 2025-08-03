# PySpark script: Advanced Sales KPI Calculation and Test Data Generation for Databricks
# Purpose: Generate comprehensive test data and perform advanced sales KPI calculations on 'purgo_playground.sales_window'
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script generates diverse test data for the 'sales_window' table, including happy path, edge, error, null, and special character scenarios. It then calculates Total Sales, Sales Trends, Top Selling Products, and aggregates them into a Final KPI DataFrame, displaying all outputs as required.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import Row  
from pyspark.sql.types import (StructType, StructField, LongType, StringType)  
from pyspark.sql.functions import (col, sum as _sum, avg, round as _round, lag, dense_rank, isnan, when)  
from pyspark.sql.window import Window  

# -------------------------------
# Test Data Generation Section
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
    # Happy path: Valid data for multiple regions, products, years, quarters
    data = [
        # North region, WidgetA, 2023, all quarters
        Row(Sales_ID=1, Product="WidgetA", Region="North", Year=2023, Quarter=1, Sales_Amount=10000),
        Row(Sales_ID=2, Product="WidgetA", Region="North", Year=2023, Quarter=2, Sales_Amount=12000),
        Row(Sales_ID=3, Product="WidgetA", Region="North", Year=2023, Quarter=3, Sales_Amount=11000),
        Row(Sales_ID=4, Product="WidgetA", Region="North", Year=2023, Quarter=4, Sales_Amount=13000),
        # North region, WidgetB, 2023, all quarters
        Row(Sales_ID=5, Product="WidgetB", Region="North", Year=2023, Quarter=1, Sales_Amount=9000),
        Row(Sales_ID=6, Product="WidgetB", Region="North", Year=2023, Quarter=2, Sales_Amount=9500),
        Row(Sales_ID=7, Product="WidgetB", Region="North", Year=2023, Quarter=3, Sales_Amount=9700),
        Row(Sales_ID=8, Product="WidgetB", Region="North", Year=2023, Quarter=4, Sales_Amount=9800),
        # South region, WidgetC, 2023, all quarters
        Row(Sales_ID=9, Product="WidgetC", Region="South", Year=2023, Quarter=1, Sales_Amount=15000),
        Row(Sales_ID=10, Product="WidgetC", Region="South", Year=2023, Quarter=2, Sales_Amount=15500),
        Row(Sales_ID=11, Product="WidgetC", Region="South", Year=2023, Quarter=3, Sales_Amount=16000),
        Row(Sales_ID=12, Product="WidgetC", Region="South", Year=2023, Quarter=4, Sales_Amount=16500),
        # South region, WidgetD, 2023, all quarters
        Row(Sales_ID=13, Product="WidgetD", Region="South", Year=2023, Quarter=1, Sales_Amount=14000),
        Row(Sales_ID=14, Product="WidgetD", Region="South", Year=2023, Quarter=2, Sales_Amount=14200),
        Row(Sales_ID=15, Product="WidgetD", Region="South", Year=2023, Quarter=3, Sales_Amount=14500),
        Row(Sales_ID=16, Product="WidgetD", Region="South", Year=2023, Quarter=4, Sales_Amount=14800),
        # East region, WidgetE, 2023, all quarters
        Row(Sales_ID=17, Product="WidgetE", Region="East", Year=2023, Quarter=1, Sales_Amount=8000),
        Row(Sales_ID=18, Product="WidgetE", Region="East", Year=2023, Quarter=2, Sales_Amount=8500),
        Row(Sales_ID=19, Product="WidgetE", Region="East", Year=2023, Quarter=3, Sales_Amount=8700),
        Row(Sales_ID=20, Product="WidgetE", Region="East", Year=2023, Quarter=4, Sales_Amount=9000),
        # Edge case: Zero sales
        Row(Sales_ID=21, Product="WidgetF", Region="West", Year=2023, Quarter=1, Sales_Amount=0),
        # Edge case: Very large sales amount
        Row(Sales_ID=22, Product="WidgetF", Region="West", Year=2023, Quarter=2, Sales_Amount=999999999),
        # Edge case: Negative sales amount (error scenario)
        Row(Sales_ID=23, Product="WidgetF", Region="West", Year=2023, Quarter=3, Sales_Amount=-500),
        # NULL handling: Null in Sales_Amount
        Row(Sales_ID=24, Product="WidgetG", Region="North", Year=2023, Quarter=1, Sales_Amount=None),
        # NULL handling: Null in Product
        Row(Sales_ID=25, Product=None, Region="North", Year=2023, Quarter=2, Sales_Amount=5000),
        # NULL handling: Null in Region
        Row(Sales_ID=26, Product="WidgetH", Region=None, Year=2023, Quarter=3, Sales_Amount=6000),
        # NULL handling: Null in Year
        Row(Sales_ID=27, Product="WidgetI", Region="South", Year=None, Quarter=4, Sales_Amount=7000),
        # NULL handling: Null in Quarter
        Row(Sales_ID=28, Product="WidgetJ", Region="East", Year=2023, Quarter=None, Sales_Amount=8000),
        # Special characters in Product and Region
        Row(Sales_ID=29, Product="WidgétΩ", Region="Nørth", Year=2023, Quarter=1, Sales_Amount=12345),
        Row(Sales_ID=30, Product="ウィジェットK", Region="南", Year=2023, Quarter=2, Sales_Amount=54321),
        # Error case: Out-of-range quarter
        Row(Sales_ID=31, Product="WidgetL", Region="South", Year=2023, Quarter=5, Sales_Amount=1000),
        # Error case: Out-of-range year
        Row(Sales_ID=32, Product="WidgetM", Region="East", Year=1899, Quarter=1, Sales_Amount=2000),
        # Error case: Non-numeric Sales_Amount (will be handled as null)
        Row(Sales_ID=33, Product="WidgetN", Region="West", Year=2023, Quarter=2, Sales_Amount=None),
    ]
    return data

# Define schema for sales_window table
sales_window_schema = StructType([
    StructField("Sales_ID", LongType(), True),
    StructField("Product", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Year", LongType(), True),
    StructField("Quarter", LongType(), True),
    StructField("Sales_Amount", LongType(), True)
])

# Try to create DataFrame, handle missing/invalid data gracefully
try:
    sales_window_test_data = generate_sales_window_test_data()
    sales_window_df = spark.createDataFrame(sales_window_test_data, schema=sales_window_schema)
except Exception as e:
    raise RuntimeError("Failed to generate or load test data for sales_window: {}".format(str(e)))

# -------------------------------
# Data Cleansing Section
# -------------------------------

def cleanse_sales_window_df(df):
    """
    Cleanses the sales_window DataFrame by:
    - Removing rows with nulls in critical columns (Product, Region, Year, Quarter, Sales_Amount)
    - Ensuring correct data types for numeric columns

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: Cleansed DataFrame
    """
    # Remove rows with nulls in any critical column
    cleansed_df = df.filter(
        col("Product").isNotNull() &
        col("Region").isNotNull() &
        col("Year").isNotNull() &
        col("Quarter").isNotNull() &
        col("Sales_Amount").isNotNull()
    )
    # Remove rows with non-numeric or out-of-range values for Year, Quarter, Sales_Amount
    cleansed_df = cleansed_df.filter(
        (col("Year") >= 1900) & (col("Year") <= 2100) &
        (col("Quarter") >= 1) & (col("Quarter") <= 4)
    )
    return cleansed_df

sales_window_clean_df = cleanse_sales_window_df(sales_window_df)

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

# Calculate Total_Sales DataFrame
total_sales_df = calculate_total_sales(sales_window_clean_df)

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

# Calculate Sales_Trends DataFrame
sales_trends_df = calculate_sales_trends(sales_window_clean_df)

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

# Calculate Top_Selling_Products DataFrame
top_selling_products_df = calculate_top_selling_products(total_sales_df, top_n=3)

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
    # Join keys: Region, Product
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

# Calculate Final_KPI DataFrame
final_kpi_df = aggregate_final_kpi(total_sales_df, top_selling_products_df, sales_trends_df)

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

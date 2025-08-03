spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script: Advanced Multi-Dimensional Sales KPI Calculation for Databricks
# Purpose: Calculate Total Sales, Sales Trends, Top Selling Products, and aggregate Final KPI from 'purgo_playground.sales_window'
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script reads the 'sales_window' table from Unity Catalog, cleanses the data, computes Total Sales and Average Sales by Region and Product, calculates quarter-over-quarter sales trends, identifies top 3 selling products per region, and aggregates these KPIs into a final DataFrame. All outputs are displayed as DataFrames. The script includes robust error handling, data validation, and is optimized for Databricks best practices.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import StructType, StructField, LongType, StringType  
from pyspark.sql.functions import col, sum as _sum, avg, round as _round, lag, dense_rank  
from pyspark.sql.window import Window  

# -------------------------------
# Data Loading and Validation Section
# -------------------------------

def load_sales_window_table():
    """
    Loads the 'purgo_playground.sales_window' table from Unity Catalog.
    Performs existence and emptiness checks.

    Returns:
        DataFrame: Loaded DataFrame

    Raises:
        RuntimeError: If table does not exist or is empty
    """
    try:
        df = spark.table("purgo_playground.sales_window")
    except Exception as e:
        raise RuntimeError("Source table purgo_playground.sales_window does not exist or contains no data")
    if df.head(1) == []:
        raise RuntimeError("Source table purgo_playground.sales_window does not exist or contains no data")
    return df

# Load source data with error handling
try:
    sales_window_df = load_sales_window_table()
except RuntimeError as err:
    print(str(err))
    # Early exit if table is missing or empty
    raise

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

    Raises:
        RuntimeError: If invalid data types are detected
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
    try:
        cleansed_df = cleansed_df.filter(
            (col("Year") >= 1900) & (col("Year") <= 2100) &
            (col("Quarter") >= 1) & (col("Quarter") <= 4)
        )
    except Exception as e:
        raise RuntimeError("Invalid data type detected in Sales_Amount, Year, or Quarter columns")
    # Validate numeric types
    dtypes = dict(cleansed_df.dtypes)
    for field in ["Year", "Quarter", "Sales_Amount"]:
        if dtypes.get(field) not in ("bigint", "int", "long", "double", "float"):
            raise RuntimeError("Invalid data type detected in Sales_Amount, Year, or Quarter columns")
    return cleansed_df

# Cleanse data and handle invalid types
try:
    sales_window_clean_df = cleanse_sales_window_df(sales_window_df)
except RuntimeError as err:
    print(str(err))
    raise

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
    # Exclude rows with null join keys (Region, Product)
    kpi_df = kpi_df.filter(col("Region").isNotNull() & col("Product").isNotNull())
    return kpi_df

# Calculate Final_KPI DataFrame
final_kpi_df = aggregate_final_kpi(total_sales_df, top_selling_products_df, sales_trends_df)

# -------------------------------
# Output Section
# -------------------------------

# Display Total_Sales DataFrame
# Output: Total_Sales by Region and Product
print("=== Total_Sales DataFrame ===")
total_sales_df.show(truncate=False)

# Display Sales_Trends DataFrame
# Output: Sales Trends by Region, Product, Year, Quarter
print("=== Sales_Trends DataFrame ===")
sales_trends_df.show(truncate=False)

# Display Final_KPI DataFrame
# Output: Aggregated Final KPI for Top Selling Products
print("=== Final_KPI DataFrame ===")
final_kpi_df.show(truncate=False)

# spark.stop()  # Do not stop SparkSession in Databricks

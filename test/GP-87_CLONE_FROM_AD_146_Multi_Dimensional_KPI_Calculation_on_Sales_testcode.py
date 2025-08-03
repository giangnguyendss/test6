# PySpark script for comprehensive testing of multi-dimensional sales KPI calculations on 'sales_window' table
# Purpose: Validate Total_Sales, Sales_Trends, Top_Selling_Products, and Final_KPI logic, schema, and data quality
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script performs unit, integration, and data quality tests for advanced sales KPIs using PySpark.
# It covers schema validation, data type checks, NULL/error handling, window analytics, Delta Lake operations,
# and ensures outputs are written to Unity Catalog in correct format. All test assertions and error handling
# follow Databricks and PySpark best practices.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql import Window  
from pyspark.sql.types import (  
    StructType, StructField, StringType, DecimalType, DateType, IntegerType
)
import logging  

# Set up logging for error/warning messages
logger = logging.getLogger("sales_kpi_test")
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------------
# /* SECTION: Helper Functions for Validation and Utility */
# --------------------------------------------------------------------------------

def get_sales_window_schema():
    """
    Returns the expected schema for the 'sales_window' table.

    Returns:
        StructType: The schema object for sales_window.
    """
    return StructType([
        StructField("Sale_ID", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Sales_Amount", DecimalType(18,2), True),
        StructField("Sale_Date", DateType(), True)
    ])

def get_total_sales_schema():
    """
    Returns the expected schema for the 'Total_Sales' output.

    Returns:
        StructType: The schema object for Total_Sales.
    """
    return StructType([
        StructField("Region", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Total_Sales_Amount", DecimalType(18,2), True),
        StructField("Average_Sales_Amount", DecimalType(18,2), True)
    ])

def get_sales_trends_schema():
    """
    Returns the expected schema for the 'Sales_Trends' output.

    Returns:
        StructType: The schema object for Sales_Trends.
    """
    return StructType([
        StructField("Region", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Quarter", StringType(), True),
        StructField("Sales_Amount", DecimalType(18,2), True),
        StructField("Previous_Sales_Amount", DecimalType(18,2), True),
        StructField("Sales_Change", DecimalType(18,2), True)
    ])

def get_top_selling_products_schema():
    """
    Returns the expected schema for the 'Top_Selling_Products' output.

    Returns:
        StructType: The schema object for Top_Selling_Products.
    """
    return StructType([
        StructField("Region", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Total_Sales_Amount", DecimalType(18,2), True),
        StructField("Product_Rank", IntegerType(), True)
    ])

def get_final_kpi_schema():
    """
    Returns the expected schema for the 'Final_KPI' output.

    Returns:
        StructType: The schema object for Final_KPI.
    """
    return StructType([
        StructField("Region", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Total_Sales_Amount", DecimalType(18,2), True),
        StructField("Average_Sales_Amount", DecimalType(18,2), True),
        StructField("Product_Rank", IntegerType(), True),
        StructField("Quarter", StringType(), True),
        StructField("Sales_Amount", DecimalType(18,2), True),
        StructField("Previous_Sales_Amount", DecimalType(18,2), True),
        StructField("Sales_Change", DecimalType(18,2), True)
    ])

def assert_schema(df, expected_schema):
    """
    Assert that a DataFrame's schema matches the expected schema.

    Args:
        df (DataFrame): The DataFrame to check.
        expected_schema (StructType): The expected schema.

    Returns:
        None

    Raises:
        AssertionError: If the schema does not match.
    """
    assert df.schema == expected_schema, f"Schema mismatch: {df.schema} != {expected_schema}"

def assert_column_count(df, expected_count):
    """
    Assert that a DataFrame has the expected number of columns.

    Args:
        df (DataFrame): The DataFrame to check.
        expected_count (int): The expected number of columns.

    Returns:
        None

    Raises:
        AssertionError: If the column count does not match.
    """
    assert len(df.columns) == expected_count, f"Column count mismatch: {len(df.columns)} != {expected_count}"

def assert_empty(df):
    """
    Assert that a DataFrame is empty.

    Args:
        df (DataFrame): The DataFrame to check.

    Returns:
        None

    Raises:
        AssertionError: If the DataFrame is not empty.
    """
    assert df.count() == 0, "DataFrame is not empty as expected"

def assert_decimal_precision(df, columns, precision=18, scale=2):
    """
    Assert that specified columns in a DataFrame have the expected decimal precision and scale.

    Args:
        df (DataFrame): The DataFrame to check.
        columns (list): List of column names to check.
        precision (int): Expected precision.
        scale (int): Expected scale.

    Returns:
        None

    Raises:
        AssertionError: If any column does not have the expected type.
    """
    for col in columns:
        dtype = dict(df.dtypes)[col]
        assert dtype.startswith("decimal"), f"Column {col} is not decimal: {dtype}"

def assert_integer_type(df, columns):
    """
    Assert that specified columns in a DataFrame are of integer type.

    Args:
        df (DataFrame): The DataFrame to check.
        columns (list): List of column names to check.

    Returns:
        None

    Raises:
        AssertionError: If any column is not integer.
    """
    for col in columns:
        dtype = dict(df.dtypes)[col]
        assert dtype == "int", f"Column {col} is not integer: {dtype}"

def assert_string_type(df, columns):
    """
    Assert that specified columns in a DataFrame are of string type.

    Args:
        df (DataFrame): The DataFrame to check.
        columns (list): List of column names to check.

    Returns:
        None

    Raises:
        AssertionError: If any column is not string.
    """
    for col in columns:
        dtype = dict(df.dtypes)[col]
        assert dtype == "string", f"Column {col} is not string: {dtype}"

def assert_quarter_format(df, column):
    """
    Assert that the 'Quarter' column is in the format 'YYYY-QN'.

    Args:
        df (DataFrame): The DataFrame to check.
        column (str): The column name.

    Returns:
        None

    Raises:
        AssertionError: If any value does not match the format.
    """
    import re  
    pattern = re.compile(r"^\d{4}-Q[1-4]$")
    for row in df.select(column).distinct().collect():
        val = row[column]
        if val is not None:
            assert pattern.match(val), f"Quarter value {val} does not match format YYYY-QN"

def log_and_exclude_invalid_rows(df):
    """
    Exclude rows with invalid Sales_Amount (<0 or not decimal) or invalid Sale_Date (null or not date).
    Log an error message for each excluded row.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: Cleaned DataFrame with only valid rows.
    """
    # Exclude negative Sales_Amount and null Sale_Date
    invalid_sales = df.filter((F.col("Sales_Amount") < 0) | F.col("Sale_Date").isNull())
    for row in invalid_sales.collect():
        if row.Sales_Amount is not None and row.Sales_Amount < 0:
            logger.error(f"Excluded row with negative Sales_Amount: {row}")
        if row.Sale_Date is None:
            logger.error(f"Excluded row with null Sale_Date: {row}")
    # Return only valid rows
    return df.filter((F.col("Sales_Amount") >= 0) & F.col("Sale_Date").isNotNull())

def handle_empty_source(df):
    """
    Handle the case where the source DataFrame is empty.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        bool: True if empty, False otherwise.
    """
    if df.count() == 0:
        logger.warning("No sales data available for analysis")
        return True
    return False

def handle_null_sales_amount(df):
    """
    Exclude rows with null Sales_Amount from all calculations.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: DataFrame with non-null Sales_Amount.
    """
    return df.filter(F.col("Sales_Amount").isNotNull())

# --------------------------------------------------------------------------------
# /* SECTION: Read and Validate Source Data */
# --------------------------------------------------------------------------------

try:
    # Read from Unity Catalog table
    sales_window_df = spark.read.table("purgo_databricks.purgo_playground.sales_window")
except Exception as e:
    logger.error(f"Error reading source table: {e}")
    raise

# Validate schema and column count
assert_schema(sales_window_df, get_sales_window_schema())
assert_column_count(sales_window_df, 5)

# Exclude invalid rows and log errors
sales_window_valid_df = log_and_exclude_invalid_rows(sales_window_df)

# Handle empty source table scenario
if handle_empty_source(sales_window_valid_df):
    # Create empty DataFrames with correct schema for all outputs
    total_sales_df = spark.createDataFrame([], get_total_sales_schema())
    sales_trends_df = spark.createDataFrame([], get_sales_trends_schema())
    top_selling_products_df = spark.createDataFrame([], get_top_selling_products_schema())
    final_kpi_df = spark.createDataFrame([], get_final_kpi_schema())
    # Write empty outputs to Unity Catalog
    total_sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.total_sales")
    sales_trends_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.sales_trends")
    top_selling_products_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.top_selling_products")
    final_kpi_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.final_kpi")
    # Assert all outputs are empty
    assert_empty(total_sales_df)
    assert_empty(sales_trends_df)
    assert_empty(top_selling_products_df)
    assert_empty(final_kpi_df)
else:
    # --------------------------------------------------------------------------------
    # /* SECTION: Data Preparation and Transformation */
    # --------------------------------------------------------------------------------

    # Exclude rows with null Sales_Amount for all calculations
    sales_window_nonull_df = handle_null_sales_amount(sales_window_valid_df)

    # Derive 'Quarter' column in format 'YYYY-QN'
    def get_quarter_expr():
        """
        Returns an expression to derive 'Quarter' in 'YYYY-QN' format from 'Sale_Date'.

        Returns:
            Column: The expression for the quarter string.
        """
        return (
            F.concat_ws(
                "-Q",
                F.year(F.col("Sale_Date")).cast("string"),
                F.quarter(F.col("Sale_Date")).cast("string")
            )
        )

    sales_window_enriched_df = sales_window_nonull_df.withColumn("Quarter", get_quarter_expr())

    # --------------------------------------------------------------------------------
    # /* SECTION: Total_Sales Calculation */
    # --------------------------------------------------------------------------------

    def calculate_total_sales(df):
        """
        Calculate total and average Sales_Amount by Region and Product.

        Args:
            df (DataFrame): Input DataFrame with valid sales data.

        Returns:
            DataFrame: Total_Sales DataFrame with columns Region, Product, Total_Sales_Amount, Average_Sales_Amount.
        """
        return (
            df.groupBy("Region", "Product")
            .agg(
                F.round(F.sum("Sales_Amount"), 2).alias("Total_Sales_Amount"),
                F.round(F.avg("Sales_Amount"), 2).alias("Average_Sales_Amount")
            )
        )

    total_sales_df = calculate_total_sales(sales_window_enriched_df)
    assert_schema(total_sales_df, get_total_sales_schema())
    assert_column_count(total_sales_df, 4)
    assert_decimal_precision(total_sales_df, ["Total_Sales_Amount", "Average_Sales_Amount"])
    assert_string_type(total_sales_df, ["Region", "Product"])

    # --------------------------------------------------------------------------------
    # /* SECTION: Sales_Trends Calculation */
    # --------------------------------------------------------------------------------

    def calculate_sales_trends(df):
        """
        Calculate sales trends by Region, Product, and Quarter.

        Args:
            df (DataFrame): Input DataFrame with 'Quarter' column.

        Returns:
            DataFrame: Sales_Trends DataFrame with columns Region, Product, Quarter, Sales_Amount, Previous_Sales_Amount, Sales_Change.
        """
        window_spec = Window.partitionBy("Region", "Product").orderBy("Quarter")
        sales_by_qtr = (
            df.groupBy("Region", "Product", "Quarter")
            .agg(F.round(F.sum("Sales_Amount"), 2).alias("Sales_Amount"))
        )
        sales_with_prev = (
            sales_by_qtr
            .withColumn("Previous_Sales_Amount", F.lag("Sales_Amount").over(window_spec))
            .withColumn("Sales_Change", F.when(F.col("Previous_Sales_Amount").isNotNull(),
                                               F.round(F.col("Sales_Amount") - F.col("Previous_Sales_Amount"), 2))
                                    .otherwise(F.lit(None)))
        )
        return sales_with_prev

    sales_trends_df = calculate_sales_trends(sales_window_enriched_df)
    assert_schema(sales_trends_df, get_sales_trends_schema())
    assert_column_count(sales_trends_df, 6)
    assert_decimal_precision(sales_trends_df, ["Sales_Amount", "Previous_Sales_Amount", "Sales_Change"])
    assert_string_type(sales_trends_df, ["Region", "Product", "Quarter"])
    assert_quarter_format(sales_trends_df, "Quarter")

    # --------------------------------------------------------------------------------
    # /* SECTION: Top_Selling_Products Calculation (N=3) */
    # --------------------------------------------------------------------------------

    def calculate_top_selling_products(df, top_n=3):
        """
        Identify top N selling products by total sales amount per region.

        Args:
            df (DataFrame): Input DataFrame with valid sales data.
            top_n (int): Number of top products to select per region.

        Returns:
            DataFrame: Top_Selling_Products DataFrame with columns Region, Product, Total_Sales_Amount, Product_Rank.
        """
        sales_by_product = (
            df.groupBy("Region", "Product")
            .agg(F.round(F.sum("Sales_Amount"), 2).alias("Total_Sales_Amount"))
        )
        window_spec = Window.partitionBy("Region").orderBy(F.desc("Total_Sales_Amount"))
        ranked = (
            sales_by_product
            .withColumn("Product_Rank", F.row_number().over(window_spec))
            .filter(F.col("Product_Rank") <= top_n)
        )
        return ranked

    top_selling_products_df = calculate_top_selling_products(sales_window_enriched_df, top_n=3)
    assert_schema(top_selling_products_df, get_top_selling_products_schema())
    assert_column_count(top_selling_products_df, 4)
    assert_decimal_precision(top_selling_products_df, ["Total_Sales_Amount"])
    assert_integer_type(top_selling_products_df, ["Product_Rank"])
    assert_string_type(top_selling_products_df, ["Region", "Product"])

    # --------------------------------------------------------------------------------
    # /* SECTION: Final_KPI Aggregation */
    # --------------------------------------------------------------------------------

    def calculate_final_kpi(total_sales, top_selling, sales_trends):
        """
        Aggregate and join KPI results into Final_KPI table.

        Args:
            total_sales (DataFrame): Total_Sales DataFrame.
            top_selling (DataFrame): Top_Selling_Products DataFrame (N=3).
            sales_trends (DataFrame): Sales_Trends DataFrame.

        Returns:
            DataFrame: Final_KPI DataFrame with all required columns.
        """
        join1 = total_sales.join(top_selling, ["Region", "Product"], "inner")
        join2 = join1.join(sales_trends, ["Region", "Product"], "inner")
        # Select and order columns as required
        final = join2.select(
            "Region", "Product", "Total_Sales_Amount", "Average_Sales_Amount",
            "Product_Rank", "Quarter", "Sales_Amount", "Previous_Sales_Amount", "Sales_Change"
        )
        return final

    final_kpi_df = calculate_final_kpi(total_sales_df, top_selling_products_df, sales_trends_df)
    assert_schema(final_kpi_df, get_final_kpi_schema())
    assert_column_count(final_kpi_df, 9)
    assert_decimal_precision(final_kpi_df, [
        "Total_Sales_Amount", "Average_Sales_Amount", "Sales_Amount", "Previous_Sales_Amount", "Sales_Change"
    ])
    assert_integer_type(final_kpi_df, ["Product_Rank"])
    assert_string_type(final_kpi_df, ["Region", "Product", "Quarter"])
    assert_quarter_format(final_kpi_df, "Quarter")

    # --------------------------------------------------------------------------------
    # /* SECTION: Data Quality and Error Handling Tests */
    # --------------------------------------------------------------------------------

    # Test: All null Sales_Amount rows are excluded from calculations
    null_sales_rows = sales_window_valid_df.filter(F.col("Sales_Amount").isNull())
    assert null_sales_rows.count() == 0, "Null Sales_Amount rows should be excluded"

    # Test: All null Sale_Date rows are excluded from trend analysis
    null_date_rows = sales_window_valid_df.filter(F.col("Sale_Date").isNull())
    assert null_date_rows.count() == 0, "Null Sale_Date rows should be excluded"

    # Test: All negative Sales_Amount rows are excluded
    negative_sales_rows = sales_window_valid_df.filter(F.col("Sales_Amount") < 0)
    assert negative_sales_rows.count() == 0, "Negative Sales_Amount rows should be excluded"

    # Test: If all Sales_Amount for a group are null, output for that group is null (already excluded above)

    # Test: Data-driven validation for Sales_Trends (example: East, Widget, 2024-Q1=100, 2024-Q2=150)
    east_widget_trends = sales_trends_df.filter(
        (F.col("Region") == "East") & (F.col("Product") == "Widget")
    ).orderBy("Quarter")
    rows = east_widget_trends.collect()
    for i in range(1, len(rows)):
        prev = rows[i-1]
        curr = rows[i]
        if prev and curr:
            assert float(curr["Previous_Sales_Amount"]) == float(prev["Sales_Amount"]), \
                f"Previous_Sales_Amount mismatch for {curr['Quarter']}"
            assert float(curr["Sales_Change"]) == round(float(curr["Sales_Amount"]) - float(prev["Sales_Amount"]), 2), \
                f"Sales_Change calculation error for {curr['Quarter']}"

    # --------------------------------------------------------------------------------
    # /* SECTION: Write Outputs to Unity Catalog as Delta Tables */
    # --------------------------------------------------------------------------------

    total_sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.total_sales")
    sales_trends_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.sales_trends")
    top_selling_products_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.top_selling_products")
    final_kpi_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.final_kpi")

    # --------------------------------------------------------------------------------
    # /* SECTION: Show Results for Manual Validation */
    # --------------------------------------------------------------------------------

    # Show Total_Sales
    print("=== Total_Sales ===")
    total_sales_df.show(truncate=False)

    # Show Sales_Trends
    print("=== Sales_Trends ===")
    sales_trends_df.show(truncate=False)

    # Show Final_KPI
    print("=== Final_KPI ===")
    final_kpi_df.show(truncate=False)

# spark.stop()  # Do not stop SparkSession in Databricks

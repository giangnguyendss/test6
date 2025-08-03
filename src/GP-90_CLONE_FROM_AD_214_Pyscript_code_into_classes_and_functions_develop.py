spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script: Refactored Sales KPI pipeline using reusable classes and functions for Databricks
# Purpose: Compute sales KPIs (YoY Growth, Penetration Flag, Sales Rank, etc.) for products using Unity Catalog tables
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script restructures the sales KPI pipeline into maintainable, reusable classes and functions.
#              It loads data from Unity Catalog tables, processes and aggregates sales, product, and market share data,
#              and writes the results to purgo_playground.sales_kpi. All business logic and data quality rules are preserved.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import DataFrame  
from pyspark.sql import functions as F  
from pyspark.sql.types import (  
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType
)
from pyspark.sql.window import Window  

# -------------------------------
# Pipeline Configuration
# -------------------------------

PRODUCT_DATA_TABLE = "purgo_playground.product_data"
PRODUCT_SALES_DATA_TABLE = "purgo_playground.product_sales_data"
PRODUCT_MARKETSHARE_DATA_TABLE = "purgo_playground.product_marketshare_data"
SALES_KPI_TABLE = "purgo_playground.sales_kpi"

# -------------------------------
# Utility Functions
# -------------------------------

def safe_read_table(table_name: str) -> DataFrame:
    """
    Safely read a Unity Catalog table as a DataFrame.
    Args:
        table_name (str): Fully qualified table name.
    Returns:
        DataFrame: Loaded DataFrame, or empty DataFrame if error.
    """
    try:
        return spark.read.table(table_name)
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")
        return spark.createDataFrame([], StructType([]))

def safe_write_table(df: DataFrame, table_name: str):
    """
    Safely write a DataFrame to a Unity Catalog table in overwrite mode.
    Args:
        df (DataFrame): DataFrame to write.
        table_name (str): Fully qualified table name.
    """
    try:
        df.write.mode("overwrite").saveAsTable(table_name)
    except Exception as e:
        print(f"Error writing to table {table_name}: {e}")

# -------------------------------
# Sales KPI Pipeline Class
# -------------------------------

class SalesKpiPipeline:
    """
    Encapsulates the sales KPI pipeline logic for maintainability and reuse.
    """

    def __init__(self):
        """
        Initialize the pipeline by loading input DataFrames from Unity Catalog.
        """
        self.product_df = safe_read_table(PRODUCT_DATA_TABLE)
        self.sales_df = safe_read_table(PRODUCT_SALES_DATA_TABLE)
        self.market_share_df = safe_read_table(PRODUCT_MARKETSHARE_DATA_TABLE)

    def preprocess_sales(self) -> DataFrame:
        """
        Filter out rows with null sales_amount and add sales_year column.
        Returns:
            DataFrame: Preprocessed sales DataFrame.
        """
        return (
            self.sales_df
            .filter(F.col("sales_amount").isNotNull())
            .withColumn("sales_year", F.year("sales_date"))
        )

    def join_product(self, sales_df: DataFrame) -> DataFrame:
        """
        Join sales DataFrame with product DataFrame on product_id.
        Args:
            sales_df (DataFrame): The sales DataFrame.
        Returns:
            DataFrame: Joined DataFrame with product info.
        """
        return (
            sales_df
            .join(self.product_df, sales_df.sales_product_id == self.product_df.product_id, "left")
            .drop(self.product_df.product_id)
        )

    def join_market_share(self, sales_product_df: DataFrame) -> DataFrame:
        """
        Join sales-product DataFrame with market share DataFrame on product_id.
        Args:
            sales_product_df (DataFrame): The sales-product DataFrame.
        Returns:
            DataFrame: Joined DataFrame with market share info.
        """
        ms_df = self.market_share_df.withColumnRenamed("product_id", "ms_product_id")
        return (
            sales_product_df
            .join(ms_df, sales_product_df.sales_product_id == ms_df.ms_product_id, "left")
            .drop("ms_product_id")
        )

    def aggregate_kpis(self, joined_df: DataFrame) -> DataFrame:
        """
        Aggregate KPIs: total_sales, prev_year_sales, yoy_growth_pct, avg_market_share, market_penetration_flag, sales_rank.
        Args:
            joined_df (DataFrame): The joined DataFrame.
        Returns:
            DataFrame: Aggregated KPI DataFrame.
        """
        window_spec = Window.partitionBy("sales_product_id").orderBy("sales_year")
        rank_window = Window.partitionBy("sales_year").orderBy(F.col("total_sales").desc())

        agg_df = (
            joined_df
            .groupBy("sales_product_id", "sales_year", "product_name", "market_segment")
            .agg(
                F.sum("sales_amount").alias("total_sales"),
                F.round(F.avg("market_share_pct"), 2).alias("avg_market_share")
            )
            .withColumn("prev_year_sales", F.lag("total_sales", 1).over(window_spec))
            .withColumn(
                "yoy_growth_pct",
                F.round(
                    ((F.col("total_sales") - F.col("prev_year_sales")) / F.col("prev_year_sales")) * 100, 2
                )
            )
            .withColumn(
                "market_penetration_flag",
                F.when(F.col("avg_market_share") > 25, F.lit("High"))
                .when((F.col("avg_market_share") <= 25) & (F.col("avg_market_share") >= 10), F.lit("Medium"))
                .otherwise(F.lit("Low"))
            )
            .withColumn("sales_rank", F.row_number().over(rank_window))
        )
        return agg_df

    def get_final_df(self) -> DataFrame:
        """
        Run the full pipeline and return the final DataFrame with all KPIs.
        Returns:
            DataFrame: Final DataFrame with all KPIs.
        """
        sales_pre = self.preprocess_sales()
        sales_prod = self.join_product(sales_pre)
        sales_market = self.join_market_share(sales_prod)
        agg_df = self.aggregate_kpis(sales_market)
        final_df = agg_df.select(
            "sales_year", "sales_product_id", "product_name", "market_segment",
            "total_sales", "prev_year_sales", "yoy_growth_pct",
            "avg_market_share", "market_penetration_flag", "sales_rank"
        )
        return final_df

    def write_output(self, df: DataFrame):
        """
        Write the final DataFrame to the output Unity Catalog table.
        Args:
            df (DataFrame): The DataFrame to write.
        """
        safe_write_table(df, SALES_KPI_TABLE)

# -------------------------------
# Main Pipeline Execution
# -------------------------------

def main():
    """
    Main function to execute the sales KPI pipeline.
    """
    pipeline = SalesKpiPipeline()
    final_df = pipeline.get_final_df()
    # Display the DataFrame in Databricks notebook
    display(final_df)
    pipeline.write_output(final_df)

# -------------------------------
# Run the pipeline
# -------------------------------

if __name__ == "__main__":
    main()
# End of script

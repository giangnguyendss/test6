spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script: Refactored Sales KPI pipeline using reusable classes and functions for Databricks
# Purpose: Compute sales KPIs (YoY Growth, Penetration Flag, Sales Rank, etc.) for products using Unity Catalog tables
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script restructures the sales KPI pipeline into maintainable, reusable classes and functions.
#              It loads product, sales, and market share data from Unity Catalog, processes and joins them,
#              computes all required KPIs, and writes the results to purgo_playground.sales_kpi.
#              All business logic and data quality rules are preserved from the original script.

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import DataFrame  
from pyspark.sql import functions as F  
from pyspark.sql.window import Window  

# -------------------------------
# Pipeline Class and Functions
# -------------------------------

class SalesKpiPipeline:
    """
    SalesKpiPipeline encapsulates the logic for computing product sales KPIs.
    
    Methods:
        load_data(): Loads input DataFrames from Unity Catalog.
        preprocess_sales(sales_df): Filters and prepares sales data.
        join_product(sales_df, product_df): Joins sales with product info.
        join_market_share(sales_product_df, market_share_df): Joins with market share info.
        aggregate_kpis(joined_df): Aggregates and computes all KPIs.
        get_final_df(): Runs the full pipeline and returns the final DataFrame.
        write_output(df): Writes the final DataFrame to the output Unity Catalog table.
    """

    def __init__(self):
        """
        Initialize the pipeline and load all required input DataFrames.
        """
        self.product_df, self.sales_df, self.market_share_df = self.load_data()

    @staticmethod
    def load_data():
        """
        Load product, sales, and market share DataFrames from Unity Catalog.
        
        Returns:
            tuple: (product_df, sales_df, market_share_df)
        """
        product_df = spark.read.table("purgo_playground.product_data")
        sales_df = spark.read.table("purgo_playground.product_sales_data")
        market_share_df = spark.read.table("purgo_playground.product_marketshare_data")
        return product_df, sales_df, market_share_df

    @staticmethod
    def preprocess_sales(sales_df: DataFrame) -> DataFrame:
        """
        Filter out rows with null sales_amount and add sales_year column.
        
        Args:
            sales_df (DataFrame): Raw sales DataFrame.
        Returns:
            DataFrame: Preprocessed sales DataFrame.
        """
        return (
            sales_df
            .filter(F.col("sales_amount").isNotNull())
            .withColumn("sales_year", F.year(F.col("sales_date")))
        )

    @staticmethod
    def join_product(sales_df: DataFrame, product_df: DataFrame) -> DataFrame:
        """
        Join sales DataFrame with product DataFrame on product_id.
        
        Args:
            sales_df (DataFrame): Preprocessed sales DataFrame.
            product_df (DataFrame): Product DataFrame.
        Returns:
            DataFrame: Joined DataFrame with product info.
        """
        return (
            sales_df
            .join(product_df, sales_df.sales_product_id == product_df.product_id, "left")
            .drop(product_df.product_id)
        )

    @staticmethod
    def join_market_share(sales_product_df: DataFrame, market_share_df: DataFrame) -> DataFrame:
        """
        Join sales-product DataFrame with market share DataFrame.
        
        Args:
            sales_product_df (DataFrame): DataFrame after joining with product info.
            market_share_df (DataFrame): Market share DataFrame.
        Returns:
            DataFrame: Joined DataFrame with market share info.
        """
        ms_df = market_share_df.withColumnRenamed("product_id", "ms_product_id")
        return (
            sales_product_df
            .join(ms_df, sales_product_df.sales_product_id == ms_df.ms_product_id, "left")
            .drop("ms_product_id")
        )

    @staticmethod
    def aggregate_kpis(joined_df: DataFrame) -> DataFrame:
        """
        Aggregate KPIs: total_sales, prev_year_sales, yoy_growth_pct, avg_market_share,
        market_penetration_flag, and sales_rank.
        
        Args:
            joined_df (DataFrame): DataFrame after joining all sources.
        Returns:
            DataFrame: Aggregated KPI DataFrame.
        """
        # Window for YoY and lag
        window_spec = Window.partitionBy("sales_product_id").orderBy("sales_year")
        # Window for sales rank per year
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
            DataFrame: Final DataFrame with all required columns.
        """
        sales_pre = self.preprocess_sales(self.sales_df)
        sales_prod = self.join_product(sales_pre, self.product_df)
        sales_market = self.join_market_share(sales_prod, self.market_share_df)
        agg_df = self.aggregate_kpis(sales_market)
        final_df = agg_df.select(
            "sales_year", "sales_product_id", "product_name", "market_segment",
            "total_sales", "prev_year_sales", "yoy_growth_pct",
            "avg_market_share", "market_penetration_flag", "sales_rank"
        )
        return final_df

    @staticmethod
    def write_output(df: DataFrame):
        """
        Write the final DataFrame to the output Unity Catalog table with overwrite mode.
        
        Args:
            df (DataFrame): The DataFrame to write.
        """
        df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi")

# -------------------------------
# Pipeline Execution
# -------------------------------

def main():
    """
    Main function to execute the sales KPI pipeline.
    """
    pipeline = SalesKpiPipeline()
    final_df = pipeline.get_final_df()
    # Optionally display the DataFrame in Databricks notebook
    display(final_df)
    pipeline.write_output(final_df)

# Run the pipeline
main()

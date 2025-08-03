spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script: Cold chain temperature violation detection and aggregation (Databricks)
# Purpose: Detect temperature violations in cold chain data, annotate violations with reasons, and aggregate by location
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script reads cold chain temperature data from Unity Catalog, flags violations based on compliance range, provides violation reasons, writes detailed results to a violations table, and aggregates by location for average temperature and violation count. It includes robust error handling for missing/invalid data and ensures schema consistency.

from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType  
from pyspark.sql.functions import col, when, lit, isnan, avg, count, round as pyspark_round  

# -----------------------------------------------------------
# Output Table Schemas (for reference and schema enforcement)
# -----------------------------------------------------------

violations_schema = StructType([
    StructField("ReadingID", LongType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Location", StringType(), True),
    StructField("violation", StringType(), True),
    StructField("violation_reason", StringType(), True)
])

location_agg_schema = StructType([
    StructField("Location", StringType(), True),
    StructField("avg_temperature", DoubleType(), True),
    StructField("violation_count", LongType(), True)
])

# -----------------------------------------------------------
# Function: detect_temperature_violations
# -----------------------------------------------------------
def detect_temperature_violations(df):
    """
    Detect temperature violations and annotate each record with violation flag and reason.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame with columns ReadingID, Timestamp, Temperature, Location

    Returns:
        pyspark.sql.DataFrame: DataFrame with additional columns: violation (string), violation_reason (string)
    """
    # Compliance range: -8.0 <= Temperature <= 0.0
    # Violation if Temperature < -8.0 or Temperature > 0.0
    # Violation if any required field is null or Temperature is not a valid double
    # Violation reasons as per requirements
    return (
        df.withColumn(
            "violation",
            when(
                col("ReadingID").isNull() | col("Timestamp").isNull() | col("Temperature").isNull() | col("Location").isNull(),
                lit("yes")
            ).when(
                isnan(col("Temperature")),
                lit("yes")
            ).when(
                col("Temperature") < -8.0,
                lit("yes")
            ).when(
                col("Temperature") > 0.0,
                lit("yes")
            ).otherwise(lit("no"))
        ).withColumn(
            "violation_reason",
            when(
                col("ReadingID").isNull() | col("Timestamp").isNull() | col("Temperature").isNull() | col("Location").isNull(),
                lit("Missing required field(s)")
            ).when(
                isnan(col("Temperature")),
                lit("Invalid temperature value")
            ).when(
                col("Temperature") < -8.0,
                lit("Temperature below minimum threshold")
            ).when(
                col("Temperature") > 0.0,
                lit("Temperature above maximum threshold")
            )
        )
    )

# -----------------------------------------------------------
# Function: aggregate_by_location
# -----------------------------------------------------------
def aggregate_by_location(df):
    """
    Aggregate temperature data by location, computing average temperature and count of violations.

    Args:
        df (pyspark.sql.DataFrame): DataFrame with columns: Location, Temperature, violation

    Returns:
        pyspark.sql.DataFrame: DataFrame with columns: Location, avg_temperature, violation_count
    """
    return (
        df.groupBy("Location")
        .agg(
            pyspark_round(avg(col("Temperature")), 2).alias("avg_temperature"),
            count(when(col("violation") == "yes", True)).alias("violation_count")
        )
    )

# -----------------------------------------------------------
# MAIN LOGIC: Read, Transform, Write
# -----------------------------------------------------------

try:
    # Read input data from Unity Catalog table
    input_df = spark.read.table("purgo_playground.cold_chain_temperature")
except Exception as e:
    raise RuntimeError(f"Failed to read input table: {e}")

# Detect violations and annotate with reasons
violations_df = detect_temperature_violations(input_df)

# Enforce output schema for violations table
violations_df = violations_df.select(
    col("ReadingID").cast(LongType()),
    col("Timestamp").cast(StringType()),
    col("Temperature").cast(DoubleType()),
    col("Location").cast(StringType()),
    col("violation").cast(StringType()),
    col("violation_reason").cast(StringType())
)

# Write detailed violations to Delta table (overwrite for idempotency)
try:
    violations_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.cold_chain_temperature_with_violations")
except Exception as e:
    raise RuntimeError(f"Failed to write violations table: {e}")

# Aggregate by location: average temperature and count of violations
location_agg_df = aggregate_by_location(violations_df)

# Enforce output schema for location aggregation table
location_agg_df = location_agg_df.select(
    col("Location").cast(StringType()),
    col("avg_temperature").cast(DoubleType()),
    col("violation_count").cast(LongType())
)

# Write aggregation to Delta table (overwrite for idempotency)
try:
    location_agg_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.cold_chain_temperature_location_agg")
except Exception as e:
    raise RuntimeError(f"Failed to write location aggregation table: {e}")

# -----------------------------------------------------------
# CTE: Show output of violations table for validation
# -----------------------------------------------------------
# -- This CTE selects all records from the violations table for validation
violations_cte = spark.sql("""
    WITH violations AS (
        SELECT * FROM purgo_playground.cold_chain_temperature_with_violations
    )
    SELECT * FROM violations
""")
violations_cte.show(truncate=False)

# -----------------------------------------------------------
# CTE: Show output of location aggregation table for validation
# -----------------------------------------------------------
# -- This CTE selects all records from the location aggregation table for validation
location_agg_cte = spark.sql("""
    WITH location_agg AS (
        SELECT * FROM purgo_playground.cold_chain_temperature_location_agg
    )
    SELECT * FROM location_agg
""")
location_agg_cte.show(truncate=False)

# End of script

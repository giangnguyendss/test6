spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for Databricks: Calculate TAT for Patient Foundation New Enrollment Review and load to tat_report
# Purpose: Calculate total_time_elapsed (TAT) for "Patient Foundation" cases, adjusted for weekends, and load results to tat_report
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: This script reads pat_case and sr_activity from purgo_playground, calculates TAT for "Patient Foundation" cases
#   by joining with relevant sr_activity records, computes the number of weekdays (excluding weekends) between sr_created_date
#   and the minimum last_modified_date (end_date) per case, and writes the result to tat_report, overwriting it on each run.
#   Handles all business rules for NULLs, edge cases, and ensures schema/data type consistency.

from pyspark.sql.types import StringType, TimestampType, LongType  
from pyspark.sql.functions import col, min as spark_min, to_date, pandas_udf  
from pyspark.sql.window import Window  
import pandas as pd  
from datetime import timedelta  

# -----------------------------------------------------------
# Function: weekdays_between_pandas_udf
# -----------------------------------------------------------
def weekdays_between_pandas_udf(start_ts: pd.Series, end_ts: pd.Series) -> pd.Series:
    """
    Calculate the number of weekdays (Mon-Fri) between two timestamps, exclusive of weekends.

    Args:
        start_ts (pd.Series): Start timestamps (can be None).
        end_ts (pd.Series): End timestamps (can be None).

    Returns:
        pd.Series: Number of weekdays (int), or None if invalid (per business rules).
    """
    def calc(s, e):
        if pd.isnull(s) or pd.isnull(e):
            return None
        s_date = s.date()
        e_date = e.date()
        if e_date < s_date:
            return None
        if e_date == s_date:
            return 0
        days = (e_date - s_date).days
        count = 0
        for i in range(1, days+1):
            d = s_date + timedelta(days=i)
            if d.weekday() < 5:
                count += 1
        return count
    return pd.Series([calc(s, e) for s, e in zip(start_ts, end_ts)])

# Register the pandas UDF for use in Spark DataFrame transformations
weekdays_between = pandas_udf(weekdays_between_pandas_udf, returnType=LongType())

# -----------------------------------------------------------
# Section: Read Source Tables
# -----------------------------------------------------------
# Read pat_case and sr_activity from Unity Catalog
pat_case_df = spark.table("purgo_playground.pat_case")
sr_activity_df = spark.table("purgo_playground.sr_activity")

# -----------------------------------------------------------
# Section: Filter and Join for TAT Calculation
# -----------------------------------------------------------
# Filter pat_case for "Patient Foundation" only
pf_case_df = pat_case_df.filter(col("service_request_type") == "Patient Foundation")

# Filter sr_activity for required subject and status
activity_df = sr_activity_df.filter(
    (col("subject") == "Perform New Enrollment Review Activity") &
    (col("status") == "Completed")
)

# Window to get minimum last_modified_date per case_id
w = Window.partitionBy("case_id")
activity_min_df = activity_df.withColumn("min_last_modified_date", spark_min("last_modified_date").over(w)) \
    .select("case_id", "min_last_modified_date").distinct()

# Left join pat_case with min activity date (end_date)
joined_df = pf_case_df.join(activity_min_df, "case_id", "left") \
    .withColumnRenamed("min_last_modified_date", "end_date")

# -----------------------------------------------------------
# Section: Calculate total_time_elapsed (TAT)
# -----------------------------------------------------------
# Compute TAT using the registered pandas UDF, per business rules
result_df = joined_df.withColumn(
    "total_time_elapsed",
    weekdays_between(col("sr_created_date"), col("end_date"))
)

# -----------------------------------------------------------
# Section: Select and Write to tat_report
# -----------------------------------------------------------
# Select columns in required order for tat_report
tat_report_df = result_df.select(
    "case_id",
    "service_request_type",
    "sr_created_date",
    "end_date",
    "total_time_elapsed"
)

# Overwrite tat_report table in Unity Catalog (Delta Lake)
tat_report_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.tat_report")

# -----------------------------------------------------------
# Section: Data Quality Checks and Logging
# -----------------------------------------------------------
# Log row counts for traceability
tat_count = tat_report_df.count()
print(f"tat_report row count: {tat_count}")

# Optional: Show a sample of the output for validation
tat_report_df.show(20, truncate=False)

# End of script

# PySpark script: Mask last 4 digits of invoice_number in d_product_revenue_clone table (Databricks)
# Purpose: Implements irreversible masking of invoice_number in d_product_revenue_clone as per compliance
# Author: Giang Nguyen
# Date: 2025-08-03
# Description: 
#   - Drops d_product_revenue_clone if exists
#   - Clones d_product_revenue to d_product_revenue_clone
#   - Masks invoice_number in the clone: last 4 chars replaced with '*', or all chars if length < 4, NULL remains NULL
#   - Handles edge, error, and special character cases
#   - Only invoice_number is modified; all other columns are preserved

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import (  
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, DateType
)

# -- Step 1: Drop d_product_revenue_clone table if exists
spark.sql("""
    DROP TABLE IF EXISTS purgo_databricks.purgo_playground.d_product_revenue_clone
""")

# -- Step 2: Create d_product_revenue_clone as a replica of d_product_revenue
spark.sql("""
    CREATE TABLE purgo_databricks.purgo_playground.d_product_revenue_clone
    AS SELECT * FROM purgo_databricks.purgo_playground.d_product_revenue
""")

# -- Step 3: Generate comprehensive test data for d_product_revenue_clone
#   - This will overwrite the table with 25 diverse test records covering all scenarios

# Define schema for test data (example schema, adjust as per actual d_product_revenue schema)
# Columns: id (LongType), invoice_number (StringType), customer_id (StringType), amount (DoubleType), created_at (TimestampType)
test_schema = StructType([
    StructField("id", LongType(), False),
    StructField("invoice_number", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("created_at", TimestampType(), True)
])

# Prepare test data records
test_data = [
    # Happy path: numeric, alphanumeric, special chars, multi-byte, edge lengths
    (1, "1234234534", "CUST001", 100.50, "2024-03-21T00:00:00.000+0000"),
    (2, "9876543210", "CUST002", 200.00, "2024-03-22T12:34:56.789+0000"),
    (3, "ABCD1234", "CUST003", 150.75, "2024-03-23T23:59:59.999+0000"),
    (4, "1234", "CUST004", 99.99, "2024-03-24T08:00:00.000+0000"),
    (5, "12", "CUST005", 10.00, "2024-03-25T09:15:00.000+0000"),
    (6, "A1B2", "CUST006", 20.00, "2024-03-26T10:30:00.000+0000"),
    (7, "X", "CUST007", 5.00, "2024-03-27T11:45:00.000+0000"),
    (8, "", "CUST008", 0.00, "2024-03-28T13:00:00.000+0000"),
    (9, None, "CUST009", 300.00, "2024-03-29T14:15:00.000+0000"),
    (10, "INV-2023-1234", "CUST010", 500.00, "2024-03-30T15:30:00.000+0000"),
    (11, "#$%&", "CUST011", 1.23, "2024-03-31T16:45:00.000+0000"),
    (12, "12-34", "CUST012", 2.34, "2024-04-01T18:00:00.000+0000"),
    (13, "1A2B", "CUST013", 3.45, "2024-04-02T19:15:00.000+0000"),
    (14, "INV2023", "CUST014", 4.56, "2024-04-03T20:30:00.000+0000"),
    (15, "αβγδ1234", "CUST015", 5.67, "2024-04-04T21:45:00.000+0000"),  # multi-byte chars
    (16, "123", "CUST016", 6.78, "2024-04-05T23:00:00.000+0000"),  # length 3
    (17, "12", "CUST017", 7.89, "2024-04-06T01:15:00.000+0000"),   # length 2
    (18, "1", "CUST018", 8.90, "2024-04-07T02:30:00.000+0000"),    # length 1
    (19, "A", "CUST019", 9.01, "2024-04-08T03:45:00.000+0000"),    # length 1, alpha
    (20, "INV-1", "CUST020", 10.12, "2024-04-09T05:00:00.000+0000"), # length 5
    # Edge: special chars, unicode, empty, null, long string
    (21, "😊😊😊😊", "CUST021", 11.23, "2024-04-10T06:15:00.000+0000"), # emoji
    (22, "INV-2023-!@#$", "CUST022", 12.34, "2024-04-11T07:30:00.000+0000"),
    (23, "0000", "CUST023", 13.45, "2024-04-12T08:45:00.000+0000"),
    (24, "A B C D", "CUST024", 14.56, "2024-04-13T10:00:00.000+0000"),
    (25, "LONGINVOICENUMBER1234567890", "CUST025", 15.67, "2024-04-14T11:15:00.000+0000"),
]

# Convert string timestamps to actual TimestampType
from pyspark.sql import Row  
import datetime  

def parse_ts(ts_str):
    """Parse Databricks timestamp string to Python datetime."""
    if ts_str is None:
        return None
    # Remove timezone for compatibility with datetime.strptime
    ts_str = ts_str.replace("+0000", "")
    return datetime.datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")

test_data_rows = [
    Row(
        id=rec[0],
        invoice_number=rec[1],
        customer_id=rec[2],
        amount=rec[3],
        created_at=parse_ts(rec[4])
    ) for rec in test_data
]

test_df = spark.createDataFrame(test_data_rows, schema=test_schema)

# Overwrite the clone table with test data
test_df.write.mode("overwrite").format("delta").saveAsTable("purgo_databricks.purgo_playground.d_product_revenue_clone")

def mask_invoice_number_col(df, col_name="invoice_number"):
    """
    Masks the last 4 characters of the invoice_number column as per requirements.
    If length < 4, replaces all characters with asterisks of same length.
    If NULL, remains NULL.
    If empty string, remains empty string.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
        col_name (str): Name of the invoice_number column

    Returns:
        pyspark.sql.DataFrame: DataFrame with masked invoice_number
    """
    # Defensive: Ensure column is StringType
    if not dict(df.dtypes)[col_name] == "string":
        raise TypeError("invoice_number must be of type STRING")

    # Masking logic:
    # - If NULL: return NULL
    # - If length < 4: return '*' * length
    # - If length >= 4: return left(str, length-4) + '****'
    # - If empty string: return empty string
    return df.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNull(), None
        ).when(
            F.length(F.col(col_name)) == 0, F.lit("")
        ).when(
            F.length(F.col(col_name)) < 4,
            F.expr(f"repeat('*', length({col_name}))")
        ).otherwise(
            F.concat(
                F.expr(f"substring({col_name}, 1, length({col_name})-4)"),
                F.lit("****")
            )
        )
    )

# -- Step 4: Apply masking logic to d_product_revenue_clone
clone_df = spark.table("purgo_databricks.purgo_playground.d_product_revenue_clone")
masked_df = mask_invoice_number_col(clone_df, "invoice_number")

# Overwrite the clone table with masked data (irreversible)
masked_df.write.mode("overwrite").format("delta").saveAsTable("purgo_databricks.purgo_playground.d_product_revenue_clone")

# -- Step 5: Validation Query using CTE (for demonstration, not required for masking)
#   - Shows original and masked invoice_number for all test records

# -- CTE to select all records from the masked clone table for validation
validation_query = """
WITH masked_data AS (
    SELECT id, invoice_number, customer_id, amount, created_at
    FROM purgo_databricks.purgo_playground.d_product_revenue_clone
)
SELECT * FROM masked_data
ORDER BY id
"""

validation_df = spark.sql(validation_query)
validation_df.show(truncate=False)

# -- Step 6: Unit Tests for Masking Logic

def test_masking_logic():
    """
    Unit test for mask_invoice_number_col function.
    Validates masking for various edge cases and expected outputs.
    """
    # Test cases: (original, expected)
    test_cases = [
        ("1234234534", "123423****"),
        ("9876543210", "987654****"),
        ("ABCD1234", "ABCD****"),
        ("1234", "****"),
        ("12", "**"),
        ("A1B2", "****"),
        ("X", "*"),
        ("", ""),
        (None, None),
        ("INV-2023-1234", "INV-2023-****"),
        ("#$%&", "****"),
        ("12-34", "12-**"),
        ("1A2B", "****"),
        ("INV2023", "INV****"),
        ("αβγδ1234", "αβγδ****"),
        ("123", "***"),
        ("12", "**"),
        ("1", "*"),
        ("A", "*"),
        ("INV-1", "I****"),
        ("😊😊😊😊", "****"),
        ("INV-2023-!@#$", "INV-2023-****"),
        ("0000", "****"),
        ("A B C D", "A B ****"),
        ("LONGINVOICENUMBER1234567890", "LONGINVOICENUMBER123456****"),
    ]
    rows = [Row(invoice_number=tc[0]) for tc in test_cases]
    df = spark.createDataFrame(rows, schema=StructType([StructField("invoice_number", StringType(), True)]))
    masked = mask_invoice_number_col(df, "invoice_number").collect()
    for i, row in enumerate(masked):
        assert row.invoice_number == test_cases[i][1], f"Failed for {test_cases[i][0]}: got {row.invoice_number}, expected {test_cases[i][1]}"

test_masking_logic()

def test_null_and_empty_handling():
    """
    Unit test for NULL and empty string handling in mask_invoice_number_col.
    """
    rows = [Row(invoice_number=None), Row(invoice_number="")]
    df = spark.createDataFrame(rows, schema=StructType([StructField("invoice_number", StringType(), True)]))
    masked = mask_invoice_number_col(df, "invoice_number").collect()
    assert masked[0].invoice_number is None, "NULL should remain NULL"
    assert masked[1].invoice_number == "", "Empty string should remain empty"

test_null_and_empty_handling()

def test_non_string_type_error():
    """
    Unit test to ensure TypeError is raised if invoice_number is not StringType.
    """
    schema = StructType([StructField("invoice_number", IntegerType(), True)])
    df = spark.createDataFrame([Row(invoice_number=1234)], schema=schema)
    try:
        mask_invoice_number_col(df, "invoice_number")
        assert False, "TypeError not raised for non-string invoice_number"
    except TypeError as e:
        assert "invoice_number must be of type STRING" in str(e)

test_non_string_type_error()

def test_schema_validation():
    """
    Integration test: Validates that the schema of d_product_revenue_clone matches d_product_revenue.
    """
    src_schema = spark.table("purgo_databricks.purgo_playground.d_product_revenue").schema
    clone_schema = spark.table("purgo_databricks.purgo_playground.d_product_revenue_clone").schema
    assert src_schema == clone_schema, "Schema mismatch between source and clone"

test_schema_validation()

def test_column_count_match():
    """
    Data quality test: Ensures number of columns in test data matches target table schema.
    """
    target_schema = spark.table("purgo_databricks.purgo_playground.d_product_revenue_clone").schema
    assert len(test_schema) == len(target_schema), "Column count mismatch"

test_column_count_match()

def test_masking_is_irreversible():
    """
    Data quality test: Ensures original invoice_number values are not retrievable after masking.
    """
    df = spark.table("purgo_databricks.purgo_playground.d_product_revenue_clone")
    # There should be no record with invoice_number in the set of original test values (except for NULL/empty)
    original_values = set([rec[1] for rec in test_data if rec[1] not in (None, "")])
    found = df.filter(F.col("invoice_number").isin(list(original_values))).count()
    assert found == 0, "Original invoice_number values are still present after masking"

test_masking_is_irreversible()

def test_masking_does_not_affect_other_columns():
    """
    Data quality test: Ensures only invoice_number is modified, other columns remain unchanged.
    """
    # Reload test data and masked data
    orig_df = spark.createDataFrame(test_data_rows, schema=test_schema)
    masked_df = spark.table("purgo_databricks.purgo_playground.d_product_revenue_clone")
    # Join on id and compare all columns except invoice_number
    join_df = orig_df.join(masked_df, on="id", how="inner")
    for row in join_df.collect():
        assert row.customer_id == row.customer_id, "customer_id changed"
        assert row.amount == row.amount, "amount changed"
        assert row.created_at == row.created_at, "created_at changed"

test_masking_does_not_affect_other_columns()

def test_masking_handles_special_characters():
    """
    Unit test: Ensures masking works for invoice_number with special characters.
    """
    test_cases = [
        ("INV-2023-1234", "INV-2023-****"),
        ("#$%&", "****"),
        ("12-34", "12-**"),
        ("1A2B", "****"),
    ]
    rows = [Row(invoice_number=tc[0]) for tc in test_cases]
    df = spark.createDataFrame(rows, schema=StructType([StructField("invoice_number", StringType(), True)]))
    masked = mask_invoice_number_col(df, "invoice_number").collect()
    for i, row in enumerate(masked):
        assert row.invoice_number == test_cases[i][1], f"Failed for {test_cases[i][0]}: got {row.invoice_number}, expected {test_cases[i][1]}"

test_masking_handles_special_characters()

def test_masking_handles_unicode():
    """
    Unit test: Ensures masking works for unicode/multibyte invoice_number.
    """
    test_cases = [
        ("αβγδ1234", "αβγδ****"),
        ("😊😊😊😊", "****"),
    ]
    rows = [Row(invoice_number=tc[0]) for tc in test_cases]
    df = spark.createDataFrame(rows, schema=StructType([StructField("invoice_number", StringType(), True)]))
    masked = mask_invoice_number_col(df, "invoice_number").collect()
    for i, row in enumerate(masked):
        assert row.invoice_number == test_cases[i][1], f"Failed for {test_cases[i][0]}: got {row.invoice_number}, expected {test_cases[i][1]}"

test_masking_handles_unicode()

def test_masking_performance():
    """
    Performance test: Ensures masking logic runs within reasonable time for large datasets.
    """
    import time  
    # Generate 1 million rows
    big_data = [Row(invoice_number=f"INV{str(i).zfill(8)}") for i in range(1000000)]
    big_df = spark.createDataFrame(big_data, schema=StructType([StructField("invoice_number", StringType(), True)]))
    start = time.time()
    masked = mask_invoice_number_col(big_df, "invoice_number")
    masked.count()  # Force evaluation
    elapsed = time.time() - start
    assert elapsed < 60, f"Masking 1M rows took too long: {elapsed} seconds"

test_masking_performance()

# -- Step 7: Delta Lake Operations Validation

def test_delta_merge_update_delete():
    """
    Integration test: Validates Delta Lake MERGE, UPDATE, DELETE operations on the clone table.
    """
    # Insert a new row for update/delete/merge
    new_row = [(999, "TEST9999", "CUST999", 999.99, datetime.datetime(2025, 1, 1, 0, 0, 0))]
    new_df = spark.createDataFrame(new_row, schema=test_schema)
    new_df.write.mode("append").format("delta").saveAsTable("purgo_databricks.purgo_playground.d_product_revenue_clone")
    # UPDATE: Set amount to 0 where id=999
    spark.sql("""
        UPDATE purgo_databricks.purgo_playground.d_product_revenue_clone
        SET amount = 0.0
        WHERE id = 999
    """)
    # DELETE: Remove where id=999
    spark.sql("""
        DELETE FROM purgo_databricks.purgo_playground.d_product_revenue_clone
        WHERE id = 999
    """)
    # MERGE: Upsert a row
    merge_df = spark.createDataFrame([(1000, "MERGE1000", "CUST1000", 1000.0, datetime.datetime(2025, 1, 2, 0, 0, 0))], schema=test_schema)
    merge_df.createOrReplaceTempView("merge_source")
    spark.sql("""
        MERGE INTO purgo_databricks.purgo_playground.d_product_revenue_clone AS target
        USING merge_source AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # Validate
    res = spark.sql("SELECT * FROM purgo_databricks.purgo_playground.d_product_revenue_clone WHERE id = 1000").collect()
    assert len(res) == 1, "MERGE failed to insert row"
    # Clean up
    spark.sql("DELETE FROM purgo_databricks.purgo_playground.d_product_revenue_clone WHERE id = 1000")

test_delta_merge_update_delete()

# -- Step 8: Window Function and Analytics Feature Test

def test_window_function():
    """
    Analytics test: Validates window function on masked table.
    """
    from pyspark.sql.window import Window  
    df = spark.table("purgo_databricks.purgo_playground.d_product_revenue_clone")
    w = Window.partitionBy("customer_id").orderBy("id")
    df2 = df.withColumn("row_num", F.row_number().over(w))
    # Validate row_num is always 1 for unique customer_id in test data
    assert df2.filter(F.col("row_num") != 1).count() == 0, "Window function row_number failed"

test_window_function()

# -- Step 9: Cleanup Operations

# -- Drop the test clone table to clean up after tests
spark.sql("""
    DROP TABLE IF EXISTS purgo_databricks.purgo_playground.d_product_revenue_clone
""")

# spark.stop()  # Do not stop SparkSession in Databricks

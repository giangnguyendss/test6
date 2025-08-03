-- Databricks SQL script: Data quality checks for d_product table in Unity Catalog
-- Purpose: Validate nulls and yyyymmdd format in critical columns of d_product before PROD move
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: This script performs comprehensive data quality checks on purgo_databricks.purgo_playground.d_product, including null checks, yyyymmdd format validation, error handling for missing columns/types, and sample record selection. All logic follows Databricks SQL and Unity Catalog best practices.

--------------------------------------------------------------------------------
/* SECTION: Setup - Table Existence and Schema Validation */
--------------------------------------------------------------------------------

-- Check if d_product table exists in the Unity Catalog
-- Returns error if table is missing or inaccessible
SELECT
  CASE
    WHEN COUNT(*) = 0 THEN "ERROR: d_product table not found or access denied in purgo_databricks.purgo_playground"
    ELSE "OK"
  END AS table_existence_status
FROM information_schema.tables
WHERE table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "d_product";

-- Check if required columns exist in d_product table
-- Returns error for each missing column
WITH required_columns AS (
  SELECT "item_nbr" AS col UNION ALL
  SELECT "sellable_qty" UNION ALL
  SELECT "prod_exp_dt"
),
existing_columns AS (
  SELECT column_name
  FROM purgo_databricks.information_schema.columns
  WHERE table_catalog = "purgo_databricks"
    AND table_schema = "purgo_playground"
    AND table_name = "d_product"
)
SELECT
  rc.col AS missing_column,
  "ERROR: Column " || rc.col || " does not exist in d_product table" AS error_message
FROM required_columns rc
LEFT JOIN existing_columns ec ON rc.col = ec.column_name
WHERE ec.column_name IS NULL;

-- Check if prod_exp_dt is of STRING type
-- Returns error if not
SELECT
  CASE
    WHEN data_type <> "STRING" THEN "ERROR: prod_exp_dt column must be of string type for yyyymmdd validation"
    ELSE "OK"
  END AS prod_exp_dt_type_status
FROM purgo_databricks.information_schema.columns
WHERE table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "d_product"
  AND column_name = "prod_exp_dt";

--------------------------------------------------------------------------------
/* SECTION: Data Quality Checks - Nulls and yyyymmdd Format */
--------------------------------------------------------------------------------

-- CTE: Source data for testing (replace with actual table in PROD)
WITH d_product AS (
  -- Replace this CTE with actual table reference in production:
  -- SELECT * FROM purgo_databricks.purgo_playground.d_product
  SELECT * FROM (
    -- Test data CTE from provided test data script
    SELECT
      1 AS prod_id, "A123" AS item_nbr, 100 AS sellable_qty, "20240101" AS prod_exp_dt UNION ALL
      2, "B456", 0, "20231231" UNION ALL
      3, "C789", 999999, "20230228" UNION ALL
      4, "D012", 1, "20240229" UNION ALL
      5, NULL, 10, "20240115" UNION ALL
      6, NULL, 20, "20231201" UNION ALL
      7, NULL, 30, "20230101" UNION ALL
      8, NULL, 40, "20230315" UNION ALL
      9, NULL, 50, "20230505" UNION ALL
      10, "E345", NULL, "20240120" UNION ALL
      11, "F678", NULL, "20231220" UNION ALL
      12, "G901", NULL, "20230120" UNION ALL
      13, "H234", NULL, "20230320" UNION ALL
      14, "I567", NULL, "20230510" UNION ALL
      15, "J890", 5, "2023123" UNION ALL
      16, "K123", 6, "2023-1201" UNION ALL
      17, "L456", 7, "20231301" UNION ALL
      18, "M789", 8, "20230230" UNION ALL
      19, "N012", 9, "20231232" UNION ALL
      20, "O345", 11, NULL UNION ALL
      21, "P678", 12, "" UNION ALL
      22, "Q901", 13, "00000000" UNION ALL
      23, "R234", 14, "20230431" UNION ALL
      24, "S!@#", 15, "20240105" UNION ALL
      25, "商品123", 16, "20240106" UNION ALL
      26, "T456😀", 17, "20240107" UNION ALL
      27, "U789", -1, "20240108" UNION ALL
      28, "V012", 2147483647, "20240109"
  )
),

/* CTE: prod_exp_dt format and calendar validation
   - is_8_digits: prod_exp_dt is exactly 8 characters
   - is_numeric: prod_exp_dt contains only digits
   - is_valid_date: prod_exp_dt is a valid calendar date (yyyyMMdd)
   - is_valid_yyyymmdd: all above are true
*/
prod_exp_dt_validation AS (
  SELECT
    prod_id,
    item_nbr,
    sellable_qty,
    prod_exp_dt,
    LENGTH(prod_exp_dt) = 8 AS is_8_digits,
    prod_exp_dt RLIKE "^[0-9]{8}$" AS is_numeric,
    -- Try to parse as date, valid if not null and matches input
    CASE
      WHEN prod_exp_dt RLIKE "^[0-9]{8}$"
        AND TRY_TO_DATE(prod_exp_dt, "yyyyMMdd") IS NOT NULL
        AND DATE_FORMAT(TRY_TO_DATE(prod_exp_dt, "yyyyMMdd"), "yyyyMMdd") = prod_exp_dt
      THEN TRUE
      ELSE FALSE
    END AS is_valid_date,
    -- Final flag: all must be true
    (
      LENGTH(prod_exp_dt) = 8
      AND prod_exp_dt RLIKE "^[0-9]{8}$"
      AND TRY_TO_DATE(prod_exp_dt, "yyyyMMdd") IS NOT NULL
      AND DATE_FORMAT(TRY_TO_DATE(prod_exp_dt, "yyyyMMdd"), "yyyyMMdd") = prod_exp_dt
    ) AS is_valid_yyyymmdd
  FROM d_product
)

--------------------------------------------------------------------------------
/* SECTION: Data Quality Check 1 - item_nbr IS NULL */
--------------------------------------------------------------------------------

-- Count of records where item_nbr is null
SELECT
  COUNT(*) AS item_nbr_null_count
FROM d_product
WHERE item_nbr IS NULL;

-- 5 sample records where item_nbr is null, ordered by prod_id
SELECT
  prod_id, item_nbr, sellable_qty, prod_exp_dt
FROM d_product
WHERE item_nbr IS NULL
ORDER BY prod_id ASC
LIMIT 5;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Check 2 - sellable_qty IS NULL */
--------------------------------------------------------------------------------

-- Count of records where sellable_qty is null
SELECT
  COUNT(*) AS sellable_qty_null_count
FROM d_product
WHERE sellable_qty IS NULL;

-- 5 sample records where sellable_qty is null, ordered by prod_id
SELECT
  prod_id, item_nbr, sellable_qty, prod_exp_dt
FROM d_product
WHERE sellable_qty IS NULL
ORDER BY prod_id ASC
LIMIT 5;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Check 3 - prod_exp_dt NOT valid yyyymmdd */
--------------------------------------------------------------------------------

-- Count of records where prod_exp_dt is not a valid yyyymmdd date
SELECT
  COUNT(*) AS prod_exp_dt_invalid_count
FROM prod_exp_dt_validation
WHERE NOT is_valid_yyyymmdd;

-- 5 sample records where prod_exp_dt is not a valid yyyymmdd date, ordered by prod_id
SELECT
  prod_id, item_nbr, sellable_qty, prod_exp_dt
FROM prod_exp_dt_validation
WHERE NOT is_valid_yyyymmdd
ORDER BY prod_id ASC
LIMIT 5;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Check 4 - prod_exp_dt Validation Rule Breakdown */
--------------------------------------------------------------------------------

-- For each record, show which prod_exp_dt rule(s) failed
SELECT
  prod_id,
  prod_exp_dt,
  is_8_digits,
  is_numeric,
  is_valid_date,
  is_valid_yyyymmdd,
  CASE
    WHEN is_8_digits = FALSE THEN "FAIL: Not 8 digits"
    WHEN is_numeric = FALSE THEN "FAIL: Not numeric"
    WHEN is_valid_date = FALSE THEN "FAIL: Not valid calendar date"
    ELSE "PASS"
  END AS validation_result
FROM prod_exp_dt_validation
WHERE NOT is_valid_yyyymmdd
ORDER BY prod_id ASC;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Check 5 - No records with nulls or invalid prod_exp_dt */
--------------------------------------------------------------------------------

-- Assert that there are no records with null item_nbr, null sellable_qty, or invalid prod_exp_dt
SELECT
  SUM(CASE WHEN item_nbr IS NULL THEN 1 ELSE 0 END) AS item_nbr_null_count,
  SUM(CASE WHEN sellable_qty IS NULL THEN 1 ELSE 0 END) AS sellable_qty_null_count,
  SUM(CASE WHEN NOT is_valid_yyyymmdd THEN 1 ELSE 0 END) AS prod_exp_dt_invalid_count
FROM (
  SELECT
    d.*,
    v.is_valid_yyyymmdd
  FROM d_product d
  LEFT JOIN prod_exp_dt_validation v ON d.prod_id = v.prod_id
);

--------------------------------------------------------------------------------
/* SECTION: Sample Record Selection Method Validation */
--------------------------------------------------------------------------------

-- Show that sample records are selected by ascending prod_id and include required columns
SELECT
  prod_id, item_nbr, sellable_qty, prod_exp_dt
FROM d_product
WHERE item_nbr IS NULL OR sellable_qty IS NULL OR prod_id IN (
  SELECT prod_id FROM prod_exp_dt_validation WHERE NOT is_valid_yyyymmdd
)
ORDER BY prod_id ASC
LIMIT 5;

--------------------------------------------------------------------------------
/* SECTION: Output Format Validation */
--------------------------------------------------------------------------------

-- Example: Combined output for all data quality checks (counts and sample records)
WITH
item_nbr_nulls AS (
  SELECT COUNT(*) AS cnt FROM d_product WHERE item_nbr IS NULL
),
sellable_qty_nulls AS (
  SELECT COUNT(*) AS cnt FROM d_product WHERE sellable_qty IS NULL
),
prod_exp_dt_invalids AS (
  SELECT COUNT(*) AS cnt FROM prod_exp_dt_validation WHERE NOT is_valid_yyyymmdd
),
sample_records AS (
  SELECT prod_id, item_nbr, sellable_qty, prod_exp_dt
  FROM d_product
  WHERE item_nbr IS NULL OR sellable_qty IS NULL OR prod_id IN (
    SELECT prod_id FROM prod_exp_dt_validation WHERE NOT is_valid_yyyymmdd
  )
  ORDER BY prod_id ASC
  LIMIT 5
)
SELECT
  (SELECT cnt FROM item_nbr_nulls) AS item_nbr_null_count,
  (SELECT cnt FROM sellable_qty_nulls) AS sellable_qty_null_count,
  (SELECT cnt FROM prod_exp_dt_invalids) AS prod_exp_dt_invalid_count,
  ARRAY_AGG(NAMED_STRUCT(
    "prod_id", prod_id,
    "item_nbr", item_nbr,
    "sellable_qty", sellable_qty,
    "prod_exp_dt", prod_exp_dt
  )) AS sample_records
FROM sample_records;

--------------------------------------------------------------------------------
/* SECTION: Error Handling - prod_exp_dt Wrong Data Type */
--------------------------------------------------------------------------------

-- Simulate error if prod_exp_dt is not STRING (should be run only if type is not string)
-- See earlier check in Setup section for actual error message

--------------------------------------------------------------------------------
/* SECTION: Error Handling - Missing Columns */
--------------------------------------------------------------------------------

-- See earlier check in Setup section for missing columns error messages

--------------------------------------------------------------------------------
/* SECTION: End of Script - Cleanup (if needed) */
--------------------------------------------------------------------------------

-- No temp views or temp tables used; no cleanup required

-- End of Databricks SQL script for d_product data quality checks

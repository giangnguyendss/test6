-- Databricks SQL script: Test data generation for d_product table data quality checks
-- Purpose: Generate comprehensive test data for d_product table to validate data quality logic
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: This script creates a CTE with diverse test records for the d_product table, covering happy path, edge cases, error cases, NULL handling, and special/multibyte characters. Data types and formats strictly follow Databricks conventions and the provided schema.

-- CTE: Generate 28 diverse test records for d_product table
-- Covers: valid data, NULLs, invalid prod_exp_dt (pattern and calendar), special/multibyte chars, edge numeric values

WITH test_d_product AS (
  SELECT
    -- Happy path: all valid
    1 AS prod_id, 'A123' AS item_nbr, 100 AS sellable_qty, '20240101' AS prod_exp_dt UNION ALL
    2 AS prod_id, 'B456' AS item_nbr, 0 AS sellable_qty, '20231231' AS prod_exp_dt UNION ALL
    3 AS prod_id, 'C789' AS item_nbr, 999999 AS sellable_qty, '20230228' AS prod_exp_dt UNION ALL
    4 AS prod_id, 'D012' AS item_nbr, 1 AS sellable_qty, '20240229' AS prod_exp_dt -- leap year
    UNION ALL
    -- Edge: item_nbr NULL
    5 AS prod_id, NULL AS item_nbr, 10 AS sellable_qty, '20240115' AS prod_exp_dt UNION ALL
    6 AS prod_id, NULL AS item_nbr, 20 AS sellable_qty, '20231201' AS prod_exp_dt UNION ALL
    7 AS prod_id, NULL AS item_nbr, 30 AS sellable_qty, '20230101' AS prod_exp_dt UNION ALL
    8 AS prod_id, NULL AS item_nbr, 40 AS sellable_qty, '20230315' AS prod_exp_dt UNION ALL
    9 AS prod_id, NULL AS item_nbr, 50 AS sellable_qty, '20230505' AS prod_exp_dt
    UNION ALL
    -- Edge: sellable_qty NULL
    10 AS prod_id, 'E345' AS item_nbr, NULL AS sellable_qty, '20240120' AS prod_exp_dt UNION ALL
    11 AS prod_id, 'F678' AS item_nbr, NULL AS sellable_qty, '20231220' AS prod_exp_dt UNION ALL
    12 AS prod_id, 'G901' AS item_nbr, NULL AS sellable_qty, '20230120' AS prod_exp_dt UNION ALL
    13 AS prod_id, 'H234' AS item_nbr, NULL AS sellable_qty, '20230320' AS prod_exp_dt UNION ALL
    14 AS prod_id, 'I567' AS item_nbr, NULL AS sellable_qty, '20230510' AS prod_exp_dt
    UNION ALL
    -- Edge: prod_exp_dt invalid pattern (not 8 digits)
    15 AS prod_id, 'J890' AS item_nbr, 5 AS sellable_qty, '2023123' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt contains non-digit
    16 AS prod_id, 'K123' AS item_nbr, 6 AS sellable_qty, '2023-1201' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt invalid month
    17 AS prod_id, 'L456' AS item_nbr, 7 AS sellable_qty, '20231301' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt invalid day (Feb 30)
    18 AS prod_id, 'M789' AS item_nbr, 8 AS sellable_qty, '20230230' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt day out of range
    19 AS prod_id, 'N012' AS item_nbr, 9 AS sellable_qty, '20231232' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt all NULL
    20 AS prod_id, 'O345' AS item_nbr, 11 AS sellable_qty, NULL AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt empty string
    21 AS prod_id, 'P678' AS item_nbr, 12 AS sellable_qty, '' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt all zeros
    22 AS prod_id, 'Q901' AS item_nbr, 13 AS sellable_qty, '00000000' AS prod_exp_dt UNION ALL
    -- Edge: prod_exp_dt valid pattern but invalid date (April 31)
    23 AS prod_id, 'R234' AS item_nbr, 14 AS sellable_qty, '20230431' AS prod_exp_dt
    UNION ALL
    -- Special: item_nbr with special characters
    24 AS prod_id, 'S!@#' AS item_nbr, 15 AS sellable_qty, '20240105' AS prod_exp_dt UNION ALL
    -- Special: item_nbr with multibyte unicode
    25 AS prod_id, '商品123' AS item_nbr, 16 AS sellable_qty, '20240106' AS prod_exp_dt UNION ALL
    -- Special: item_nbr with emoji
    26 AS prod_id, 'T456😀' AS item_nbr, 17 AS sellable_qty, '20240107' AS prod_exp_dt UNION ALL
    -- Edge: sellable_qty negative
    27 AS prod_id, 'U789' AS item_nbr, -1 AS sellable_qty, '20240108' AS prod_exp_dt UNION ALL
    -- Edge: sellable_qty very large
    28 AS prod_id, 'V012' AS item_nbr, 2147483647 AS sellable_qty, '20240109' AS prod_exp_dt
)

-- The CTE above can be used in downstream validation queries for data quality checks.

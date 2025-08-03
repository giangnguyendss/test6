-- Databricks SQL script: Comprehensive test suite for patient_therapy_shipment enrichment logic
-- Purpose: Validate enrichment logic, data types, NULL handling, window functions, and business rules for derived columns in purgo_playground.patient_therapy_shipment
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: This script tests the enrichment of the patient_therapy_shipment table with derived metrics for adherence, therapy gaps, age demographics, and discontinuation type. It covers schema validation, data type checks, NULL handling, window analytics, Delta Lake operations, and business logic for all derived and source columns.

-- /* ===========================
--      SETUP & CONFIGURATION
-- ============================ */

-- -- No SparkSession initialization required in Databricks
-- -- No need to create temp views or temp tables

-- /* ===========================
--      SCHEMA VALIDATION TESTS
-- ============================ */

-- -- Validate that the source table exists and has the expected schema
-- -- The schema must match the columns_directly_from_source and support all derived columns

-- Check source table schema
DESCRIBE purgo_playground.patient_therapy_shipment;

-- Assert that all required columns exist in the source table
-- (Manual review or automated schema comparison can be performed here)

-- /* ===========================
--      DERIVED COLUMN LOGIC
-- ============================ */

-- -- CTE: Enrich patient_therapy_shipment with derived columns as per business logic
-- -- {calctime} parameter is set for test purposes; in production, this should be parameterized

-- Set calculation date for test scenarios
-- (In Databricks SQL, use a variable or replace manually for each test run)
-- For this test, we use '2024-02-01' as calctime for the first scenario, and override as needed in others

-- /* ===========================
--      UNIT & INTEGRATION TESTS
-- ============================ */

-- -- CTE: Enriched data with all derived columns
WITH enriched_shipment AS (
  SELECT
    -- Source columns
    product,
    ship_date,
    days_supply,
    qty,
    treatment_id,
    dob,
    first_ship_date,
    refill_status,
    patient_id,
    ship_type,
    shipment_arrived_status,
    delivery_ontime,

    -- Derived columns
    -- shipment_expiry: Use COALESCE to fallback to qty logic if days_supply is NULL
    DATE_ADD(
      ship_date,
      CAST(
        COALESCE(
          CAST(days_supply AS INT),
          CAST(qty AS INT) / 3 * 7
        ) AS INT
      )
    ) AS shipment_expiry,

    -- discontinuation_date: shipment_expiry + 91 days
    DATE_ADD(
      DATE_ADD(
        ship_date,
        CAST(
          COALESCE(
            CAST(days_supply AS INT),
            CAST(qty AS INT) / 3 * 7
          ) AS INT
        )
      ),
      91
    ) AS discontinuation_date,

    -- days_until_next_ship: (shipment_expiry - {calctime}) + 1
    (DATEDIFF(
      DATE_ADD(
        ship_date,
        CAST(
          COALESCE(
            CAST(days_supply AS INT),
            CAST(qty AS INT) / 3 * 7
          ) AS INT
        )
      ),
      DATE '2024-02-01'
    ) + 1) AS days_until_next_ship,

    -- days_since_last_fill: ({calctime} - ship_date) + 1
    (DATEDIFF(DATE '2024-02-01', ship_date) + 1) AS days_since_last_fill,

    -- expected_refill_date: {calctime} + days_until_next_ship
    DATE_ADD(
      DATE '2024-02-01',
      (DATEDIFF(
        DATE_ADD(
          ship_date,
          CAST(
            COALESCE(
              CAST(days_supply AS INT),
              CAST(qty AS INT) / 3 * 7
            ) AS INT
          )
        ),
        DATE '2024-02-01'
      ) + 1)
    ) AS expected_refill_date,

    -- prior_ship: LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date)
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS prior_ship,

    -- days_between: ship_date - prior_ship
    CASE
      WHEN LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) IS NOT NULL
      THEN DATEDIFF(ship_date, LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date))
      ELSE NULL
    END AS days_between,

    -- days_since_supply_out: If {calctime} >= shipment_expiry, then difference, else NULL
    CASE
      WHEN DATEDIFF(
        DATE '2024-02-01',
        DATE_ADD(
          ship_date,
          CAST(
            COALESCE(
              CAST(days_supply AS INT),
              CAST(qty AS INT) / 3 * 7
            ) AS INT
          )
        )
      ) >= 0
      THEN DATEDIFF(
        DATE '2024-02-01',
        DATE_ADD(
          ship_date,
          CAST(
            COALESCE(
              CAST(days_supply AS INT),
              CAST(qty AS INT) / 3 * 7
            ) AS INT
          )
        )
      )
      ELSE NULL
    END AS days_since_supply_out,

    -- age: FLOOR(DATEDIFF({calctime}, dob) / 365.25)
    CASE
      WHEN dob IS NOT NULL
      THEN FLOOR(DATEDIFF(DATE '2024-02-01', dob) / 365.25)
      ELSE NULL
    END AS age,

    -- age_at_first_ship: ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
    CASE
      WHEN dob IS NOT NULL AND first_ship_date IS NOT NULL
      THEN ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
      ELSE NULL
    END AS age_at_first_ship,

    -- latest_therapy_ships: COUNT(ship_date) OVER (PARTITION BY patient_id, treatment_id WHERE ship_type='commercial')
    COUNT(CASE WHEN ship_type = "commercial" THEN ship_date END)
      OVER (PARTITION BY patient_id, treatment_id) AS latest_therapy_ships,

    -- discontinuation_type: CASE logic
    CASE
      WHEN refill_status = "DC - Standard" THEN "STANDARD"
      WHEN refill_status = "DC-PERMANENT" THEN "PERMANENT"
      ELSE NULL
    END AS discontinuation_type

  FROM purgo_playground.patient_therapy_shipment
)

-- /* ===========================
--      DATA QUALITY VALIDATION
-- ============================ */

-- -- Validate that all derived columns are present and have correct data types
-- -- Validate NULL handling for dob, first_ship_date, days_supply, qty

-- CTE: Data type and NULL validation for enriched_shipment
, validation_checks AS (
  SELECT
    -- Check data types: All date fields should be DATE, integer fields should be INT or NULL, string fields should be STRING or NULL
    -- Use typeof() for type validation
    typeof(product) AS product_type,
    typeof(ship_date) AS ship_date_type,
    typeof(days_supply) AS days_supply_type,
    typeof(qty) AS qty_type,
    typeof(treatment_id) AS treatment_id_type,
    typeof(dob) AS dob_type,
    typeof(first_ship_date) AS first_ship_date_type,
    typeof(refill_status) AS refill_status_type,
    typeof(patient_id) AS patient_id_type,
    typeof(ship_type) AS ship_type_type,
    typeof(shipment_arrived_status) AS shipment_arrived_status_type,
    typeof(delivery_ontime) AS delivery_ontime_type,

    typeof(shipment_expiry) AS shipment_expiry_type,
    typeof(discontinuation_date) AS discontinuation_date_type,
    typeof(days_until_next_ship) AS days_until_next_ship_type,
    typeof(days_since_last_fill) AS days_since_last_fill_type,
    typeof(expected_refill_date) AS expected_refill_date_type,
    typeof(prior_ship) AS prior_ship_type,
    typeof(days_between) AS days_between_type,
    typeof(days_since_supply_out) AS days_since_supply_out_type,
    typeof(age) AS age_type,
    typeof(age_at_first_ship) AS age_at_first_ship_type,
    typeof(latest_therapy_ships) AS latest_therapy_ships_type,
    typeof(discontinuation_type) AS discontinuation_type_type,

    -- NULL checks for key fields
    CASE WHEN dob IS NULL THEN 1 ELSE 0 END AS is_dob_null,
    CASE WHEN first_ship_date IS NULL THEN 1 ELSE 0 END AS is_first_ship_date_null,
    CASE WHEN days_supply IS NULL THEN 1 ELSE 0 END AS is_days_supply_null,
    CASE WHEN qty IS NULL THEN 1 ELSE 0 END AS is_qty_null,

    -- Check for negative values in days_supply and qty
    CASE WHEN days_supply < 0 THEN 1 ELSE 0 END AS is_days_supply_negative,
    CASE WHEN qty < 0 THEN 1 ELSE 0 END AS is_qty_negative
  FROM enriched_shipment
)

-- /* ===========================
--      ASSERTIONS & TEST CASES
-- ============================ */

-- -- 1. Assert that all required columns exist in the enriched output
-- -- 2. Assert that data types are as expected
-- -- 3. Assert that NULL handling is correct for dob, first_ship_date, days_supply, qty
-- -- 4. Assert that negative values are flagged
-- -- 5. Assert that window functions and business logic produce expected results

-- -- Test 1: Display enriched data for manual review (integration test)
SELECT * FROM enriched_shipment;

-- -- Test 2: Data type validation (unit test)
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT product_type) AS product_type_variants,
  COUNT(DISTINCT ship_date_type) AS ship_date_type_variants,
  COUNT(DISTINCT days_supply_type) AS days_supply_type_variants,
  COUNT(DISTINCT qty_type) AS qty_type_variants,
  COUNT(DISTINCT treatment_id_type) AS treatment_id_type_variants,
  COUNT(DISTINCT dob_type) AS dob_type_variants,
  COUNT(DISTINCT first_ship_date_type) AS first_ship_date_type_variants,
  COUNT(DISTINCT refill_status_type) AS refill_status_type_variants,
  COUNT(DISTINCT patient_id_type) AS patient_id_type_variants,
  COUNT(DISTINCT ship_type_type) AS ship_type_type_variants,
  COUNT(DISTINCT shipment_arrived_status_type) AS shipment_arrived_status_type_variants,
  COUNT(DISTINCT delivery_ontime_type) AS delivery_ontime_type_variants,
  COUNT(DISTINCT shipment_expiry_type) AS shipment_expiry_type_variants,
  COUNT(DISTINCT discontinuation_date_type) AS discontinuation_date_type_variants,
  COUNT(DISTINCT days_until_next_ship_type) AS days_until_next_ship_type_variants,
  COUNT(DISTINCT days_since_last_fill_type) AS days_since_last_fill_type_variants,
  COUNT(DISTINCT expected_refill_date_type) AS expected_refill_date_type_variants,
  COUNT(DISTINCT prior_ship_type) AS prior_ship_type_variants,
  COUNT(DISTINCT days_between_type) AS days_between_type_variants,
  COUNT(DISTINCT days_since_supply_out_type) AS days_since_supply_out_type_variants,
  COUNT(DISTINCT age_type) AS age_type_variants,
  COUNT(DISTINCT age_at_first_ship_type) AS age_at_first_ship_type_variants,
  COUNT(DISTINCT latest_therapy_ships_type) AS latest_therapy_ships_type_variants,
  COUNT(DISTINCT discontinuation_type_type) AS discontinuation_type_type_variants
FROM validation_checks;

-- -- Test 3: NULL handling for dob and first_ship_date (unit test)
SELECT
  COUNT(*) AS total_records,
  SUM(is_dob_null) AS dob_null_count,
  SUM(is_first_ship_date_null) AS first_ship_date_null_count
FROM validation_checks;

-- -- Test 4: NULL handling for days_supply and qty (unit test)
SELECT
  COUNT(*) AS total_records,
  SUM(is_days_supply_null) AS days_supply_null_count,
  SUM(is_qty_null) AS qty_null_count
FROM validation_checks;

-- -- Test 5: Negative value detection (data quality test)
SELECT
  COUNT(*) AS total_records,
  SUM(is_days_supply_negative) AS days_supply_negative_count,
  SUM(is_qty_negative) AS qty_negative_count
FROM validation_checks;

-- -- Test 6: Discontinuation type logic (unit test)
SELECT
  discontinuation_type,
  COUNT(*) AS count
FROM enriched_shipment
GROUP BY discontinuation_type;

-- -- Test 7: Window function correctness (unit test)
-- For a given patient_id and treatment_id, latest_therapy_ships should equal the count of commercial shipments
WITH window_check AS (
  SELECT
    patient_id,
    treatment_id,
    COUNT(CASE WHEN ship_type = "commercial" THEN 1 END) AS expected_count,
    MAX(latest_therapy_ships) AS max_latest_therapy_ships
  FROM enriched_shipment
  GROUP BY patient_id, treatment_id
)
SELECT
  *,
  CASE WHEN expected_count = max_latest_therapy_ships THEN "PASS" ELSE "FAIL" END AS window_test_result
FROM window_check;

-- -- Test 8: Data type and format validation for date and integer fields (unit test)
SELECT
  COUNT(*) AS total_records,
  COUNT(CASE WHEN typeof(ship_date) = "date" THEN 1 END) AS ship_date_is_date,
  COUNT(CASE WHEN typeof(shipment_expiry) = "date" THEN 1 END) AS shipment_expiry_is_date,
  COUNT(CASE WHEN typeof(discontinuation_date) = "date" THEN 1 END) AS discontinuation_date_is_date,
  COUNT(CASE WHEN typeof(expected_refill_date) = "date" THEN 1 END) AS expected_refill_date_is_date,
  COUNT(CASE WHEN typeof(prior_ship) = "date" OR prior_ship IS NULL THEN 1 END) AS prior_ship_is_date_or_null,
  COUNT(CASE WHEN typeof(days_until_next_ship) = "int" OR days_until_next_ship IS NULL THEN 1 END) AS days_until_next_ship_is_int_or_null,
  COUNT(CASE WHEN typeof(days_since_last_fill) = "int" OR days_since_last_fill IS NULL THEN 1 END) AS days_since_last_fill_is_int_or_null,
  COUNT(CASE WHEN typeof(days_between) = "int" OR days_between IS NULL THEN 1 END) AS days_between_is_int_or_null,
  COUNT(CASE WHEN typeof(days_since_supply_out) = "int" OR days_since_supply_out IS NULL THEN 1 END) AS days_since_supply_out_is_int_or_null,
  COUNT(CASE WHEN typeof(age) = "int" OR age IS NULL THEN 1 END) AS age_is_int_or_null,
  COUNT(CASE WHEN typeof(age_at_first_ship) = "int" OR age_at_first_ship IS NULL THEN 1 END) AS age_at_first_ship_is_int_or_null,
  COUNT(CASE WHEN typeof(latest_therapy_ships) = "bigint" OR latest_therapy_ships IS NULL THEN 1 END) AS latest_therapy_ships_is_bigint_or_null
FROM enriched_shipment;

-- /* ===========================
--      DELTA LAKE OPERATIONS TESTS
-- ============================ */

-- -- Test Delta Lake MERGE, UPDATE, DELETE operations on a test table
-- -- Create a test Delta table for safe testing

-- Create test Delta table if not exists
CREATE TABLE IF NOT EXISTS purgo_playground.patient_therapy_shipment_enriched_test
AS
SELECT * FROM enriched_shipment
WHERE 1=0;

-- Insert a sample record for MERGE/UPDATE/DELETE tests
INSERT INTO purgo_playground.patient_therapy_shipment_enriched_test
SELECT * FROM enriched_shipment LIMIT 1;

-- MERGE: Upsert logic test
MERGE INTO purgo_playground.patient_therapy_shipment_enriched_test AS target
USING (
  SELECT * FROM enriched_shipment LIMIT 1
) AS source
ON target.treatment_id = source.treatment_id AND target.ship_date = source.ship_date
WHEN MATCHED THEN
  UPDATE SET
    product = source.product,
    qty = source.qty
WHEN NOT MATCHED THEN
  INSERT *;

-- UPDATE: Set delivery_ontime to 'NO' for test
UPDATE purgo_playground.patient_therapy_shipment_enriched_test
SET delivery_ontime = "NO"
WHERE delivery_ontime = "YES";

-- DELETE: Remove test record
DELETE FROM purgo_playground.patient_therapy_shipment_enriched_test
WHERE 1=1;

-- Drop test table after test
DROP TABLE IF EXISTS purgo_playground.patient_therapy_shipment_enriched_test;

-- /* ===========================
--      PERFORMANCE TESTS
-- ============================ */

-- -- Performance: Count records and check query execution time for enrichment logic
SELECT COUNT(*) AS record_count FROM enriched_shipment;

-- /* ===========================
--      CLEANUP OPERATIONS
-- ============================ */

-- -- No temp views or temp tables to clean up

-- /* ===========================
--      END OF TEST SUITE
-- ============================ */

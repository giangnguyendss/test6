-- Databricks SQL script: Test data generation for purgo_playground.patient_therapy_shipment
-- Purpose: Generate comprehensive test data for patient_therapy_shipment table covering happy path, edge, error, NULL, and special character scenarios
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: This script creates a CTE with 24 test records for the purgo_playground.patient_therapy_shipment table, ensuring coverage of all business logic, data types, NULLs, edge cases, and special/multibyte characters. All data types and formats conform to Databricks SQL standards.

-- CTE: Generate diverse test data for patient_therapy_shipment
WITH test_patient_therapy_shipment AS (
  SELECT
    -- Happy path: all fields populated, standard values
    'DrugA' AS product,
    DATE '2024-01-01' AS ship_date,
    28 AS days_supply,
    84 AS qty,
    'TREAT123' AS treatment_id,
    DATE '1980-06-15' AS dob,
    DATE '2024-01-01' AS first_ship_date,
    'DC - Standard' AS refill_status,
    'PAT001' AS patient_id,
    'commercial' AS ship_type,
    'ARRIVED' AS shipment_arrived_status,
    'YES' AS delivery_ontime
  UNION ALL
  SELECT
    -- Happy path: different product, permanent DC
    'DrugB',
    DATE '2024-03-10',
    30,
    90,
    'TREAT456',
    DATE '1975-12-01',
    DATE '2024-03-10',
    'DC-PERMANENT',
    'PAT002',
    'commercial',
    'ARRIVED',
    'NO'
  UNION ALL
  SELECT
    -- days_supply NULL, fallback to qty logic
    'DrugC',
    DATE '2024-05-01',
    NULL,
    63,
    'TREAT789',
    DATE '1990-01-01',
    DATE '2024-05-01',
    'ACTIVE',
    'PAT003',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- dob is NULL, age and age_at_first_ship should be NULL
    'DrugD',
    DATE '2024-07-01',
    28,
    84,
    'TREAT321',
    NULL,
    DATE '2024-07-01',
    'ACTIVE',
    'PAT004',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- first_ship_date is NULL, age_at_first_ship should be NULL
    'DrugE',
    DATE '2024-08-01',
    28,
    84,
    'TREAT654',
    DATE '1985-05-05',
    NULL,
    'ACTIVE',
    'PAT005',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Both days_supply and qty are NULL, derived fields should be NULL
    'DrugF',
    DATE '2024-10-01',
    NULL,
    NULL,
    'TREAT987',
    DATE '1970-10-10',
    DATE '2024-10-01',
    'ACTIVE',
    'PAT006',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Discontinuation type: DC - Standard
    'DrugG',
    DATE '2024-12-01',
    28,
    84,
    'TREAT111',
    DATE '1995-03-03',
    DATE '2024-12-01',
    'DC - Standard',
    'PAT007',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Discontinuation type: DC-PERMANENT
    'DrugH',
    DATE '2025-01-01',
    28,
    84,
    'TREAT222',
    DATE '1992-04-04',
    DATE '2025-01-01',
    'DC-PERMANENT',
    'PAT008',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Discontinuation type: ACTIVE (should be NULL)
    'DrugI',
    DATE '2025-02-01',
    28,
    84,
    'TREAT333',
    DATE '1991-05-05',
    DATE '2025-02-01',
    'ACTIVE',
    'PAT009',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Validate data types and formats, all fields populated
    'DrugJ',
    DATE '2025-03-01',
    28,
    84,
    'TREAT444',
    DATE '1988-07-07',
    DATE '2025-03-01',
    'ACTIVE',
    'PAT010',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Edge: days_supply = 0, qty = 0
    'DrugK',
    DATE '2025-04-01',
    0,
    0,
    'TREAT555',
    DATE '2000-01-01',
    DATE '2025-04-01',
    'ACTIVE',
    'PAT011',
    'commercial',
    'ARRIVED',
    'NO'
  UNION ALL
  SELECT
    -- Edge: days_supply = 1, qty = 1
    'DrugL',
    DATE '2025-05-01',
    1,
    1,
    'TREAT666',
    DATE '2010-12-31',
    DATE '2025-05-01',
    'ACTIVE',
    'PAT012',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Edge: days_supply = 365, qty = 1095 (1 year supply)
    'DrugM',
    DATE '2025-06-01',
    365,
    1095,
    'TREAT777',
    DATE '1960-06-15',
    DATE '2025-06-01',
    'ACTIVE',
    'PAT013',
    'commercial',
    'ARRIVED',
    'NO'
  UNION ALL
  SELECT
    -- Special characters in product and patient_id
    'DrügN-特殊字符',
    DATE '2025-07-01',
    28,
    84,
    'TREAT888',
    DATE '1982-02-02',
    DATE '2025-07-01',
    'ACTIVE',
    'PAT014-特殊字符',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Multi-byte characters in refill_status and ship_type
    'DrugO',
    DATE '2025-08-01',
    28,
    84,
    'TREAT999',
    DATE '1999-09-09',
    DATE '2025-08-01',
    'アクティブ',
    'PAT015',
    '商業',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- delivery_ontime special value
    'DrugP',
    DATE '2025-09-01',
    28,
    84,
    'TREAT000',
    DATE '1987-07-07',
    DATE '2025-09-01',
    'ACTIVE',
    'PAT016',
    'commercial',
    'ARRIVED',
    'N/A'
  UNION ALL
  SELECT
    -- shipment_arrived_status special value
    'DrugQ',
    DATE '2025-10-01',
    28,
    84,
    'TREAT101',
    DATE '1983-03-03',
    DATE '2025-10-01',
    'ACTIVE',
    'PAT017',
    'commercial',
    'DELAYED',
    'YES'
  UNION ALL
  SELECT
    -- ship_type = 'sample'
    'DrugR',
    DATE '2025-11-01',
    28,
    84,
    'TREAT202',
    DATE '1977-07-07',
    DATE '2025-11-01',
    'ACTIVE',
    'PAT018',
    'sample',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- ship_type = 'free_trial'
    'DrugS',
    DATE '2025-12-01',
    28,
    84,
    'TREAT303',
    DATE '1993-03-03',
    DATE '2025-12-01',
    'ACTIVE',
    'PAT019',
    'free_trial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- refill_status = NULL
    'DrugT',
    DATE '2026-01-01',
    28,
    84,
    'TREAT404',
    DATE '1986-06-06',
    DATE '2026-01-01',
    NULL,
    'PAT020',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- All string fields are empty strings
    '',
    DATE '2026-02-01',
    28,
    84,
    '',
    DATE '1984-04-04',
    DATE '2026-02-01',
    '',
    '',
    '',
    '',
    ''
  UNION ALL
  SELECT
    -- All string fields are NULL
    NULL,
    DATE '2026-03-01',
    28,
    84,
    NULL,
    DATE '1981-01-01',
    DATE '2026-03-01',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
  UNION ALL
  SELECT
    -- days_supply negative (invalid, error case)
    'DrugU',
    DATE '2026-04-01',
    -7,
    21,
    'TREAT505',
    DATE '1979-09-09',
    DATE '2026-04-01',
    'ACTIVE',
    'PAT021',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- qty negative (invalid, error case)
    'DrugV',
    DATE '2026-05-01',
    28,
    -84,
    'TREAT606',
    DATE '1989-08-08',
    DATE '2026-05-01',
    'ACTIVE',
    'PAT022',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- Both days_supply and qty negative (invalid, error case)
    'DrugW',
    DATE '2026-06-01',
    -28,
    -84,
    'TREAT707',
    DATE '1994-04-04',
    DATE '2026-06-01',
    'ACTIVE',
    'PAT023',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- dob and first_ship_date both NULL
    'DrugX',
    DATE '2026-07-01',
    28,
    84,
    'TREAT808',
    NULL,
    NULL,
    'ACTIVE',
    'PAT024',
    'commercial',
    'ARRIVED',
    'YES'
  UNION ALL
  SELECT
    -- ship_date in the future
    'DrugY',
    DATE '2027-01-01',
    28,
    84,
    'TREAT909',
    DATE '2001-01-01',
    DATE '2027-01-01',
    'ACTIVE',
    'PAT025',
    'commercial',
    'ARRIVED',
    'YES'
)

-- Validation Query: Select all test data for review
-- This query returns all generated test records for validation and downstream use
SELECT *
FROM test_patient_therapy_shipment
;

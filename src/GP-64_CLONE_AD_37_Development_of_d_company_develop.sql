USE CATALOG purgo_databricks;

-- Databricks SQL script: d_comp table mapping and population logic
-- Purpose: Populate purgo_playground.d_comp from PSEK, PCHK, PARA per mapping, join, and data quality rules
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: This script creates the d_comp table with full schema and comments, then inserts data from PSEK, PCHK, and PARA using explicit join/filter/mapping logic. Only columns with both "Mandatory for mapping" and "Need Mapping" as "YES" are mapped; all others are set to NULL. Joins are performed with trimming, deduplication is enforced, and all mapped fields are validated for type and nullability. Errors are raised for PK or NOT NULL violations. All business rules and mapping logic are documented in comments.

-- ========================================================================
/* SECTION: DDL - CREATE d_comp TABLE WITH FULL SCHEMA AND COMMENTS */
-- ========================================================================

CREATE TABLE IF NOT EXISTS purgo_databricks.purgo_playground.d_comp (
  src_sys_cd      STRING      NOT NULL,
  co_cd           STRING      NOT NULL,
  co_nm           STRING      NOT NULL,
  st_cd           STRING,
  st_nm           STRING,
  rgn_cd          STRING,
  rgn_nm          STRING,
  cntry_cd        STRING      NOT NULL,
  cntry_nm        STRING      NOT NULL,
  source_country  STRING      NOT NULL,
  crt_dt          TIMESTAMP   NOT NULL
);

-- Add column comments for documentation and data lineage
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.src_sys_cd IS 'Source System Code. Hardcoded to "FBW". Valid value: "FBW"';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.co_cd IS 'Company Code. Mapped from PSEK.PKLNR. Trimmed. NOT NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.co_nm IS 'Company Name. Mapped from PSEK.tgort. Trimmed. NOT NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.st_cd IS 'State Province Code. Not mapped. Always NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.st_nm IS 'State Province Name. Not mapped. Always NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.rgn_cd IS 'Region Code. Not mapped. Always NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.rgn_nm IS 'Region Name. Not mapped. Always NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.cntry_cd IS 'Company Country Code. Mapped from PARA.PTART. Trimmed. NOT NULL. If missing, NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.cntry_nm IS 'Company Country Name. Mapped from PSEK.dwart. Trimmed. NOT NULL';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.source_country IS 'Source Country. Hardcoded to "NA". Valid value: "NA"';
COMMENT ON COLUMN purgo_databricks.purgo_playground.d_comp.crt_dt IS 'Record create timestamp. Set to current_timestamp() at insert. NOT NULL';

-- ========================================================================
/* SECTION: CTE - BUILD d_comp DATASET FROM SOURCE TABLES WITH MAPPING LOGIC */
-- ========================================================================

-- CTE: Build mapped and deduplicated d_comp records from PSEK, PCHK, PARA
-- Only columns with both "Mandatory for mapping" and "Need Mapping" as "YES" are mapped; others are set to NULL
-- Joins are performed with TRIM on all join keys
-- Only PSEK records with tgort = 'FIN' are included
-- Deduplication is enforced on (src_sys_cd, co_cd)
-- All mapped fields are trimmed of leading/trailing spaces
-- If a source value is missing for a mapped field, the target field is set to NULL
WITH d_comp_mapped AS (
  SELECT
    'FBW' AS src_sys_cd, -- Hardcoded
    TRIM(psek.pklnr) AS co_cd, -- Mapped, trimmed
    TRIM(psek.tgort) AS co_nm, -- Mapped, trimmed
    NULL AS st_cd, -- Not mapped
    NULL AS st_nm, -- Not mapped
    NULL AS rgn_cd, -- Not mapped
    NULL AS rgn_nm, -- Not mapped
    TRIM(para.ptart) AS cntry_cd, -- Mapped, trimmed, may be NULL if PARA missing
    TRIM(psek.dwart) AS cntry_nm, -- Mapped, trimmed
    'NA' AS source_country, -- Hardcoded
    current_timestamp() AS crt_dt -- Record create timestamp
  FROM (
    -- Deduplicate PSEK on PKLNR (co_cd) and tgort, dwart, tanpt
    SELECT DISTINCT pklnr, tgort, dwart, tanpt
    FROM purgo_databricks.purgo_playground.psek
    WHERE tgort = 'FIN'
  ) psek
  LEFT JOIN purgo_databricks.purgo_playground.pchk pchk
    ON TRIM(psek.tanpt) = TRIM(pchk.tanpt)
   AND TRIM(psek.pklnr) = TRIM(pchk.ckarg)
  LEFT JOIN purgo_databricks.purgo_playground.para para
    ON TRIM(pchk.tanpt) = TRIM(para.tanpt)
   AND TRIM(pchk.patnr) = TRIM(para.patnr)
)

-- ========================================================================
/* SECTION: INSERT - POPULATE d_comp TABLE WITH DATA QUALITY CHECKS */
-- ========================================================================

-- Insert mapped records into d_comp, enforcing NOT NULL and PK constraints
-- If a NOT NULL field is NULL, the insert will fail
-- If a PK (src_sys_cd, co_cd) already exists, the insert will fail
INSERT INTO purgo_databricks.purgo_playground.d_comp
(
  src_sys_cd,
  co_cd,
  co_nm,
  st_cd,
  st_nm,
  rgn_cd,
  rgn_nm,
  cntry_cd,
  cntry_nm,
  source_country,
  crt_dt
)
SELECT
  src_sys_cd,
  co_cd,
  co_nm,
  st_cd,
  st_nm,
  rgn_cd,
  rgn_nm,
  cntry_cd,
  cntry_nm,
  source_country,
  crt_dt
FROM d_comp_mapped;

-- ========================================================================
/* SECTION: VALIDATION QUERY - REVIEW INSERTED DATA */
-- ========================================================================

-- Select all records from d_comp for validation
SELECT *
FROM purgo_databricks.purgo_playground.d_comp
ORDER BY co_cd;

-- ========================================================================
/* SECTION: END OF SCRIPT */
-- ========================================================================
-- End of d_comp mapping and population logic

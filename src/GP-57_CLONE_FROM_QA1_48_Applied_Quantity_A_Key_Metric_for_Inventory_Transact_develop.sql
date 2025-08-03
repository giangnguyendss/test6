USE CATALOG purgo_databricks;

-- Databricks SQL script: Calculate and update apl_qty in purgo_playground.f_inv_movmnt_apl_qty according to business logic
-- Purpose: Implements the applied quantity (apl_qty) calculation for inventory movement transactions
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: This script defines a scalar UDF for the apl_qty calculation logic, applies it to the target table, and updates the apl_qty field. The logic covers all specified business conditions, handles nulls, and ensures correct decimal precision and rounding. All operations are performed in-place on the production table, with comments and structure for maintainability and auditability.

-- Drop the UDF if it exists to avoid conflicts and ensure idempotency
DROP FUNCTION IF EXISTS purgo_playground.udf_apl_qty;

-- Create a scalar UDF to encapsulate the business logic for apl_qty calculation
-- This UDF returns the calculated applied quantity based on the specified conditions
CREATE OR REPLACE FUNCTION purgo_playground.udf_apl_qty(
  ref_txn_qty DECIMAL(3,1),
  cumulative_txn_qty DECIMAL(4,1),
  cumulative_ref_ord_sched_qty DECIMAL(4,1),
  ref_ord_sched_qty DECIMAL(3,1),
  prior_cumulative_txn_qty DECIMAL(3,1),
  prior_cumulative_ref_ord_sched_qty DECIMAL(3,1)
)
RETURNS DECIMAL(5,1)
RETURN
  CASE
    WHEN ref_txn_qty IS NULL OR cumulative_txn_qty IS NULL OR cumulative_ref_ord_sched_qty IS NULL
         OR ref_ord_sched_qty IS NULL OR prior_cumulative_txn_qty IS NULL OR prior_cumulative_ref_ord_sched_qty IS NULL
      THEN NULL
    WHEN ref_txn_qty > 0 AND cumulative_txn_qty >= cumulative_ref_ord_sched_qty THEN
      CASE
        WHEN prior_cumulative_ref_ord_sched_qty < prior_cumulative_txn_qty
          THEN ROUND(ref_ord_sched_qty - (prior_cumulative_txn_qty - prior_cumulative_ref_ord_sched_qty), 1)
        ELSE ref_ord_sched_qty
      END
    WHEN ref_txn_qty > 0 AND cumulative_ref_ord_sched_qty >= cumulative_txn_qty THEN
      CASE
        WHEN prior_cumulative_ref_ord_sched_qty > prior_cumulative_txn_qty
          THEN ROUND(ref_txn_qty - (prior_cumulative_ref_ord_sched_qty - prior_cumulative_txn_qty), 1)
        ELSE ref_txn_qty
      END
    WHEN ref_txn_qty < 0 AND cumulative_txn_qty != 0 AND cumulative_ref_ord_sched_qty > 0
      THEN ref_txn_qty
    ELSE NULL
  END
;

-- Update the apl_qty field in the production table using the UDF and explicit column mapping
-- This ensures that all records are recalculated according to the latest business logic
UPDATE purgo_playground.f_inv_movmnt_apl_qty
SET apl_qty = purgo_playground.udf_apl_qty(
  ref_txn_qty,
  cumulative_txn_qty,
  cumulative_ref_ord_sched_qty,
  ref_ord_sched_qty,
  prior_cumulative_txn_qty,
  prior_cumulative_ref_ord_sched_qty
)
;

-- Add column comments for documentation and data governance
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.txn_id IS 'Unique transaction identifier. String. May include special characters.';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.ref_txn_qty IS 'Reference transaction quantity. DECIMAL(3,1). May be positive or negative.';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.cumulative_txn_qty IS 'Cumulative transaction quantity. DECIMAL(4,1).';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.cumulative_ref_ord_sched_qty IS 'Cumulative reference order schedule quantity. DECIMAL(4,1).';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.ref_ord_sched_qty IS 'Reference order schedule quantity. DECIMAL(3,1).';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.prior_cumulative_txn_qty IS 'Prior cumulative transaction quantity. DECIMAL(3,1).';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.prior_cumulative_ref_ord_sched_qty IS 'Prior cumulative reference order schedule quantity. DECIMAL(3,1).';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt_apl_qty.apl_qty IS
  'Applied quantity calculated as follows:
   - Condition 1: If ref_txn_qty > 0 AND cumulative_txn_qty >= cumulative_ref_ord_sched_qty:
       - If prior_cumulative_ref_ord_sched_qty < prior_cumulative_txn_qty:
           apl_qty = ref_ord_sched_qty - (prior_cumulative_txn_qty - prior_cumulative_ref_ord_sched_qty)
       - Else:
           apl_qty = ref_ord_sched_qty
   - Condition 2: If ref_txn_qty > 0 AND cumulative_ref_ord_sched_qty >= cumulative_txn_qty:
       - If prior_cumulative_ref_ord_sched_qty > prior_cumulative_txn_qty:
           apl_qty = ref_txn_qty - (prior_cumulative_ref_ord_sched_qty - prior_cumulative_txn_qty)
       - Else:
           apl_qty = ref_txn_qty
   - Condition 3: If ref_txn_qty < 0 AND cumulative_txn_qty != 0 AND cumulative_ref_ord_sched_qty > 0:
       - apl_qty = ref_txn_qty
   - Default: If none of the above conditions are met or any input is NULL, apl_qty = NULL.
   DECIMAL(5,1). Rounded to 1 decimal place.';

-- End of script: All logic, documentation, and updates complete

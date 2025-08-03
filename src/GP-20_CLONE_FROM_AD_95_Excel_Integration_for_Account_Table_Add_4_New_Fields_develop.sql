USE CATALOG purgo_databricks;

-- Databricks SQL script: Create and update 'Account' table for patient-related details in centralized account management
-- Purpose: Create the 'Account' table with initial columns, then add 4 new patient-related columns as per requirements
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: Drops the 'Account' table if it exists, creates it with specified columns and datatypes, 
-- then alters the table to add 4 new columns from 'patient_addition_field.xlsx'. 
-- All columns are nullable, no default values, and no primary/unique constraints.

-- Drop the 'Account' table if it already exists for idempotency
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.Account;

-- Create the 'Account' table with initial columns and datatypes
CREATE TABLE purgo_databricks.purgo_playground.Account (
  Organization_Corporate_Parent__c STRING,
  Organization_Level__c STRING,
  Address__c STRING,
  Health_Industry_Number__c STRING,
  Class_of_Trade_Facility_Type__c STRING,
  Classification_type__c STRING,
  Classification_sub_type__c STRING,
  Contracted_340B__c BOOLEAN
)
USING DELTA;

-- Add 4 new columns from patient_addition_field.xlsx
ALTER TABLE purgo_databricks.purgo_playground.Account ADD COLUMNS (
  Operational_Status STRING,
  Provider_Network_Type STRING,
  Regulatory_Compliance_Code STRING,
  Tax_Identification_Number STRING
);

-- Add column descriptions for documentation and data governance
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Organization_Corporate_Parent__c IS 'Corporate parent organization name. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Organization_Level__c IS 'Organization level. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Address__c IS 'Address of the organization. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Health_Industry_Number__c IS 'Health Industry Number. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Class_of_Trade_Facility_Type__c IS 'Class of trade or facility type. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Classification_type__c IS 'Classification type. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Classification_sub_type__c IS 'Classification sub type. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Contracted_340B__c IS 'Indicates if contracted 340B. BOOLEAN. Valid values: TRUE, FALSE, NULL.';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Operational_Status IS 'Operational status of the account. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Provider_Network_Type IS 'Provider network type. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Regulatory_Compliance_Code IS 'Regulatory compliance code. STRING(255).';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Tax_Identification_Number IS 'Tax identification number. STRING(255).';

-- Validation CTE: Check that all columns exist with correct data types and nullability
-- This CTE can be used in further validation queries as needed
WITH account_schema_validation AS (
  SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
  FROM information_schema.columns
  WHERE table_catalog = 'purgo_databricks'
    AND table_schema = 'purgo_playground'
    AND table_name = 'Account'
)
SELECT * FROM account_schema_validation
ORDER BY column_name;

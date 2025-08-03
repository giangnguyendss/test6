USE CATALOG purgo_databricks;

-- Databricks SQL script: Create and alter 'Account' table in purgo_playground schema with required columns and comments
-- Purpose: Create the 'Account' table with columns from patient_reg.xlsx, add 4 new columns from patient_addition_field.xlsx, and enforce schema requirements
-- Author: Giang Nguyen
-- Date: 2025-08-03
-- Description: Drops 'Account' table if it exists, creates it with specified columns and types, adds 4 new columns, and documents schema with column comments. All columns are nullable, no default values, no primary key, no partitioning, and table is managed.

-- Drop the 'Account' table if it already exists to ensure a clean creation
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.Account;

-- Create the 'Account' table with initial columns from patient_reg.xlsx
CREATE TABLE purgo_databricks.purgo_playground.Account (
  Organization_Corporate_Parent__c STRING,         -- Organization corporate parent, max 255 chars, nullable
  Organization_Level__c STRING,                    -- Organization level, max 255 chars, nullable
  Address__c STRING,                              -- Address, max 255 chars, nullable
  Health_Industry_Number__c STRING,               -- Health industry number, max 255 chars, nullable
  Class_of_Trade_Facility_Type__c STRING,         -- Class of trade/facility type, max 255 chars, nullable
  Classification_type__c STRING,                  -- Classification type, max 255 chars, nullable
  Classification_sub_type__c STRING,              -- Classification sub type, max 255 chars, nullable
  Contracted_340B__c BOOLEAN                      -- Contracted 340B flag, nullable
)
USING DELTA;

-- Add 4 new columns from patient_addition_field.xlsx to the 'Account' table
ALTER TABLE purgo_databricks.purgo_playground.Account ADD COLUMNS (
  Operational_Status STRING,                           -- Operational status, nullable
  Provider_Network_Type STRING,                        -- Provider network type, nullable
  Regulatory_Compliance_Code STRING,                   -- Regulatory compliance code, nullable
  Tax_Identification_Number STRING                     -- Tax identification number, nullable
);

-- Add column comments for documentation and data governance
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Organization_Corporate_Parent__c IS
  'Organization corporate parent (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Organization_Level__c IS
  'Organization level (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Address__c IS
  'Address (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Health_Industry_Number__c IS
  'Health industry number (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Class_of_Trade_Facility_Type__c IS
  'Class of trade/facility type (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Classification_type__c IS
  'Classification type (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Classification_sub_type__c IS
  'Classification sub type (max 255 chars, nullable)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Contracted_340B__c IS
  'Contracted 340B flag (BOOLEAN, valid values: TRUE, FALSE, NULL)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Operational_Status IS
  'Operational status (nullable, valid values documented in business glossary)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Provider_Network_Type IS
  'Provider network type (nullable, valid values documented in business glossary)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Regulatory_Compliance_Code IS
  'Regulatory compliance code (nullable, valid values documented in business glossary)';
COMMENT ON COLUMN purgo_databricks.purgo_playground.Account.Tax_Identification_Number IS
  'Tax identification number (nullable, valid values documented in business glossary)';

-- CTE: Validation query to check column data types and lengths
-- This CTE returns the column name, data type, and character maximum length for all columns in the 'Account' table
WITH column_info AS (
  SELECT
    column_name,
    data_type,
    character_maximum_length
  FROM information_schema.columns
  WHERE table_catalog = 'purgo_databricks'
    AND table_schema = 'purgo_playground'
    AND table_name = 'Account'
)
SELECT * FROM column_info;

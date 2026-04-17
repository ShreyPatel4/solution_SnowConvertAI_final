-- =============================================================================
-- Snowflake view reconstructions
-- =============================================================================
-- Views reconstructed from proc call-sites (original Views/ folder was empty):
--   * vw_AllocationRuleTargets  — shreds AllocationRule.TargetSpecification
--
-- AllocationRule.TargetSpecification is stored as VARIANT on Snowflake
-- (migrated from XML on SQL Server).  Seeding will populate it as JSON:
--   {
--     "Targets": [
--       {"CostCenterID": 10, "Percentage": 0.3, "IsActive": true},
--       ...
--     ]
--   }
-- The view uses LATERAL FLATTEN to shred the array into one row per target,
-- replacing SQL Server's XML .nodes() + .value() pattern.
-- =============================================================================

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


CREATE OR REPLACE VIEW vw_AllocationRuleTargets AS
SELECT
    ar.AllocationRuleID,
    tgt.VALUE:CostCenterID::NUMBER        AS TargetCostCenterID,
    tgt.VALUE:Percentage::NUMBER(8,6)     AS TargetAllocationPct,
    tgt.VALUE:IsActive::BOOLEAN           AS TargetIsActive
FROM AllocationRule ar,
     LATERAL FLATTEN(input => ar.TargetSpecification:Targets) tgt;

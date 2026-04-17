-- =============================================================================
-- Snowflake UDF reconstructions
-- =============================================================================
-- Functions reconstructed from proc call-sites (original Functions/ folder
-- was empty in the provided zip):
--   * tvf_ExplodeCostCenterHierarchy  — UDTF (table UDF) with recursive CTE
--   * fn_GetAllocationFactor          — scalar UDF
--
-- Call-site translation notes:
--   SQL Server:  FROM Planning.tvf_ExplodeCostCenterHierarchy(NULL, 10, 0, GETDATE()) h
--   Snowflake:   FROM TABLE(tvf_ExplodeCostCenterHierarchy(NULL::INT, 10, FALSE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)) h
--   -> table UDFs in Snowflake are called inside TABLE(...).
-- =============================================================================

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


-- -----------------------------------------------------------------------------
-- tvf_ExplodeCostCenterHierarchy
--   Multi-statement TVF -> Snowflake SQL UDTF with a recursive CTE body.
--   Returns descendants of @RootID (or all roots when RootID IS NULL) up to
--   MaxDepth, filtered by effective-date and optionally IsActive.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION tvf_ExplodeCostCenterHierarchy(
    root_id           NUMBER,
    max_depth         NUMBER,
    include_inactive  BOOLEAN,
    as_of_date        TIMESTAMP_NTZ
)
RETURNS TABLE (
    CostCenterID          NUMBER,
    ParentCostCenterID    NUMBER,
    HierarchyLevel        NUMBER,
    CostCenterCode        VARCHAR,
    IsActive              BOOLEAN,
    Depth                 NUMBER
)
AS
$$
    WITH RECURSIVE h AS (
        SELECT
            cc.CostCenterID,
            cc.ParentCostCenterID,
            0::NUMBER AS HierarchyLevel,
            cc.CostCenterCode,
            cc.IsActive,
            0::NUMBER AS Depth
        FROM PLANNING.CostCenter cc
        WHERE ((root_id IS NULL AND cc.ParentCostCenterID IS NULL)
               OR cc.CostCenterID = root_id)
          AND (include_inactive OR cc.IsActive)
          AND cc.EffectiveFromDate <= as_of_date::DATE
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= as_of_date::DATE)

        UNION ALL

        SELECT
            cc.CostCenterID,
            cc.ParentCostCenterID,
            h.HierarchyLevel + 1,
            cc.CostCenterCode,
            cc.IsActive,
            h.Depth + 1
        FROM PLANNING.CostCenter cc
        INNER JOIN h ON cc.ParentCostCenterID = h.CostCenterID
        WHERE h.Depth < max_depth
          AND (include_inactive OR cc.IsActive)
          AND cc.EffectiveFromDate <= as_of_date::DATE
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= as_of_date::DATE)
    )
    SELECT CostCenterID, ParentCostCenterID, HierarchyLevel, CostCenterCode, IsActive, Depth
    FROM h
$$;


-- -----------------------------------------------------------------------------
-- fn_GetAllocationFactor
--   Scalar UDF.  Returns the target cost center's AllocationWeight as the
--   allocation factor (verification stub; original basis-specific logic
--   was not included in the zip).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_GetAllocationFactor(
    source_cc          NUMBER,
    target_cc          NUMBER,
    allocation_basis   VARCHAR,
    fiscal_period_id   NUMBER,
    budget_header_id   NUMBER
)
RETURNS NUMBER(8,6)
AS
$$
    COALESCE(
        (SELECT AllocationWeight
         FROM PLANNING.CostCenter
         WHERE CostCenterID = target_cc AND IsActive),
        0.0
    )
$$;

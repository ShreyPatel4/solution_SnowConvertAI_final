-- =============================================================================
-- Reconstructed Functions + Views for the SQL Server baseline
-- =============================================================================
-- The provided zip omits Functions/ and Views/.  Signatures reconstructed from
-- the source README's object inventory + actual procedure call sites:
--   * tvf_ExplodeCostCenterHierarchy — usp_ProcessBudgetConsolidation:218
--   * fn_GetAllocationFactor         — usp_ExecuteCostAllocation:285, :294
--   * vw_AllocationRuleTargets       — usp_ExecuteCostAllocation:307-310
--
-- Scope: only the objects required by proc 1 (Consolidation) and proc 3
-- (CostAllocation).  Deferred (not reconstructed): fn_GetHierarchyPath,
-- tvf_GetBudgetVariance, vw_BudgetConsolidationSummary.
-- =============================================================================

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

USE Planning;
GO

-- -----------------------------------------------------------------------------
-- tvf_ExplodeCostCenterHierarchy
--   Called as: tvf_ExplodeCostCenterHierarchy(NULL, 10, 0, GETDATE())
--   Caller reads h.CostCenterID, h.ParentCostCenterID, h.HierarchyLevel.
--
--   Original was a multi-statement TVF per README (table variable + WHILE
--   loop).  Reconstructed as an INLINE TVF with a recursive CTE — simpler,
--   inlines into the caller's plan, same tabular contract.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('Planning.tvf_ExplodeCostCenterHierarchy', 'IF') IS NOT NULL
    DROP FUNCTION Planning.tvf_ExplodeCostCenterHierarchy;
GO

CREATE FUNCTION Planning.tvf_ExplodeCostCenterHierarchy(
    @RootID            INT,
    @MaxDepth          INT,
    @IncludeInactive   BIT,
    @AsOfDate          DATETIME
)
RETURNS TABLE
AS
RETURN (
    WITH Hierarchy AS (
        -- Anchor: @RootID's node, or all roots when @RootID IS NULL.
        SELECT
            cc.CostCenterID,
            cc.ParentCostCenterID,
            cc.HierarchyLevel,
            cc.CostCenterCode,
            cc.IsActive,
            0 AS Depth
        FROM Planning.CostCenter cc
        WHERE ((@RootID IS NULL AND cc.ParentCostCenterID IS NULL)
               OR cc.CostCenterID = @RootID)
          AND (@IncludeInactive = 1 OR cc.IsActive = 1)
          AND cc.EffectiveFromDate <= CAST(@AsOfDate AS DATE)
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= CAST(@AsOfDate AS DATE))

        UNION ALL

        SELECT
            cc.CostCenterID,
            cc.ParentCostCenterID,
            cc.HierarchyLevel,
            cc.CostCenterCode,
            cc.IsActive,
            h.Depth + 1
        FROM Planning.CostCenter cc
        INNER JOIN Hierarchy h ON cc.ParentCostCenterID = h.CostCenterID
        WHERE h.Depth < @MaxDepth
          AND (@IncludeInactive = 1 OR cc.IsActive = 1)
          AND cc.EffectiveFromDate <= CAST(@AsOfDate AS DATE)
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= CAST(@AsOfDate AS DATE))
    )
    SELECT
        CostCenterID,
        ParentCostCenterID,
        HierarchyLevel,
        CostCenterCode,
        IsActive,
        Depth
    FROM Hierarchy
);
GO


-- -----------------------------------------------------------------------------
-- fn_GetAllocationFactor
--   Called as: fn_GetAllocationFactor(source_cc, target_cc, basis, period, budget)
--   Returns a DECIMAL(8,6) allocation percentage.
--
--   Reconstruction: returns the target cost center's AllocationWeight as the
--   factor.  The original business logic (presumably basis-specific — e.g.
--   HEADCOUNT lookups, REVENUE ratios) was not included.  This stub lets the
--   caller execute and produces deterministic numbers for verification.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('Planning.fn_GetAllocationFactor', 'FN') IS NOT NULL
    DROP FUNCTION Planning.fn_GetAllocationFactor;
GO

CREATE FUNCTION Planning.fn_GetAllocationFactor(
    @SourceCostCenterID    INT,
    @TargetCostCenterID    INT,
    @AllocationBasis       VARCHAR(30),
    @FiscalPeriodID        INT,
    @BudgetHeaderID        INT
)
RETURNS DECIMAL(8,6)
AS
BEGIN
    DECLARE @factor DECIMAL(8,6);

    SELECT @factor = cc.AllocationWeight
    FROM Planning.CostCenter cc
    WHERE cc.CostCenterID = @TargetCostCenterID
      AND cc.IsActive = 1;

    RETURN ISNULL(@factor, 0.0);
END;
GO


-- -----------------------------------------------------------------------------
-- vw_AllocationRuleTargets
--   Shreds AllocationRule.TargetSpecification XML into one row per target.
--   Expected output columns (from usp_ExecuteCostAllocation usage):
--     AllocationRuleID, TargetCostCenterID, TargetAllocationPct, TargetIsActive
--
--   Assumed XML shape (seeded consistently):
--     <Targets>
--       <Target>
--         <CostCenterID>10</CostCenterID>
--         <Percentage>0.3</Percentage>
--         <IsActive>1</IsActive>
--       </Target>
--       ...
--     </Targets>
-- -----------------------------------------------------------------------------
IF OBJECT_ID('Planning.vw_AllocationRuleTargets', 'V') IS NOT NULL
    DROP VIEW Planning.vw_AllocationRuleTargets;
GO

CREATE VIEW Planning.vw_AllocationRuleTargets
AS
SELECT
    ar.AllocationRuleID,
    t.c.value('(CostCenterID)[1]', 'INT')          AS TargetCostCenterID,
    t.c.value('(Percentage)[1]', 'DECIMAL(8,6)')   AS TargetAllocationPct,
    t.c.value('(IsActive)[1]', 'BIT')              AS TargetIsActive
FROM Planning.AllocationRule ar
CROSS APPLY ar.TargetSpecification.nodes('/Targets/Target') AS t(c);
GO

-- =============================================================================
-- Deterministic seed fixtures for Snowflake target
-- =============================================================================
-- Mirror of sqlserver/10_seed.sql.  Identical surrogate IDs (via explicit
-- column values on AUTOINCREMENT cols, which Snowflake accepts) and identical
-- natural keys, so proc 1 output is row-for-row comparable.
--
-- Snowflake-specific differences vs. SQL Server seed:
--   * HierarchyPath is a VARCHAR (materialized path), not HIERARCHYID
--   * HierarchyLevel is a regular INT, populated explicitly (SQL Server derives
--     it from HierarchyPath.GetLevel() via a PERSISTED computed column)
--   * FinalAmount is populated explicitly (SQL Server PERSISTED computed col)
--   * RowHash is computed via SHA2 with the same formula as the SQL Server
--     schema.  Cross-engine bitwise equality is best-effort — if SQL Server's
--     CAST AS VARCHAR of a DECIMAL formats differently from Snowflake's, the
--     hashes will diverge.  The verification harness compares value columns
--     (not RowHash) as the authoritative diff.
-- =============================================================================

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


-- -----------------------------------------------------------------------------
-- Clear prior seed (reverse FK order).  Snowflake has no SYSTEM_VERSIONING
-- toggle; CostCenterHistory is just a mirror table maintained by procs/tasks.
-- -----------------------------------------------------------------------------
DELETE FROM ConsolidationJournalLine;
DELETE FROM ConsolidationJournal;
DELETE FROM BudgetLineItem;
DELETE FROM AllocationRule;
DELETE FROM BudgetHeader;
DELETE FROM GLAccount;
DELETE FROM CostCenter;
DELETE FROM CostCenterHistory;
DELETE FROM FiscalPeriod;


-- -----------------------------------------------------------------------------
-- FiscalPeriod
-- -----------------------------------------------------------------------------
INSERT INTO FiscalPeriod
    (FiscalPeriodID, FiscalYear, FiscalQuarter, FiscalMonth, PeriodName,
     PeriodStartDate, PeriodEndDate, IsClosed, IsAdjustmentPeriod)
VALUES
    (1, 2026, 1, 1, 'Jan 2026', '2026-01-01', '2026-01-31', FALSE, FALSE),
    (2, 2026, 1, 2, 'Feb 2026', '2026-02-01', '2026-02-28', FALSE, FALSE),
    (3, 2026, 1, 3, 'Mar 2026', '2026-03-01', '2026-03-31', FALSE, FALSE);


-- -----------------------------------------------------------------------------
-- CostCenter
--   HierarchyPath is materialized-path VARCHAR; HierarchyLevel is provided
--   explicitly (proc-maintained on writes in the migrated code).
-- -----------------------------------------------------------------------------
INSERT INTO CostCenter
    (CostCenterID, CostCenterCode, CostCenterName, ParentCostCenterID,
     HierarchyPath, HierarchyLevel, IsActive, EffectiveFromDate, AllocationWeight)
VALUES
    (1, 'CORP',     'Corporate',                NULL, '/1/',     0, TRUE, '2020-01-01', 1.0000),
    (2, 'SALES',    'Sales Division',           1,    '/1/2/',   1, TRUE, '2020-01-01', 0.4000),
    (3, 'OPS',      'Operations',               1,    '/1/3/',   1, TRUE, '2020-01-01', 0.4000),
    (4, 'IT',       'Information Technology',   1,    '/1/4/',   1, TRUE, '2020-01-01', 0.2000),
    (5, 'SALES_NA', 'Sales North America',      2,    '/1/2/5/', 2, TRUE, '2020-01-01', 0.6000),
    (6, 'SALES_EU', 'Sales Europe',             2,    '/1/2/6/', 2, TRUE, '2020-01-01', 0.4000),
    (7, 'OPS_MFG',  'Operations Manufacturing', 3,    '/1/3/7/', 2, TRUE, '2020-01-01', 1.0000);


-- -----------------------------------------------------------------------------
-- GLAccount
-- -----------------------------------------------------------------------------
INSERT INTO GLAccount
    (GLAccountID, AccountNumber, AccountName, AccountType,
     IsPostable, IsBudgetable, NormalBalance, IntercompanyFlag, IsActive)
VALUES
    (1, '4000', 'Revenue',                 'R', TRUE, TRUE, 'C', FALSE, TRUE),
    (2, '5000', 'COGS',                    'X', TRUE, TRUE, 'D', FALSE, TRUE),
    (3, '6000', 'Operating Expenses',      'X', TRUE, TRUE, 'D', FALSE, TRUE),
    (4, '7000', 'Intercompany Payable',    'L', TRUE, TRUE, 'C', TRUE,  TRUE),
    (5, '8000', 'Intercompany Receivable', 'A', TRUE, TRUE, 'D', TRUE,  TRUE);


-- -----------------------------------------------------------------------------
-- BudgetHeader
-- -----------------------------------------------------------------------------
INSERT INTO BudgetHeader
    (BudgetHeaderID, BudgetCode, BudgetName, BudgetType, ScenarioType,
     FiscalYear, StartPeriodID, EndPeriodID, StatusCode, VersionNumber, IsLocked)
VALUES
    (1, 'FY26_BASE', 'FY26 Base Budget', 'ANNUAL', 'BASE',
     2026, 1, 3, 'APPROVED', 1, FALSE);


-- -----------------------------------------------------------------------------
-- BudgetLineItem (12 rows).  FinalAmount and RowHash computed inline.
-- -----------------------------------------------------------------------------
INSERT INTO BudgetLineItem
    (BudgetLineItemID, BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
     OriginalAmount, AdjustedAmount, FinalAmount, IsAllocated, RowHash)
SELECT
    id, 1, gl, cc, fp,
    orig, adj,
    orig + adj,
    FALSE,
    SHA2(
        CAST(gl AS VARCHAR) || '|' ||
        CAST(cc AS VARCHAR) || '|' ||
        CAST(fp AS VARCHAR) || '|' ||
        CAST(orig + adj AS VARCHAR),
        256
    )
FROM VALUES
    (1,  1, 5, 1, 10000.0000,    0.0000),
    (2,  1, 6, 1,  8000.0000,    0.0000),
    (3,  1, 5, 2, 11000.0000,  500.0000),
    (4,  1, 6, 2,  9000.0000,    0.0000),
    (5,  2, 5, 1,  4000.0000,    0.0000),
    (6,  2, 6, 1,  3000.0000,    0.0000),
    (7,  3, 7, 1,  2000.0000,    0.0000),
    (8,  3, 4, 1,  1500.0000,    0.0000),
    (9,  4, 5, 1,  1000.0000,    0.0000),
    (10, 5, 5, 1, -1000.0000,    0.0000),
    (11, 3, 5, 3,     0.0000,    0.0000),
    (12, 2, 7, 3,  5000.0000, -500.0000)
AS v(id, gl, cc, fp, orig, adj);


-- -----------------------------------------------------------------------------
-- AllocationRule (for proc 3 — usp_ExecuteCostAllocation).
-- Mirrors sqlserver/10_seed.sql.  TargetSpecification is VARIANT JSON here
-- (vs. XML on SQL Server); shape matches the vw_AllocationRuleTargets view's
-- JSON path reader (Targets[].CostCenterID / Percentage / IsActive).
-- IDs 10, 11 match the SQL Server side.
-- -----------------------------------------------------------------------------
INSERT INTO AllocationRule
    (AllocationRuleID, RuleCode, RuleName, RuleType, AllocationMethod,
     SourceCostCenterID, SourceCostCenterPattern, SourceAccountPattern,
     TargetSpecification, AllocationBasis, RoundingMethod, RoundingPrecision,
     ExecutionSequence, DependsOnRuleID, EffectiveFromDate, EffectiveToDate, IsActive)
SELECT
    id, code, rname, rtype, amethod,
    scc, sccp, sap,
    PARSE_JSON(tspec),
    basis, rmeth, rprec,
    eseq, depid, effrom, effto, act
FROM VALUES
    (10, 'RULE_NA_SPLIT', 'NA Rev Split', 'DIRECT', 'FIXED_PCT',
     5, NULL, '4%',
     '{"Targets":[{"CostCenterID":1,"Percentage":0.600000,"IsActive":true},{"CostCenterID":4,"Percentage":0.400000,"IsActive":true}]}',
     'FIXED', 'NEAREST', 2,
     10, NULL, '2020-01-01'::DATE, NULL, TRUE),
    (11, 'RULE_EU_CORP', 'EU Rev to Sales', 'DIRECT', 'FIXED_PCT',
     6, NULL, '4%',
     '{"Targets":[{"CostCenterID":2,"Percentage":1.000000,"IsActive":true}]}',
     'FIXED', 'NEAREST', 2,
     20, NULL, '2020-01-01'::DATE, NULL, TRUE)
AS v(id, code, rname, rtype, amethod, scc, sccp, sap, tspec, basis, rmeth, rprec, eseq, depid, effrom, effto, act);


-- -----------------------------------------------------------------------------
-- Row-count verification (expected: 3, 7, 5, 1, 12, 2)
-- -----------------------------------------------------------------------------
SELECT 'FiscalPeriod'   AS TableName, COUNT(*) AS N FROM FiscalPeriod
UNION ALL SELECT 'CostCenter',     COUNT(*) FROM CostCenter
UNION ALL SELECT 'GLAccount',      COUNT(*) FROM GLAccount
UNION ALL SELECT 'BudgetHeader',   COUNT(*) FROM BudgetHeader
UNION ALL SELECT 'BudgetLineItem', COUNT(*) FROM BudgetLineItem
UNION ALL SELECT 'AllocationRule', COUNT(*) FROM AllocationRule
ORDER BY TableName;

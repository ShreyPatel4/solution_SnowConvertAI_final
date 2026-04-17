-- =============================================================================
-- Deterministic seed fixtures for SQL Server baseline
-- =============================================================================
-- Scope: the 5 tables proc 1 needs (FiscalPeriod, CostCenter, GLAccount,
-- BudgetHeader, BudgetLineItem).  AllocationRule / Journal deferred.
--
-- Design goals:
--   * Identical natural keys (and surrogate IDs, via IDENTITY_INSERT) on both
--     engines so row-by-row diffing is direct.
--   * Exercises proc 1's branches: 3-level hierarchy, APPROVED source budget,
--     offsetting intercompany pair, zero amount, negative adjustment.
--
-- Re-runnable: starts with a DELETE (with temporal versioning toggled off on
-- CostCenter so history rows can also be wiped).
-- =============================================================================

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

USE Planning;
GO

-- -----------------------------------------------------------------------------
-- Clear prior seed for idempotency (reverse FK dependency order).
-- Temporal CostCenter needs SYSTEM_VERSIONING off to allow DELETE on history.
-- -----------------------------------------------------------------------------
IF EXISTS (
    SELECT 1 FROM sys.tables
    WHERE name = 'CostCenter' AND SCHEMA_NAME(schema_id) = 'Planning'
      AND temporal_type = 2
)
    ALTER TABLE Planning.CostCenter SET (SYSTEM_VERSIONING = OFF);
GO

DELETE FROM Planning.ConsolidationJournalLine;
DELETE FROM Planning.ConsolidationJournal;
DELETE FROM Planning.BudgetLineItem;
DELETE FROM Planning.AllocationRule;
DELETE FROM Planning.BudgetHeader;
DELETE FROM Planning.GLAccount;
DELETE FROM Planning.CostCenter;
IF OBJECT_ID('Planning.CostCenterHistory') IS NOT NULL
    DELETE FROM Planning.CostCenterHistory;
DELETE FROM Planning.FiscalPeriod;
GO

ALTER TABLE Planning.CostCenter
    SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = Planning.CostCenterHistory));
GO


-- -----------------------------------------------------------------------------
-- FiscalPeriod (3 rows: Q1 2026 by month)
-- -----------------------------------------------------------------------------
SET IDENTITY_INSERT Planning.FiscalPeriod ON;
INSERT INTO Planning.FiscalPeriod
    (FiscalPeriodID, FiscalYear, FiscalQuarter, FiscalMonth, PeriodName,
     PeriodStartDate, PeriodEndDate, IsClosed, IsAdjustmentPeriod)
VALUES
    (1, 2026, 1, 1, 'Jan 2026', '2026-01-01', '2026-01-31', 0, 0),
    (2, 2026, 1, 2, 'Feb 2026', '2026-02-01', '2026-02-28', 0, 0),
    (3, 2026, 1, 3, 'Mar 2026', '2026-03-01', '2026-03-31', 0, 0);
SET IDENTITY_INSERT Planning.FiscalPeriod OFF;
GO


-- -----------------------------------------------------------------------------
-- CostCenter (7 rows, 3-level hierarchy)
--   Level 0:   1=CORP
--   Level 1:   2=SALES, 3=OPS, 4=IT
--   Level 2:   5=SALES_NA, 6=SALES_EU, 7=OPS_MFG
-- HierarchyPath populated via HIERARCHYID parse; HierarchyLevel auto-computed
-- (PERSISTED col via HierarchyPath.GetLevel()).
-- -----------------------------------------------------------------------------
SET IDENTITY_INSERT Planning.CostCenter ON;

INSERT INTO Planning.CostCenter
    (CostCenterID, CostCenterCode, CostCenterName, ParentCostCenterID,
     HierarchyPath, IsActive, EffectiveFromDate, AllocationWeight)
VALUES
    (1, 'CORP',    'Corporate',               NULL, CAST('/1/'      AS HIERARCHYID), 1, '2020-01-01', 1.0000);

INSERT INTO Planning.CostCenter
    (CostCenterID, CostCenterCode, CostCenterName, ParentCostCenterID,
     HierarchyPath, IsActive, EffectiveFromDate, AllocationWeight)
VALUES
    (2, 'SALES',   'Sales Division',          1,    CAST('/1/2/'    AS HIERARCHYID), 1, '2020-01-01', 0.4000),
    (3, 'OPS',     'Operations',              1,    CAST('/1/3/'    AS HIERARCHYID), 1, '2020-01-01', 0.4000),
    (4, 'IT',      'Information Technology',  1,    CAST('/1/4/'    AS HIERARCHYID), 1, '2020-01-01', 0.2000);

INSERT INTO Planning.CostCenter
    (CostCenterID, CostCenterCode, CostCenterName, ParentCostCenterID,
     HierarchyPath, IsActive, EffectiveFromDate, AllocationWeight)
VALUES
    (5, 'SALES_NA','Sales North America',     2,    CAST('/1/2/5/'  AS HIERARCHYID), 1, '2020-01-01', 0.6000),
    (6, 'SALES_EU','Sales Europe',            2,    CAST('/1/2/6/'  AS HIERARCHYID), 1, '2020-01-01', 0.4000),
    (7, 'OPS_MFG', 'Operations Manufacturing',3,    CAST('/1/3/7/'  AS HIERARCHYID), 1, '2020-01-01', 1.0000);

SET IDENTITY_INSERT Planning.CostCenter OFF;
GO


-- -----------------------------------------------------------------------------
-- GLAccount (5 rows, including intercompany-flagged pair for elimination tests)
-- -----------------------------------------------------------------------------
SET IDENTITY_INSERT Planning.GLAccount ON;
INSERT INTO Planning.GLAccount
    (GLAccountID, AccountNumber, AccountName, AccountType,
     IsPostable, IsBudgetable, NormalBalance, IntercompanyFlag, IsActive)
VALUES
    (1, '4000', 'Revenue',                 'R', 1, 1, 'C', 0, 1),
    (2, '5000', 'COGS',                    'X', 1, 1, 'D', 0, 1),
    (3, '6000', 'Operating Expenses',      'X', 1, 1, 'D', 0, 1),
    (4, '7000', 'Intercompany Payable',    'L', 1, 1, 'C', 1, 1),
    (5, '8000', 'Intercompany Receivable', 'A', 1, 1, 'D', 1, 1);
SET IDENTITY_INSERT Planning.GLAccount OFF;
GO


-- -----------------------------------------------------------------------------
-- BudgetHeader (1 row, APPROVED — required by proc 1's validation guard)
-- -----------------------------------------------------------------------------
SET IDENTITY_INSERT Planning.BudgetHeader ON;
INSERT INTO Planning.BudgetHeader
    (BudgetHeaderID, BudgetCode, BudgetName, BudgetType, ScenarioType,
     FiscalYear, StartPeriodID, EndPeriodID, StatusCode, VersionNumber)
VALUES
    (1, 'FY26_BASE', 'FY26 Base Budget', 'ANNUAL', 'BASE',
     2026, 1, 3, 'APPROVED', 1);
SET IDENTITY_INSERT Planning.BudgetHeader OFF;
GO


-- -----------------------------------------------------------------------------
-- BudgetLineItem (12 rows)
--   FinalAmount and RowHash are PERSISTED computed — auto-populated on insert.
--   Natural key (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID).
--   Patterns exercised:
--     - Rows 1-4:  Revenue across NA/EU, Jan/Feb (row 3 has adjustment)
--     - Rows 5-6:  COGS NA/EU Jan
--     - Rows 7-8:  OpEx MFG, OpEx IT Jan
--     - Rows 9-10: offsetting intercompany pair (+1000 / -1000)
--     - Row 11:    zero amount (edge case)
--     - Row 12:    negative adjustment
-- -----------------------------------------------------------------------------
SET IDENTITY_INSERT Planning.BudgetLineItem ON;
INSERT INTO Planning.BudgetLineItem
    (BudgetLineItemID, BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
     OriginalAmount, AdjustedAmount, IsAllocated)
VALUES
    (1,  1, 1, 5, 1,  10000.0000,    0.0000, 0),
    (2,  1, 1, 6, 1,   8000.0000,    0.0000, 0),
    (3,  1, 1, 5, 2,  11000.0000,  500.0000, 0),
    (4,  1, 1, 6, 2,   9000.0000,    0.0000, 0),
    (5,  1, 2, 5, 1,   4000.0000,    0.0000, 0),
    (6,  1, 2, 6, 1,   3000.0000,    0.0000, 0),
    (7,  1, 3, 7, 1,   2000.0000,    0.0000, 0),
    (8,  1, 3, 4, 1,   1500.0000,    0.0000, 0),
    (9,  1, 4, 5, 1,   1000.0000,    0.0000, 0),
    (10, 1, 5, 5, 1,  -1000.0000,    0.0000, 0),
    (11, 1, 3, 5, 3,      0.0000,    0.0000, 0),
    (12, 1, 2, 7, 3,   5000.0000, -500.0000, 0);
SET IDENTITY_INSERT Planning.BudgetLineItem OFF;
GO


-- -----------------------------------------------------------------------------
-- Row-count verification (expected: 3, 7, 5, 1, 12)
-- -----------------------------------------------------------------------------
SELECT 'FiscalPeriod'   AS TableName, COUNT(*) AS N FROM Planning.FiscalPeriod
UNION ALL
SELECT 'CostCenter',     COUNT(*) FROM Planning.CostCenter
UNION ALL
SELECT 'GLAccount',      COUNT(*) FROM Planning.GLAccount
UNION ALL
SELECT 'BudgetHeader',   COUNT(*) FROM Planning.BudgetHeader
UNION ALL
SELECT 'BudgetLineItem', COUNT(*) FROM Planning.BudgetLineItem
ORDER BY TableName;
GO

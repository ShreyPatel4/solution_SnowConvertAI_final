-- Baseline SQL Server schema (Docker SQL Server 2022).
-- Generated from original/src/Tables/*.sql + UserDefinedTypes/*.sql in dep order.
-- Modifications vs. original:
--   1. 'FILESTREAM' keyword stripped (FILESTREAM not enabled on the container)
--   2. SET options prepended (required for filtered idx, XML idx, computed-col idx)

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

USE Planning;
GO

-- ==================== FiscalPeriod ====================
/*
    FiscalPeriod - Core reference table for fiscal calendar
    Dependencies: None (base table)
*/
CREATE TABLE Planning.FiscalPeriod (
    FiscalPeriodID          INT IDENTITY(1,1) NOT NULL,
    FiscalYear              SMALLINT NOT NULL,
    FiscalQuarter           TINYINT NOT NULL,
    FiscalMonth             TINYINT NOT NULL,
    PeriodName              NVARCHAR(50) NOT NULL,
    PeriodStartDate         DATE NOT NULL,
    PeriodEndDate           DATE NOT NULL,
    IsClosed                BIT NOT NULL DEFAULT 0,
    ClosedByUserID          INT NULL,
    ClosedDateTime          DATETIME2(7) NULL,
    IsAdjustmentPeriod      BIT NOT NULL DEFAULT 0,
    WorkingDays             TINYINT NULL,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    RowVersionStamp         ROWVERSION NOT NULL,  -- ROWVERSION doesn't exist in Snowflake
    CONSTRAINT PK_FiscalPeriod PRIMARY KEY CLUSTERED (FiscalPeriodID),
    CONSTRAINT UQ_FiscalPeriod_YearMonth UNIQUE (FiscalYear, FiscalMonth),
    CONSTRAINT CK_FiscalPeriod_Quarter CHECK (FiscalQuarter BETWEEN 1 AND 4),
    CONSTRAINT CK_FiscalPeriod_Month CHECK (FiscalMonth BETWEEN 1 AND 13), -- 13 for adjustment period
    CONSTRAINT CK_FiscalPeriod_DateRange CHECK (PeriodEndDate >= PeriodStartDate)
);
GO

-- Filtered index - Snowflake doesn't support filtered indexes
CREATE NONCLUSTERED INDEX IX_FiscalPeriod_OpenPeriods 
ON Planning.FiscalPeriod (FiscalYear, FiscalMonth)
WHERE IsClosed = 0;
GO

-- Include columns in index - different syntax in Snowflake
CREATE NONCLUSTERED INDEX IX_FiscalPeriod_Dates
ON Planning.FiscalPeriod (PeriodStartDate, PeriodEndDate)
INCLUDE (FiscalYear, FiscalQuarter, FiscalMonth);
GO
GO

-- ==================== CostCenter ====================
/*
    CostCenter - Organizational hierarchy for cost allocation
    Dependencies: None (base table)
    
    Note: Uses HierarchyID which has no Snowflake equivalent
*/
CREATE TABLE Planning.CostCenter (
    CostCenterID            INT IDENTITY(1,1) NOT NULL,
    CostCenterCode          VARCHAR(20) NOT NULL,
    CostCenterName          NVARCHAR(100) NOT NULL,
    ParentCostCenterID      INT NULL,
    HierarchyPath           HIERARCHYID NULL,  -- No Snowflake equivalent
    HierarchyLevel          AS HierarchyPath.GetLevel() PERSISTED,  -- Computed column from HierarchyID
    ManagerEmployeeID       INT NULL,
    DepartmentCode          VARCHAR(10) NULL,
    IsActive                BIT NOT NULL DEFAULT 1,
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    AllocationWeight        DECIMAL(5,4) NOT NULL DEFAULT 1.0000,
    ValidFrom               DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,  -- Temporal table
    ValidTo                 DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo),  -- System-versioned temporal table
    CONSTRAINT PK_CostCenter PRIMARY KEY CLUSTERED (CostCenterID),
    CONSTRAINT UQ_CostCenter_Code UNIQUE (CostCenterCode),
    CONSTRAINT FK_CostCenter_Parent FOREIGN KEY (ParentCostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT CK_CostCenter_Weight CHECK (AllocationWeight BETWEEN 0 AND 1)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = Planning.CostCenterHistory));
GO

-- Spatial index on HierarchyID - no equivalent in Snowflake
CREATE UNIQUE INDEX IX_CostCenter_Hierarchy 
ON Planning.CostCenter (HierarchyPath);
GO
GO

-- ==================== GLAccount ====================
/*
    GLAccount - General Ledger Account master
    Dependencies: None (base table)
*/
CREATE TABLE Planning.GLAccount (
    GLAccountID             INT IDENTITY(1,1) NOT NULL,
    AccountNumber           VARCHAR(20) NOT NULL,
    AccountName             NVARCHAR(150) NOT NULL,
    AccountType             CHAR(1) NOT NULL,  -- A=Asset, L=Liability, E=Equity, R=Revenue, X=Expense
    AccountSubType          VARCHAR(30) NULL,
    ParentAccountID         INT NULL,
    AccountLevel            TINYINT NOT NULL DEFAULT 1,
    IsPostable              BIT NOT NULL DEFAULT 1,
    IsBudgetable            BIT NOT NULL DEFAULT 1,
    IsStatistical           BIT NOT NULL DEFAULT 0,
    NormalBalance           CHAR(1) NOT NULL DEFAULT 'D',  -- D=Debit, C=Credit
    CurrencyCode            CHAR(3) NOT NULL DEFAULT 'USD',
    ConsolidationAccountID  INT NULL,
    IntercompanyFlag        BIT NOT NULL DEFAULT 0,
    IsActive                BIT NOT NULL DEFAULT 1,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    -- Sparse columns for rarely-populated attributes - Snowflake doesn't support SPARSE
    TaxCode                 VARCHAR(20) SPARSE NULL,
    StatutoryAccountCode    VARCHAR(30) SPARSE NULL,
    IFRSAccountCode         VARCHAR(30) SPARSE NULL,
    CONSTRAINT PK_GLAccount PRIMARY KEY CLUSTERED (GLAccountID),
    CONSTRAINT UQ_GLAccount_Number UNIQUE (AccountNumber),
    CONSTRAINT FK_GLAccount_Parent FOREIGN KEY (ParentAccountID) 
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT CK_GLAccount_Type CHECK (AccountType IN ('A','L','E','R','X')),
    CONSTRAINT CK_GLAccount_Balance CHECK (NormalBalance IN ('D','C'))
);
GO

-- Columnstore index for analytics - Different implementation in Snowflake
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_GLAccount_Analytics
ON Planning.GLAccount (AccountNumber, AccountName, AccountType, AccountLevel, IsActive);
GO
GO

-- ==================== BudgetHeader ====================
/*
    BudgetHeader - Budget version and scenario header
    Dependencies: FiscalPeriod
*/
CREATE TABLE Planning.BudgetHeader (
    BudgetHeaderID          INT IDENTITY(1,1) NOT NULL,
    BudgetCode              VARCHAR(30) NOT NULL,
    BudgetName              NVARCHAR(100) NOT NULL,
    BudgetType              VARCHAR(20) NOT NULL,  -- ANNUAL, QUARTERLY, ROLLING, FORECAST
    ScenarioType            VARCHAR(20) NOT NULL,  -- BASE, OPTIMISTIC, PESSIMISTIC, STRETCH
    FiscalYear              SMALLINT NOT NULL,
    StartPeriodID           INT NOT NULL,
    EndPeriodID             INT NOT NULL,
    BaseBudgetHeaderID      INT NULL,  -- For variance calculations
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    SubmittedByUserID       INT NULL,
    SubmittedDateTime       DATETIME2(7) NULL,
    ApprovedByUserID        INT NULL,
    ApprovedDateTime        DATETIME2(7) NULL,
    LockedDateTime          DATETIME2(7) NULL,
    IsLocked                AS CASE WHEN LockedDateTime IS NOT NULL THEN 1 ELSE 0 END PERSISTED,
    VersionNumber           INT NOT NULL DEFAULT 1,
    Notes                   NVARCHAR(MAX) NULL,
    -- XML column for flexible metadata - Snowflake handles XML differently (VARIANT)
    ExtendedProperties      XML NULL,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_BudgetHeader PRIMARY KEY CLUSTERED (BudgetHeaderID),
    CONSTRAINT UQ_BudgetHeader_Code_Year UNIQUE (BudgetCode, FiscalYear, VersionNumber),
    CONSTRAINT FK_BudgetHeader_StartPeriod FOREIGN KEY (StartPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_EndPeriod FOREIGN KEY (EndPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_BaseBudget FOREIGN KEY (BaseBudgetHeaderID) 
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT CK_BudgetHeader_Status CHECK (StatusCode IN ('DRAFT','SUBMITTED','APPROVED','REJECTED','LOCKED','ARCHIVED'))
);
GO

-- XML index - No equivalent in Snowflake
CREATE PRIMARY XML INDEX PXML_BudgetHeader_ExtendedProperties
ON Planning.BudgetHeader (ExtendedProperties);
GO

CREATE XML INDEX SXML_BudgetHeader_ExtendedProperties_Path
ON Planning.BudgetHeader (ExtendedProperties)
USING XML INDEX PXML_BudgetHeader_ExtendedProperties
FOR PATH;
GO
GO

-- ==================== AllocationRule ====================
/*
    AllocationRule - Rules for cost allocation across cost centers
    Dependencies: CostCenter, GLAccount
*/
CREATE TABLE Planning.AllocationRule (
    AllocationRuleID        INT IDENTITY(1,1) NOT NULL,
    RuleCode                VARCHAR(30) NOT NULL,
    RuleName                NVARCHAR(100) NOT NULL,
    RuleDescription         NVARCHAR(500) NULL,
    RuleType                VARCHAR(20) NOT NULL,  -- DIRECT, STEP_DOWN, RECIPROCAL, ACTIVITY_BASED
    AllocationMethod        VARCHAR(20) NOT NULL,  -- FIXED_PCT, HEADCOUNT, SQUARE_FOOTAGE, REVENUE, CUSTOM
    -- Source specification
    SourceCostCenterID      INT NULL,  -- NULL means all cost centers matching pattern
    SourceCostCenterPattern VARCHAR(50) NULL,  -- Regex pattern for cost center matching
    SourceAccountPattern    VARCHAR(50) NULL,  -- Regex pattern for account matching
    -- Target specification using XML for flexibility
    TargetSpecification     XML NOT NULL,  -- Complex target definitions
    -- Calculation parameters
    AllocationBasis         VARCHAR(30) NULL,
    AllocationPercentage    DECIMAL(8,6) NULL,
    RoundingMethod          VARCHAR(10) NOT NULL DEFAULT 'NEAREST',  -- NEAREST, UP, DOWN, NONE
    RoundingPrecision       TINYINT NOT NULL DEFAULT 2,
    MinimumAmount           DECIMAL(19,4) NULL,
    -- Execution order for step-down allocations
    ExecutionSequence       INT NOT NULL DEFAULT 100,
    DependsOnRuleID         INT NULL,
    -- Validity
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    IsActive                BIT NOT NULL DEFAULT 1,
    -- Audit
    CreatedByUserID         INT NULL,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedByUserID        INT NULL,
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_AllocationRule PRIMARY KEY CLUSTERED (AllocationRuleID),
    CONSTRAINT UQ_AllocationRule_Code UNIQUE (RuleCode),
    CONSTRAINT FK_AllocationRule_SourceCC FOREIGN KEY (SourceCostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_AllocationRule_DependsOn FOREIGN KEY (DependsOnRuleID) 
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_AllocationRule_Type CHECK (RuleType IN ('DIRECT','STEP_DOWN','RECIPROCAL','ACTIVITY_BASED')),
    CONSTRAINT CK_AllocationRule_Rounding CHECK (RoundingMethod IN ('NEAREST','UP','DOWN','NONE'))
);
GO

-- Primary XML index on target specification
CREATE PRIMARY XML INDEX PXML_AllocationRule_TargetSpec
ON Planning.AllocationRule (TargetSpecification);
GO
GO

-- ==================== BudgetLineItem ====================
/*
    BudgetLineItem - Individual budget amounts by account/cost center/period
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod
*/
CREATE TABLE Planning.BudgetLineItem (
    BudgetLineItemID        BIGINT IDENTITY(1,1) NOT NULL,
    BudgetHeaderID          INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    -- Amounts in multiple representations
    OriginalAmount          DECIMAL(19,4) NOT NULL DEFAULT 0,
    AdjustedAmount          DECIMAL(19,4) NOT NULL DEFAULT 0,
    FinalAmount             AS (OriginalAmount + AdjustedAmount) PERSISTED,  -- Computed persisted
    LocalCurrencyAmount     DECIMAL(19,4) NULL,
    ReportingCurrencyAmount DECIMAL(19,4) NULL,
    StatisticalQuantity     DECIMAL(18,6) NULL,
    UnitOfMeasure           VARCHAR(10) NULL,
    -- Spreading pattern for forecast
    SpreadMethodCode        VARCHAR(10) NULL,  -- EVEN, SEASONAL, CUSTOM, PRIOR_YEAR
    SeasonalityFactor       DECIMAL(8,6) NULL,
    -- Source tracking
    SourceSystem            VARCHAR(30) NULL,
    SourceReference         VARCHAR(100) NULL,
    ImportBatchID           UNIQUEIDENTIFIER NULL,  -- GUID type
    -- Allocation tracking
    IsAllocated             BIT NOT NULL DEFAULT 0,
    AllocationSourceLineID  BIGINT NULL,
    AllocationPercentage    DECIMAL(8,6) NULL,
    -- Audit columns
    LastModifiedByUserID    INT NULL,
    LastModifiedDateTime    DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    RowHash                 AS HASHBYTES('SHA2_256', 
                               CONCAT(CAST(GLAccountID AS VARCHAR), '|',
                                      CAST(CostCenterID AS VARCHAR), '|',
                                      CAST(FiscalPeriodID AS VARCHAR), '|',
                                      CAST(/*FinalAmount*/ OriginalAmount + AdjustedAmount AS VARCHAR))) PERSISTED,  -- HASHBYTES computed
    CONSTRAINT PK_BudgetLineItem PRIMARY KEY CLUSTERED (BudgetLineItemID),
    CONSTRAINT FK_BudgetLineItem_Header FOREIGN KEY (BudgetHeaderID) 
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_BudgetLineItem_Account FOREIGN KEY (GLAccountID) 
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT FK_BudgetLineItem_CostCenter FOREIGN KEY (CostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_BudgetLineItem_Period FOREIGN KEY (FiscalPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetLineItem_AllocationSource FOREIGN KEY (AllocationSourceLineID) 
        REFERENCES Planning.BudgetLineItem (BudgetLineItemID)
);
GO

-- Unique constraint for natural key
CREATE UNIQUE NONCLUSTERED INDEX UQ_BudgetLineItem_NaturalKey
ON Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID)
WITH (IGNORE_DUP_KEY = ON);  -- IGNORE_DUP_KEY not in Snowflake
GO

-- Filtered index for allocated items
CREATE NONCLUSTERED INDEX IX_BudgetLineItem_Allocated
ON Planning.BudgetLineItem (AllocationSourceLineID, AllocationPercentage)
WHERE IsAllocated = 1;
GO

-- Columnstore for analytics
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_BudgetLineItem_Analytics
ON Planning.BudgetLineItem (
    BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
    OriginalAmount, AdjustedAmount, LocalCurrencyAmount, ReportingCurrencyAmount
);
GO
GO

-- ==================== ConsolidationJournal ====================
/*
    ConsolidationJournal - Journal entries for consolidation adjustments
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod
*/
CREATE TABLE Planning.ConsolidationJournal (
    JournalID               BIGINT IDENTITY(1,1) NOT NULL,
    JournalNumber           VARCHAR(30) NOT NULL,
    JournalType             VARCHAR(20) NOT NULL,  -- ELIMINATION, RECLASSIFICATION, TRANSLATION, ADJUSTMENT
    BudgetHeaderID          INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    PostingDate             DATE NOT NULL,
    Description             NVARCHAR(500) NULL,
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    -- Entity tracking for multi-entity consolidation
    SourceEntityCode        VARCHAR(20) NULL,
    TargetEntityCode        VARCHAR(20) NULL,
    -- Reversal handling
    IsAutoReverse           BIT NOT NULL DEFAULT 0,
    ReversalPeriodID        INT NULL,
    ReversedFromJournalID   BIGINT NULL,
    IsReversed              BIT NOT NULL DEFAULT 0,
    -- Totals (denormalized for performance)
    TotalDebits             DECIMAL(19,4) NOT NULL DEFAULT 0,
    TotalCredits            DECIMAL(19,4) NOT NULL DEFAULT 0,
    IsBalanced              AS CASE WHEN TotalDebits = TotalCredits THEN 1 ELSE 0 END,
    -- Approval workflow
    PreparedByUserID        INT NULL,
    PreparedDateTime        DATETIME2(7) NULL,
    ReviewedByUserID        INT NULL,
    ReviewedDateTime        DATETIME2(7) NULL,
    ApprovedByUserID        INT NULL,
    ApprovedDateTime        DATETIME2(7) NULL,
    PostedByUserID          INT NULL,
    PostedDateTime          DATETIME2(7) NULL,
    -- Attachments stored as (no Snowflake equivalent)
    AttachmentData          VARBINARY(MAX) NULL,
    AttachmentRowGuid       UNIQUEIDENTIFIER ROWGUIDCOL NOT NULL DEFAULT NEWSEQUENTIALID(),
    CONSTRAINT PK_ConsolidationJournal PRIMARY KEY CLUSTERED (JournalID),
    CONSTRAINT UQ_ConsolidationJournal_Number UNIQUE (JournalNumber),
    CONSTRAINT FK_ConsolidationJournal_Header FOREIGN KEY (BudgetHeaderID) 
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_ConsolidationJournal_Period FOREIGN KEY (FiscalPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversalPeriod FOREIGN KEY (ReversalPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversedFrom FOREIGN KEY (ReversedFromJournalID) 
        REFERENCES Planning.ConsolidationJournal (JournalID)
);
GO

-- Unique constraint with ROWGUIDCOL for FILESTREAM
CREATE UNIQUE NONCLUSTERED INDEX IX_ConsolidationJournal_RowGuid
ON Planning.ConsolidationJournal (AttachmentRowGuid);
GO
GO

-- ==================== ConsolidationJournalLine ====================
/*
    ConsolidationJournalLine - Line items for consolidation journal entries
    Dependencies: ConsolidationJournal, GLAccount, CostCenter
*/
CREATE TABLE Planning.ConsolidationJournalLine (
    JournalLineID           BIGINT IDENTITY(1,1) NOT NULL,
    JournalID               BIGINT NOT NULL,
    LineNumber              INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    DebitAmount             DECIMAL(19,4) NOT NULL DEFAULT 0,
    CreditAmount            DECIMAL(19,4) NOT NULL DEFAULT 0,
    NetAmount               AS (DebitAmount - CreditAmount) PERSISTED,
    LocalCurrencyCode       CHAR(3) NOT NULL DEFAULT 'USD',
    LocalCurrencyAmount     DECIMAL(19,4) NULL,
    ExchangeRate            DECIMAL(18,10) NULL,
    Description             NVARCHAR(255) NULL,
    ReferenceNumber         VARCHAR(50) NULL,
    -- Intercompany tracking
    PartnerEntityCode       VARCHAR(20) NULL,
    PartnerAccountID        INT NULL,
    -- Statistical tracking
    StatisticalQuantity     DECIMAL(18,6) NULL,
    StatisticalUOM          VARCHAR(10) NULL,
    -- Allocation tracking
    AllocationRuleID        INT NULL,
    -- Audit
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_ConsolidationJournalLine PRIMARY KEY CLUSTERED (JournalLineID),
    CONSTRAINT UQ_ConsolidationJournalLine_JournalLine UNIQUE (JournalID, LineNumber),
    CONSTRAINT FK_ConsolidationJournalLine_Journal FOREIGN KEY (JournalID) 
        REFERENCES Planning.ConsolidationJournal (JournalID) ON DELETE CASCADE,
    CONSTRAINT FK_ConsolidationJournalLine_Account FOREIGN KEY (GLAccountID) 
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT FK_ConsolidationJournalLine_CostCenter FOREIGN KEY (CostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_ConsolidationJournalLine_AllocationRule FOREIGN KEY (AllocationRuleID) 
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_ConsolidationJournalLine_DebitCredit CHECK (
        (DebitAmount >= 0 AND CreditAmount >= 0) AND
        NOT (DebitAmount > 0 AND CreditAmount > 0)  -- Cannot have both
    )
);
GO

-- Columnstore for reporting
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_ConsolidationJournalLine
ON Planning.ConsolidationJournalLine (
    JournalID, GLAccountID, CostCenterID, 
    DebitAmount, CreditAmount, LocalCurrencyAmount
);
GO
GO

-- ==================== User-Defined Table Types ====================

-- ==================== HierarchyNodeTableType ====================
/*
    HierarchyNodeTableType - For passing hierarchy traversal data
    Dependencies: None
*/
CREATE TYPE Planning.HierarchyNodeTableType AS TABLE (
    NodeID                  INT NOT NULL,
    ParentNodeID            INT NULL,
    NodeLevel               INT NOT NULL,
    NodePath                VARCHAR(500) NOT NULL,
    SortOrder               INT NOT NULL,
    IsLeaf                  BIT NOT NULL,
    AggregationWeight       DECIMAL(8,6) NOT NULL DEFAULT 1.0,
    PRIMARY KEY CLUSTERED (NodeID)
);
GO
GO

-- ==================== BudgetLineItemTableType ====================
/*
    BudgetLineItemTableType - Table-valued parameter type for bulk budget operations
    Dependencies: None
    
    NOTE: User-defined table types (TVPs) have NO equivalent in Snowflake.
    These require significant refactoring - typically to:
    1. Temporary tables with INSERT statements
    2. JSON/VARIANT arrays
    3. Staged files
*/
CREATE TYPE Planning.BudgetLineItemTableType AS TABLE (
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    OriginalAmount          DECIMAL(19,4) NOT NULL,
    AdjustedAmount          DECIMAL(19,4) NULL,
    SpreadMethodCode        VARCHAR(10) NULL,
    Notes                   NVARCHAR(500) NULL,
    -- Table types can have indexes in SQL Server 2014+
    INDEX IX_AccountPeriod (GLAccountID, FiscalPeriodID),
    PRIMARY KEY CLUSTERED (GLAccountID, CostCenterID, FiscalPeriodID)
);
GO
GO

-- ==================== AllocationResultTableType ====================
/*
    AllocationResultTableType - Table type for returning allocation results
    Dependencies: None
    
    NOTE: No Snowflake equivalent - must be refactored to temp tables or VARIANT
*/
CREATE TYPE Planning.AllocationResultTableType AS TABLE (
    SourceBudgetLineItemID  BIGINT NOT NULL,
    TargetCostCenterID      INT NOT NULL,
    TargetGLAccountID       INT NOT NULL,
    AllocatedAmount         DECIMAL(19,4) NOT NULL,
    AllocationPercentage    DECIMAL(8,6) NOT NULL,
    AllocationRuleID        INT NOT NULL,
    ProcessingSequence      INT NOT NULL,
    INDEX IX_Source (SourceBudgetLineItemID),
    INDEX IX_Target (TargetCostCenterID, TargetGLAccountID)
);
GO
GO

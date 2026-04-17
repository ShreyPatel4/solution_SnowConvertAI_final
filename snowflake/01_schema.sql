-- =============================================================================
-- Snowflake migrated schema for Planning DB
-- Target: PLANNING_DB.PLANNING
-- =============================================================================
-- Global translation decisions (see per-table notes for specifics):
--   * IDENTITY(1,1)           -> AUTOINCREMENT  (surrogate IDs will differ
--                                from SQL Server; verification MUST diff on
--                                natural keys + HASH_AGG of value columns.)
--   * NVARCHAR/NVARCHAR(MAX)  -> VARCHAR        (Snowflake is UTF-8 native.)
--   * BIT                     -> BOOLEAN
--   * DATETIME2(7)            -> TIMESTAMP_NTZ(9)
--   * SYSUTCDATETIME()        -> CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
--   * ROWVERSION              -> TIMESTAMP_NTZ maintained by the writing proc.
--   * HIERARCHYID + GetLevel()-> materialized path VARCHAR + INT level column;
--                                maintained by hierarchy-building function/proc.
--   * SYSTEM_VERSIONING (temporal) -> Snowflake Time Travel (1-day default /
--                                90-day max on Enterprise) + an explicit
--                                <Table>History mirror for longer retention.
--                                LOSSY: semantics are not identical.
--   * XML                     -> VARIANT (PARSE_XML on ingest).
--   * XQuery .value()/.nodes() -> GET_PATH / LATERAL FLATTEN (done in procs).
--   * XML primary/secondary INDEX -> no-op (Snowflake auto-optimizes VARIANT).
--   * PERSISTED computed columns -> regular columns maintained by the writing
--                                proc.  We choose stored values over virtual
--                                columns so cross-engine HASH_AGG diffs work.
--   * Filtered / INCLUDE / COLUMNSTORE indexes -> no-op + CLUSTER BY where
--                                useful; Snowflake micro-partition storage is
--                                already columnar and auto-pruned.
--   * IGNORE_DUP_KEY = ON     -> MERGE / dedupe at insert site (in procs).
--   * FILESTREAM              -> inline BINARY (small attachments only; real
--                                workloads should use an external stage).
--   * UNIQUEIDENTIFIER + NEWSEQUENTIALID() -> VARCHAR(36) + UUID_STRING().
--                                LOSSY: UUID_STRING() is random, not sequential.
--   * ON DELETE CASCADE       -> not supported; cascade logic moves into procs.
--   * CHECK / UNIQUE / FOREIGN KEY -> documented in DDL but NOT enforced by
--                                Snowflake (except NOT NULL).  Enforcement
--                                belongs to the procedure layer.
-- =============================================================================

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;

-- -----------------------------------------------------------------------------
-- 1. FiscalPeriod
--    T-SQL quirks: ROWVERSION, filtered index (WHERE IsClosed=0), INCLUDE idx.
--    Snowflake:  ROWVERSION -> TIMESTAMP_NTZ maintained by procs.
--                Filtered+INCLUDE -> CLUSTER BY (IsClosed, FiscalYear, FiscalMonth).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE FiscalPeriod (
    FiscalPeriodID          INT AUTOINCREMENT PRIMARY KEY,
    FiscalYear              SMALLINT NOT NULL,
    FiscalQuarter           TINYINT NOT NULL,
    FiscalMonth             TINYINT NOT NULL,
    PeriodName              VARCHAR(50) NOT NULL,
    PeriodStartDate         DATE NOT NULL,
    PeriodEndDate           DATE NOT NULL,
    IsClosed                BOOLEAN NOT NULL DEFAULT FALSE,
    ClosedByUserID          INT,
    ClosedDateTime          TIMESTAMP_NTZ,
    IsAdjustmentPeriod      BOOLEAN NOT NULL DEFAULT FALSE,
    WorkingDays             TINYINT,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    RowVersionStamp         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    CONSTRAINT UQ_FiscalPeriod_YearMonth UNIQUE (FiscalYear, FiscalMonth),
    CONSTRAINT CK_FiscalPeriod_Quarter CHECK (FiscalQuarter BETWEEN 1 AND 4),
    CONSTRAINT CK_FiscalPeriod_Month CHECK (FiscalMonth BETWEEN 1 AND 13),
    CONSTRAINT CK_FiscalPeriod_DateRange CHECK (PeriodEndDate >= PeriodStartDate)
)
CLUSTER BY (IsClosed, FiscalYear, FiscalMonth);


-- -----------------------------------------------------------------------------
-- 2. CostCenter
--    T-SQL quirks: HIERARCHYID + GetLevel() PERSISTED, SYSTEM_VERSIONING,
--                  spatial index on HierarchyPath.
--    Snowflake:  HIERARCHYID -> VARCHAR materialized path (e.g. '/1/3/7/').
--                HierarchyLevel -> plain INT, maintained by hierarchy proc.
--                Temporal -> Time Travel + explicit CostCenterHistory mirror.
--                Spatial index -> CLUSTER BY (HierarchyPath).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CostCenter (
    CostCenterID            INT AUTOINCREMENT PRIMARY KEY,
    CostCenterCode          VARCHAR(20) NOT NULL,
    CostCenterName          VARCHAR(100) NOT NULL,
    ParentCostCenterID      INT,
    HierarchyPath           VARCHAR(500),
    HierarchyLevel          INT,
    ManagerEmployeeID       INT,
    DepartmentCode          VARCHAR(10),
    IsActive                BOOLEAN NOT NULL DEFAULT TRUE,
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE,
    AllocationWeight        NUMBER(5,4) NOT NULL DEFAULT 1.0000,
    ValidFrom               TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    ValidTo                 TIMESTAMP_NTZ NOT NULL DEFAULT '9999-12-31 23:59:59.999999999'::TIMESTAMP_NTZ,
    CONSTRAINT UQ_CostCenter_Code UNIQUE (CostCenterCode),
    CONSTRAINT FK_CostCenter_Parent FOREIGN KEY (ParentCostCenterID) REFERENCES CostCenter(CostCenterID),
    CONSTRAINT CK_CostCenter_Weight CHECK (AllocationWeight BETWEEN 0 AND 1)
)
CLUSTER BY (HierarchyPath);

-- Mirror history table (Time Travel replaces system-versioning for recent
-- history; this table captures rows beyond the Time Travel window via an
-- explicit stream/task or proc — to be configured in Phase 2).
CREATE OR REPLACE TABLE CostCenterHistory LIKE CostCenter;


-- -----------------------------------------------------------------------------
-- 3. GLAccount
--    T-SQL quirks: SPARSE columns, non-clustered COLUMNSTORE index.
--    Snowflake:  SPARSE -> plain NULL (micro-partitions compress NULLs).
--                COLUMNSTORE -> no-op (columnar storage is implicit).
--                CLUSTER BY (AccountType, AccountNumber) for analytics.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GLAccount (
    GLAccountID             INT AUTOINCREMENT PRIMARY KEY,
    AccountNumber           VARCHAR(20) NOT NULL,
    AccountName             VARCHAR(150) NOT NULL,
    AccountType             CHAR(1) NOT NULL,
    AccountSubType          VARCHAR(30),
    ParentAccountID         INT,
    AccountLevel            TINYINT NOT NULL DEFAULT 1,
    IsPostable              BOOLEAN NOT NULL DEFAULT TRUE,
    IsBudgetable            BOOLEAN NOT NULL DEFAULT TRUE,
    IsStatistical           BOOLEAN NOT NULL DEFAULT FALSE,
    NormalBalance           CHAR(1) NOT NULL DEFAULT 'D',
    CurrencyCode            CHAR(3) NOT NULL DEFAULT 'USD',
    ConsolidationAccountID  INT,
    IntercompanyFlag        BOOLEAN NOT NULL DEFAULT FALSE,
    IsActive                BOOLEAN NOT NULL DEFAULT TRUE,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    TaxCode                 VARCHAR(20),
    StatutoryAccountCode    VARCHAR(30),
    IFRSAccountCode         VARCHAR(30),
    CONSTRAINT UQ_GLAccount_Number UNIQUE (AccountNumber),
    CONSTRAINT FK_GLAccount_Parent FOREIGN KEY (ParentAccountID) REFERENCES GLAccount(GLAccountID),
    CONSTRAINT CK_GLAccount_Type CHECK (AccountType IN ('A','L','E','R','X')),
    CONSTRAINT CK_GLAccount_Balance CHECK (NormalBalance IN ('D','C'))
)
CLUSTER BY (AccountType, AccountNumber);


-- -----------------------------------------------------------------------------
-- 4. BudgetHeader
--    T-SQL quirks: XML + PRIMARY/SECONDARY XML INDEX, PERSISTED computed IsLocked.
--    Snowflake:  XML -> VARIANT (ingest via PARSE_XML).
--                XML indexes -> no-op.
--                IsLocked -> plain BOOLEAN; proc sets it when LockedDateTime is set.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BudgetHeader (
    BudgetHeaderID          INT AUTOINCREMENT PRIMARY KEY,
    BudgetCode              VARCHAR(30) NOT NULL,
    BudgetName              VARCHAR(100) NOT NULL,
    BudgetType              VARCHAR(20) NOT NULL,
    ScenarioType            VARCHAR(20) NOT NULL,
    FiscalYear              SMALLINT NOT NULL,
    StartPeriodID           INT NOT NULL,
    EndPeriodID             INT NOT NULL,
    BaseBudgetHeaderID      INT,
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    SubmittedByUserID       INT,
    SubmittedDateTime       TIMESTAMP_NTZ,
    ApprovedByUserID        INT,
    ApprovedDateTime        TIMESTAMP_NTZ,
    LockedDateTime          TIMESTAMP_NTZ,
    IsLocked                BOOLEAN NOT NULL DEFAULT FALSE,
    VersionNumber           INT NOT NULL DEFAULT 1,
    Notes                   VARCHAR,
    ExtendedProperties      VARIANT,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    CONSTRAINT UQ_BudgetHeader_Code_Year UNIQUE (BudgetCode, FiscalYear, VersionNumber),
    CONSTRAINT FK_BudgetHeader_StartPeriod FOREIGN KEY (StartPeriodID) REFERENCES FiscalPeriod(FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_EndPeriod FOREIGN KEY (EndPeriodID) REFERENCES FiscalPeriod(FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_BaseBudget FOREIGN KEY (BaseBudgetHeaderID) REFERENCES BudgetHeader(BudgetHeaderID),
    CONSTRAINT CK_BudgetHeader_Status CHECK (StatusCode IN ('DRAFT','SUBMITTED','APPROVED','REJECTED','LOCKED','ARCHIVED'))
);


-- -----------------------------------------------------------------------------
-- 5. AllocationRule
--    T-SQL quirks: XML TargetSpecification + PRIMARY XML INDEX.
--    Snowflake:  XML -> VARIANT; XML index -> no-op.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE AllocationRule (
    AllocationRuleID        INT AUTOINCREMENT PRIMARY KEY,
    RuleCode                VARCHAR(30) NOT NULL,
    RuleName                VARCHAR(100) NOT NULL,
    RuleDescription         VARCHAR(500),
    RuleType                VARCHAR(20) NOT NULL,
    AllocationMethod        VARCHAR(20) NOT NULL,
    SourceCostCenterID      INT,
    SourceCostCenterPattern VARCHAR(50),
    SourceAccountPattern    VARCHAR(50),
    TargetSpecification     VARIANT NOT NULL,
    AllocationBasis         VARCHAR(30),
    AllocationPercentage    NUMBER(8,6),
    RoundingMethod          VARCHAR(10) NOT NULL DEFAULT 'NEAREST',
    RoundingPrecision       TINYINT NOT NULL DEFAULT 2,
    MinimumAmount           NUMBER(19,4),
    ExecutionSequence       INT NOT NULL DEFAULT 100,
    DependsOnRuleID         INT,
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE,
    IsActive                BOOLEAN NOT NULL DEFAULT TRUE,
    CreatedByUserID         INT,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    ModifiedByUserID        INT,
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    CONSTRAINT UQ_AllocationRule_Code UNIQUE (RuleCode),
    CONSTRAINT FK_AllocationRule_SourceCC FOREIGN KEY (SourceCostCenterID) REFERENCES CostCenter(CostCenterID),
    CONSTRAINT FK_AllocationRule_DependsOn FOREIGN KEY (DependsOnRuleID) REFERENCES AllocationRule(AllocationRuleID),
    CONSTRAINT CK_AllocationRule_Type CHECK (RuleType IN ('DIRECT','STEP_DOWN','RECIPROCAL','ACTIVITY_BASED')),
    CONSTRAINT CK_AllocationRule_Rounding CHECK (RoundingMethod IN ('NEAREST','UP','DOWN','NONE'))
);


-- -----------------------------------------------------------------------------
-- 6. BudgetLineItem
--    T-SQL quirks: PERSISTED FinalAmount = OriginalAmount+AdjustedAmount,
--                  PERSISTED RowHash = HASHBYTES('SHA2_256', ...),
--                  UNIQUEIDENTIFIER ImportBatchID, filtered idx on IsAllocated=1,
--                  UNIQUE idx with IGNORE_DUP_KEY, COLUMNSTORE.
--    Snowflake:  FinalAmount -> NUMBER(19,4), proc computes on insert/update.
--                RowHash     -> VARCHAR(64), proc uses SHA2(CONCAT(...), 256).
--                UNIQUEIDENTIFIER -> VARCHAR(36).
--                Filtered idx -> CLUSTER BY (BudgetHeaderID, FiscalPeriodID, CostCenterID).
--                IGNORE_DUP_KEY -> ingest proc uses MERGE on the natural key.
--                COLUMNSTORE -> no-op.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BudgetLineItem (
    BudgetLineItemID        NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    BudgetHeaderID          INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    OriginalAmount          NUMBER(19,4) NOT NULL DEFAULT 0,
    AdjustedAmount          NUMBER(19,4) NOT NULL DEFAULT 0,
    FinalAmount             NUMBER(19,4) NOT NULL DEFAULT 0,
    LocalCurrencyAmount     NUMBER(19,4),
    ReportingCurrencyAmount NUMBER(19,4),
    StatisticalQuantity     NUMBER(18,6),
    UnitOfMeasure           VARCHAR(10),
    SpreadMethodCode        VARCHAR(10),
    SeasonalityFactor       NUMBER(8,6),
    SourceSystem            VARCHAR(30),
    SourceReference         VARCHAR(100),
    ImportBatchID           VARCHAR(36),
    IsAllocated             BOOLEAN NOT NULL DEFAULT FALSE,
    AllocationSourceLineID  NUMBER(38,0),
    AllocationPercentage    NUMBER(8,6),
    LastModifiedByUserID    INT,
    LastModifiedDateTime    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    RowHash                 VARCHAR(64),
    CONSTRAINT UQ_BudgetLineItem_NaturalKey UNIQUE (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID),
    CONSTRAINT FK_BudgetLineItem_Header FOREIGN KEY (BudgetHeaderID) REFERENCES BudgetHeader(BudgetHeaderID),
    CONSTRAINT FK_BudgetLineItem_Account FOREIGN KEY (GLAccountID) REFERENCES GLAccount(GLAccountID),
    CONSTRAINT FK_BudgetLineItem_CostCenter FOREIGN KEY (CostCenterID) REFERENCES CostCenter(CostCenterID),
    CONSTRAINT FK_BudgetLineItem_Period FOREIGN KEY (FiscalPeriodID) REFERENCES FiscalPeriod(FiscalPeriodID),
    CONSTRAINT FK_BudgetLineItem_AllocationSource FOREIGN KEY (AllocationSourceLineID) REFERENCES BudgetLineItem(BudgetLineItemID)
)
CLUSTER BY (BudgetHeaderID, FiscalPeriodID, CostCenterID);


-- -----------------------------------------------------------------------------
-- 7. ConsolidationJournal
--    T-SQL quirks: FILESTREAM AttachmentData, ROWGUIDCOL + NEWSEQUENTIALID(),
--                  PERSISTED IsBalanced.
--    Snowflake:  FILESTREAM -> inline BINARY.  For real workloads, external
--                stage + file URL is preferred; inline is fine for tests here.
--                NEWSEQUENTIALID() -> UUID_STRING().  LOSSY: random, not
--                sequential — so clustering on the RowGuid is NOT a good idea.
--                IsBalanced -> BOOLEAN set by proc on TotalDebits/TotalCredits update.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ConsolidationJournal (
    JournalID               NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    JournalNumber           VARCHAR(30) NOT NULL,
    JournalType             VARCHAR(20) NOT NULL,
    BudgetHeaderID          INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    PostingDate             DATE NOT NULL,
    Description             VARCHAR(500),
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    SourceEntityCode        VARCHAR(20),
    TargetEntityCode        VARCHAR(20),
    IsAutoReverse           BOOLEAN NOT NULL DEFAULT FALSE,
    ReversalPeriodID        INT,
    ReversedFromJournalID   NUMBER(38,0),
    IsReversed              BOOLEAN NOT NULL DEFAULT FALSE,
    TotalDebits             NUMBER(19,4) NOT NULL DEFAULT 0,
    TotalCredits            NUMBER(19,4) NOT NULL DEFAULT 0,
    IsBalanced              BOOLEAN NOT NULL DEFAULT FALSE,
    PreparedByUserID        INT,
    PreparedDateTime        TIMESTAMP_NTZ,
    ReviewedByUserID        INT,
    ReviewedDateTime        TIMESTAMP_NTZ,
    ApprovedByUserID        INT,
    ApprovedDateTime        TIMESTAMP_NTZ,
    PostedByUserID          INT,
    PostedDateTime          TIMESTAMP_NTZ,
    AttachmentData          BINARY,
    AttachmentRowGuid       VARCHAR(36) NOT NULL DEFAULT UUID_STRING(),
    CONSTRAINT UQ_ConsolidationJournal_Number UNIQUE (JournalNumber),
    CONSTRAINT FK_ConsolidationJournal_Header FOREIGN KEY (BudgetHeaderID) REFERENCES BudgetHeader(BudgetHeaderID),
    CONSTRAINT FK_ConsolidationJournal_Period FOREIGN KEY (FiscalPeriodID) REFERENCES FiscalPeriod(FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversalPeriod FOREIGN KEY (ReversalPeriodID) REFERENCES FiscalPeriod(FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversedFrom FOREIGN KEY (ReversedFromJournalID) REFERENCES ConsolidationJournal(JournalID)
);


-- -----------------------------------------------------------------------------
-- 8. ConsolidationJournalLine
--    T-SQL quirks: PERSISTED NetAmount = Debit-Credit, COLUMNSTORE,
--                  ON DELETE CASCADE to ConsolidationJournal.
--    Snowflake:  NetAmount -> NUMBER; proc maintains.
--                COLUMNSTORE -> no-op.
--                ON DELETE CASCADE: Snowflake FK doesn't support it.  Cascade
--                cleanup must be explicit in the procedure that deletes journals.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ConsolidationJournalLine (
    JournalLineID           NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,
    JournalID               NUMBER(38,0) NOT NULL,
    LineNumber              INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    DebitAmount             NUMBER(19,4) NOT NULL DEFAULT 0,
    CreditAmount            NUMBER(19,4) NOT NULL DEFAULT 0,
    NetAmount               NUMBER(19,4) NOT NULL DEFAULT 0,
    LocalCurrencyCode       CHAR(3) NOT NULL DEFAULT 'USD',
    LocalCurrencyAmount     NUMBER(19,4),
    ExchangeRate            NUMBER(18,10),
    Description             VARCHAR(255),
    ReferenceNumber         VARCHAR(50),
    PartnerEntityCode       VARCHAR(20),
    PartnerAccountID        INT,
    StatisticalQuantity     NUMBER(18,6),
    StatisticalUOM          VARCHAR(10),
    AllocationRuleID        INT,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    CONSTRAINT UQ_ConsolidationJournalLine_JournalLine UNIQUE (JournalID, LineNumber),
    CONSTRAINT FK_ConsolidationJournalLine_Journal FOREIGN KEY (JournalID) REFERENCES ConsolidationJournal(JournalID),
    CONSTRAINT FK_ConsolidationJournalLine_Account FOREIGN KEY (GLAccountID) REFERENCES GLAccount(GLAccountID),
    CONSTRAINT FK_ConsolidationJournalLine_CostCenter FOREIGN KEY (CostCenterID) REFERENCES CostCenter(CostCenterID),
    CONSTRAINT FK_ConsolidationJournalLine_AllocationRule FOREIGN KEY (AllocationRuleID) REFERENCES AllocationRule(AllocationRuleID),
    CONSTRAINT CK_ConsolidationJournalLine_DebitCredit CHECK ((DebitAmount >= 0 AND CreditAmount >= 0) AND NOT (DebitAmount > 0 AND CreditAmount > 0))
);


-- =============================================================================
-- User-Defined Table Types (UDTTs)
-- =============================================================================
-- SQL Server TVPs have NO Snowflake equivalent.  Migrated procedures use one
-- of two patterns instead:
--
--   Pattern A — ARRAY of OBJECTs (small, structured inputs):
--     CALL usp_BulkImportBudgetData(ARRAY_CONSTRUCT(
--         OBJECT_CONSTRUCT('GLAccountID', 101, 'CostCenterID', 5, ...),
--         ...
--     ));
--
--   Pattern B — session temporary table (larger inputs):
--     CREATE TEMPORARY TABLE t_budget_lines (... matching columns ...);
--     INSERT INTO t_budget_lines VALUES ...;
--     CALL usp_BulkImportBudgetData('T_BUDGET_LINES');
--
-- No CREATE TYPE DDL is emitted.  Each UDTT's column shape is documented below
-- so the replacement temp tables / OBJECT schemas match the original contract.
--
--   HierarchyNodeTableType
--     NodeID INT, ParentNodeID INT NULL, NodeLevel INT, NodePath VARCHAR(500),
--     SortOrder INT, IsLeaf BOOLEAN, AggregationWeight NUMBER(8,6) DEFAULT 1.0
--
--   BudgetLineItemTableType
--     GLAccountID INT, CostCenterID INT, FiscalPeriodID INT,
--     OriginalAmount NUMBER(19,4), AdjustedAmount NUMBER(19,4) NULL,
--     SpreadMethodCode VARCHAR(10) NULL, Notes VARCHAR(500) NULL
--
--   AllocationResultTableType
--     SourceBudgetLineItemID NUMBER(38,0), TargetCostCenterID INT,
--     TargetGLAccountID INT, AllocatedAmount NUMBER(19,4),
--     AllocationPercentage NUMBER(8,6), AllocationRuleID INT,
--     ProcessingSequence INT
--
-- =============================================================================


-- =============================================================================
-- Inventory check
-- =============================================================================
SHOW TABLES IN SCHEMA PLANNING_DB.PLANNING;

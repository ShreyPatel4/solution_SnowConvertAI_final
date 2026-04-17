/*
    ConsolidationJournal - Journal entries for consolidation adjustments
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod
*/
    -- ELIMINATION, RECLASSIFICATION, TRANSLATION, ADJUSTMENT
CREATE OR REPLACE TABLE Planning.ConsolidationJournal (
    JournalID BIGINT IDENTITY(1,1) ORDER NOT NULL,
    JournalNumber VARCHAR(30) NOT NULL,
    JournalType VARCHAR(20) NOT NULL,
    BudgetHeaderID INT NOT NULL,
    FiscalPeriodID INT NOT NULL,
    PostingDate DATE NOT NULL,
    Description NVARCHAR(500) NULL,
    StatusCode VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    -- Entity tracking for multi-entity consolidation
    SourceEntityCode VARCHAR(20) NULL,
    TargetEntityCode VARCHAR(20) NULL,
    -- Reversal handling
    IsAutoReverse BOOLEAN NOT NULL DEFAULT false,
    ReversalPeriodID INT NULL,
    ReversedFromJournalID BIGINT NULL,
    IsReversed BOOLEAN NOT NULL DEFAULT false,
    -- Totals (denormalized for performance)
    TotalDebits DECIMAL(19, 4) NOT NULL DEFAULT 0,
    TotalCredits DECIMAL(19, 4) NOT NULL DEFAULT 0,
    IsBalanced NUMERIC AS CASE WHEN TotalDebits = TotalCredits
            THEN 1 ELSE 0 END /*** SSC-FDM-TS0014 - COMPUTED COLUMN WAS TRANSFORMED TO ITS SNOWFLAKE EQUIVALENT, FUNCTIONAL EQUIVALENCE VERIFICATION PENDING. ***/,
    -- Approval workflow
    PreparedByUserID INT NULL,
    PreparedDateTime TIMESTAMP_NTZ(7) NULL,
    ReviewedByUserID INT NULL,
    ReviewedDateTime TIMESTAMP_NTZ(7) NULL,
    ApprovedByUserID INT NULL,
    ApprovedDateTime TIMESTAMP_NTZ(7) NULL,
    PostedByUserID INT NULL,
    PostedDateTime TIMESTAMP_NTZ(7) NULL,
    -- Attachments stored as FILESTREAM (no Snowflake equivalent)
    AttachmentData VARBINARY
                             !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'FILESTREAM COLUMN OPTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                             FILESTREAM NULL,
    AttachmentRowGuid VARCHAR
                              !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'ROWGUIDCOL COLUMN OPTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                              ROWGUIDCOL NOT NULL DEFAULT NEWSEQUENTIALID() !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'NEWSEQUENTIALID' NODE ***/!!!,
    CONSTRAINT PK_ConsolidationJournal PRIMARY KEY (JournalID),
    CONSTRAINT UQ_ConsolidationJournal_Number UNIQUE (JournalNumber),
    CONSTRAINT FK_ConsolidationJournal_Header FOREIGN KEY (BudgetHeaderID)
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_ConsolidationJournal_Period FOREIGN KEY (FiscalPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversalPeriod FOREIGN KEY (ReversalPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversedFrom FOREIGN KEY (ReversedFromJournalID)
        REFERENCES Planning.ConsolidationJournal (JournalID)
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
;
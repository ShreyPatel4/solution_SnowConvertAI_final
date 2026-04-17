/*
    GLAccount - General Ledger Account master
    Dependencies: None (base table)
*/
    -- A=Asset, L=Liability, E=Equity, R=Revenue, X=Expense
    -- D=Debit, C=Credit
CREATE OR REPLACE TABLE Planning.GLAccount (
    GLAccountID INT IDENTITY(1,1) ORDER NOT NULL,
    AccountNumber VARCHAR(20) NOT NULL,
    AccountName NVARCHAR(150) NOT NULL,
    AccountType CHAR(1) NOT NULL,
    AccountSubType VARCHAR(30) NULL,
    ParentAccountID INT NULL,
    AccountLevel TINYINT NOT NULL DEFAULT 1,
    IsPostable BOOLEAN NOT NULL DEFAULT true,
    IsBudgetable BOOLEAN NOT NULL DEFAULT true,
    IsStatistical BOOLEAN NOT NULL DEFAULT false,
    NormalBalance CHAR(1) NOT NULL DEFAULT 'D',
    CurrencyCode CHAR(3) NOT NULL DEFAULT 'USD',
    ConsolidationAccountID INT NULL,
    IntercompanyFlag BOOLEAN NOT NULL DEFAULT false,
    IsActive BOOLEAN NOT NULL DEFAULT true,
    CreatedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    ModifiedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    -- Sparse columns for rarely-populated attributes - Snowflake doesn't support SPARSE
    TaxCode VARCHAR(20)
                        !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'SPARSE COLUMN OPTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                        SPARSE NULL,
    StatutoryAccountCode VARCHAR(30)
                                     !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'SPARSE COLUMN OPTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                                     SPARSE NULL,
    IFRSAccountCode VARCHAR(30)
                                !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'SPARSE COLUMN OPTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                                SPARSE NULL,
    CONSTRAINT PK_GLAccount PRIMARY KEY (GLAccountID),
    CONSTRAINT UQ_GLAccount_Number UNIQUE (AccountNumber),
    CONSTRAINT FK_GLAccount_Parent FOREIGN KEY (ParentAccountID)
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT CK_GLAccount_Type CHECK (AccountType IN ('A','L','E','R','X')) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!,
    CONSTRAINT CK_GLAccount_Balance CHECK (NormalBalance IN ('D','C')) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
;
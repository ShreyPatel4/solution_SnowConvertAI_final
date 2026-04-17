/*
    BudgetHeader - Budget version and scenario header
    Dependencies: FiscalPeriod
*/
    -- ANNUAL, QUARTERLY, ROLLING, FORECAST
    -- BASE, OPTIMISTIC, PESSIMISTIC, STRETCH
    -- For variance calculations
CREATE OR REPLACE TABLE Planning.BudgetHeader (
    BudgetHeaderID INT IDENTITY(1,1) ORDER NOT NULL,
    BudgetCode VARCHAR(30) NOT NULL,
    BudgetName NVARCHAR(100) NOT NULL,
    BudgetType VARCHAR(20) NOT NULL,
    ScenarioType VARCHAR(20) NOT NULL,
    FiscalYear SMALLINT NOT NULL,
    StartPeriodID INT NOT NULL,
    EndPeriodID INT NOT NULL,
    BaseBudgetHeaderID INT NULL,
    StatusCode VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    SubmittedByUserID INT NULL,
    SubmittedDateTime TIMESTAMP_NTZ(7) NULL,
    ApprovedByUserID INT NULL,
    ApprovedDateTime TIMESTAMP_NTZ(7) NULL,
    LockedDateTime TIMESTAMP_NTZ(7) NULL,
    IsLocked NUMERIC AS CASE WHEN LockedDateTime IS NOT NULL THEN 1 ELSE 0 END /*** SSC-FDM-TS0014 - COMPUTED COLUMN WAS TRANSFORMED TO ITS SNOWFLAKE EQUIVALENT, FUNCTIONAL EQUIVALENCE VERIFICATION PENDING. ***/,
    VersionNumber INT NOT NULL DEFAULT 1,
    Notes NVARCHAR NULL,
    -- XML column for flexible metadata - Snowflake handles XML differently (VARIANT)
    ExtendedProperties VARIANT !!!RESOLVE EWI!!! /*** SSC-EWI-0036 - XML DATA TYPE CONVERTED TO VARIANT ***/!!! NULL,
    CreatedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    ModifiedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    CONSTRAINT PK_BudgetHeader PRIMARY KEY (BudgetHeaderID),
    CONSTRAINT UQ_BudgetHeader_Code_Year UNIQUE (BudgetCode, FiscalYear, VersionNumber),
    CONSTRAINT FK_BudgetHeader_StartPeriod FOREIGN KEY (StartPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_EndPeriod FOREIGN KEY (EndPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_BaseBudget FOREIGN KEY (BaseBudgetHeaderID)
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT CK_BudgetHeader_Status CHECK (StatusCode IN ('DRAFT','SUBMITTED','APPROVED','REJECTED','LOCKED','ARCHIVED')) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
;
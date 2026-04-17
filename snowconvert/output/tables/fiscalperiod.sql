/*
    FiscalPeriod - Core reference table for fiscal calendar
    Dependencies: None (base table)
*/
    -- ROWVERSION doesn't exist in Snowflake
    -- 13 for adjustment period
CREATE OR REPLACE TABLE Planning.FiscalPeriod (
    FiscalPeriodID INT IDENTITY(1,1) ORDER NOT NULL,
    FiscalYear SMALLINT NOT NULL,
    FiscalQuarter TINYINT NOT NULL,
    FiscalMonth TINYINT NOT NULL,
    PeriodName NVARCHAR(50) NOT NULL,
    PeriodStartDate DATE NOT NULL,
    PeriodEndDate DATE NOT NULL,
    IsClosed BOOLEAN NOT NULL DEFAULT false,
    ClosedByUserID INT NULL,
    ClosedDateTime TIMESTAMP_NTZ(7) NULL,
    IsAdjustmentPeriod BOOLEAN NOT NULL DEFAULT false,
    WorkingDays TINYINT NULL,
    CreatedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    ModifiedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    RowVersionStamp BINARY(8) /*** SSC-FDM-TS0046 - ROWVERSION/TIMESTAMP DATA TYPE AUTO-GENERATES UNIQUE VALUES ON INSERT AND UPDATE IN SQL SERVER. THIS BEHAVIOR IS NOT REPLICATED IN SNOWFLAKE BINARY(8). ***/ NOT NULL,
    CONSTRAINT PK_FiscalPeriod PRIMARY KEY (FiscalPeriodID),
    CONSTRAINT UQ_FiscalPeriod_YearMonth UNIQUE (FiscalYear, FiscalMonth),
    CONSTRAINT CK_FiscalPeriod_Quarter CHECK (FiscalQuarter BETWEEN 1 AND 4) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!,
    CONSTRAINT CK_FiscalPeriod_Month CHECK (FiscalMonth BETWEEN 1 AND 13) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!,
    CONSTRAINT CK_FiscalPeriod_DateRange CHECK (PeriodEndDate >= PeriodStartDate) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
;
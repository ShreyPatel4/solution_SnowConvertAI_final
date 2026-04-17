/*
    AllocationRule - Rules for cost allocation across cost centers
    Dependencies: CostCenter, GLAccount
*/
    -- DIRECT, STEP_DOWN, RECIPROCAL, ACTIVITY_BASED
    -- FIXED_PCT, HEADCOUNT, SQUARE_FOOTAGE, REVENUE, CUSTOM
    -- NULL means all cost centers matching pattern
    -- Regex pattern for cost center matching
    -- Regex pattern for account matching
    -- Complex target definitions
    -- NEAREST, UP, DOWN, NONE
CREATE OR REPLACE TABLE Planning.AllocationRule (
    AllocationRuleID INT IDENTITY(1,1) ORDER NOT NULL,
    RuleCode VARCHAR(30) NOT NULL,
    RuleName NVARCHAR(100) NOT NULL,
    RuleDescription NVARCHAR(500) NULL,
    RuleType VARCHAR(20) NOT NULL,
    AllocationMethod VARCHAR(20) NOT NULL,
    -- Source specification
    SourceCostCenterID INT NULL,
    SourceCostCenterPattern VARCHAR(50) NULL,
    SourceAccountPattern VARCHAR(50) NULL,
    -- Target specification using XML for flexibility
    TargetSpecification VARIANT !!!RESOLVE EWI!!! /*** SSC-EWI-0036 - XML DATA TYPE CONVERTED TO VARIANT ***/!!! NOT NULL,
    -- Calculation parameters
    AllocationBasis VARCHAR(30) NULL,
    AllocationPercentage DECIMAL(8, 6) NULL,
    RoundingMethod VARCHAR(10) NOT NULL DEFAULT 'NEAREST',
    RoundingPrecision TINYINT NOT NULL DEFAULT 2,
    MinimumAmount DECIMAL(19, 4) NULL,
    -- Execution order for step-down allocations
    ExecutionSequence INT NOT NULL DEFAULT 100,
    DependsOnRuleID INT NULL,
    -- Validity
    EffectiveFromDate DATE NOT NULL,
    EffectiveToDate DATE NULL,
    IsActive BOOLEAN NOT NULL DEFAULT true,
    -- Audit
    CreatedByUserID INT NULL,
    CreatedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    ModifiedByUserID INT NULL,
    ModifiedDateTime TIMESTAMP_NTZ(7) NOT NULL DEFAULT SYSDATE(),
    CONSTRAINT PK_AllocationRule PRIMARY KEY (AllocationRuleID),
    CONSTRAINT UQ_AllocationRule_Code UNIQUE (RuleCode),
    CONSTRAINT FK_AllocationRule_SourceCC FOREIGN KEY (SourceCostCenterID)
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_AllocationRule_DependsOn FOREIGN KEY (DependsOnRuleID)
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_AllocationRule_Type CHECK (RuleType IN ('DIRECT','STEP_DOWN','RECIPROCAL','ACTIVITY_BASED')) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!,
    CONSTRAINT CK_AllocationRule_Rounding CHECK (RoundingMethod IN ('NEAREST','UP','DOWN','NONE')) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
;
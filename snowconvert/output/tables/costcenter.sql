/*
    CostCenter - Organizational hierarchy for cost allocation
    Dependencies: None (base table)

    Note: Uses HierarchyID which has no Snowflake equivalent
*/
    -- No Snowflake equivalent
    -- Computed column from HierarchyID
    -- Temporal table
    -- System-versioned temporal table
CREATE OR REPLACE TABLE Planning.CostCenter (
    CostCenterID INT IDENTITY(1,1) ORDER NOT NULL,
    CostCenterCode VARCHAR(20) NOT NULL,
    CostCenterName NVARCHAR(100) NOT NULL,
    ParentCostCenterID INT NULL,
    HierarchyPath VARCHAR NULL,
    HierarchyLevel VARIANT AS PUBLIC.HIERARCHY_GET_LEVEL_UDF(HierarchyPath) /*** SSC-FDM-TS0014 - COMPUTED COLUMN WAS TRANSFORMED TO ITS SNOWFLAKE EQUIVALENT, FUNCTIONAL EQUIVALENCE VERIFICATION PENDING. ***/,
    ManagerEmployeeID INT NULL,
    DepartmentCode VARCHAR(10) NULL,
    IsActive BOOLEAN NOT NULL DEFAULT true,
    EffectiveFromDate DATE NOT NULL,
    EffectiveToDate DATE NULL,
    AllocationWeight DECIMAL(5, 4) NOT NULL DEFAULT 1.0000,
    ValidFrom TIMESTAMP_NTZ(7) GENERATED ALWAYS AS ROW START !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'GeneratedClause' NODE ***/!!! NOT NULL,
    ValidTo TIMESTAMP_NTZ(7) GENERATED ALWAYS AS ROW END !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'GeneratedClause' NODE ***/!!! NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'TablePeriodDefinition' NODE ***/!!!,
    CONSTRAINT PK_CostCenter PRIMARY KEY (CostCenterID),
    CONSTRAINT UQ_CostCenter_Code UNIQUE (CostCenterCode),
    CONSTRAINT FK_CostCenter_Parent FOREIGN KEY (ParentCostCenterID)
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT CK_CostCenter_Weight CHECK (AllocationWeight BETWEEN 0 AND 1) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CheckConstraintDefinition' NODE ***/!!!
)
   WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = Planning.CostCenterHistory) !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'TableOptionSystemVersioning' NODE ***/!!!)
   COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
;
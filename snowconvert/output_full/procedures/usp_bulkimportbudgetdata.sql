/*
    usp_BulkImportBudgetData - Bulk import budget data with validation and transformation

    Dependencies:
        - Tables: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod
        - Types: BudgetLineItemTableType
        - Functions: fn_GetHierarchyPath

    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. BULK INSERT from file - Snowflake uses COPY INTO with stages
    2. FORMAT FILE specification - No equivalent
    3. OPENROWSET for ad-hoc file access - Use external stages
    4. bcp format patterns - Different bulk loading paradigm
    5. Table-valued parameters as INPUT - Must use temp tables or stages
    6. SET IDENTITY_INSERT - Snowflake has different sequence handling
    7. @@IDENTITY vs SCOPE_IDENTITY vs IDENT_CURRENT - All different
    8. BULK INSERT error handling (MAXERRORS, ERRORFILE) - Different patterns
    9. Memory-optimized table variables - No equivalent
    10. Inline table-valued constructor (VALUES clause with many rows)
    11. TRUNCATE TABLE with foreign keys
    12. COLUMNS_UPDATED() and UPDATE() trigger functions
    ============================================================================
*/
    --** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "#MergeOutput" **
CREATE OR REPLACE PROCEDURE Planning.usp_BulkImportBudgetData
    --** SSC-FDM-0041 - DEFAULT PARAMETERS WERE REORDERED TO THE END OF THE PARAMETER LIST TO MATCH SNOWFLAKE REQUIREMENTS. CALLERS USING POSITIONAL ARGUMENTS MAY NEED TO BE UPDATED **
    (
    -- FILE, TVP, STAGING_TABLE, LINKED_SERVER
    -- For FILE import
    -- For TVP import
    -- For STAGING_TABLE import
    -- For LINKED_SERVER import
    -- STRICT, LENIENT, NONE
    -- REJECT, UPDATE, SKIP
    IMPORTSOURCE STRING, BUDGETDATA Planning.BudgetLineItemTableType !!!RESOLVE EWI!!! /*** SSC-EWI-0058 - FUNCTIONALITY FOR 'READONLY PARAMETERS' IS NOT CURRENTLY SUPPORTED BY SNOWFLAKE SCRIPTING ***/!!!, TARGETBUDGETHEADERID INT, FILEPATH STRING DEFAULT NULL, FORMATFILEPATH STRING DEFAULT NULL, STAGINGTABLENAME STRING DEFAULT NULL, LINKEDSERVERNAME STRING DEFAULT NULL, LINKEDSERVERQUERY STRING DEFAULT NULL, VALIDATIONMODE STRING DEFAULT 'STRICT', DUPLICATEHANDLING STRING DEFAULT 'REJECT', BATCHSIZE INT DEFAULT 10000, USEPARALLELLOAD BOOLEAN DEFAULT 1, MAXDEGREEOFPARALLELISM INT DEFAULT 4, IMPORTRESULTS TEXT DEFAULT NULL, ROWSIMPORTED INT DEFAULT NULL, ROWSREJECTED INT DEFAULT NULL)
    RETURNS VARCHAR
    LANGUAGE SQL
    COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
    EXECUTE AS CALLER
    AS
    $$
        DECLARE
            STARTTIME TIMESTAMP_NTZ(7) := SYSDATE();
            IMPORTBATCHID VARCHAR := UUID_STRING();
            ERRORMESSAGE NVARCHAR(4000);
            DYNAMICSQL NVARCHAR;
            TOTALROWS INT := 0;
            VALIDROWS INT := 0;
            INVALIDROWS INT := 0;
            PROCESSEDBATCHES INT := 0;

            -- =====================================================================
            -- Step 4: Process imports in batches
            -- =====================================================================
            BATCHNUMBER INT := 0;
            ROWSTHISBATCH INT := 1;
        BEGIN
--            --** SSC-FDM-TS0029 - SET NOCOUNT STATEMENT IS COMMENTED OUT, WHICH IS NOT APPLICABLE IN SNOWFLAKE. **
--            SET NOCOUNT ON;
            !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'SIMPLE SET STATEMENT' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
            SET XACT_ABORT ON;

            -- Staging table for imported data
            -- Alternative lookup
            -- Alternative lookup
            -- Alternative lookup
            -- Alternative lookup
            CREATE OR REPLACE TEMPORARY TABLE PUBLIC.T_ImportStaging (
                RowID INT IDENTITY(1,1) ORDER PRIMARY KEY,
                GLAccountID INT NULL,
                AccountNumber VARCHAR(20) NULL,
                CostCenterID INT NULL,
                CostCenterCode VARCHAR(20) NULL,
                FiscalPeriodID INT NULL,
                FiscalYear SMALLINT NULL,
                FiscalMonth TINYINT NULL,
                OriginalAmount DECIMAL(19, 4) NULL,
                AdjustedAmount DECIMAL(19, 4) NULL,
                SpreadMethodCode VARCHAR(10) NULL,
                Notes NVARCHAR(500) NULL,
                -- Validation tracking
                IsValid BOOLEAN DEFAULT true,
                ValidationErrors NVARCHAR NULL,
                -- Processing tracking
                IsProcessed BOOLEAN DEFAULT false,
                ProcessedDateTime TIMESTAMP_NTZ(7) NULL,
                ResultLineItemID BIGINT NULL
--                                            ,
--                --** SSC-FDM-0021 - CREATE INDEX IS NOT SUPPORTED BY SNOWFLAKE **
--                INDEX IX_Valid (IsValid, IsProcessed)
);
            -- Error tracking table
            CREATE OR REPLACE TEMPORARY TABLE PUBLIC.T_ImportErrors (
                ErrorID INT IDENTITY(1,1) ORDER PRIMARY KEY,
                RowID INT,
                ErrorCode VARCHAR(20),
                ErrorMessage NVARCHAR(500),
                ColumnName NVARCHAR(128),
                OriginalValue NVARCHAR(500),
                Severity VARCHAR(10) -- ERROR, WARNING
            );

            -- =====================================================================
            -- Step 4: Process imports in batches
            -- =====================================================================
            BEGIN
                -- =====================================================================
                -- Step 1: Load data into staging based on source type
                -- =====================================================================
                IF (:IMPORTSOURCE = 'FILE') THEN
                BEGIN
                    IF (:FILEPATH IS NULL) THEN
                        BEGIN
                            --** SSC-FDM-TS0019 - RAISERROR ERROR MESSAGE MAY DIFFER BECAUSE OF THE SQL SERVER STRING FORMAT **
                            SELECT
                                PUBLIC.RAISERROR_UDF('File path is required for FILE import source', 16, 1, array_construct());
                            RETURN -1;
                        END;
                    END IF;
                    -- BULK INSERT - Very different in Snowflake (COPY INTO)
                    IF (:FORMATFILEPATH IS NOT NULL) THEN
                    BEGIN
                            DYNAMICSQL := 'CREATE OR REPLACE FILE FORMAT FILE_FORMAT_639120387991225610
FORMATFILE = ''' || :FORMATFILEPATH || '''
SKIP_HEADER = 2
MAXERRORS = 1000
TABLOCK
ROWS_PER_BATCH = ' || CAST(:BATCHSIZE AS NVARCHAR) || '
ORDER
ERRORFILE = ''' || :FILEPATH || '.errors'';

CREATE OR REPLACE STAGE STAGE_639120387991225610
FILE_FORMAT = FILE_FORMAT_639120387991225610;

PUT file://' || :FILEPATH || ' @STAGE_639120387991225610 AUTO_COMPRESS = FALSE;

COPY INTO PUBLIC.T_ImportStaging
FROM @STAGE_639120387991225610/' || :FILEPATH;
                    END;
                    ELSE
                    BEGIN
                            DYNAMICSQL := 'CREATE OR REPLACE FILE FORMAT FILE_FORMAT_639120387991306680
FIELD_DELIMITER = '',''
RECORD_DELIMITER = ''\n''
SKIP_HEADER = 2
MAXERRORS = 1000
CODEPAGE = ''65001''
TABLOCK;

CREATE OR REPLACE STAGE STAGE_639120387991306680
FILE_FORMAT = FILE_FORMAT_639120387991306680;

PUT file://' || :FILEPATH || ' @STAGE_639120387991306680 AUTO_COMPRESS = FALSE;

COPY INTO PUBLIC.T_ImportStaging
FROM @STAGE_639120387991306680/' || :FILEPATH;
                    END;
                    END IF;
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!
                    EXECUTE IMMEDIATE :DYNAMICSQL;
                    TOTALROWS := SQLROWCOUNT;
                END;
                ELSEIF (:IMPORTSOURCE = 'TVP') THEN
                BEGIN
                    -- Load from table-valued parameter
                    INSERT INTO PUBLIC.T_ImportStaging (GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes)
                    SELECT
                        GLAccountID,
                        CostCenterID,
                        FiscalPeriodID,
                        OriginalAmount,
                        AdjustedAmount,
                        SpreadMethodCode,
                        Notes
                    FROM
                        T_BudgetData;
                    TOTALROWS := SQLROWCOUNT;
                END;
                ELSEIF (:IMPORTSOURCE = 'STAGING_TABLE') THEN
                BEGIN
                    IF (:STAGINGTABLENAME IS NULL) THEN
                        BEGIN
                            --** SSC-FDM-TS0019 - RAISERROR ERROR MESSAGE MAY DIFFER BECAUSE OF THE SQL SERVER STRING FORMAT **
                            SELECT
                                PUBLIC.RAISERROR_UDF('Staging table name is required for STAGING_TABLE import source', 16, 1, array_construct());
                            RETURN -1;
                        END;
                    END IF;
                    -- Dynamic insert from staging table
                    DYNAMICSQL := '
                INSERT INTO PUBLIC.T_ImportStaging (AccountNumber, CostCenterCode, FiscalYear, FiscalMonth, OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes)
                SELECT
                   AccountNumber,
                   CostCenterCode,
                   FiscalYear,
                   FiscalMonth,
                   OriginalAmount,
                   AdjustedAmount,
                   SpreadMethodCode,
                   Notes
                FROM
                   ' || PUBLIC.QUOTENAME_UDF(:STAGINGTABLENAME) || ';';
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!
                    EXECUTE IMMEDIATE :DYNAMICSQL;
                    TOTALROWS := SQLROWCOUNT;
                END;
                ELSEIF (:IMPORTSOURCE = 'LINKED_SERVER') THEN
                BEGIN
                    -- OPENQUERY for linked server access - No Snowflake equivalent
                    IF (:LINKEDSERVERNAME IS NULL OR :LINKEDSERVERQUERY IS NULL) THEN
                    BEGIN
                            --** SSC-FDM-TS0019 - RAISERROR ERROR MESSAGE MAY DIFFER BECAUSE OF THE SQL SERVER STRING FORMAT **
                            SELECT
                                PUBLIC.RAISERROR_UDF('Linked server name and query are required for LINKED_SERVER import', 16, 1, array_construct());
                            RETURN -1;
                    END;
                    END IF;
                    DYNAMICSQL := '
                INSERT INTO PUBLIC.T_ImportStaging (AccountNumber, CostCenterCode, FiscalYear, FiscalMonth, OriginalAmount, AdjustedAmount, Notes)
                SELECT
                   *
                FROM OPENQUERY(' || PUBLIC.QUOTENAME_UDF(:LINKEDSERVERNAME) || ',
                    ''' || REPLACE(:LINKEDSERVERQUERY, '''', '''''') || ''');';
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!
                    EXECUTE IMMEDIATE :DYNAMICSQL;
                    TOTALROWS := SQLROWCOUNT;
                END;
                END IF;

            -- =====================================================================
            -- Step 2: Resolve lookups (IDs from codes)
            -- =====================================================================

            -- Resolve GLAccountID from AccountNumber
            UPDATE T_ImportStaging stg
            SET
                    stg.GLAccountID = gla.GLAccountID
                FROM
                    Planning.GLAccount gla
                WHERE
                    stg.AccountNumber = gla.AccountNumber
                    AND (stg.GLAccountID IS NULL
              AND stg.AccountNumber IS NOT NULL);

            -- Resolve CostCenterID from CostCenterCode
            UPDATE T_ImportStaging stg
            SET
                    stg.CostCenterID = cc.CostCenterID
                FROM
                    Planning.CostCenter cc
                WHERE
                    stg.CostCenterCode = cc.CostCenterCode
                    AND (stg.CostCenterID IS NULL
              AND stg.CostCenterCode IS NOT NULL);

            -- Resolve FiscalPeriodID from Year/Month
            UPDATE T_ImportStaging stg
            SET
                    stg.FiscalPeriodID = fp.FiscalPeriodID
                FROM
                    Planning.FiscalPeriod fp
                WHERE
                    stg.FiscalYear = fp.FiscalYear
                    AND stg.FiscalMonth = fp.FiscalMonth
                    AND (stg.FiscalPeriodID IS NULL
              AND stg.FiscalYear IS NOT NULL
              AND stg.FiscalMonth IS NOT NULL);
                -- =====================================================================
                -- Step 3: Validate data
                -- =====================================================================
                IF (:VALIDATIONMODE <> 'NONE') THEN
                BEGIN
                    -- Check for missing required fields
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
                    SELECT
                        RowID, 'MISSING_ACCOUNT', 'GL Account not found or not specified', 'GLAccountID',
                           CASE
                            :VALIDATIONMODE
                            WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
                    FROM
                        PUBLIC.T_ImportStaging
                    WHERE
                        GLAccountID IS NULL;

                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
                    SELECT
                        RowID, 'MISSING_COSTCENTER', 'Cost Center not found or not specified', 'CostCenterID',
                           CASE
                            :VALIDATIONMODE
                            WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
                    FROM
                        PUBLIC.T_ImportStaging
                    WHERE
                        CostCenterID IS NULL;

                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
                    SELECT
                        RowID, 'MISSING_PERIOD', 'Fiscal Period not found or not specified', 'FiscalPeriodID',
                           CASE
                            :VALIDATIONMODE
                            WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
                    FROM
                        PUBLIC.T_ImportStaging
                    WHERE
                        FiscalPeriodID IS NULL;

                    -- Check for invalid amounts
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, OriginalValue, Severity)
                    SELECT
                        RowID, 'INVALID_AMOUNT', 'Amount is NULL', 'OriginalAmount', NULL, 'ERROR'
                    FROM
                        PUBLIC.T_ImportStaging
                    WHERE
                        OriginalAmount IS NULL;

                    -- Check account is budgetable
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
                    SELECT
                        stg.RowID, 'NON_BUDGETABLE', 'Account is not marked as budgetable', 'GLAccountID', 'WARNING'
                    FROM
                        PUBLIC.T_ImportStaging stg
                    INNER JOIN
                            Planning.GLAccount gla
                            ON stg.GLAccountID = gla.GLAccountID
                    WHERE
                        gla.IsBudgetable = 0;

                    -- Check cost center is active
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
                    SELECT
                        stg.RowID, 'INACTIVE_CC', 'Cost Center is inactive', 'CostCenterID',
                           CASE
                            :VALIDATIONMODE
                            WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
                    FROM
                        PUBLIC.T_ImportStaging stg
                    INNER JOIN
                            Planning.CostCenter cc
                            ON stg.CostCenterID = cc.CostCenterID
                    WHERE
                        cc.IsActive = 0;

                    -- Check for duplicates within import

                    -- Check period is not closed
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
                    SELECT
                        stg.RowID, 'CLOSED_PERIOD', 'Fiscal period is closed', 'FiscalPeriodID', 'ERROR'
                    FROM
                        PUBLIC.T_ImportStaging stg
                    INNER JOIN
                            Planning.FiscalPeriod fp
                            ON stg.FiscalPeriodID = fp.FiscalPeriodID
                    WHERE
                        fp.IsClosed = 1;
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, Severity)
                    WITH DuplicateCheck AS (
                        SELECT
                            RowID,
                            ROW_NUMBER() OVER (
                                PARTITION BY
                                GLAccountID, CostCenterID, FiscalPeriodID
                                ORDER BY RowID
                            ) AS RowNum
                        FROM
                            PUBLIC.T_ImportStaging
                        WHERE
                            GLAccountID IS NOT NULL
                          AND CostCenterID IS NOT NULL
                          AND FiscalPeriodID IS NOT NULL
                    )
                    SELECT
                        RowID, 'DUPLICATE_IN_BATCH', 'Duplicate entry within import batch', 'WARNING'
                    FROM
                        DuplicateCheck AS DuplicateCheck
                    WHERE
                        RowNum > 1;

                    -- Check for existing records in target
                    INSERT INTO PUBLIC.T_ImportErrors (RowID, ErrorCode, ErrorMessage, Severity)
                    SELECT
                        stg.RowID, 'ALREADY_EXISTS', 'Record already exists in target budget',
                           CASE
                            :DUPLICATEHANDLING
                            WHEN 'REJECT' THEN 'ERROR' ELSE 'WARNING' END
                    FROM
                        PUBLIC.T_ImportStaging stg
                    INNER JOIN
                            Planning.BudgetLineItem bli
                        ON stg.GLAccountID = bli.GLAccountID
                        AND stg.CostCenterID = bli.CostCenterID
                        AND stg.FiscalPeriodID = bli.FiscalPeriodID
                    WHERE
                        bli.BudgetHeaderID = :TARGETBUDGETHEADERID;

                    -- Aggregate validation errors to staging table
                    UPDATE T_ImportStaging stg
                    SET
                            IsValid = CASE WHEN EXISTS (
                            SELECT 1 FROM
                                PUBLIC.T_ImportErrors e
                            WHERE
                                e.RowID = stg.RowID
                                AND e.Severity = 'ERROR'
                        ) THEN 0 ELSE 1 END,
                            ValidationErrors = (
                            SELECT
                            LISTAGG(CONCAT(ErrorCode, ': ', ErrorMessage), '; ')
                            FROM
                            PUBLIC.T_ImportErrors e
                            WHERE
                            e.RowID = stg.RowID
                        );
                END;
                END IF;

            -- Count valid/invalid
            SELECT
                SUM(CASE WHEN IsValid = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsValid = 0 THEN 1 ELSE 0 END)
                INTO
                :VALIDROWS,
                :INVALIDROWS
            FROM
                PUBLIC.T_ImportStaging;
                 
                 
                WHILE (:ROWSTHISBATCH > 0) LOOP
                BATCHNUMBER := :BATCHNUMBER + 1;
                -- Use MERGE for upsert based on duplicate handling
                IF (:DUPLICATEHANDLING = 'UPDATE') THEN
                BEGIN
                        MERGE INTO Planning.BudgetLineItem AS target
                        USING (
                                               SELECT
                            :TARGETBUDGETHEADERID AS BudgetHeaderID,
                            GLAccountID,
                            CostCenterID,
                            FiscalPeriodID,
                            OriginalAmount,
                            NVL(AdjustedAmount, 0) AS AdjustedAmount,
                            SpreadMethodCode,
                                                   'BULK_IMPORT' AS SourceSystem,
                                                   CAST(:IMPORTBATCHID AS VARCHAR(50)) AS SourceReference,
                            RowID
                                               FROM
                            PUBLIC.T_ImportStaging
                                               WHERE
                            IsValid = 1
                                                 AND IsProcessed = 0
                                               ORDER BY RowID
                            LIMIT (:BATCHSIZE)
                                           ) AS source
                        ON target.BudgetHeaderID = source.BudgetHeaderID
                                              AND target.GLAccountID = source.GLAccountID
                                              AND target.CostCenterID = source.CostCenterID
                                              AND target.FiscalPeriodID = source.FiscalPeriodID
                        WHEN MATCHED THEN
                                               UPDATE SET
                            target.OriginalAmount = source.OriginalAmount,
                            target.AdjustedAmount = source.AdjustedAmount,
                            target.SpreadMethodCode = source.SpreadMethodCode,
                            target.SourceReference = source.SourceReference,
                            target.LastModifiedDateTime = SYSDATE()
                        WHEN NOT MATCHED THEN
                                               INSERT (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem, SourceReference, LastModifiedDateTime)
                                               VALUES (source.BudgetHeaderID, source.GLAccountID, source.CostCenterID, source.FiscalPeriodID, source.OriginalAmount, source.AdjustedAmount, source.SpreadMethodCode, source.SourceSystem, source.SourceReference, SYSDATE())
                        !!!RESOLVE EWI!!! /*** SSC-EWI-0021 - OUTPUT CLAUSE NOT SUPPORTED IN SNOWFLAKE ***/!!!
                                           OUTPUT
                                               $action,
                                               inserted.BudgetLineItemID,
                                               source.RowID
                                           INTO #MergeOutput (Action, LineItemID, SourceRowID);
                        ROWSTHISBATCH := SQLROWCOUNT;
                END;
                -- SKIP duplicates or REJECT (already filtered by IsValid)
                ELSE
                BEGIN
                        -- Using OUTPUT clause to track inserted rows
                        CREATE OR REPLACE TEMPORARY TABLE T_INSERTEDROWS (
                            LineItemID BIGINT,
                            SourceRowID INT
                    );

                    INSERT INTO Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem, SourceReference, ImportBatchID, LastModifiedDateTime)
                        !!!RESOLVE EWI!!! /*** SSC-EWI-0021 - OUTPUT CLAUSE NOT SUPPORTED IN SNOWFLAKE ***/!!!
                    OUTPUT
                        inserted.BudgetLineItemID,
                        inserted.GLAccountID  -- We'll need to join back
                    INTO @InsertedRows (LineItemID, SourceRowID)
                    SELECT
                            :TARGETBUDGETHEADERID,
                            GLAccountID,
                            CostCenterID,
                            FiscalPeriodID,
                            OriginalAmount,
                            NVL(AdjustedAmount, 0),
                            SpreadMethodCode,
                        'BULK_IMPORT',
                        CAST(:IMPORTBATCHID AS VARCHAR(50)),
                            :IMPORTBATCHID,
                            SYSDATE()
                    FROM
                            PUBLIC.T_ImportStaging stg
                    WHERE
                            IsValid = 1
                      AND IsProcessed = 0
                      AND (:DUPLICATEHANDLING = 'REJECT'
                           OR NOT EXISTS
                                         --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                         (
                               SELECT 1 FROM
                            Planning.BudgetLineItem bli
                               WHERE
                            bli.BudgetHeaderID = :TARGETBUDGETHEADERID
                                 AND bli.GLAccountID = stg.GLAccountID
                                 AND bli.CostCenterID = stg.CostCenterID
                                 AND bli.FiscalPeriodID = stg.FiscalPeriodID
                           ))
                    ORDER BY RowID
                        LIMIT (:BATCHSIZE) ;
                        ROWSTHISBATCH := SQLROWCOUNT;
                END;
                END IF;

                -- Mark processed rows
                UPDATE T_ImportStaging stg
                SET
                        IsProcessed = 1,
                        ProcessedDateTime = SYSDATE()
                WHERE
                        IsValid = 1
                  AND IsProcessed = 0
                  AND stg.RowID <= (
                      SELECT
                            MAX(RowID) FROM (
                          SELECT
                                RowID
                          FROM
                                PUBLIC.T_ImportStaging
                          WHERE
                                IsValid = 1 AND IsProcessed = 0
                          ORDER BY RowID
                            LIMIT (:BATCHSIZE)
                      ) AS processed
                  );
                PROCESSEDBATCHES := :PROCESSEDBATCHES + 1;
                END LOOP;

            -- Set output parameters
            SELECT
                SUM(CASE WHEN IsProcessed = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsValid = 0 THEN 1 ELSE 0 END)
                INTO
                :ROWSIMPORTED,
                :ROWSREJECTED
            FROM
                PUBLIC.T_ImportStaging;
            EXCEPTION
                WHEN OTHER THEN
                ERRORMESSAGE := SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/;
                ROWSIMPORTED := 0;
                ROWSREJECTED := :TOTALROWS;
            END;

            -- =========================================================================
            -- Build results XML
            -- =========================================================================
            IMPORTRESULTS := (
                       SELECT
                -- Summary
                -- Error summary by type
                -- Sample rejected rows (first 100)
                --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('BatchID', :IMPORTBATCHID, 'Source', :IMPORTSOURCE, 'TargetBudgetID', :TARGETBUDGETHEADERID, 'DurationMs', DATEDIFF(MILLISECOND, :STARTTIME, SYSDATE())), 'ImportResults')
                   );
            -- Cleanup
            DROP TABLE IF EXISTS PUBLIC.T_ImportStaging;
            DROP TABLE IF EXISTS PUBLIC.T_ImportErrors;
            DROP TABLE IF EXISTS PUBLIC.T_MergeOutput;
            DROP TABLE T_INSERTEDROWS;
            RETURN CASE WHEN :ERRORMESSAGE IS NULL THEN 0 ELSE -1 END;
        END;
    $$;
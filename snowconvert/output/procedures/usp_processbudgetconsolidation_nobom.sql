/*
    usp_ProcessBudgetConsolidation - Complex budget consolidation with hierarchy rollup

    Dependencies: 
        - Tables: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod, ConsolidationJournal
        - Views: vw_BudgetConsolidationSummary
        - Functions: fn_GetHierarchyPath, tvf_ExplodeCostCenterHierarchy
        - Types: BudgetLineItemTableType, AllocationResultTableType

    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. CURSOR with FAST_FORWARD and SCROLL options - Snowflake has no cursor support
    2. Table variables with indexes - No equivalent
    3. WHILE loops with complex break conditions
    4. Nested transactions with named savepoints - Limited in Snowflake
    5. TRY-CATCH with THROW/RAISERROR - Different exception model
    6. SCOPE_IDENTITY() after inserts
    7. OUTPUT clause capturing inserted rows
    8. Cross-apply with table-valued function
    9. Dynamic SQL with sp_executesql and output parameters
    10. MERGE with complex matching and OUTPUT
    11. @@TRANCOUNT and transaction nesting
    12. SET XACT_ABORT, NOCOUNT patterns
    ============================================================================
*/
    --** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "Planning.tvf_ExplodeCostCenterHierarchy" **
CREATE OR REPLACE PROCEDURE Planning.usp_ProcessBudgetConsolidation (SOURCEBUDGETHEADERID INT, TARGETBUDGETHEADERID INT DEFAULT NULL, CONSOLIDATIONTYPE STRING DEFAULT 'FULL',         -- FULL, INCREMENTAL, DELTA
INCLUDEELIMINATIONS BOOLEAN DEFAULT 1, RECALCULATEALLOCATIONS BOOLEAN DEFAULT 1, PROCESSINGOPTIONS TEXT DEFAULT NULL, USERID INT DEFAULT NULL, DEBUGMODE BOOLEAN DEFAULT 0, ROWSPROCESSED INT DEFAULT NULL, ERRORMESSAGE STRING DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE

        -- =========================================================================
        -- Variable declarations
        -- =========================================================================
        PROCSTARTTIME TIMESTAMP_NTZ(7) := SYSDATE();
        STEPSTARTTIME TIMESTAMP_NTZ(7);
        CURRENTSTEP NVARCHAR(100);
        RETURNCODE INT := 0;
        TOTALROWSPROCESSED INT := 0;
        BATCHSIZE INT := 5000;
        CURRENTBATCH INT := 0;
        MAXITERATIONS INT := 1000;
        CONSOLIDATIONRUNID VARCHAR := UUID_STRING();

        -- =========================================================================
        -- Cursor declarations - No Snowflake equivalent
        -- =========================================================================
        CURSORCOSTCENTERID INT;
        CURSORLEVEL INT;
        CURSORPARENTID INT;
        CURSORSUBTOTAL DECIMAL(19, 4);
        --** SSC-FDM-TS0013 - SNOWFLAKE SCRIPTING CURSOR ROWS ARE NOT MODIFIABLE **
        HierarchyCursor CURSOR
        FOR
            SELECT
                NodeID,
                NodeLevel,
                ParentNodeID
            FROM
                T_HierarchyNodes
            ORDER BY NodeLevel DESC, NodeID; -- Process bottom-up
    -- Secondary cursor for elimination entries
        ELIMACCOUNTID INT;
        ELIMCOSTCENTERID INT;
        ELIMAMOUNT DECIMAL(19, 4);
        PARTNERENTITYCODE VARCHAR(20);
        !!!RESOLVE EWI!!! /*** SSC-EWI-TS0037 - SNOWFLAKE SCRIPTING CURSORS ARE NON-SCROLLABLE, ONLY FETCH NEXT IS SUPPORTED ***/!!!
        --** SSC-FDM-TS0013 - SNOWFLAKE SCRIPTING CURSOR ROWS ARE NOT MODIFIABLE **
        !!!RESOLVE EWI!!! /*** SSC-EWI-0058 - FUNCTIONALITY FOR 'FOR UPDATE' IS NOT CURRENTLY SUPPORTED BY SNOWFLAKE SCRIPTING ***/!!!
        EliminationCursor CURSOR
        FOR
            SELECT
                bli.GLAccountID,
                bli.CostCenterID,
                bli.FinalAmount,
                gla.StatutoryAccountCode -- Uses SPARSE column
            FROM
                Planning.BudgetLineItem bli
            INNER JOIN
                    Planning.GLAccount gla
                    ON bli.GLAccountID = gla.GLAccountID
            WHERE
                bli.BudgetHeaderID = :SOURCEBUDGETHEADERID
              AND gla.IntercompanyFlag = 1
            ORDER BY bli.GLAccountID, bli.CostCenterID; -- Updateable cursor
        ELIMINATIONCOUNT INT := 0;
        -- Check for matching offsetting entry
        OFFSETEXISTS BOOLEAN := 0;
        OFFSETAMOUNT DECIMAL(19, 4);
        DYNAMICSQL NVARCHAR;
        PARAMDEFINITION NVARCHAR(500);
        ALLOCATIONROWCOUNT INT;
        INCLUDEZEROBALANCES BOOLEAN;
        ROUNDINGPRECISION INT;
    BEGIN
--        --** SSC-FDM-TS0029 - SET NOCOUNT STATEMENT IS COMMENTED OUT, WHICH IS NOT APPLICABLE IN SNOWFLAKE. **
--        SET NOCOUNT ON;
        !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'SIMPLE SET STATEMENT' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
        SET XACT_ABORT OFF;  -- We'll handle errors manually

        -- Table variables - These don't exist in Snowflake
        CREATE OR REPLACE TEMPORARY TABLE T_PROCESSINGLOG (
            LogID INT IDENTITY(1,1) ORDER PRIMARY KEY,
            StepName NVARCHAR(100),
            StartTime TIMESTAMP_NTZ(7),
            EndTime TIMESTAMP_NTZ(7),
            RowsAffected INT,
            StatusCode VARCHAR(20),
            Message NVARCHAR
--                            ,
--            --** SSC-FDM-0021 - CREATE INDEX IS NOT SUPPORTED BY SNOWFLAKE **
--            INDEX IX_StepName (StepName)
        );
        CREATE OR REPLACE TEMPORARY TABLE T_HIERARCHYNODES (
            NodeID INT PRIMARY KEY,
            ParentNodeID INT,
            NodeLevel INT,
            ProcessingOrder INT,
            IsProcessed BOOLEAN DEFAULT false,
            SubtotalAmount DECIMAL(19, 4)
--                                         ,
--            --** SSC-FDM-0021 - CREATE INDEX IS NOT SUPPORTED BY SNOWFLAKE **
--            INDEX IX_Level (NodeLevel, IsProcessed)
        );
        CREATE OR REPLACE TEMPORARY TABLE T_CONSOLIDATEDAMOUNTS (
            GLAccountID INT NOT NULL,
            CostCenterID INT NOT NULL,
            FiscalPeriodID INT NOT NULL,
            ConsolidatedAmount DECIMAL(19, 4) NOT NULL,
            EliminationAmount DECIMAL(19, 4) DEFAULT 0,
            FinalAmount DECIMAL(19, 4),
            SourceCount INT,
            PRIMARY KEY (GLAccountID, CostCenterID, FiscalPeriodID)
        );

        -- =========================================================================
        -- TRY-CATCH Error Handling - Different in Snowflake
        -- =========================================================================
        BEGIN
            -- Validate input parameters
            CURRENTSTEP := 'Parameter Validation';
            STEPSTARTTIME := SYSDATE();
            IF (NOT EXISTS (SELECT 1 FROM
                    Planning.BudgetHeader
                WHERE
                    BudgetHeaderID = :SOURCEBUDGETHEADERID
            )) THEN
                BEGIN
                    ERRORMESSAGE := 'Source budget header not found: ' || CAST(:SOURCEBUDGETHEADERID AS VARCHAR);
                    --** SSC-FDM-TS0019 - RAISERROR ERROR MESSAGE MAY DIFFER BECAUSE OF THE SQL SERVER STRING FORMAT **
                    SELECT
                        PUBLIC.RAISERROR_UDF(:ERRORMESSAGE, 16, 1, array_construct());
                END;
            END IF;
            -- Check if source is locked
            IF (EXISTS (
                SELECT 1 FROM
                    Planning.BudgetHeader
                WHERE
                    BudgetHeaderID = :SOURCEBUDGETHEADERID
                  AND StatusCode NOT IN ('APPROVED', 'LOCKED')
            )) THEN
            BEGIN
                    ERRORMESSAGE := 'Source budget must be in APPROVED or LOCKED status for consolidation';
                    CALL PUBLIC.THROW_UDP(50001, :ERRORMESSAGE, 1);  -- THROW syntax differs from RAISERROR
            END;
            END IF;
            INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), 0, 'COMPLETED');
            -- =====================================================================
            -- Create or update target budget header
            -- =====================================================================
            CURRENTSTEP := 'Create Target Budget';
            STEPSTARTTIME := SYSDATE();
            BEGIN TRANSACTION
            !!!RESOLVE EWI!!! /*** SSC-EWI-0101 - COMMENTED OUT TRANSACTION LABEL NAME BECAUSE IS NOT APPLICABLE IN SNOWFLAKE ***/!!!
            ConsolidationTran;
            IF (:TARGETBUDGETHEADERID IS NULL) THEN
                BEGIN
                    -- Create new consolidated budget header using OUTPUT clause
                    CREATE OR REPLACE TEMPORARY TABLE T_INSERTEDHEADERS (
                        BudgetHeaderID INT,
                        BudgetCode VARCHAR(30)
                    );

                    INSERT INTO Planning.BudgetHeader (BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear, StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode, VersionNumber, ExtendedProperties)
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0021 - OUTPUT CLAUSE NOT SUPPORTED IN SNOWFLAKE ***/!!!
                    OUTPUT inserted.BudgetHeaderID, inserted.BudgetCode INTO @InsertedHeaders
                    SELECT
                        BudgetCode || '_CONSOL_' || TO_CHAR(CURRENT_TIMESTAMP() :: TIMESTAMP, 'YYYYMMDD'),
                        BudgetName || ' - Consolidated',
                        'CONSOLIDATED',
                        ScenarioType,
                        FiscalYear,
                        StartPeriodID,
                        EndPeriodID,
                        BudgetHeaderID,
                        'DRAFT',
                        1,
                        -- XML modification - very different in Snowflake
                        CAST(
                            '<Root>' ||
                        '<ConsolidationRun RunID="' || CAST(:CONSOLIDATIONRUNID AS VARCHAR(36)) || '" ' ||
                        'SourceID="' || CAST(:SOURCEBUDGETHEADERID AS VARCHAR(20)) || '" ' ||
                        'Timestamp="' || CAST(:PROCSTARTTIME AS VARCHAR(30)) || '"/>' || NVL(CAST(ExtendedProperties AS NVARCHAR), '') ||
                        '</Root>' AS VARIANT !!!RESOLVE EWI!!! /*** SSC-EWI-0036 - XML DATA TYPE CONVERTED TO VARIANT ***/!!!
                        )
                    FROM
                        Planning.BudgetHeader
                    WHERE
                        BudgetHeaderID = :SOURCEBUDGETHEADERID;
                    SELECT
                        BudgetHeaderID
                    INTO
                        :TARGETBUDGETHEADERID
                    FROM
                        T_InsertedHeaders;
                    IF (:TARGETBUDGETHEADERID IS NULL) THEN
                        BEGIN
                            ERRORMESSAGE := 'Failed to create target budget header';
                            CALL PUBLIC.THROW_UDP(50002, :ERRORMESSAGE, 1);
                        END;
                    END IF;
                END;
            END IF;

            -- Savepoint for partial rollback - Limited Snowflake support
            !!!RESOLVE EWI!!! /*** SSC-EWI-TS0106 - CREATING TRANSACTION SAVEPOINTS IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
            SAVE TRANSACTION SavePoint_AfterHeader;
            INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), 1, 'COMPLETED');
            -- =====================================================================
            -- Build hierarchy for bottom-up rollup using TVF
            -- =====================================================================
            CURRENTSTEP := 'Build Hierarchy';
            STEPSTARTTIME := SYSDATE();
            INSERT INTO T_HIERARCHYNODES (NodeID, ParentNodeID, NodeLevel, ProcessingOrder)
            SELECT
                h.CostCenterID,
                h.ParentCostCenterID,
                h.HierarchyLevel,
                ROW_NUMBER() OVER (ORDER BY h.HierarchyLevel DESC, h.CostCenterID)
            FROM
                TABLE(Planning.tvf_ExplodeCostCenterHierarchy(NULL, 10, 0, CURRENT_TIMESTAMP() :: TIMESTAMP)) h;  -- CROSS APPLY to TVF

            INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), SQLROWCOUNT, 'COMPLETED');
            -- =====================================================================
            -- Process consolidation using cursor (bottom-up hierarchy traversal)
            -- =====================================================================
            CURRENTSTEP := 'Hierarchy Consolidation';
            STEPSTARTTIME := SYSDATE();
            OPEN HierarchyCursor;
            FETCH
                HierarchyCursor
            INTO
                :CURSORCOSTCENTERID,
                :CURSORLEVEL,
                :CURSORPARENTID;
            WHILE (:FETCH_STATUS = 0 AND :CURRENTBATCH < :MAXITERATIONS) LOOP
                CURRENTBATCH := :CURRENTBATCH + 1;

                -- Calculate subtotal for this node
                SELECT
                    SUM(bli.FinalAmount)
                INTO
                    :CURSORSUBTOTAL
                FROM
                    Planning.BudgetLineItem bli
                WHERE
                    bli.BudgetHeaderID = :SOURCEBUDGETHEADERID
                  AND bli.CostCenterID = :CURSORCOSTCENTERID;

                -- Add child subtotals (already processed due to bottom-up order)
                SELECT
                    NVL(:CURSORSUBTOTAL, 0) + NVL(SUM(h.SubtotalAmount), 0)
                INTO
                    :CURSORSUBTOTAL
                FROM
                    T_HierarchyNodes h
                WHERE
                    h.ParentNodeID = :CURSORCOSTCENTERID
                  AND h.IsProcessed = 1;

                -- Update node
                UPDATE T_HierarchyNodes
                SET
                        SubtotalAmount = :CURSORSUBTOTAL,
                        IsProcessed = 1
                WHERE
                        NodeID = :CURSORCOSTCENTERID;
                -- MERGE to update or insert consolidated amounts
                MERGE INTO T_ConsolidatedAmounts AS target
                USING (
                    SELECT
                        bli.GLAccountID,
                        :CURSORCOSTCENTERID AS CostCenterID,
                        bli.FiscalPeriodID,
                        SUM(bli.FinalAmount) AS Amount,
                        COUNT(*) AS SourceCnt
                    FROM
                        Planning.BudgetLineItem bli
                    WHERE
                        bli.BudgetHeaderID = :SOURCEBUDGETHEADERID
                      AND bli.CostCenterID = :CURSORCOSTCENTERID
                    GROUP BY
                        bli.GLAccountID,
                        bli.FiscalPeriodID
                ) AS source
                ON target.GLAccountID = source.GLAccountID
                   AND target.CostCenterID = source.CostCenterID
                   AND target.FiscalPeriodID = source.FiscalPeriodID
                WHEN MATCHED THEN
                    UPDATE SET
                        ConsolidatedAmount = target.ConsolidatedAmount + source.Amount,
                        SourceCount = target.SourceCount + source.SourceCnt
                WHEN NOT MATCHED THEN
                    INSERT (GLAccountID, CostCenterID, FiscalPeriodID, ConsolidatedAmount, SourceCount)
                    VALUES (source.GLAccountID, source.CostCenterID, source.FiscalPeriodID, source.Amount, source.SourceCnt);
                TOTALROWSPROCESSED := :TOTALROWSPROCESSED + SQLROWCOUNT;
                --** SSC-PRF-0003 - FETCH INSIDE A LOOP IS CONSIDERED A COMPLEX PATTERN, THIS COULD DEGRADE SNOWFLAKE PERFORMANCE. **
                FETCH
                    HierarchyCursor
                INTO
                    :CURSORCOSTCENTERID,
                    :CURSORLEVEL,
                    :CURSORPARENTID;
            END LOOP;
            CLOSE HierarchyCursor;
--            --** SSC-FDM-TS0057 - DEALLOCATE IS NOT REQUIRED IN SNOWFLAKE SCRIPTING. CURSORS ARE AUTOMATICALLY DEALLOCATED WHEN THEY GO OUT OF SCOPE. **
--            DEALLOCATE HierarchyCursor
                                      ;
            INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), :TOTALROWSPROCESSED, 'COMPLETED');
            -- =====================================================================
            -- Process intercompany eliminations using updateable cursor
            -- =====================================================================
            IF (:INCLUDEELIMINATIONS = 1) THEN
            BEGIN
                    CURRENTSTEP := 'Intercompany Eliminations';
                    STEPSTARTTIME := SYSDATE();
                     

                -- Savepoint before eliminations
                    !!!RESOLVE EWI!!! /*** SSC-EWI-TS0106 - CREATING TRANSACTION SAVEPOINTS IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                SAVE TRANSACTION SavePoint_BeforeEliminations;
                    OPEN EliminationCursor;
                    FETCH
                        EliminationCursor
                INTO
                        :ELIMACCOUNTID,
                        :ELIMCOSTCENTERID,
                        :ELIMAMOUNT,
                        :PARTNERENTITYCODE;
                    WHILE (:FETCH_STATUS = 0) LOOP
                        -- Complex elimination logic with scrollable cursor
                        IF (:ELIMAMOUNT <> 0) THEN
                            -- Check for matching offsetting entry
                        BEGIN
                                 
                                 

                                -- Use cursor positioning to look for offset
                                !!!RESOLVE EWI!!! /*** SSC-EWI-TS0037 - SNOWFLAKE SCRIPTING CURSORS ARE NON-SCROLLABLE, ONLY FETCH NEXT IS SUPPORTED ***/!!!
                                --** SSC-PRF-0003 - FETCH INSIDE A LOOP IS CONSIDERED A COMPLEX PATTERN, THIS COULD DEGRADE SNOWFLAKE PERFORMANCE. **
                                FETCH
                                    EliminationCursor
                                INTO
                                    :ELIMACCOUNTID,
                                    :ELIMCOSTCENTERID,
                                    :OFFSETAMOUNT,
                                    :PARTNERENTITYCODE;
                                IF (:FETCH_STATUS = 0 AND :OFFSETAMOUNT = -:ELIMAMOUNT) THEN
                                    BEGIN
                                        OFFSETEXISTS := 1;

                                        -- Create elimination entry
                                        UPDATE T_ConsolidatedAmounts
                                        SET
                                            EliminationAmount = EliminationAmount + :ELIMAMOUNT
                                        WHERE
                                            GLAccountID = :ELIMACCOUNTID
                                          AND CostCenterID = :ELIMCOSTCENTERID;
                                        ELIMINATIONCOUNT := :ELIMINATIONCOUNT + 1;
                                    END;
                                END IF;
                                -- Move back if no offset found
                                IF (:OFFSETEXISTS = 0) THEN
                                    !!!RESOLVE EWI!!! /*** SSC-EWI-TS0037 - SNOWFLAKE SCRIPTING CURSORS ARE NON-SCROLLABLE, ONLY FETCH NEXT IS SUPPORTED ***/!!!
                                    --** SSC-PRF-0003 - FETCH INSIDE A LOOP IS CONSIDERED A COMPLEX PATTERN, THIS COULD DEGRADE SNOWFLAKE PERFORMANCE. **
                                    FETCH
                                        EliminationCursor
                                    INTO
                                        :ELIMACCOUNTID,
                                        :ELIMCOSTCENTERID,
                                        :ELIMAMOUNT,
                                        :PARTNERENTITYCODE;
                                END IF;
                        END;
                        END IF;
                        --** SSC-PRF-0003 - FETCH INSIDE A LOOP IS CONSIDERED A COMPLEX PATTERN, THIS COULD DEGRADE SNOWFLAKE PERFORMANCE. **
                        FETCH
                            EliminationCursor
                    INTO
                            :ELIMACCOUNTID,
                            :ELIMCOSTCENTERID,
                            :ELIMAMOUNT,
                            :PARTNERENTITYCODE;
                    END LOOP;
                    CLOSE EliminationCursor;
--                    --** SSC-FDM-TS0057 - DEALLOCATE IS NOT REQUIRED IN SNOWFLAKE SCRIPTING. CURSORS ARE AUTOMATICALLY DEALLOCATED WHEN THEY GO OUT OF SCOPE. **
--                DEALLOCATE EliminationCursor
                                            ;
                INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
                VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), :ELIMINATIONCOUNT, 'COMPLETED');
            END;
            END IF;
            -- =====================================================================
            -- Recalculate allocations using dynamic SQL
            -- =====================================================================
            IF (:RECALCULATEALLOCATIONS = 1) THEN
            BEGIN
                    CURRENTSTEP := 'Recalculate Allocations';
                    STEPSTARTTIME := SYSDATE();
                     
                     
                     

                    -- Build dynamic SQL based on processing options
                    DYNAMICSQL := '
                UPDATE ca
                   SET
                      FinalAmount = ca.ConsolidatedAmount - ca.EliminationAmount
                FROM
                      T_ConsolidatedAmounts ca
                WHERE
                      ca.ConsolidatedAmount <> 0
                  OR ca.EliminationAmount <> 0;

                SET @RowCountOUT = @@ROWCOUNT';
                    -- Extract options from XML if provided
                    IF (:PROCESSINGOPTIONS IS NOT NULL) THEN
                    BEGIN
                             
                             

                        SELECT
                                GET(XMLGET(:PROCESSINGOPTIONS, 'IncludeZeroBalances', 0), '$') :: BOOLEAN,
                                GET(XMLGET(:PROCESSINGOPTIONS, 'RoundingPrecision', 0), '$') :: INT
                            INTO
                                :INCLUDEZEROBALANCES,
                                :ROUNDINGPRECISION;
                            -- Modify SQL based on options
                            IF (:INCLUDEZEROBALANCES = 0) THEN
                                DYNAMICSQL := REPLACE(:DYNAMICSQL,
                                    'WHERE ca.ConsolidatedAmount <> 0',
                                    'WHERE ca.ConsolidatedAmount <> 0 AND ca.FinalAmount <> 0');
                            END IF;
                            IF (:ROUNDINGPRECISION IS NOT NULL) THEN
                                DYNAMICSQL := REPLACE(:DYNAMICSQL,
                                'ca.ConsolidatedAmount - ca.EliminationAmount',
                                'ROUND(ca.ConsolidatedAmount - ca.EliminationAmount, ' || CAST(:ROUNDINGPRECISION AS VARCHAR) || ')');
                            END IF;
                    END;
                    END IF;
                    PARAMDEFINITION := '@RowCountOUT INT OUTPUT';
                    -- This pattern with table variables in dynamic SQL is very SQL Server-specific
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!
                    EXECUTE IMMEDIATE PUBLIC.TRANSFORM_SP_EXECUTE_SQL_STRING_UDF(:DYNAMICSQL, :PARAMDEFINITION, ARRAY_CONSTRUCT('ROWCOUNTOUT'), ARRAY_CONSTRUCT(:ALLOCATIONROWCOUNT));

                INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
                VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), :ALLOCATIONROWCOUNT, 'COMPLETED');
            END;
            END IF;
            -- =====================================================================
            -- Insert final results with OUTPUT clause
            -- =====================================================================
            CURRENTSTEP := 'Insert INTO Results ;';
            STEPSTARTTIME := SYSDATE();
            CREATE OR REPLACE TEMPORARY TABLE T_INSERTEDLINES (
                BudgetLineItemID BIGINT,
                GLAccountID INT,
                CostCenterID INT,
                Amount DECIMAL(19, 4)
            );

            INSERT INTO Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem, SourceReference, IsAllocated, LastModifiedByUserID, LastModifiedDateTime)
            !!!RESOLVE EWI!!! /*** SSC-EWI-0021 - OUTPUT CLAUSE NOT SUPPORTED IN SNOWFLAKE ***/!!!
            OUTPUT
                inserted.BudgetLineItemID,
                inserted.GLAccountID,
                inserted.CostCenterID,
                inserted.OriginalAmount
            INTO @InsertedLines
            SELECT
                :TARGETBUDGETHEADERID,
                ca.GLAccountID,
                ca.CostCenterID,
                ca.FiscalPeriodID,
                ca.FinalAmount,
                0,
                'CONSOLIDATED',
                'CONSOLIDATION_PROC',
                CAST(:CONSOLIDATIONRUNID AS VARCHAR(50)),
                0,
                :USERID,
                SYSDATE()
            FROM
                T_ConsolidatedAmounts ca
            WHERE
                ca.FinalAmount IS NOT NULL;
            TOTALROWSPROCESSED := :TOTALROWSPROCESSED + SQLROWCOUNT;
            INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), SQLROWCOUNT, 'COMPLETED');
            -- =====================================================================
            -- Commit transaction
            -- =====================================================================
            IF (:TRANCOUNT > 0) THEN
                COMMIT;
            END IF;
            ROWSPROCESSED := :TOTALROWSPROCESSED;
            -- Debug output
            IF (:DEBUGMODE = 1) THEN
            BEGIN
                SELECT
                        *
                    FROM
                        T_ProcessingLog
                    ORDER BY LogID;
                SELECT
                        *
                    FROM
                        T_InsertedLines;
            END;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                -- =====================================================================
                -- Error handling block - Pattern differs significantly in Snowflake
                -- =====================================================================
                RETURNCODE := SQLCODE /*** SSC-FDM-TS0023 - ERROR NUMBER COULD BE DIFFERENT IN SNOWFLAKE ***/;
                ERRORMESSAGE := SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/;
                -- Check transaction state and rollback appropriately
                IF (:TRANCOUNT > 0) THEN
                BEGIN
                        -- Try to rollback to savepoint first if possible
                        IF (CURRENT_TRANSACTION() = 1) THEN
                        BEGIN
                            ROLLBACK SavePoint_AfterHeader !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'RollbackName' NODE ***/!!!;
                        END;
                        ELSE
                        BEGIN
                            ROLLBACK ConsolidationTran !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'RollbackName' NODE ***/!!!;
                        END;
                        END IF;
                END;
                END IF;
                -- Cleanup cursors if open
                IF (CURSOR_STATUS('local', 'HierarchyCursor') !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CURSOR_STATUS' NODE ***/!!! >= 0) THEN
                BEGIN
                        CLOSE HierarchyCursor;
--                        --** SSC-FDM-TS0057 - DEALLOCATE IS NOT REQUIRED IN SNOWFLAKE SCRIPTING. CURSORS ARE AUTOMATICALLY DEALLOCATED WHEN THEY GO OUT OF SCOPE. **
--                    DEALLOCATE HierarchyCursor
                                              ;
                END;
                END IF;
                IF (CURSOR_STATUS('local', 'EliminationCursor') !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'CURSOR_STATUS' NODE ***/!!! >= 0) THEN
                    BEGIN
                        CLOSE EliminationCursor;
--                        --** SSC-FDM-TS0057 - DEALLOCATE IS NOT REQUIRED IN SNOWFLAKE SCRIPTING. CURSORS ARE AUTOMATICALLY DEALLOCATED WHEN THEY GO OUT OF SCOPE. **
--                        DEALLOCATE EliminationCursor
                                                    ;
                    END;
                END IF;

            -- Log the error
            INSERT INTO T_PROCESSINGLOG (StepName, StartTime, EndTime, RowsAffected, StatusCode, Message)
            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), 0, 'ERROR', :ERRORMESSAGE);
                -- Re-throw the error
                LET DECLARED_EXCEPTION EXCEPTION;
                RAISE DECLARED_EXCEPTION;
        END;
        DROP TABLE T_PROCESSINGLOG;
        DROP TABLE T_HIERARCHYNODES;
        DROP TABLE T_CONSOLIDATEDAMOUNTS;
        DROP TABLE T_INSERTEDHEADERS;
        DROP TABLE T_INSERTEDLINES;
        RETURN :RETURNCODE;
    END;
$$;
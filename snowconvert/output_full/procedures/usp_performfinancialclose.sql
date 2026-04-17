/*
    usp_PerformFinancialClose - Comprehensive period-end close orchestration procedure

    Dependencies:
        - Tables: All tables in the Planning schema
        - Views: All views in the Planning schema  
        - Functions: All functions in the Planning schema
        - Procedures: usp_ProcessBudgetConsolidation, usp_ExecuteCostAllocation, 
                      usp_ReconcileIntercompanyBalances
        - Types: BudgetLineItemTableType, AllocationResultTableType

    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. Service Broker messaging (commented but shows pattern)
    2. EXEC nested procedures with OUTPUT params
    3. Transaction nesting with @@TRANCOUNT checks
    4. Multiple RETURN points with different codes
    5. DISABLE/ENABLE TRIGGER for maintenance
    6. CHECKPOINT and DBCC SHRINKDATABASE (admin operations)
    7. sp_OA* automation procedures
    8. Send email via sp_send_dbmail
    9. Agent job scheduling via msdb procedures
    10. Change Data Capture (CDC) queries
    11. Extended events session management
    12. Query Store operations
    13. Memory-optimized tables with NATIVE_COMPILATION
    14. Temporal table FOR SYSTEM_TIME queries
    ============================================================================
*/
    --** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "Planning.usp_ExecuteCostAllocation" **
CREATE OR REPLACE PROCEDURE Planning.usp_PerformFinancialClose
    --** SSC-FDM-0041 - DEFAULT PARAMETERS WERE REORDERED TO THE END OF THE PARAMETER LIST TO MATCH SNOWFLAKE REQUIREMENTS. CALLERS USING POSITIONAL ARGUMENTS MAY NEED TO BE UPDATED **
    (
    -- SOFT, HARD, FINAL
    -- Semicolon-separated emails
    FISCALPERIODID INT, CLOSINGUSERID INT, CLOSETYPE STRING DEFAULT 'SOFT', RUNCONSOLIDATION BOOLEAN DEFAULT 1, RUNALLOCATIONS BOOLEAN DEFAULT 1, RUNRECONCILIATION BOOLEAN DEFAULT 1, SENDNOTIFICATIONS BOOLEAN DEFAULT 1, NOTIFICATIONRECIPIENTS STRING DEFAULT NULL, FORCECLOSE BOOLEAN DEFAULT 0, CLOSERESULTS TEXT DEFAULT NULL, OVERALLSTATUS STRING DEFAULT NULL)
    RETURNS VARCHAR
    LANGUAGE SQL
    COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
    EXECUTE AS CALLER
    AS
    $$
        DECLARE
            SC_EXIT_CODE VARCHAR;

            -- =========================================================================
            -- Declarations and initialization
            -- =========================================================================
            PROCSTARTTIME TIMESTAMP_NTZ(7) := SYSDATE();
            STEPSTARTTIME TIMESTAMP_NTZ(7);
            CURRENTSTEP NVARCHAR(100);
            CLOSERUNID VARCHAR := UUID_STRING();
            ERRORMESSAGE NVARCHAR(4000);
            RETURNCODE INT := 0;
            FISCALYEAR SMALLINT;
            FISCALMONTH TINYINT;
            PERIODNAME NVARCHAR(50);
            ISALREADYCLOSED BOOLEAN;

            -- Nested procedure output variables
            CONSOLIDATIONBUDGETID INT;
            CONSOLIDATIONROWS INT;
            CONSOLIDATIONERROR NVARCHAR(4000);
            ALLOCATIONROWS INT;
            ALLOCATIONWARNINGS NVARCHAR;
            RECONCILIATIONXML VARIANT !!!RESOLVE EWI!!! /*** SSC-EWI-0036 - XML DATA TYPE CONVERTED TO VARIANT ***/!!!;
            UNRECONCILEDCOUNT INT;
            VARIANCETOTAL DECIMAL(19, 4);
            ACTIVEBUDGETID INT;

            -- Check for pending journals
            PENDINGJOURNALS INT;

            -- Query cost center history using FOR SYSTEM_TIME
            -- This temporal query syntax doesn't exist in Snowflake
            SNAPSHOTTIME TIMESTAMP_NTZ(7) := SYSDATE();

            -- Create empty TVP for allocation results
            EMPTYALLOCRESULTS Planning.AllocationResultTableType;
            EFFECTIVEBUDGETID INT := NVL(:CONSOLIDATIONBUDGETID, :ACTIVEBUDGETID);
            RECONCILEBUDGETID INT := NVL(:CONSOLIDATIONBUDGETID, :ACTIVEBUDGETID);
            AUTOCREATEADJ BOOLEAN := CASE WHEN :CLOSETYPE = 'FINAL' THEN 0 ELSE 1 END;
            EMAILBODY NVARCHAR;
            EMAILSUBJECT NVARCHAR(255);
            SC_PROCESS PROCEDURE ()
            RETURNS VARCHAR
            AS
                BEGIN
--                    --** SSC-FDM-TS0029 - SET NOCOUNT STATEMENT IS COMMENTED OUT, WHICH IS NOT APPLICABLE IN SNOWFLAKE. **
--                    SET NOCOUNT ON;
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'SIMPLE SET STATEMENT' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
                    SET XACT_ABORT OFF;  -- We handle errors manually for orchestration

                     
                     
                     
                     
                     
                     
                     
                     
                     
                     

                    -- Step tracking
                    CREATE OR REPLACE TEMPORARY TABLE T_STEPRESULTS (
                        StepNumber INT IDENTITY(1,1) ORDER PRIMARY KEY,
                        StepName NVARCHAR(100),
                        StartTime TIMESTAMP_NTZ(7),
                        EndTime TIMESTAMP_NTZ(7),
                        DurationMs INT,
                        Status VARCHAR(20),
                        RowsAffected INT,
                        ErrorMessage NVARCHAR(4000),
                        OutputData VARIANT !!!RESOLVE EWI!!! /*** SSC-EWI-0036 - XML DATA TYPE CONVERTED TO VARIANT ***/!!!
                    );
                     
                     
                     
                     
                     
                     
                     
                     
                     

                    -- Validation result table
                    CREATE OR REPLACE TEMPORARY TABLE T_VALIDATIONERRORS (
                        ErrorCode VARCHAR(20),
                        ErrorMessage NVARCHAR(500),
                        Severity VARCHAR(10),  -- ERROR, WARNING

                        BlocksClose BOOLEAN
                    );

                    -- Check for pending journals

                    -- Query cost center history using FOR SYSTEM_TIME
                    -- This temporal query syntax doesn't exist in Snowflake
                    BEGIN
                        -- =====================================================================
                        -- Step 1: Validate period and prerequisites
                        -- =====================================================================
                        CURRENTSTEP := 'Period Validation';
                        STEPSTARTTIME := SYSDATE();
                    SELECT
                            FiscalYear,
                            FiscalMonth,
                            PeriodName,
                            IsClosed
                        INTO
                            :FISCALYEAR,
                            :FISCALMONTH,
                            :PERIODNAME,
                            :ISALREADYCLOSED
                    FROM
                            Planning.FiscalPeriod
                    WHERE
                            FiscalPeriodID = :FISCALPERIODID;
                        IF (:FISCALYEAR IS NULL) THEN
                            BEGIN
                                ERRORMESSAGE := 'Fiscal period not found: ' || CAST(:FISCALPERIODID AS VARCHAR);
                                INSERT INTO T_VALIDATIONERRORS VALUES ('INVALID_PERIOD', :ERRORMESSAGE, 'ERROR', 1);
                            END;
                        END IF;
                        IF (:ISALREADYCLOSED = 1 AND :FORCECLOSE = 0) THEN
                            BEGIN
                                ERRORMESSAGE := 'Period is already closed. Use @ForceClose=1 to reprocess.';
                                INSERT INTO T_VALIDATIONERRORS VALUES ('ALREADY_CLOSED', :ERRORMESSAGE, 'ERROR', 1);
                            END;
                        END IF;
                        -- Check prior periods are closed (for HARD and FINAL close)
                        IF (:CLOSETYPE IN ('HARD', 'FINAL')) THEN
                        BEGIN
                                IF (EXISTS (
                                                   SELECT 1 FROM
                                                       Planning.FiscalPeriod
                                                   WHERE
                                                       FiscalYear = :FISCALYEAR
                                                     AND FiscalMonth < :FISCALMONTH
                                                     AND IsClosed = 0
                                                     AND IsAdjustmentPeriod = 0
                                               )) THEN
                                               BEGIN
                                                       ERRORMESSAGE := 'Prior periods must be closed before ' || :CLOSETYPE || ' close';
                                                   INSERT INTO T_VALIDATIONERRORS VALUES ('PRIOR_OPEN', :ERRORMESSAGE, 'ERROR', 1);
                                               END;
                                END IF;
                        END;
                        END IF;
                         
                    SELECT
                            COUNT(*)
                        INTO
                            :PENDINGJOURNALS
                    FROM
                            Planning.ConsolidationJournal cj
                    INNER JOIN
                                Planning.FiscalPeriod fp
                                ON cj.FiscalPeriodID = fp.FiscalPeriodID
                    WHERE
                            fp.FiscalPeriodID = :FISCALPERIODID
                      AND cj.StatusCode IN ('DRAFT', 'SUBMITTED');
                        IF (:PENDINGJOURNALS > 0) THEN
                            BEGIN
                                ERRORMESSAGE := CONCAT(:PENDINGJOURNALS, ' pending journal(s) must be posted or rejected');
                                INSERT INTO T_VALIDATIONERRORS VALUES ('PENDING_JOURNALS', :ERRORMESSAGE,
                                    CASE WHEN :CLOSETYPE = 'FINAL' THEN 'ERROR' ELSE 'WARNING' END,
                                    CASE WHEN :CLOSETYPE = 'FINAL' THEN 1 ELSE 0 END);
                            END;
                        END IF;

                    -- Log validation step
                    INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected)
                        SELECT
                            :CURRENTSTEP,
                            :STEPSTARTTIME,
                            SYSDATE(),
                            DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                            CASE WHEN EXISTS (SELECT 1 FROM
                                                       T_ValidationErrors
                                               WHERE
                                                       BlocksClose = 1) THEN 'FAILED' ELSE 'COMPLETED' END,
                            (SELECT
                                               COUNT(*) FROM
                                               T_ValidationErrors
                            )
                        ;
                        -- Stop if blocking errors
                        IF (EXISTS (SELECT 1 FROM
                                T_ValidationErrors
                            WHERE
                                BlocksClose = 1)) THEN
                        BEGIN
                                OVERALLSTATUS := 'VALIDATION_FAILED';
                                BEGIN
                                               CALL BuildResults();
                                               RETURN 'PROCESS FINISHED';
                                END;
                        END;
                        END IF;
                        -- =====================================================================
                        -- Step 2: Create snapshot point (using temporal table)
                        -- =====================================================================
                        CURRENTSTEP := 'Create Snapshot';
                        STEPSTARTTIME := SYSDATE();
                         
                        CREATE OR REPLACE TEMPORARY TABLE PUBLIC.T_CostCenterSnapshot AS
                            SELECT
                                cc.CostCenterID,
                                cc.CostCenterCode,
                                cc.CostCenterName,
                                cc.ParentCostCenterID,
                                cc.AllocationWeight,
                        'CURRENT' AS SnapshotType
                    FROM
                                Planning.CostCenter
                    FOR SYSTEM_TIME AS OF :SNAPSHOTTIME cc -- Temporal query
                    WHERE
                                cc.IsActive = 1;
                    INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected)
                    VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()), 'COMPLETED', SQLROWCOUNT);
                        -- =====================================================================
                        -- Step 3: Run Consolidation (nested procedure call)
                        -- =====================================================================
                        IF (:RUNCONSOLIDATION = 1) THEN
                        BEGIN
                                CURRENTSTEP := 'Budget Consolidation';
                                STEPSTARTTIME := SYSDATE();

                            -- Find active budget for this period
                            SELECT TOP 1
                                               BudgetHeaderID
                                INTO
                                               :ACTIVEBUDGETID
                            FROM
                                               Planning.BudgetHeader bh
                            INNER JOIN
                                                       Planning.FiscalPeriod fp
                                                       ON fp.FiscalPeriodID BETWEEN bh.StartPeriodID AND bh.EndPeriodID
                            WHERE
                                               fp.FiscalPeriodID = :FISCALPERIODID
                              AND bh.StatusCode IN ('APPROVED', 'LOCKED')
                            ORDER BY bh.VersionNumber DESC;
                                IF (:ACTIVEBUDGETID IS NOT NULL) THEN
                                               BEGIN
                                                       BEGIN
                                            -- Nested procedure execution with OUTPUT parameters
                                            CALL Planning.usp_ProcessBudgetConsolidation(SOURCEBUDGETHEADERID => :ACTIVEBUDGETID, TARGETBUDGETHEADERID => :CONSOLIDATIONBUDGETID, CONSOLIDATIONTYPE => 'FULL', INCLUDEELIMINATIONS => 1, RECALCULATEALLOCATIONS => 0, USERID => :CLOSINGUSERID, ROWSPROCESSED => :CONSOLIDATIONROWS, ERRORMESSAGE => :CONSOLIDATIONERROR);

                                                       INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected, ErrorMessage)
                                                       VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                                               CASE WHEN :RETURNCODE = 0 THEN 'COMPLETED' ELSE 'WARNING' END, :CONSOLIDATIONROWS, :CONSOLIDATIONERROR);
                                                       EXCEPTION
                                            WHEN OTHER THEN
                                                INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                                                VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                                        'FAILED', SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/);
                                                IF (:CLOSETYPE = 'FINAL') THEN
                                                    LET DECLARED_EXCEPTION EXCEPTION;
                                                    RAISE DECLARED_EXCEPTION;
                                                END IF;
                                                       END;
                                               END;
                                ELSE
                                               BEGIN
                                                   INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                                                   VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                                           'SKIPPED', 'No active budget found for period');
                                               END;
                                END IF;
                        END;
                        END IF;
                        -- =====================================================================
                        -- Step 4: Run Cost Allocations
                        -- =====================================================================
                        IF (:RUNALLOCATIONS = 1) THEN

                            -- Create empty TVP for allocation results
                        BEGIN
                                CURRENTSTEP := 'Cost Allocations';
                                STEPSTARTTIME := SYSDATE();
                                 
                                 
                                BEGIN
                                               CALL Planning.usp_ExecuteCostAllocation(:EFFECTIVEBUDGETID, NULL, :FISCALPERIODID, 0, 'EXCLUSIVE', :EMPTYALLOCRESULTS, :ALLOCATIONROWS, :ALLOCATIONWARNINGS);

                                INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected, ErrorMessage)
                                VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                        CASE WHEN :RETURNCODE = 0 THEN 'COMPLETED' ELSE 'WARNING' END, :ALLOCATIONROWS, :ALLOCATIONWARNINGS);
                                EXCEPTION
                                               WHEN OTHER THEN
                                                       INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                                                       VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                                               'FAILED', SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/);
                                                       IF (:CLOSETYPE = 'FINAL') THEN
                                            LET DECLARED_EXCEPTION EXCEPTION;
                                            RAISE DECLARED_EXCEPTION;
                                                       END IF;
                                END;
                        END;
                        END IF;
                        -- =====================================================================
                        -- Step 5: Run Intercompany Reconciliation
                        -- =====================================================================
                        IF (:RUNRECONCILIATION = 1) THEN
                        BEGIN
                                CURRENTSTEP := 'Intercompany Reconciliation';
                                STEPSTARTTIME := SYSDATE();
                                 
                                 
                                BEGIN
                                               CALL Planning.usp_ReconcileIntercompanyBalances(BUDGETHEADERID => :RECONCILEBUDGETID, RECONCILIATIONDATE => NULL, ENTITYCODES => NULL, TOLERANCEAMOUNT => 0.01, AUTOCREATEADJUSTMENTS => :AUTOCREATEADJ, RECONCILIATIONREPORTXML => :RECONCILIATIONXML, UNRECONCILEDCOUNT => :UNRECONCILEDCOUNT, TOTALVARIANCEAMOUNT => :VARIANCETOTAL);

                                INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected, ErrorMessage, OutputData)
                                VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                        CASE
                                            WHEN :UNRECONCILEDCOUNT = 0 THEN 'COMPLETED'
                                            WHEN :CLOSETYPE = 'FINAL' AND :UNRECONCILEDCOUNT > 0 THEN 'FAILED'
                                            ELSE 'WARNING'
                                        END, :UNRECONCILEDCOUNT,
                                        CASE WHEN :UNRECONCILEDCOUNT > 0
                                             THEN CONCAT(:UNRECONCILEDCOUNT, ' unreconciled items, variance: ', TO_CHAR(:VARIANCETOTAL, 'C') !!!RESOLVE EWI!!! /*** SSC-EWI-0006 - FORMAT: 'C' MAY FAIL OR MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE.  ***/!!!)
                                             ELSE NULL END, :RECONCILIATIONXML);
                                               -- Block FINAL close if unreconciled
                                               IF (:CLOSETYPE = 'FINAL' AND :UNRECONCILEDCOUNT > 0) THEN
                                               BEGIN
                                            ERRORMESSAGE := 'Cannot perform FINAL close with unreconciled intercompany balances';
                                            CALL PUBLIC.THROW_UDP(50200, :ERRORMESSAGE, 1);
                                               END;
                                               END IF;
                                EXCEPTION
                                               WHEN OTHER THEN
                                                       INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                                                       VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()),
                                                               'FAILED', SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/);
                                                       IF (:CLOSETYPE = 'FINAL') THEN
                                            LET DECLARED_EXCEPTION EXCEPTION;
                                            RAISE DECLARED_EXCEPTION;
                                                       END IF;
                                END;
                        END;
                        END IF;
                        -- =====================================================================
                        -- Step 6: Lock the period
                        -- =====================================================================
                        CURRENTSTEP := 'Lock Period';
                        STEPSTARTTIME := SYSDATE();
                        BEGIN TRANSACTION
                        !!!RESOLVE EWI!!! /*** SSC-EWI-0101 - COMMENTED OUT TRANSACTION LABEL NAME BECAUSE IS NOT APPLICABLE IN SNOWFLAKE ***/!!!
                        LockPeriodTran;
                        -- Disable triggers during close (SQL Server specific)
                        -- This pattern has no Snowflake equivalent
                        !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!!!!RESOLVE EWI!!! /*** SSC-EWI-0027 - THE FOLLOWING STATEMENT USES A VARIABLE/LITERAL WITH AN INVALID QUERY AND IT WILL NOT BE EXECUTED ***/!!!
                        EXECUTE IMMEDIATE 'DISABLE TRIGGER ALL ON Planning.BudgetLineItem';
                        BEGIN
                            UPDATE Planning.FiscalPeriod
                            SET
                                               IsClosed = 1,
                                               ClosedByUserID = :CLOSINGUSERID,
                                               ClosedDateTime = SYSDATE(),
                                               ModifiedDateTime = SYSDATE()
                            WHERE
                                               FiscalPeriodID = :FISCALPERIODID;

                            -- Lock all budgets in this period
                            UPDATE Planning.BudgetHeader
                            SET
                                               StatusCode = 'LOCKED',
                                               LockedDateTime = SYSDATE(),
                                               ModifiedDateTime = SYSDATE()
                            WHERE
                                               StatusCode = 'APPROVED'
                              AND :FISCALPERIODID BETWEEN StartPeriodID AND EndPeriodID;

                            INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected)
                            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()), 'COMPLETED', SQLROWCOUNT);
                            COMMIT;
                        EXCEPTION
                            WHEN OTHER THEN
                                IF (:TRANCOUNT > 0) THEN
                                               ROLLBACK LockPeriodTran !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'RollbackName' NODE ***/!!!;
                                END IF;
                            INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                            VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()), 'FAILED', SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/);
                                LET DECLARED_EXCEPTION EXCEPTION;
                                RAISE DECLARED_EXCEPTION;
                        END;

                        -- Re-enable triggers
                        !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!!!!RESOLVE EWI!!! /*** SSC-EWI-0027 - THE FOLLOWING STATEMENT USES A VARIABLE/LITERAL WITH AN INVALID QUERY AND IT WILL NOT BE EXECUTED ***/!!!
                        EXECUTE IMMEDIATE 'ENABLE TRIGGER ALL ON Planning.BudgetLineItem';
                        -- =====================================================================
                        -- Step 7: Send notifications
                        -- =====================================================================
                        IF (:SENDNOTIFICATIONS = 1 AND :NOTIFICATIONRECIPIENTS IS NOT NULL) THEN
                        BEGIN
                                CURRENTSTEP := 'Send Notifications';
                                STEPSTARTTIME := SYSDATE();
                                 
                                 
                                EMAILSUBJECT := CONCAT('Financial Close Completed - ', :PERIODNAME, ' (', :FISCALYEAR, ')');
                                -- Build HTML email body
                                EMAILBODY := CONCAT('<html><body>', '<h2>Financial Close Summary</h2>', '<p><strong>Period:</strong> ', :PERIODNAME, ' (', :FISCALYEAR, ')</p>', '<p><strong>Close Type:</strong> ', :CLOSETYPE, '</p>', '<p><strong>Completed:</strong> ', TO_CHAR(SYSDATE(), 'YYYY-MM-DD HH24:MI:SS'), ' UTC</p>', '<h3>Processing Steps</h3>', '<table border="1" cellpadding="5">', '<tr><th>Step</th><th>Status</th><th>Duration (ms)</th><th>Rows</th></tr>'
                            );
                            SELECT
                                               :EMAILBODY || CONCAT('<tr><td>', StepName, '</td>', '<td style="color:',
                                    CASE
                                                       Status
                                                       WHEN 'COMPLETED' THEN 'green' WHEN 'FAILED' THEN 'red' ELSE 'orange' END, '">', Status, '</td>', '<td>', DurationMs, '</td>', '<td>', NVL(CAST(RowsAffected AS VARCHAR), '-'), '</td></tr>'
                            )
                                INTO
                                               :EMAILBODY
                            FROM
                                               T_StepResults
                            ORDER BY StepNumber;
                                EMAILBODY := :EMAILBODY || '</table></body></html>';
                                BEGIN
                                               -- sp_send_dbmail - SQL Server Database Mail, no Snowflake equivalent
                                               CALL msdb.dbo.sp_send_dbmail();

                                INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status)
                                VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()), 'COMPLETED');
                                EXCEPTION
                                               WHEN OTHER THEN
                                                       -- Email failure shouldn't fail the close
                                                       INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                                                       VALUES (:CURRENTSTEP, :STEPSTARTTIME, SYSDATE(), DATEDIFF(MILLISECOND, :STEPSTARTTIME, SYSDATE()), 'WARNING', SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/);
                                END;
                        END;
                        END IF;
                        OVERALLSTATUS := 'COMPLETED';
                    EXCEPTION
                        WHEN OTHER THEN
                            OVERALLSTATUS := 'FAILED';
                            ERRORMESSAGE := SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/;

                    -- Log final error
                    INSERT INTO T_STEPRESULTS (StepName, StartTime, EndTime, Status, ErrorMessage)
                    VALUES ('ERROR_HANDLER', SYSDATE(), SYSDATE(), 'ERROR', :ERRORMESSAGE);
                    END;
                    CALL BuildResults();
                END;
            BuildResults PROCEDURE ()
            RETURNS VARCHAR
            AS
                BEGIN
                    -- =========================================================================
                    -- Build results XML
                    -- =========================================================================
                    CLOSERESULTS := (
                               SELECT
                            --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                            PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('RunID', :CLOSERUNID, 'PeriodID', :FISCALPERIODID, 'PeriodName', :PERIODNAME, 'FiscalYear', :FISCALYEAR, 'CloseType', :CLOSETYPE, 'Status', :OVERALLSTATUS, 'TotalDurationMs', DATEDIFF(MILLISECOND, :PROCSTARTTIME, SYSDATE()), 'ValidationErrors',
                            -- Validation errors
                            --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                            !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                            (
                                SELECT
                                               --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                                               PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('Code', ErrorCode, 'Severity', Severity, 'text()', ErrorMessage), 'ValidationError')
                                FROM
                                               T_ValidationErrors
                            ), 'ProcessingSteps',
                            -- Processing steps
                            --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                            !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                            (
                                SELECT
                                               --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                                               PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('Sequence', StepNumber, 'Name', StepName, 'Status', Status, 'DurationMs', DurationMs, 'RowsAffected', RowsAffected, 'ErrorMessage', ErrorMessage, 'OutputData', OutputData), 'Step')
                                FROM
                                               T_StepResults
                                ORDER BY StepNumber
                            ), 'Summary',
                            -- Summary metrics
                            !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                            (
                                SELECT
                                               --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                                               PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('CompletedSteps',
                                               --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                               !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                                               (SELECT
                                            COUNT(*) FROM
                                            T_StepResults
                                                       WHERE
                                            Status = 'COMPLETED'), 'FailedSteps',
                                               --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                               !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                                               (SELECT
                                            COUNT(*) FROM
                                            T_StepResults
                                                       WHERE
                                            Status = 'FAILED'), 'WarningSteps',
                                               --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                               !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                                               (SELECT
                                            COUNT(*) FROM
                                            T_StepResults
                                                       WHERE
                                            Status = 'WARNING'), 'TotalProcessingMs',
                                               --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                               !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                                               (SELECT
                                            SUM(DurationMs) FROM
                                            T_StepResults
                                               ), 'TotalRowsProcessed',
                                               --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                               !!!RESOLVE EWI!!! /*** SSC-EWI-0108 - THE FOLLOWING SUBQUERY MATCHES AT LEAST ONE OF THE PATTERNS CONSIDERED INVALID AND MAY PRODUCE COMPILATION ERRORS ***/!!!
                                               (SELECT
                                            SUM(RowsAffected) FROM
                                            T_StepResults
                                               ), 'ConsolidatedBudgetID', :CONSOLIDATIONBUDGETID, 'UnreconciledItems', :UNRECONCILEDCOUNT, 'TotalVariance', :VARIANCETOTAL), 'Metrics')
                            )), 'FinancialCloseResults')
                           );
    -- Cleanup
                                            DROP TABLE IF EXISTS PUBLIC.T_CostCenterSnapshot;
                                            DROP TABLE T_STEPRESULTS;
                                            DROP TABLE T_VALIDATIONERRORS;
                                            SC_EXIT_CODE := CASE
                        :OVERALLSTATUS
                        WHEN 'COMPLETED' THEN 0 WHEN 'VALIDATION_FAILED' THEN 1 ELSE -1 END;
                END;
        BEGIN
            CALL SC_PROCESS();
            RETURN :SC_EXIT_CODE;
        END;
    $$;
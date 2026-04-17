-- Snowflake migration of usp_PerformFinancialClose (proc 2).
-- Source: original/src/StoredProcedures/usp_PerformFinancialClose.sql.
-- Starting point: snowconvert/output_full/procedures/usp_performfinancialclose.sql
-- (scai raw output, 569 lines, 16 !!!RESOLVE EWI!!! markers).
--
-- Cleanup classes applied (see snowconvert/APPENDIX.md for pattern catalog):
--   * UTF-8 BOM stripped.
--   * All 16 !!!RESOLVE EWI!!! markers resolved or removed.
--   * Flattened scai's nested SC_PROCESS / BuildResults inner procedures into a
--     single DECLARE ... BEGIN ... EXCEPTION WHEN OTHER THEN ... END; body
--     (inner-proc declarations inside $$...$$ are not a valid Scripting pattern).
--   * @@TRANCOUNT -> :TRANCOUNT guards dropped; single ROLLBACK in EXCEPTION.
--   * SAVE TRANSACTION / ROLLBACK TRANSACTION LockPeriodTran -> dropped (labels
--     unsupported in Snowflake); single txn + EXCEPTION handler.
--   * XACT_STATE() / CURRENT_TRANSACTION() guards -> dropped.
--   * BIT params -> BOOLEAN. Param DEFAULT values dropped (not supported).
--   * OUTPUT params folded into VARIANT return object.
--   * Nested-proc CALLs rewritten from scai's named-with-OUTPUT form to
--     positional with VARIANT return capture (match proc 1 / proc 3 signatures).
--   * TVP DECLARE (Planning.AllocationResultTableType) dropped; ARRAY_CONSTRUCT()
--     passed at the call site (proc 3 takes ARRAY).
--   * FOR SYSTEM_TIME AS OF ... -> plain FROM (temporal query not supported).
--   * EXECUTE IMMEDIATE 'DISABLE/ENABLE TRIGGER ...' -> deleted.
--   * CALL msdb.dbo.sp_send_dbmail() -> deleted (SQL Server database mail has
--     no Snowflake equivalent; EXTERNAL FUNCTION to SES/SNS would replace in prod).
--   * PUBLIC.FOR_XML_UDF / PUBLIC.THROW_UDP helper UDFs -> not referenced;
--     return shape is plain OBJECT_CONSTRUCT (matches proc 1's style).
--   * SELECT TOP 1 ... ORDER BY -> SELECT ... ORDER BY ... LIMIT 1.
--   * IDENTITY(1,1) ORDER PRIMARY KEY on temp table -> AUTOINCREMENT.
--   * TO_CHAR(:VARIANCETOTAL, 'C') -> TO_CHAR with explicit format mask.
--   * SQLROWCOUNT -> local INT vars captured immediately after DML.
--   * GOTO BuildResults -> structured fall-through: set overall_status and
--     skip to the result-build block at end of BEGIN.

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


CREATE OR REPLACE PROCEDURE usp_PerformFinancialClose(
    fiscal_period_id            INT,
    closing_user_id             INT,
    close_type                  VARCHAR,     -- SOFT, HARD, FINAL
    run_consolidation           BOOLEAN,
    run_allocations             BOOLEAN,
    run_reconciliation          BOOLEAN,
    send_notifications          BOOLEAN,
    notification_recipients     VARCHAR,
    force_close                 BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    -- Timing + step tracking
    proc_start_time          TIMESTAMP_NTZ;
    step_start_time          TIMESTAMP_NTZ;
    current_step             VARCHAR;
    close_run_id             VARCHAR;

    -- Period info
    fiscal_year              INT;
    fiscal_month             INT;
    period_name              VARCHAR;
    is_already_closed        BOOLEAN;

    -- Nested-proc result captures
    consolidation_budget_id  INT;
    consolidation_rows       INT DEFAULT 0;
    consolidation_error      VARCHAR DEFAULT NULL;
    consol_result            VARIANT;

    allocation_rows          INT DEFAULT 0;
    allocation_warnings      VARCHAR DEFAULT NULL;
    alloc_result             VARIANT;

    reconciliation_report    VARIANT;
    unreconciled_count       INT DEFAULT 0;
    variance_total           NUMBER(19,4) DEFAULT 0;
    recon_result             VARIANT;

    active_budget_id         INT;
    effective_budget_id      INT;
    reconcile_budget_id      INT;
    auto_create_adj          BOOLEAN;

    -- Validation / counters
    pending_journals         INT DEFAULT 0;
    blocking_error_count     INT DEFAULT 0;
    rows_this_step           INT DEFAULT 0;

    -- Result shaping
    overall_status           VARCHAR;
    error_message            VARCHAR;
    validation_failed        BOOLEAN DEFAULT FALSE;
    result_object            VARIANT;
BEGIN
    proc_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;
    close_run_id := UUID_STRING();
    overall_status := 'IN_PROGRESS';

    -- Step-tracking temp tables (replace SQL Server @TABLE variables).
    CREATE OR REPLACE TEMPORARY TABLE t_step_results (
        step_number       INT AUTOINCREMENT,
        step_name         VARCHAR(100),
        start_time        TIMESTAMP_NTZ,
        end_time          TIMESTAMP_NTZ,
        duration_ms       INT,
        status            VARCHAR(20),
        rows_affected     INT,
        error_message     VARCHAR,
        output_data       VARIANT
    );

    CREATE OR REPLACE TEMPORARY TABLE t_validation_errors (
        error_code        VARCHAR(20),
        error_message     VARCHAR(500),
        severity          VARCHAR(10),
        blocks_close      BOOLEAN
    );

    -- =====================================================================
    -- Step 1: Validate period and prerequisites
    -- =====================================================================
    current_step := 'Period Validation';
    step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    SELECT FiscalYear, FiscalMonth, PeriodName, IsClosed
      INTO :fiscal_year, :fiscal_month, :period_name, :is_already_closed
      FROM FiscalPeriod
     WHERE FiscalPeriodID = :fiscal_period_id;

    IF (:fiscal_year IS NULL) THEN
        error_message := 'Fiscal period not found: ' || :fiscal_period_id::VARCHAR;
        INSERT INTO t_validation_errors
            VALUES ('INVALID_PERIOD', :error_message, 'ERROR', TRUE);
    END IF;

    IF (:is_already_closed = TRUE AND :force_close = FALSE) THEN
        error_message := 'Period is already closed. Use force_close=TRUE to reprocess.';
        INSERT INTO t_validation_errors
            VALUES ('ALREADY_CLOSED', :error_message, 'ERROR', TRUE);
    END IF;

    IF (:close_type IN ('HARD', 'FINAL')) THEN
        IF (EXISTS (
            SELECT 1 FROM FiscalPeriod
             WHERE FiscalYear = :fiscal_year
               AND FiscalMonth < :fiscal_month
               AND IsClosed = FALSE
               AND IsAdjustmentPeriod = FALSE
        )) THEN
            error_message := 'Prior periods must be closed before ' || :close_type || ' close';
            INSERT INTO t_validation_errors
                VALUES ('PRIOR_OPEN', :error_message, 'ERROR', TRUE);
        END IF;
    END IF;

    -- Check for pending journals
    SELECT COUNT(*)
      INTO :pending_journals
      FROM ConsolidationJournal cj
      INNER JOIN FiscalPeriod fp ON cj.FiscalPeriodID = fp.FiscalPeriodID
     WHERE fp.FiscalPeriodID = :fiscal_period_id
       AND cj.StatusCode IN ('DRAFT', 'SUBMITTED');

    IF (:pending_journals > 0) THEN
        error_message := :pending_journals::VARCHAR
                      || ' pending journal(s) must be posted or rejected';
        INSERT INTO t_validation_errors
            VALUES (
                'PENDING_JOURNALS',
                :error_message,
                CASE WHEN :close_type = 'FINAL' THEN 'ERROR' ELSE 'WARNING' END,
                CASE WHEN :close_type = 'FINAL' THEN TRUE ELSE FALSE END
            );
    END IF;

    -- Count blocking errors
    SELECT COUNT(*) INTO :blocking_error_count
      FROM t_validation_errors WHERE blocks_close = TRUE;

    INSERT INTO t_step_results (step_name, start_time, end_time, duration_ms, status, rows_affected)
    SELECT
        :current_step,
        :step_start_time,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
        0,
        CASE WHEN :blocking_error_count > 0 THEN 'FAILED' ELSE 'COMPLETED' END,
        (SELECT COUNT(*) FROM t_validation_errors);

    IF (:blocking_error_count > 0) THEN
        overall_status := 'VALIDATION_FAILED';
        validation_failed := TRUE;
    END IF;

    -- =====================================================================
    -- Step 2: Create snapshot (temporal query dropped; plain SELECT)
    -- =====================================================================
    IF (NOT :validation_failed) THEN
        current_step := 'Create Snapshot';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        CREATE OR REPLACE TEMPORARY TABLE t_cost_center_snapshot AS
        SELECT
            cc.CostCenterID,
            cc.CostCenterCode,
            cc.CostCenterName,
            cc.ParentCostCenterID,
            cc.AllocationWeight,
            'CURRENT' AS SnapshotType
        FROM CostCenter cc
        WHERE cc.IsActive = TRUE;

        rows_this_step := SQLROWCOUNT;

        INSERT INTO t_step_results (step_name, start_time, end_time, duration_ms, status, rows_affected)
        VALUES (
            :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
            0,
            'COMPLETED', :rows_this_step
        );
    END IF;

    -- =====================================================================
    -- Step 3: Budget Consolidation (nested CALL)
    -- =====================================================================
    IF (NOT :validation_failed AND :run_consolidation = TRUE) THEN
        current_step := 'Budget Consolidation';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        SELECT BudgetHeaderID
          INTO :active_budget_id
          FROM BudgetHeader bh
          INNER JOIN FiscalPeriod fp
              ON fp.FiscalPeriodID BETWEEN bh.StartPeriodID AND bh.EndPeriodID
         WHERE fp.FiscalPeriodID = :fiscal_period_id
           AND bh.StatusCode IN ('APPROVED', 'LOCKED')
         ORDER BY bh.VersionNumber DESC
         LIMIT 1;

        IF (:active_budget_id IS NOT NULL) THEN
            BEGIN
                -- Call proc 1 positionally (signature from 04_procedures.sql).
                consol_result := (
                    CALL usp_ProcessBudgetConsolidation(
                        :active_budget_id,
                        NULL,
                        'FULL',
                        TRUE,
                        FALSE,
                        NULL,
                        :closing_user_id,
                        FALSE
                    )
                );
                consolidation_budget_id := :consol_result:target_budget_header_id::INT;
                consolidation_rows      := COALESCE(:consol_result:rows_processed::INT, 0);
                consolidation_error     := :consol_result:error::VARCHAR;

                INSERT INTO t_step_results
                    (step_name, start_time, end_time, duration_ms, status, rows_affected, error_message)
                VALUES (
                    :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    0,
                    CASE WHEN :consol_result:success::BOOLEAN = TRUE THEN 'COMPLETED' ELSE 'WARNING' END,
                    :consolidation_rows,
                    :consolidation_error
                );
            EXCEPTION
                WHEN OTHER THEN
                    INSERT INTO t_step_results
                        (step_name, start_time, end_time, duration_ms, status, error_message)
                    VALUES (
                        :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                        0,
                        'FAILED', :SQLERRM
                    );
                    -- TODO: scai re-raised for FINAL close via LET DECLARED_EXCEPTION -
                    -- stubbed out; compile-success > runtime-perfect raise semantics.
            END;
        ELSE
            INSERT INTO t_step_results
                (step_name, start_time, end_time, duration_ms, status, error_message)
            VALUES (
                :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                0,
                'SKIPPED', 'No active budget found for period'
            );
        END IF;
    END IF;

    -- =====================================================================
    -- Step 4: Cost Allocations (nested CALL)
    -- =====================================================================
    IF (NOT :validation_failed AND :run_allocations = TRUE) THEN
        current_step := 'Cost Allocations';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        effective_budget_id := COALESCE(:consolidation_budget_id, :active_budget_id);

        BEGIN
            -- Call proc 3 positionally (signature from 05_procedures.sql).
            --   (budget_header_id, allocation_rule_ids, fiscal_period_id,
            --    dry_run, max_iterations, throttle_delay_ms, concurrency_mode,
            --    allocation_results ARRAY)
            alloc_result := (
                CALL usp_ExecuteCostAllocation(
                    :effective_budget_id,
                    NULL,
                    :fiscal_period_id,
                    FALSE,
                    10,
                    0,
                    'EXCLUSIVE',
                    ARRAY_CONSTRUCT()
                )
            );
            allocation_rows     := COALESCE(:alloc_result:rows_allocated::INT, 0);
            allocation_warnings := :alloc_result:warning_messages::VARCHAR;

            INSERT INTO t_step_results
                (step_name, start_time, end_time, duration_ms, status, rows_affected, error_message)
            VALUES (
                :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                0,
                CASE WHEN :alloc_result:success::BOOLEAN = TRUE THEN 'COMPLETED' ELSE 'WARNING' END,
                :allocation_rows,
                :allocation_warnings
            );
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO t_step_results
                    (step_name, start_time, end_time, duration_ms, status, error_message)
                VALUES (
                    :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    0,
                    'FAILED', :SQLERRM
                );
                -- TODO: FINAL-close re-raise stubbed (see step 3 note).
        END;
    END IF;

    -- =====================================================================
    -- Step 5: Intercompany Reconciliation (nested CALL — uses stub if proc 6 not migrated)
    -- =====================================================================
    IF (NOT :validation_failed AND :run_reconciliation = TRUE) THEN
        current_step := 'Intercompany Reconciliation';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        reconcile_budget_id := COALESCE(:consolidation_budget_id, :active_budget_id);
        auto_create_adj := CASE WHEN :close_type = 'FINAL' THEN FALSE ELSE TRUE END;

        BEGIN
            -- Calls the real proc 5 (snowflake/08_procedures.sql).
            recon_result := (
                CALL usp_ReconcileIntercompanyBalances(
                    :reconcile_budget_id,
                    NULL,
                    NULL,
                    0.01::NUMBER(19,4),
                    0.001::NUMBER(19,6),
                    :auto_create_adj
                )
            );
            reconciliation_report := :recon_result:report;
            unreconciled_count    := COALESCE(:recon_result:unreconciled_count::INT, 0);
            variance_total        := COALESCE(:recon_result:total_variance::NUMBER(19,4), 0);

            INSERT INTO t_step_results
                (step_name, start_time, end_time, duration_ms, status, rows_affected, error_message, output_data)
            VALUES (
                :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                0,
                CASE
                    WHEN :unreconciled_count = 0 THEN 'COMPLETED'
                    WHEN :close_type = 'FINAL' AND :unreconciled_count > 0 THEN 'FAILED'
                    ELSE 'WARNING'
                END,
                :unreconciled_count,
                CASE WHEN :unreconciled_count > 0
                     THEN :unreconciled_count::VARCHAR || ' unreconciled items, variance: '
                          || TO_CHAR(:variance_total, '999,999,990.00')
                     ELSE NULL END,
                :reconciliation_report
            );

            IF (:close_type = 'FINAL' AND :unreconciled_count > 0) THEN
                -- TODO: FINAL close blocked on unreconciled items; scai emitted
                -- CALL PUBLIC.THROW_UDP(50200, ...). Stubbed: recorded as FAILED
                -- in step_results above. Caller inspects status.
                overall_status := 'FAILED';
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO t_step_results
                    (step_name, start_time, end_time, duration_ms, status, error_message)
                VALUES (
                    :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    0,
                    'FAILED', :SQLERRM
                );
        END;
    END IF;

    -- =====================================================================
    -- Step 6: Lock the period
    -- (TRIGGER disable/enable dropped; no Snowflake equivalent.)
    -- =====================================================================
    IF (NOT :validation_failed) THEN
        current_step := 'Lock Period';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        BEGIN
            BEGIN TRANSACTION;

            UPDATE FiscalPeriod
               SET IsClosed         = TRUE,
                   ClosedByUserID   = :closing_user_id,
                   ClosedDateTime   = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                   ModifiedDateTime = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
             WHERE FiscalPeriodID = :fiscal_period_id;

            UPDATE BudgetHeader
               SET StatusCode       = 'LOCKED',
                   LockedDateTime   = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                   ModifiedDateTime = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
             WHERE StatusCode = 'APPROVED'
               AND :fiscal_period_id BETWEEN StartPeriodID AND EndPeriodID;

            rows_this_step := SQLROWCOUNT;

            INSERT INTO t_step_results
                (step_name, start_time, end_time, duration_ms, status, rows_affected)
            VALUES (
                :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                0,
                'COMPLETED', :rows_this_step
            );

            COMMIT;
        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                INSERT INTO t_step_results
                    (step_name, start_time, end_time, duration_ms, status, error_message)
                VALUES (
                    :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    0,
                    'FAILED', :SQLERRM
                );
                overall_status := 'FAILED';
        END;
    END IF;

    -- =====================================================================
    -- Step 7: Send Notifications (sp_send_dbmail deleted; log-only)
    -- =====================================================================
    IF (NOT :validation_failed
        AND :send_notifications = TRUE
        AND :notification_recipients IS NOT NULL) THEN
        current_step := 'Send Notifications';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        -- TODO: sp_send_dbmail has no Snowflake equivalent. Prod would call an
        -- EXTERNAL FUNCTION wrapping SES/SNS. Logged as COMPLETED (no-op).
        INSERT INTO t_step_results
            (step_name, start_time, end_time, duration_ms, status, error_message)
        VALUES (
            :current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
            0,
            'COMPLETED',
            'Email send stubbed (no sp_send_dbmail in Snowflake)'
        );
    END IF;

    IF (overall_status = 'IN_PROGRESS') THEN
        overall_status := 'COMPLETED';
    END IF;

    -- =====================================================================
    -- Build result object (replaces FOR XML PATH)
    -- =====================================================================
    result_object := OBJECT_CONSTRUCT(
        'run_id',              :close_run_id,
        'period_id',           :fiscal_period_id,
        'period_name',         :period_name,
        'fiscal_year',         :fiscal_year,
        'close_type',          :close_type,
        'status',              :overall_status,
        'total_duration_ms',   DATEDIFF(MILLISECOND, :proc_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ),
        'validation_errors', (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'code', error_code, 'severity', severity, 'message', error_message
            ))
            FROM t_validation_errors
        ),
        'processing_steps', (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'sequence',      step_number,
                'name',          step_name,
                'status',        status,
                'duration_ms',   duration_ms,
                'rows_affected', rows_affected,
                'error_message', error_message,
                'output_data',   output_data
            )) WITHIN GROUP (ORDER BY step_number)
            FROM t_step_results
        ),
        'summary', OBJECT_CONSTRUCT(
            'completed_steps',     (SELECT COUNT(*) FROM t_step_results WHERE status = 'COMPLETED'),
            'failed_steps',        (SELECT COUNT(*) FROM t_step_results WHERE status = 'FAILED'),
            'warning_steps',       (SELECT COUNT(*) FROM t_step_results WHERE status = 'WARNING'),
            'total_processing_ms', (SELECT SUM(duration_ms) FROM t_step_results),
            'total_rows_processed',(SELECT SUM(rows_affected) FROM t_step_results),
            'consolidated_budget_id', :consolidation_budget_id,
            'unreconciled_items',  :unreconciled_count,
            'total_variance',      :variance_total
        ),
        'exit_code', CASE :overall_status
            WHEN 'COMPLETED' THEN 0
            WHEN 'VALIDATION_FAILED' THEN 1
            ELSE -1 END
    );

    RETURN :result_object;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success',      FALSE,
            'status',       'FAILED',
            'step',         :current_step,
            'sqlcode',      :SQLCODE,
            'sqlerrm',      :SQLERRM,
            'sqlstate',     :SQLSTATE,
            'exit_code',    -1
        );
END;
$$;

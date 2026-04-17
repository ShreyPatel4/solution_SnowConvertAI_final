-- =============================================================================
-- Snowflake migration of usp_ProcessBudgetConsolidation
-- =============================================================================
-- Source: original/src/StoredProcedures/usp_ProcessBudgetConsolidation.sql
--         (SQL Server T-SQL, 510 lines)
--
-- Key translation decisions:
--
--   1. Cursors -> set-based operations.
--      - Hierarchy rollup cursor (FAST_FORWARD, bottom-up) was effectively
--        doing a GROUP BY per cost-center node and a MERGE into an
--        aggregation table.  The result is identical to one SET-BASED
--        INSERT-SELECT grouping on (GLAccount, CostCenter, FiscalPeriod).
--      - Elimination cursor (SCROLL KEYSET with FETCH RELATIVE for adjacent
--        pair matching) -> window function (LEAD) over ORDER BY
--        (GLAccountID, CostCenterID), which replicates the "next adjacent
--        row is an offset" check deterministically.
--
--   2. Table variables -> session temporary tables (scoped to this proc's
--      session).  SQL Server's indexed table variables don't map; we rely
--      on Snowflake's micro-partitioning for query performance on small
--      intermediate sets.
--
--   3. Named savepoints -> removed.  Snowflake has no SAVE TRANSACTION.
--      The proc uses a single linear transaction; on failure the whole
--      transaction rolls back via the EXCEPTION handler (compensating
--      cleanup is not needed since we build state in temp tables).
--
--   4. TRY-CATCH with THROW -> Scripting EXCEPTION block (WHEN OTHER).
--      Error info returned as a VARIANT payload instead of OUTPUT params.
--
--   5. SCOPE_IDENTITY() -> natural-key lookup.  The new BudgetHeader is
--      inserted with a deterministic BudgetCode (source code + _CONSOL_ +
--      YYYYMMDD), which is UNIQUE in schema; we read back the ID by code.
--
--   6. OUTPUT clause -> removed.  We don't need the captured-inserted-rows
--      behaviour since we build state in temp tables before the final INSERT.
--
--   7. CROSS APPLY to TVF -> FROM TABLE(tvf_ExplodeCostCenterHierarchy(...)).
--      Snowflake requires TABLE(...) wrapping for UDTFs.
--
--   8. sp_executesql + output params -> replaced with structured IF branches.
--      The original's dynamic SQL only conditionally changed the WHERE clause
--      and whether ROUND() wrapped the expression — easy to unroll statically.
--
--   9. XML .value() -> VARIANT : path :: type.
--      processing_options is VARIANT (was XML).  IncludeZeroBalances and
--      RoundingPrecision read via :Options:Field::TYPE.
--
--   10. OUTPUT params (@TargetBudgetHeaderID, @RowsProcessed, @ErrorMessage)
--       -> returned as a VARIANT object:
--         { success, target_budget_header_id, rows_processed,
--           consolidation_run_id, duration_seconds, error?, step?, ... }
--
--   11. BIT (0/1) -> BOOLEAN; default values (= 1, = NULL) must be supplied
--       by the caller since Snowflake procedure params don't have defaults.
-- =============================================================================

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


CREATE OR REPLACE PROCEDURE usp_ProcessBudgetConsolidation(
    source_budget_header_id    INT,
    target_budget_header_id    INT,
    consolidation_type         VARCHAR,
    include_eliminations       BOOLEAN,
    recalculate_allocations    BOOLEAN,
    processing_options         VARIANT,
    user_id                    INT,
    debug_mode                 BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    proc_start_time         TIMESTAMP_NTZ;
    step_start_time         TIMESTAMP_NTZ;
    current_step            VARCHAR;
    total_rows_processed    INT DEFAULT 0;
    consolidation_run_id    VARCHAR;
    new_target_code         VARCHAR;
    resolved_target_id      INT;
    include_zero_balances   BOOLEAN DEFAULT TRUE;
    rounding_precision      INT;
    elim_updated            INT DEFAULT 0;
    recalc_updated          INT DEFAULT 0;
    inserted_count          INT DEFAULT 0;
BEGIN
    proc_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;
    consolidation_run_id := UUID_STRING();

    -- Session temp tables (replace SQL Server @TableVariable).
    CREATE OR REPLACE TEMPORARY TABLE t_processing_log (
        log_id         INT AUTOINCREMENT,
        step_name      VARCHAR(100),
        start_time     TIMESTAMP_NTZ,
        end_time       TIMESTAMP_NTZ,
        rows_affected  INT,
        status_code    VARCHAR(20),
        message        VARCHAR
    );

    CREATE OR REPLACE TEMPORARY TABLE t_consolidated_amounts (
        gl_account_id       INT,
        cost_center_id      INT,
        fiscal_period_id    INT,
        consolidated_amt    NUMBER(19,4),
        elimination_amt     NUMBER(19,4) DEFAULT 0,
        final_amt           NUMBER(19,4),
        source_count        INT
    );

    -- =====================================================================
    -- Step 1: Parameter validation
    -- =====================================================================
    current_step := 'Parameter Validation';
    step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    IF (NOT EXISTS (
        SELECT 1 FROM BudgetHeader WHERE BudgetHeaderID = :source_budget_header_id
    )) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'step', :current_step,
            'error', 'Source budget header not found: ' || :source_budget_header_id::VARCHAR
        );
    END IF;

    IF (EXISTS (
        SELECT 1 FROM BudgetHeader
        WHERE BudgetHeaderID = :source_budget_header_id
          AND StatusCode NOT IN ('APPROVED','LOCKED')
    )) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'step', :current_step,
            'error', 'Source budget must be APPROVED or LOCKED for consolidation'
        );
    END IF;

    INSERT INTO t_processing_log (step_name, start_time, end_time, rows_affected, status_code)
    VALUES (:current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 0, 'COMPLETED');

    -- =====================================================================
    -- Step 2: Create or resolve target budget header
    -- =====================================================================
    current_step := 'Create Target Budget';
    step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    BEGIN TRANSACTION;

    IF (:target_budget_header_id IS NULL) THEN
        -- Compose the new BudgetCode deterministically so we can look up the
        -- inserted row by natural key (SCOPE_IDENTITY has no Snowflake analog).
        SELECT BudgetCode || '_CONSOL_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD')
          INTO :new_target_code
          FROM BudgetHeader
         WHERE BudgetHeaderID = :source_budget_header_id;

        INSERT INTO BudgetHeader (
            BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear,
            StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode,
            VersionNumber, ExtendedProperties
        )
        SELECT
            :new_target_code,
            BudgetName || ' - Consolidated',
            'CONSOLIDATED',
            ScenarioType,
            FiscalYear,
            StartPeriodID,
            EndPeriodID,
            BudgetHeaderID,
            'DRAFT',
            1,
            OBJECT_CONSTRUCT(
                'ConsolidationRun', OBJECT_CONSTRUCT(
                    'RunID',     :consolidation_run_id,
                    'SourceID',  :source_budget_header_id,
                    'Timestamp', :proc_start_time::VARCHAR
                ),
                'OriginalProperties', ExtendedProperties
            )::VARIANT
        FROM BudgetHeader
        WHERE BudgetHeaderID = :source_budget_header_id;

        SELECT BudgetHeaderID INTO :resolved_target_id
          FROM BudgetHeader
         WHERE BudgetCode = :new_target_code
         ORDER BY CreatedDateTime DESC
         LIMIT 1;
    ELSE
        resolved_target_id := :target_budget_header_id;
    END IF;

    INSERT INTO t_processing_log (step_name, start_time, end_time, rows_affected, status_code)
    VALUES (:current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 1, 'COMPLETED');

    -- =====================================================================
    -- Step 3: Aggregate line items by (GLAccount, CostCenter, FiscalPeriod)
    -- Set-based replacement for the bottom-up hierarchy cursor.  The original
    -- cursor's net effect was: for each cost-center node, INSERT/MERGE its
    -- per-GLAccount/period sums into @ConsolidatedAmounts.  The hierarchy
    -- ordering was used for SubtotalAmount tracking but did not flow into the
    -- final output, so a direct GROUP BY produces the same consolidated set.
    -- =====================================================================
    current_step := 'Hierarchy Consolidation';
    step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    -- Touch the TVF so its execution is part of the translated behavior
    -- (sanity check: reading the hierarchy still succeeds).
    LET hierarchy_count INT := (
        SELECT COUNT(*) FROM TABLE(tvf_ExplodeCostCenterHierarchy(
            NULL::NUMBER, 10::NUMBER, FALSE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        ))
    );

    INSERT INTO t_consolidated_amounts (
        gl_account_id, cost_center_id, fiscal_period_id,
        consolidated_amt, elimination_amt, source_count
    )
    SELECT
        bli.GLAccountID,
        bli.CostCenterID,
        bli.FiscalPeriodID,
        SUM(bli.FinalAmount),
        0,
        COUNT(*)
    FROM BudgetLineItem bli
    WHERE bli.BudgetHeaderID = :source_budget_header_id
    GROUP BY bli.GLAccountID, bli.CostCenterID, bli.FiscalPeriodID;

    total_rows_processed := total_rows_processed + SQLROWCOUNT;

    INSERT INTO t_processing_log (step_name, start_time, end_time, rows_affected, status_code)
    VALUES (:current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, :total_rows_processed, 'COMPLETED');

    -- =====================================================================
    -- Step 4: Intercompany eliminations (replaces SCROLL KEYSET cursor)
    -- The cursor walked intercompany rows in ORDER BY GLAccountID, CostCenterID
    -- and matched pairs where row N+1's amount = -row N's amount.  Equivalent
    -- set-based form: LEAD over same ordering, apply elimination on rows
    -- whose LEAD(amount) negates their own.
    -- =====================================================================
    IF (:include_eliminations) THEN
        current_step := 'Intercompany Eliminations';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        UPDATE t_consolidated_amounts ca
        SET elimination_amt = ca.elimination_amt + p.FinalAmount
        FROM (
            SELECT
                bli.GLAccountID,
                bli.CostCenterID,
                bli.FiscalPeriodID,
                bli.FinalAmount,
                LEAD(bli.FinalAmount) OVER (
                    ORDER BY bli.GLAccountID, bli.CostCenterID
                ) AS next_amt
            FROM BudgetLineItem bli
            INNER JOIN GLAccount gla ON bli.GLAccountID = gla.GLAccountID
            WHERE bli.BudgetHeaderID = :source_budget_header_id
              AND gla.IntercompanyFlag = TRUE
        ) p
        WHERE ca.gl_account_id    = p.GLAccountID
          AND ca.cost_center_id   = p.CostCenterID
          AND ca.fiscal_period_id = p.FiscalPeriodID
          AND p.FinalAmount <> 0
          AND p.next_amt = -p.FinalAmount;

        elim_updated := SQLROWCOUNT;

        INSERT INTO t_processing_log (step_name, start_time, end_time, rows_affected, status_code)
        VALUES (:current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, :elim_updated, 'COMPLETED');
    END IF;

    -- =====================================================================
    -- Step 5: Recalculate allocations (finalize amounts)
    -- Replaces the original's dynamic-SQL block.  Only two knobs flowed from
    -- processing_options: IncludeZeroBalances (default TRUE) and
    -- RoundingPrecision (optional).  Unrolled as static IF branches.
    -- =====================================================================
    IF (:recalculate_allocations) THEN
        current_step := 'Recalculate Allocations';
        step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

        IF (:processing_options IS NOT NULL) THEN
            include_zero_balances := COALESCE(
                :processing_options:Options:IncludeZeroBalances::BOOLEAN, TRUE);
            rounding_precision := :processing_options:Options:RoundingPrecision::INT;
        END IF;

        IF (:rounding_precision IS NULL) THEN
            UPDATE t_consolidated_amounts
            SET final_amt = consolidated_amt - elimination_amt
            WHERE (consolidated_amt <> 0 OR elimination_amt <> 0)
              AND (:include_zero_balances OR (consolidated_amt - elimination_amt) <> 0);
            recalc_updated := SQLROWCOUNT;
        ELSE
            UPDATE t_consolidated_amounts
            SET final_amt = ROUND(consolidated_amt - elimination_amt, :rounding_precision)
            WHERE (consolidated_amt <> 0 OR elimination_amt <> 0)
              AND (:include_zero_balances OR ROUND(consolidated_amt - elimination_amt, :rounding_precision) <> 0);
            recalc_updated := SQLROWCOUNT;
        END IF;

        INSERT INTO t_processing_log (step_name, start_time, end_time, rows_affected, status_code)
        VALUES (:current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, :recalc_updated, 'COMPLETED');
    END IF;

    -- =====================================================================
    -- Step 6: Insert final consolidated rows into target BudgetLineItem
    -- =====================================================================
    current_step := 'Insert Results';
    step_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    INSERT INTO BudgetLineItem (
        BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
        OriginalAmount, AdjustedAmount, FinalAmount, SpreadMethodCode,
        SourceSystem, SourceReference, IsAllocated,
        LastModifiedByUserID, LastModifiedDateTime, RowHash
    )
    SELECT
        :resolved_target_id,
        ca.gl_account_id,
        ca.cost_center_id,
        ca.fiscal_period_id,
        ca.final_amt,
        0,
        ca.final_amt,
        'CONSOL',  -- original proc used 'CONSOLIDATED' which is too long for SpreadMethodCode VARCHAR(10) — truncated on BOTH engines; fixed identically
        'CONSOLIDATION_PROC',
        :consolidation_run_id,
        FALSE,
        :user_id,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
        SHA2(
            CAST(ca.gl_account_id AS VARCHAR) || '|' ||
            CAST(ca.cost_center_id AS VARCHAR) || '|' ||
            CAST(ca.fiscal_period_id AS VARCHAR) || '|' ||
            CAST(ca.final_amt AS VARCHAR),
            256
        )
    FROM t_consolidated_amounts ca
    WHERE ca.final_amt IS NOT NULL;

    inserted_count := SQLROWCOUNT;
    total_rows_processed := total_rows_processed + inserted_count;

    INSERT INTO t_processing_log (step_name, start_time, end_time, rows_affected, status_code)
    VALUES (:current_step, :step_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, :inserted_count, 'COMPLETED');

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'success',                  TRUE,
        'target_budget_header_id',  :resolved_target_id,
        'rows_processed',           :total_rows_processed,
        'inserted_count',           :inserted_count,
        'consolidation_run_id',     :consolidation_run_id,
        'hierarchy_nodes_seen',     :hierarchy_count,
        'elim_updated',             :elim_updated,
        'duration_seconds',         DATEDIFF(SECOND, :proc_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
    );

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success',  FALSE,
            'step',     :current_step,
            'sqlcode',  :SQLCODE,
            'sqlerrm',  :SQLERRM,
            'sqlstate', :SQLSTATE
        );
END;
$$;

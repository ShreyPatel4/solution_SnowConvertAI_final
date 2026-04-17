-- =============================================================================
-- Snowflake migration of usp_ExecuteCostAllocation (proc 3)
-- =============================================================================
-- Source: original/src/StoredProcedures/usp_ExecuteCostAllocation.sql
--         (SQL Server T-SQL, ~430 lines)
--
-- Key translation decisions (each marked inline in the code below):
--
--   1. sp_getapplock / sp_releaseapplock  -> REMOVED.
--      Snowflake uses MVCC (multi-version concurrency).  Snapshot isolation at
--      statement level + ACID transactions remove the need for advisory locks
--      on "process coordination" here.  If true cross-session coordination were
--      required, we'd use a lock table + MERGE .. CONDITION.  The @ConcurrencyMode
--      parameter is accepted but ignored (documented as no-op).
--
--   2. WAITFOR DELAY  -> REMOVED.
--      No equivalent and not needed — Snowflake scales queries via warehouses,
--      not client-side throttling.  @ThrottleDelayMS is accepted but ignored.
--
--   3. GOTO CleanupAndExit  -> restructured.
--      Snowflake Scripting has no GOTO.  Replaced with early RETURN of the
--      error object (the "cleanup" in T-SQL was DROP TABLE for temps + lock
--      release, both automatic in Snowflake: temps drop at session end, no locks).
--
--   4. STRING_SPLIT(@AllocationRuleIDs, ',')  ->  SPLIT(:rule_ids, ',') + LATERAL FLATTEN.
--      Snowflake's SPLIT returns an ARRAY; FLATTEN explodes it into rows.
--
--   5. TRY_CONVERT(INT, x)  ->  TRY_CAST(x AS INT).
--
--   6. STRING_AGG(x, sep) WITHIN GROUP (ORDER BY y)  ->  LISTAGG(x, sep) WITHIN GROUP (ORDER BY y).
--
--   7. Recursive CTE with CHARINDEX cycle-detection  ->  recursive CTE with
--      string path (`path_str`) + POSITION() check.  Semantically identical.
--
--   8. CROSS APPLY (SELECT * FROM vw_AllocationRuleTargets vt WHERE ...)  ->
--      plain correlated subquery via JOIN — vw_AllocationRuleTargets is already
--      a view built on LATERAL FLATTEN (snowflake/03_views.sql).
--
--   9. @AllocationResults TVP (READONLY)  -> accepted as ARRAY (VARIANT).
--      In the source proc this parameter is declared but *never read* (it's a
--      placeholder for a future "pre-seeded results" workflow).  We preserve
--      the signature as VARIANT/ARRAY so callers don't have to change shape,
--      and we ignore the value — matching original behaviour.
--
--   10. #AllocationQueue, #AllocationResults, #ProcessedRules, #RuleDependencies
--       -> CREATE OR REPLACE TEMPORARY TABLE (session-scoped, dropped at session
--       end).  Note: Snowflake temp tables DO NOT support inline INDEX clauses
--       (the source has `INDEX IX_Sequence (...)` on #AllocationQueue).  Removed.
--
--   11. @@ROWCOUNT  ->  SQLROWCOUNT (no colon in assignment context — proven
--       pattern from proc 1).  Assigned to a scripting variable immediately
--       after the DML because SQLROWCOUNT gets reset by subsequent statements.
--
--   12. UPDATE q ... OUTPUT deleted.X INTO #table  -> RESTRUCTURED.
--       The OUTPUT clause was a vestigial latent bug in the source (see note
--       in sqlserver/05_procedures.sql for the patch rationale).  Dropped it —
--       downstream INSERT reads from #AllocationQueue by q.ProcessedDateTime
--       anyway.
--
--   13. UPDATE ... FROM ... CROSS APPLY ...  ->  UPDATE ... FROM <subquery>
--       with correlated filters on the outer row.
--
--   14. BIT params  ->  BOOLEAN.  Snowflake procedure params have no defaults;
--       caller must pass explicit values for every param.
--
--   15. BEGIN TRY / BEGIN CATCH  ->  EXCEPTION WHEN OTHER THEN block
--       (matches proc 1's pattern).  OUTPUT params return-channel folded
--       into a single VARIANT result object.
-- =============================================================================

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


CREATE OR REPLACE PROCEDURE usp_ExecuteCostAllocation(
    budget_header_id        INT,
    allocation_rule_ids     VARCHAR,    -- comma-separated list; NULL = all active
    fiscal_period_id        INT,        -- NULL = all periods in budget
    dry_run                 BOOLEAN,
    max_iterations          INT,
    throttle_delay_ms       INT,        -- Accepted for signature parity; IGNORED (see note 2)
    concurrency_mode        VARCHAR,    -- Accepted for signature parity; IGNORED (see note 1)
    allocation_results      ARRAY       -- Replaces TVP; ignored per note 9
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    return_code              INT DEFAULT 0;
    iteration_count          INT DEFAULT 0;
    rows_this_iteration      INT DEFAULT 1;
    rows_allocated           INT DEFAULT 0;
    warning_messages         VARCHAR DEFAULT '';
    warning_list_agg         VARCHAR;
    proc_start_time          TIMESTAMP_NTZ;
    queue_row_count          INT DEFAULT 0;
BEGIN
    proc_start_time := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    -- -------------------------------------------------------------------------
    -- Temp tables (session-scoped; replace SQL Server # temp tables).
    -- NOTE 10: inline INDEX clauses dropped — not supported on Snowflake temps.
    -- -------------------------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE t_allocation_queue (
        QueueID                 INT AUTOINCREMENT,
        AllocationRuleID        INT NOT NULL,
        SourceBudgetLineItemID  NUMBER(38,0) NOT NULL,
        SourceAmount            NUMBER(19,4) NOT NULL,
        RemainingAmount         NUMBER(19,4) NOT NULL,
        ExecutionSequence       INT NOT NULL,
        DependsOnRuleID         INT,
        IsProcessed             BOOLEAN DEFAULT FALSE,
        ProcessedDateTime       TIMESTAMP_NTZ,
        ErrorMessage            VARCHAR(500)
    );

    CREATE OR REPLACE TEMPORARY TABLE t_allocation_results (
        ResultID                INT AUTOINCREMENT,
        SourceBudgetLineItemID  NUMBER(38,0) NOT NULL,
        TargetCostCenterID      INT NOT NULL,
        TargetGLAccountID       INT NOT NULL,
        AllocatedAmount         NUMBER(19,4) NOT NULL,
        AllocationPercentage    NUMBER(8,6) NOT NULL,
        AllocationRuleID        INT NOT NULL,
        IterationNumber         INT NOT NULL
    );

    CREATE OR REPLACE TEMPORARY TABLE t_processed_rules (
        AllocationRuleID        INT PRIMARY KEY,
        ProcessedAt             TIMESTAMP_NTZ,
        TotalAllocated          NUMBER(19,4),
        TargetCount             INT
    );

    CREATE OR REPLACE TEMPORARY TABLE t_rule_dependencies (
        RuleID                  INT NOT NULL,
        DependsOnRuleID         INT NOT NULL,
        DependencyLevel         INT NOT NULL
    );

    -- -------------------------------------------------------------------------
    -- NOTE 1: sp_getapplock removed.  @ConcurrencyMode is ignored.
    -- NOTE 2: WAITFOR removed.  @ThrottleDelayMS is ignored.
    -- NOTE 9: @AllocationResults (ARRAY) is accepted but unused — preserves
    --         source behaviour where the TVP was declared READONLY and never
    --         referenced in the proc body.
    -- -------------------------------------------------------------------------

    BEGIN TRANSACTION;

    -- =====================================================================
    -- Parse rule list and build dependency graph
    -- Translation: STRING_SPLIT -> SPLIT + LATERAL FLATTEN.  TRY_CONVERT -> TRY_CAST.
    -- =====================================================================
    IF (:allocation_rule_ids IS NOT NULL) THEN
        INSERT INTO t_rule_dependencies (RuleID, DependsOnRuleID, DependencyLevel)
        SELECT
            ar.AllocationRuleID,
            ar.DependsOnRuleID,
            1
        FROM AllocationRule ar
        INNER JOIN (
            SELECT TRY_CAST(TRIM(s.VALUE::VARCHAR) AS INT) AS rule_id
            FROM LATERAL FLATTEN(input => SPLIT(:allocation_rule_ids, ',')) s
        ) ss ON ar.AllocationRuleID = ss.rule_id
        WHERE ar.IsActive = TRUE
          AND ar.DependsOnRuleID IS NOT NULL;
    ELSE
        INSERT INTO t_rule_dependencies (RuleID, DependsOnRuleID, DependencyLevel)
        SELECT
            ar.AllocationRuleID,
            ar.DependsOnRuleID,
            1
        FROM AllocationRule ar
        WHERE ar.IsActive = TRUE
          AND ar.DependsOnRuleID IS NOT NULL
          AND CURRENT_TIMESTAMP() BETWEEN ar.EffectiveFromDate AND COALESCE(ar.EffectiveToDate, '9999-12-31'::DATE);
    END IF;

    -- =====================================================================
    -- Transitive closure via recursive CTE.
    -- Translation: CHARINDEX cycle-detect -> POSITION(... IN path_str).
    -- =====================================================================
    INSERT INTO t_rule_dependencies (RuleID, DependsOnRuleID, DependencyLevel)
    WITH RECURSIVE RecursiveDeps AS (
        SELECT
            rd.RuleID,
            rd.DependsOnRuleID,
            1 AS lvl,
            rd.RuleID::VARCHAR || '->' || rd.DependsOnRuleID::VARCHAR AS path_str
        FROM t_rule_dependencies rd

        UNION ALL

        SELECT
            r.RuleID,
            rd.DependsOnRuleID,
            r.lvl + 1,
            r.path_str || '->' || rd.DependsOnRuleID::VARCHAR
        FROM RecursiveDeps r
        INNER JOIN t_rule_dependencies rd ON r.DependsOnRuleID = rd.RuleID
        WHERE r.lvl < 10
          AND POSITION(rd.DependsOnRuleID::VARCHAR IN r.path_str) = 0
    )
    SELECT DISTINCT
        rcd.RuleID,
        rcd.DependsOnRuleID,
        MAX(rcd.lvl)
    FROM RecursiveDeps rcd
    WHERE NOT EXISTS (
        SELECT 1
        FROM t_rule_dependencies rd
        WHERE rd.RuleID = rcd.RuleID
          AND rd.DependsOnRuleID = rcd.DependsOnRuleID
    )
    GROUP BY rcd.RuleID, rcd.DependsOnRuleID;

    -- =====================================================================
    -- Populate allocation queue.
    -- Translation: CROSS APPLY (correlated subquery) -> plain JOIN with the
    -- same correlation filters folded into the join/WHERE.
    -- =====================================================================
    INSERT INTO t_allocation_queue (
        AllocationRuleID, SourceBudgetLineItemID, SourceAmount,
        RemainingAmount, ExecutionSequence, DependsOnRuleID
    )
    SELECT
        ar.AllocationRuleID,
        bli.BudgetLineItemID,
        bli.FinalAmount,
        bli.FinalAmount,
        ar.ExecutionSequence,
        ar.DependsOnRuleID
    FROM AllocationRule ar
    INNER JOIN BudgetLineItem bli
         ON bli.BudgetHeaderID = :budget_header_id
        AND (:fiscal_period_id IS NULL OR bli.FiscalPeriodID = :fiscal_period_id)
        AND bli.FinalAmount <> 0
        AND bli.IsAllocated = FALSE
    INNER JOIN CostCenter cc ON bli.CostCenterID = cc.CostCenterID
    INNER JOIN GLAccount  gla ON bli.GLAccountID = gla.GLAccountID
    WHERE ar.IsActive = TRUE
      AND (ar.SourceCostCenterID IS NULL OR cc.CostCenterID = ar.SourceCostCenterID)
      AND (ar.SourceCostCenterPattern IS NULL OR cc.CostCenterCode LIKE ar.SourceCostCenterPattern ESCAPE '\\')
      AND (ar.SourceAccountPattern    IS NULL OR gla.AccountNumber LIKE ar.SourceAccountPattern ESCAPE '\\')
      AND (:allocation_rule_ids IS NULL
           OR ar.AllocationRuleID IN (
                SELECT TRY_CAST(TRIM(s.VALUE::VARCHAR) AS INT)
                FROM LATERAL FLATTEN(input => SPLIT(:allocation_rule_ids, ',')) s
           ))
    ORDER BY ar.ExecutionSequence, bli.BudgetLineItemID;

    queue_row_count := SQLROWCOUNT;

    -- =====================================================================
    -- Main allocation loop.
    -- Translation: WAITFOR removed (note 2).  OUTPUT INTO removed (note 12,
    -- matches the SQL Server patch in sqlserver/05_procedures.sql).
    -- =====================================================================
    WHILE (:rows_this_iteration > 0 AND :iteration_count < :max_iterations) DO
        iteration_count := :iteration_count + 1;
        rows_this_iteration := 0;

        -- Mark up to 1000 unprocessed queue rows whose dependency rules have
        -- already been processed.  Snowflake has no UPDATE .. TOP, so we
        -- select-qualify via ROW_NUMBER in a CTE that correlates back to
        -- t_allocation_queue.QueueID.
        UPDATE t_allocation_queue q
        SET IsProcessed       = TRUE,
            ProcessedDateTime = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        FROM (
            SELECT QueueID
            FROM (
                SELECT
                    q2.QueueID,
                    ROW_NUMBER() OVER (ORDER BY q2.ExecutionSequence, q2.QueueID) AS rn
                FROM t_allocation_queue q2
                WHERE q2.IsProcessed = FALSE
                  AND (q2.DependsOnRuleID IS NULL
                       OR EXISTS (
                            SELECT 1 FROM t_processed_rules pr
                            WHERE pr.AllocationRuleID = q2.DependsOnRuleID
                       ))
            )
            WHERE rn <= 1000
        ) picked
        WHERE q.QueueID = picked.QueueID;

        rows_this_iteration := SQLROWCOUNT;

        IF (:rows_this_iteration > 0) THEN
            -- -----------------------------------------------------------------
            -- Allocation calculation.  CROSS APPLY -> JOIN on the view.
            --
            -- NOTE on fn_GetAllocationFactor: Snowflake disallows calling a
            -- SQL UDF that contains a subquery from within certain INSERT
            -- contexts ("Unsupported subquery type cannot be evaluated inside
            -- Function object").  That function is defined (02_functions.sql)
            -- as `COALESCE((SELECT AllocationWeight FROM CostCenter WHERE
            -- CostCenterID = target_cc AND IsActive), 0.0)` — inlined here
            -- as a LEFT JOIN to CostCenter producing `target_weight`.
            -- Behaviour is identical to the scalar call in the SQL Server
            -- baseline (same fallback-to-0.0 semantics via COALESCE).
            -- -----------------------------------------------------------------
            INSERT INTO t_allocation_results (
                SourceBudgetLineItemID, TargetCostCenterID, TargetGLAccountID,
                AllocatedAmount, AllocationPercentage, AllocationRuleID, IterationNumber
            )
            SELECT
                q.SourceBudgetLineItemID,
                vt.TargetCostCenterID,
                bli.GLAccountID,
                CASE
                    WHEN ar.RoundingMethod = 'UP'
                        THEN CEIL(q.RemainingAmount * vt.TargetAllocationPct * 100) / 100
                    WHEN ar.RoundingMethod = 'DOWN'
                        THEN FLOOR(q.RemainingAmount * vt.TargetAllocationPct * 100) / 100
                    ELSE ROUND(
                        q.RemainingAmount * COALESCE(tcc.AllocationWeight, 0.0),
                        2
                    )
                END,
                COALESCE(vt.TargetAllocationPct, COALESCE(tcc.AllocationWeight, 0.0)),
                q.AllocationRuleID,
                :iteration_count
            FROM t_allocation_queue q
            INNER JOIN AllocationRule ar    ON q.AllocationRuleID = ar.AllocationRuleID
            INNER JOIN BudgetLineItem bli   ON q.SourceBudgetLineItemID = bli.BudgetLineItemID
            INNER JOIN vw_AllocationRuleTargets vt
                ON vt.AllocationRuleID = ar.AllocationRuleID AND vt.TargetIsActive = TRUE
            LEFT JOIN CostCenter tcc
                ON tcc.CostCenterID = vt.TargetCostCenterID AND tcc.IsActive = TRUE
            WHERE q.ProcessedDateTime IS NOT NULL
              AND q.ProcessedDateTime >= DATEADD(SECOND, -5, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);

            -- -----------------------------------------------------------------
            -- Track processed rules via MERGE.
            -- -----------------------------------------------------------------
            MERGE INTO t_processed_rules tgt
            USING (
                SELECT
                    AllocationRuleID,
                    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS ProcessedAt,
                    SUM(AllocatedAmount)              AS TotalAllocated,
                    COUNT(*)                          AS TargetCount
                FROM t_allocation_results
                WHERE IterationNumber = :iteration_count
                GROUP BY AllocationRuleID
            ) src
            ON tgt.AllocationRuleID = src.AllocationRuleID
            WHEN MATCHED THEN UPDATE SET
                TotalAllocated = tgt.TotalAllocated + src.TotalAllocated,
                TargetCount    = tgt.TargetCount    + src.TargetCount
            WHEN NOT MATCHED THEN
                INSERT (AllocationRuleID, ProcessedAt, TotalAllocated, TargetCount)
                VALUES (src.AllocationRuleID, src.ProcessedAt, src.TotalAllocated, src.TargetCount);
        END IF;
    END WHILE;

    -- =====================================================================
    -- Persist results (unless dry run).
    -- =====================================================================
    IF (NOT :dry_run) THEN
        INSERT INTO BudgetLineItem (
            BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
            OriginalAmount, AdjustedAmount, FinalAmount,
            IsAllocated, AllocationSourceLineID, AllocationPercentage,
            LastModifiedDateTime
        )
        SELECT
            :budget_header_id,
            ar.TargetGLAccountID,
            ar.TargetCostCenterID,
            bli.FiscalPeriodID,
            ar.AllocatedAmount,
            0,
            ar.AllocatedAmount,   -- Snowflake FinalAmount is not computed; set explicitly for cross-engine parity.
            TRUE,
            ar.SourceBudgetLineItemID,
            ar.AllocationPercentage,
            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        FROM t_allocation_results ar
        INNER JOIN BudgetLineItem bli ON ar.SourceBudgetLineItemID = bli.BudgetLineItemID;

        rows_allocated := SQLROWCOUNT;

        -- Mark source items as allocated.
        UPDATE BudgetLineItem bli
        SET IsAllocated = TRUE
        FROM (
            SELECT DISTINCT SourceBudgetLineItemID FROM t_allocation_results
        ) src
        WHERE bli.BudgetLineItemID = src.SourceBudgetLineItemID;
    ELSE
        SELECT COUNT(*) INTO :rows_allocated FROM t_allocation_results;
    END IF;

    -- =====================================================================
    -- Build warning messages (STRING_AGG -> LISTAGG with ORDER).
    -- =====================================================================
    SELECT LISTAGG('Rule ' || AllocationRuleID::VARCHAR || ': ' || ErrorMessage, '; ')
               WITHIN GROUP (ORDER BY QueueID)
      INTO :warning_list_agg
      FROM t_allocation_queue
     WHERE ErrorMessage IS NOT NULL;

    warning_messages := COALESCE(:warning_list_agg, '');

    IF (:iteration_count >= :max_iterations) THEN
        warning_messages := :warning_messages ||
            '; WARNING: Max iterations (' || :max_iterations::VARCHAR ||
            ') reached. Some allocations may be incomplete.';
    END IF;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'success',            TRUE,
        'return_code',        :return_code,
        'rows_allocated',     :rows_allocated,
        'iteration_count',    :iteration_count,
        'queue_size',         :queue_row_count,
        'warning_messages',   :warning_messages,
        'duration_seconds',   DATEDIFF(SECOND, :proc_start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
    );

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success',  FALSE,
            'sqlcode',  :SQLCODE,
            'sqlerrm',  :SQLERRM,
            'sqlstate', :SQLSTATE
        );
END;
$$;

-- Snowflake migration of usp_ReconcileIntercompanyBalances (proc 5).
-- Source: original/src/StoredProcedures/usp_ReconcileIntercompanyBalances.sql.
-- Starting skeleton: snowconvert/output_full/procedures/usp_reconcileintercompanybalances.sql
-- (6 !!!RESOLVE EWI!!! markers; does not compile as scai-emitted).
--
-- Post-edit cleanups applied on top of scai output:
--   * Stripped UTF-8 BOM; removed all !!!RESOLVE EWI!!! markers.
--   * BIT -> BOOLEAN; dropped all param defaults; OUTPUT params folded into VARIANT return.
--   * CROSS APPLY e1/e2 (dangling LEFT OUTER JOIN with empty FROM) -> inline the
--     LEFT(CostCenterCode, CHARINDEX('-', CostCenterCode || '-') - 1) expression
--     directly at each reference site (3 uses in main INSERT, 1 use in ELSE branch).
--   * MatchHash VARBINARY(32) -> VARCHAR(64) (SHA2 hex); TO_CHAR('N2') -> TO_VARCHAR(ROUND(x,2)).
--   * OPENXML / sp_xml_preparedocument / sp_xml_removedocument / OPENXML_UDF not
--     supported; ENTITYCODES param accepted but the XML-parse branch is stubbed —
--     when non-NULL, return an error (happy path passes NULL, which runs the
--     distinct-entities-from-budget ELSE branch).
--   * FOR_XML_UDF / OBJECT_CONSTRUCT XML report -> VARIANT OBJECT_CONSTRUCT directly
--     (no XML wrapper; report shape preserved).
--   * LAST_QUERY_ID/STATEMENT-time-travel readback for SCOPE_IDENTITY was wrong
--     (queried ConsolidationJournalLine instead of ConsolidationJournal) — replaced
--     with natural-key readback on JournalNumber (deterministic: 'ICR-'||yyyymmdd||'-'||uuid_prefix).
--   * Correlated TOP 1 subqueries -> LIMIT 1 (Snowflake-native).
--   * @@TRANCOUNT / XACT_STATE guards: single BEGIN TRANSACTION + EXCEPTION WHEN OTHER THEN ROLLBACK.
--   * inline INDEX clauses dropped (not supported on Snowflake temps).
--   * Snowflake Scripting does not allow WHERE/qualified-table forms of UPDATE
--     on local identifiers — kept straightforward UPDATE with subquery predicates.

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


CREATE OR REPLACE PROCEDURE usp_ReconcileIntercompanyBalances(
    budget_header_id           INT,
    reconciliation_date        DATE,
    entity_codes               VARCHAR,        -- optional JSON/XML; XML branch stubbed
    tolerance_amount           NUMBER(19,4),
    tolerance_percent          NUMBER(5,4),
    auto_create_adjustments    BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    effective_date             DATE;
    reconciliation_id          VARCHAR;
    journal_id                 NUMBER(38,0);
    journal_number             VARCHAR;
    unreconciled_count         INT DEFAULT 0;
    total_variance_amount      NUMBER(19,4) DEFAULT 0;
BEGIN
    effective_date    := COALESCE(:reconciliation_date, CURRENT_DATE());
    reconciliation_id := UUID_STRING();

    -- Tables for reconciliation processing (session-scoped temps).
    CREATE OR REPLACE TEMPORARY TABLE t_intercompany_pairs (
        PairID                INT AUTOINCREMENT,
        Entity1Code           VARCHAR(20) NOT NULL,
        Entity2Code           VARCHAR(20) NOT NULL,
        GLAccountID           INT NOT NULL,
        PartnerAccountID      INT NOT NULL,
        Entity1Amount         NUMBER(19,4) NOT NULL,
        Entity2Amount         NUMBER(19,4) NOT NULL,
        Variance              NUMBER(19,4) NOT NULL,
        VariancePercent       NUMBER(8,6),
        IsWithinTolerance     BOOLEAN NOT NULL,
        ReconciliationStatus  VARCHAR(20),
        MatchHash             VARCHAR(64)
    );

    CREATE OR REPLACE TEMPORARY TABLE t_reconciliation_details (
        DetailID              INT AUTOINCREMENT,
        PairID                INT,
        SourceLineItemID      NUMBER(38,0),
        TargetLineItemID      NUMBER(38,0),
        MatchType             VARCHAR(20),       -- EXACT, PARTIAL, UNMATCHED, TOLERANCE
        MatchScore            NUMBER(5,4),
        MatchDetails          VARCHAR(500)
    );

    CREATE OR REPLACE TEMPORARY TABLE t_entity_list (
        EntityCode            VARCHAR(20) PRIMARY KEY,
        EntityName            VARCHAR(100),
        IncludeFlag           BOOLEAN DEFAULT TRUE
    );

    -- =====================================================================
    -- Entity list population.
    -- Original parsed XML via OPENXML when @EntityCodes was non-NULL.  Snowflake
    -- has no OPENXML; we either need a JSON/VARIANT input or a stub.  For the
    -- take-home happy path we stub the non-NULL branch and honour the else.
    -- Placed BEFORE BEGIN TRANSACTION so the stubbed RETURN doesn't leak a txn.
    -- =====================================================================
    IF (:entity_codes IS NOT NULL) THEN
        -- TODO: scai EWI SSC-FDM-0007 OPENXML — stubbed; happy path passes NULL.
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error',   'entity_codes XML/JSON parsing not implemented in this migration; pass NULL to auto-derive from budget data',
            'reconciliation_id', :reconciliation_id
        );
    END IF;

    BEGIN TRANSACTION;

    -- Auto-derive entity list from distinct CostCenter prefixes in the budget.
    INSERT INTO t_entity_list (EntityCode)
    SELECT DISTINCT
        LEFT(cc.CostCenterCode, CHARINDEX('-', cc.CostCenterCode || '-') - 1)
    FROM BudgetLineItem bli
    INNER JOIN CostCenter cc ON bli.CostCenterID = cc.CostCenterID
    WHERE bli.BudgetHeaderID = :budget_header_id;

    -- =====================================================================
    -- Identify intercompany pairs and calculate variances.
    -- Inlined CROSS APPLY e1/e2 derivations as direct LEFT(...) expressions.
    -- HASHBYTES('SHA2_256', ...) -> SHA2(..., 256).
    -- =====================================================================
    INSERT INTO t_intercompany_pairs (
        Entity1Code, Entity2Code, GLAccountID, PartnerAccountID,
        Entity1Amount, Entity2Amount, Variance, VariancePercent,
        IsWithinTolerance, ReconciliationStatus, MatchHash
    )
    SELECT
        LEFT(cc1.CostCenterCode, CHARINDEX('-', cc1.CostCenterCode || '-') - 1)   AS Entity1Code,
        LEFT(COALESCE(cc2.CostCenterCode, ''), CHARINDEX('-', COALESCE(cc2.CostCenterCode, '') || '-') - 1)  AS Entity2Code,
        bli1.GLAccountID,
        gla1.ConsolidationAccountID,
        SUM(bli1.FinalAmount),
        -SUM(COALESCE(bli2.FinalAmount, 0)),
        SUM(bli1.FinalAmount) + SUM(COALESCE(bli2.FinalAmount, 0)),
        CASE
            WHEN ABS(SUM(bli1.FinalAmount)) > 0
            THEN (SUM(bli1.FinalAmount) + SUM(COALESCE(bli2.FinalAmount, 0))) / ABS(SUM(bli1.FinalAmount))
            ELSE NULL
        END,
        CASE
            WHEN ABS(SUM(bli1.FinalAmount) + SUM(COALESCE(bli2.FinalAmount, 0))) <= :tolerance_amount THEN TRUE
            WHEN ABS(SUM(bli1.FinalAmount)) > 0
                 AND ABS((SUM(bli1.FinalAmount) + SUM(COALESCE(bli2.FinalAmount, 0))) / SUM(bli1.FinalAmount)) <= :tolerance_percent THEN TRUE
            ELSE FALSE
        END,
        'PENDING',
        SHA2(
            LEFT(cc1.CostCenterCode, CHARINDEX('-', cc1.CostCenterCode || '-') - 1) || '|' ||
            LEFT(COALESCE(cc2.CostCenterCode, ''), CHARINDEX('-', COALESCE(cc2.CostCenterCode, '') || '-') - 1) || '|' ||
            CAST(bli1.GLAccountID AS VARCHAR) || '|' ||
            CAST(ABS(ROUND(SUM(bli1.FinalAmount), 0)) AS VARCHAR),
            256
        )
    FROM BudgetLineItem bli1
    INNER JOIN GLAccount  gla1 ON bli1.GLAccountID   = gla1.GLAccountID
    INNER JOIN CostCenter cc1  ON bli1.CostCenterID  = cc1.CostCenterID
    INNER JOIN t_entity_list el1
        ON LEFT(cc1.CostCenterCode, CHARINDEX('-', cc1.CostCenterCode || '-') - 1) = el1.EntityCode
        AND el1.IncludeFlag = TRUE
    LEFT JOIN BudgetLineItem bli2
        ON bli2.BudgetHeaderID = :budget_header_id
        AND bli2.GLAccountID   = gla1.ConsolidationAccountID
    LEFT JOIN CostCenter cc2
        ON bli2.CostCenterID   = cc2.CostCenterID
    LEFT JOIN t_entity_list el2
        ON LEFT(COALESCE(cc2.CostCenterCode, ''), CHARINDEX('-', COALESCE(cc2.CostCenterCode, '') || '-') - 1) = el2.EntityCode
    WHERE bli1.BudgetHeaderID = :budget_header_id
      AND gla1.IntercompanyFlag = TRUE
      AND gla1.ConsolidationAccountID IS NOT NULL
    GROUP BY
        LEFT(cc1.CostCenterCode, CHARINDEX('-', cc1.CostCenterCode || '-') - 1),
        LEFT(COALESCE(cc2.CostCenterCode, ''), CHARINDEX('-', COALESCE(cc2.CostCenterCode, '') || '-') - 1),
        bli1.GLAccountID,
        gla1.ConsolidationAccountID
    HAVING SUM(bli1.FinalAmount) <> 0 OR SUM(COALESCE(bli2.FinalAmount, 0)) <> 0;

    -- =====================================================================
    -- Detailed matching (fuzzy).  FORMAT('N2') -> TO_VARCHAR(ROUND(x, 2)).
    -- =====================================================================
    INSERT INTO t_reconciliation_details (
        PairID, SourceLineItemID, TargetLineItemID,
        MatchType, MatchScore, MatchDetails
    )
    SELECT
        ip.PairID,
        bli1.BudgetLineItemID,
        bli2.BudgetLineItemID,
        CASE
            WHEN bli1.FinalAmount = -bli2.FinalAmount THEN 'EXACT'
            WHEN ABS(bli1.FinalAmount + bli2.FinalAmount) <= :tolerance_amount THEN 'TOLERANCE'
            WHEN bli2.BudgetLineItemID IS NULL THEN 'UNMATCHED_SOURCE'
            ELSE 'PARTIAL'
        END,
        CASE
            WHEN bli1.FinalAmount = -bli2.FinalAmount THEN 1.0
            WHEN ABS(bli1.FinalAmount) > 0
                THEN 1.0 - ABS((bli1.FinalAmount + COALESCE(bli2.FinalAmount, 0)) / bli1.FinalAmount)
            ELSE 0
        END,
        'Source: ' || TO_VARCHAR(ROUND(bli1.FinalAmount, 2)) ||
        ' | Target: ' || TO_VARCHAR(ROUND(COALESCE(bli2.FinalAmount, 0), 2)) ||
        ' | Diff: '   || TO_VARCHAR(ROUND(bli1.FinalAmount + COALESCE(bli2.FinalAmount, 0), 2))
    FROM t_intercompany_pairs ip
    INNER JOIN BudgetLineItem bli1
         ON bli1.BudgetHeaderID = :budget_header_id
        AND bli1.GLAccountID    = ip.GLAccountID
    LEFT  JOIN BudgetLineItem bli2
         ON bli2.BudgetHeaderID = :budget_header_id
        AND bli2.GLAccountID    = ip.PartnerAccountID;

    -- =====================================================================
    -- Update reconciliation status (pure CASE, no FROM needed).
    -- =====================================================================
    UPDATE t_intercompany_pairs
    SET ReconciliationStatus =
        CASE
            WHEN IsWithinTolerance THEN 'RECONCILED'
            WHEN EXISTS (
                SELECT 1 FROM t_reconciliation_details rd
                WHERE rd.PairID = t_intercompany_pairs.PairID AND rd.MatchType = 'EXACT'
            ) THEN 'MATCHED'
            WHEN EXISTS (
                SELECT 1 FROM t_reconciliation_details rd
                WHERE rd.PairID = t_intercompany_pairs.PairID AND rd.MatchType = 'PARTIAL'
            ) THEN 'PARTIAL_MATCH'
            ELSE 'UNRECONCILED'
        END;

    -- =====================================================================
    -- Auto-create adjustment entries when requested.
    -- SCOPE_IDENTITY() has no Snowflake analog.  Journal is created with a
    -- deterministic JournalNumber ('ICR-'||yyyymmdd||'-'||uuid_prefix) so we can
    -- read back the surrogate JournalID via natural-key lookup (same playbook
    -- as proc 1 for BudgetHeader via UQ_BudgetHeader_Code_Year).
    -- =====================================================================
    IF (:auto_create_adjustments) THEN
        journal_number := 'ICR-' || TO_VARCHAR(:effective_date, 'YYYYMMDD') || '-' ||
                          LEFT(:reconciliation_id, 8);

        INSERT INTO ConsolidationJournal (
            JournalNumber, JournalType, BudgetHeaderID, FiscalPeriodID,
            PostingDate, Description, StatusCode
        )
        SELECT
            :journal_number,
            'ELIMINATION',
            :budget_header_id,
            (SELECT FiscalPeriodID
               FROM FiscalPeriod
              WHERE :effective_date BETWEEN PeriodStartDate AND PeriodEndDate
              LIMIT 1),
            :effective_date,
            'Auto-generated intercompany reconciliation adjustment',
            'DRAFT';

        SELECT JournalID INTO :journal_id
          FROM ConsolidationJournal
         WHERE JournalNumber = :journal_number
         LIMIT 1;

        INSERT INTO ConsolidationJournalLine (
            JournalID, LineNumber, GLAccountID, CostCenterID,
            DebitAmount, CreditAmount, Description
        )
        SELECT
            :journal_id,
            ROW_NUMBER() OVER (ORDER BY ip.PairID),
            ip.GLAccountID,
            (SELECT CostCenterID
               FROM CostCenter
              WHERE CostCenterCode LIKE ip.Entity1Code || '%'
              LIMIT 1),
            CASE WHEN ip.Variance > 0 THEN ip.Variance ELSE 0 END,
            CASE WHEN ip.Variance < 0 THEN ABS(ip.Variance) ELSE 0 END,
            'IC Adjustment: ' || ip.Entity1Code || ' <-> ' || ip.Entity2Code
        FROM t_intercompany_pairs ip
        WHERE ip.ReconciliationStatus = 'UNRECONCILED'
          AND ABS(ip.Variance) > :tolerance_amount;
    END IF;

    -- =====================================================================
    -- Summary output (replaces OUTPUT params + FOR XML PATH report).
    -- Split into two single-col SELECT...INTO assignments — proc 1/3 idiom.
    -- =====================================================================
    SELECT COUNT(*) INTO :unreconciled_count
      FROM t_intercompany_pairs
     WHERE ReconciliationStatus = 'UNRECONCILED';

    SELECT COALESCE(SUM(ABS(Variance)), 0) INTO :total_variance_amount
      FROM t_intercompany_pairs
     WHERE ReconciliationStatus = 'UNRECONCILED';

    LET report_variant VARIANT := (
        SELECT OBJECT_CONSTRUCT(
            'ReconciliationID',    :reconciliation_id,
            'ReconciliationDate',  :effective_date::VARCHAR,
            'BudgetHeaderID',      :budget_header_id,
            'ToleranceAmount',     :tolerance_amount,
            'TolerancePercent',    :tolerance_percent,
            'Statistics',          OBJECT_CONSTRUCT(
                'TotalPairs',             (SELECT COUNT(*) FROM t_intercompany_pairs),
                'Reconciled',             (SELECT COUNT(*) FROM t_intercompany_pairs WHERE ReconciliationStatus = 'RECONCILED'),
                'Unreconciled',           :unreconciled_count,
                'TotalVariance',          (SELECT COALESCE(SUM(ABS(Variance)), 0) FROM t_intercompany_pairs),
                'OutOfToleranceVariance', (SELECT COALESCE(SUM(ABS(Variance)), 0) FROM t_intercompany_pairs WHERE IsWithinTolerance = FALSE)
            ),
            'Pairs',               (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                    'PairID',            ip.PairID,
                    'Entity1',           ip.Entity1Code,
                    'Entity2',           ip.Entity2Code,
                    'Status',            ip.ReconciliationStatus,
                    'GLAccountID',       ip.GLAccountID,
                    'PartnerAccountID',  ip.PartnerAccountID,
                    'Amount1',           ip.Entity1Amount,
                    'Amount2',           ip.Entity2Amount,
                    'Variance',          ip.Variance,
                    'VariancePercent',   ip.VariancePercent,
                    'WithinTolerance',   ip.IsWithinTolerance
                )) FROM t_intercompany_pairs ip
            )
        )
    );

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'success',                TRUE,
        'reconciliation_id',      :reconciliation_id,
        'budget_header_id',       :budget_header_id,
        'reconciliation_date',    :effective_date::VARCHAR,
        'unreconciled_count',     :unreconciled_count,
        'total_variance_amount',  :total_variance_amount,
        'report',                 :report_variant
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

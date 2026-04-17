-- Snowflake migration of usp_BulkImportBudgetData (proc 6).
-- Source: original/src/StoredProcedures/usp_BulkImportBudgetData.sql (~519 LOC T-SQL).
-- Starting skeleton: snowconvert/output_full/procedures/usp_bulkimportbudgetdata.sql (585 LOC scai).
--
-- Scope note: this proc's original footprint spans four import sources
-- (FILE / TVP / STAGING_TABLE / LINKED_SERVER) with BULK INSERT, OPENROWSET,
-- OPENQUERY, and sp_executesql dynamic SQL.  Of these, only the TVP path has
-- a faithful Snowflake analogue — the other three depend on SQL Server file
-- infrastructure that has no 1:1 equivalent in Snowflake.  For the take-home
-- the migration wires the TVP path end-to-end (ARRAY VARIANT replaces the
-- BudgetLineItemTableType TVP) and stubs the remaining branches with an
-- explanatory VARIANT return.  Production migration would replace FILE with
-- `COPY INTO ... FROM @stage`, STAGING_TABLE with a fully-qualified name
-- (no dynamic SQL), and LINKED_SERVER by staging the source dataset.
--
-- Translation notes:
--   * TVP BudgetLineItemTableType (READONLY) -> ARRAY param (VARIANT rows)
--   * BULK INSERT / OPENROWSET / OPENQUERY branches -> stubbed (see above)
--   * @@TRANCOUNT / XACT_STATE / SAVE TRANSACTION -> dropped; single txn +
--     EXCEPTION WHEN OTHER (proc 1/3 pattern)
--   * OUTPUT INTO @InsertedRows / #MergeOutput -> dropped entirely (downstream
--     state is tracked via IsProcessed on staging table)
--   * @@ROWCOUNT -> SQLROWCOUNT into local INT var immediately after DML
--   * FOR XML PATH IMPORTRESULTS -> summary folded into VARIANT return
--   * OUTPUT params (@ImportResults, @RowsImported, @RowsRejected) ->
--     VARIANT return fields
--   * BIT DEFAULT 1 column references with = 1 / = 0 -> TRUE / FALSE
--   * SET XACT_ABORT ON -> dropped
--   * NVARCHAR / TIMESTAMP_NTZ(7) -> VARCHAR / TIMESTAMP_NTZ
--   * `UPDATE tgt stg SET stg.col = src.col FROM src` ->
--     `UPDATE tgt stg SET col = src.col FROM src` (no table-qualifier on LHS)

USE DATABASE PLANNING_DB;
USE SCHEMA PLANNING;
USE WAREHOUSE WH_XS;


CREATE OR REPLACE PROCEDURE usp_BulkImportBudgetData(
    import_source               VARCHAR,       -- FILE, TVP, STAGING_TABLE, LINKED_SERVER
    budget_data                 ARRAY,         -- Replaces BudgetLineItemTableType TVP
    target_budget_header_id     INT,
    file_path                   VARCHAR,
    format_file_path            VARCHAR,
    staging_table_name          VARCHAR,
    linked_server_name          VARCHAR,
    linked_server_query         VARCHAR,
    validation_mode             VARCHAR,       -- STRICT, LENIENT, NONE
    duplicate_handling          VARCHAR,       -- REJECT, UPDATE, SKIP
    batch_size                  INT,
    use_parallel_load           BOOLEAN,
    max_degree_of_parallelism   INT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    start_time             TIMESTAMP_NTZ;
    import_batch_id        VARCHAR;
    error_message          VARCHAR;
    total_rows             INT DEFAULT 0;
    valid_rows             INT DEFAULT 0;
    invalid_rows           INT DEFAULT 0;
    processed_batches      INT DEFAULT 0;
    batch_number           INT DEFAULT 0;
    rows_this_batch        INT DEFAULT 1;
    rows_imported          INT DEFAULT 0;
    rows_rejected          INT DEFAULT 0;
BEGIN
    start_time      := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;
    import_batch_id := UUID_STRING();

    -- Staging table for imported data.  Note: inline INDEX clauses from the
    -- T-SQL source are not supported on Snowflake temp tables and are dropped.
    CREATE OR REPLACE TEMPORARY TABLE t_import_staging (
        RowID                INT AUTOINCREMENT PRIMARY KEY,
        GLAccountID          INT,
        AccountNumber        VARCHAR(20),
        CostCenterID         INT,
        CostCenterCode       VARCHAR(20),
        FiscalPeriodID       INT,
        FiscalYear           SMALLINT,
        FiscalMonth          TINYINT,
        OriginalAmount       NUMBER(19,4),
        AdjustedAmount       NUMBER(19,4),
        SpreadMethodCode     VARCHAR(10),
        Notes                VARCHAR(500),
        IsValid              BOOLEAN DEFAULT TRUE,
        ValidationErrors     VARCHAR,
        IsProcessed          BOOLEAN DEFAULT FALSE,
        ProcessedDateTime    TIMESTAMP_NTZ,
        ResultLineItemID     NUMBER(38,0)
    );

    CREATE OR REPLACE TEMPORARY TABLE t_import_errors (
        ErrorID        INT AUTOINCREMENT PRIMARY KEY,
        RowID          INT,
        ErrorCode      VARCHAR(20),
        ErrorMessage   VARCHAR(500),
        ColumnName     VARCHAR(128),
        OriginalValue  VARCHAR(500),
        Severity       VARCHAR(10)
    );

    BEGIN TRANSACTION;

    -- =====================================================================
    -- Step 1: Load data into staging based on source type.
    -- Only TVP is wired; the other three sources return an explanatory
    -- VARIANT stub (see scope note at top of file).
    -- =====================================================================
    IF (:import_source = 'FILE') THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'source',  :import_source,
            'error',   'FILE import not implemented in Snowflake port; use COPY INTO from a stage in production'
        );
    ELSEIF (:import_source = 'STAGING_TABLE') THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'source',  :import_source,
            'error',   'STAGING_TABLE import not implemented in Snowflake port; use a fully-qualified table name and a static INSERT ... SELECT in production'
        );
    ELSEIF (:import_source = 'LINKED_SERVER') THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'source',  :import_source,
            'error',   'LINKED_SERVER import not implemented in Snowflake port; stage the source dataset and COPY INTO in production'
        );
    ELSEIF (:import_source = 'TVP') THEN
        -- Load from TVP-replacement ARRAY: VARIANT rows with typed members.
        INSERT INTO t_import_staging (
            GLAccountID, CostCenterID, FiscalPeriodID,
            OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes
        )
        SELECT
            v.value:GLAccountID::INT,
            v.value:CostCenterID::INT,
            v.value:FiscalPeriodID::INT,
            v.value:OriginalAmount::NUMBER(19,4),
            v.value:AdjustedAmount::NUMBER(19,4),
            v.value:SpreadMethodCode::VARCHAR,
            v.value:Notes::VARCHAR
        FROM LATERAL FLATTEN(input => :budget_data) v;

        total_rows := SQLROWCOUNT;
    ELSE
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'source',  :import_source,
            'error',   'Unknown import source: ' || COALESCE(:import_source, '<NULL>')
        );
    END IF;

    -- =====================================================================
    -- Step 2: Resolve lookups (IDs from codes).  No-op when TVP provided
    -- rows already include IDs; harmless for rows that lack them.
    -- =====================================================================
    UPDATE t_import_staging stg
    SET GLAccountID = gla.GLAccountID
    FROM GLAccount gla
    WHERE stg.AccountNumber = gla.AccountNumber
      AND stg.GLAccountID IS NULL
      AND stg.AccountNumber IS NOT NULL;

    UPDATE t_import_staging stg
    SET CostCenterID = cc.CostCenterID
    FROM CostCenter cc
    WHERE stg.CostCenterCode = cc.CostCenterCode
      AND stg.CostCenterID IS NULL
      AND stg.CostCenterCode IS NOT NULL;

    UPDATE t_import_staging stg
    SET FiscalPeriodID = fp.FiscalPeriodID
    FROM FiscalPeriod fp
    WHERE stg.FiscalYear = fp.FiscalYear
      AND stg.FiscalMonth = fp.FiscalMonth
      AND stg.FiscalPeriodID IS NULL
      AND stg.FiscalYear IS NOT NULL
      AND stg.FiscalMonth IS NOT NULL;

    -- =====================================================================
    -- Step 3: Validate data.
    -- =====================================================================
    IF (:validation_mode <> 'NONE') THEN
        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
        SELECT
            RowID, 'MISSING_ACCOUNT',
            'GL Account not found or not specified', 'GLAccountID',
            CASE :validation_mode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM t_import_staging
        WHERE GLAccountID IS NULL;

        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
        SELECT
            RowID, 'MISSING_COSTCENTER',
            'Cost Center not found or not specified', 'CostCenterID',
            CASE :validation_mode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM t_import_staging
        WHERE CostCenterID IS NULL;

        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
        SELECT
            RowID, 'MISSING_PERIOD',
            'Fiscal Period not found or not specified', 'FiscalPeriodID',
            CASE :validation_mode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM t_import_staging
        WHERE FiscalPeriodID IS NULL;

        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, OriginalValue, Severity)
        SELECT
            RowID, 'INVALID_AMOUNT', 'Amount is NULL',
            'OriginalAmount', NULL, 'ERROR'
        FROM t_import_staging
        WHERE OriginalAmount IS NULL;

        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
        SELECT
            stg.RowID, 'NON_BUDGETABLE',
            'Account is not marked as budgetable', 'GLAccountID', 'WARNING'
        FROM t_import_staging stg
        INNER JOIN GLAccount gla ON stg.GLAccountID = gla.GLAccountID
        WHERE gla.IsBudgetable = FALSE;

        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
        SELECT
            stg.RowID, 'INACTIVE_CC',
            'Cost Center is inactive', 'CostCenterID',
            CASE :validation_mode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM t_import_staging stg
        INNER JOIN CostCenter cc ON stg.CostCenterID = cc.CostCenterID
        WHERE cc.IsActive = FALSE;

        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
        SELECT
            stg.RowID, 'CLOSED_PERIOD',
            'Fiscal period is closed', 'FiscalPeriodID', 'ERROR'
        FROM t_import_staging stg
        INNER JOIN FiscalPeriod fp ON stg.FiscalPeriodID = fp.FiscalPeriodID
        WHERE fp.IsClosed = TRUE;

        -- Duplicates within batch
        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, Severity)
        WITH DuplicateCheck AS (
            SELECT
                RowID,
                ROW_NUMBER() OVER (
                    PARTITION BY GLAccountID, CostCenterID, FiscalPeriodID
                    ORDER BY RowID
                ) AS RowNum
            FROM t_import_staging
            WHERE GLAccountID IS NOT NULL
              AND CostCenterID IS NOT NULL
              AND FiscalPeriodID IS NOT NULL
        )
        SELECT
            RowID, 'DUPLICATE_IN_BATCH',
            'Duplicate entry within import batch', 'WARNING'
        FROM DuplicateCheck
        WHERE RowNum > 1;

        -- Existing records in target
        INSERT INTO t_import_errors (RowID, ErrorCode, ErrorMessage, Severity)
        SELECT
            stg.RowID, 'ALREADY_EXISTS',
            'Record already exists in target budget',
            CASE :duplicate_handling WHEN 'REJECT' THEN 'ERROR' ELSE 'WARNING' END
        FROM t_import_staging stg
        INNER JOIN BudgetLineItem bli
            ON stg.GLAccountID = bli.GLAccountID
           AND stg.CostCenterID = bli.CostCenterID
           AND stg.FiscalPeriodID = bli.FiscalPeriodID
        WHERE bli.BudgetHeaderID = :target_budget_header_id;

        -- Aggregate validation errors to staging table.
        UPDATE t_import_staging stg
        SET IsValid = CASE WHEN EXISTS (
                SELECT 1 FROM t_import_errors e
                WHERE e.RowID = stg.RowID
                  AND e.Severity = 'ERROR'
            ) THEN FALSE ELSE TRUE END,
            ValidationErrors = (
                SELECT LISTAGG(ErrorCode || ': ' || ErrorMessage, '; ')
                  FROM t_import_errors e
                 WHERE e.RowID = stg.RowID
            );
    END IF;

    -- Count valid/invalid
    SELECT
        SUM(CASE WHEN IsValid = TRUE  THEN 1 ELSE 0 END),
        SUM(CASE WHEN IsValid = FALSE THEN 1 ELSE 0 END)
      INTO :valid_rows, :invalid_rows
      FROM t_import_staging;

    -- =====================================================================
    -- Step 4: Process imports in batches.
    -- =====================================================================
    WHILE (:rows_this_batch > 0) DO
        batch_number := :batch_number + 1;

        IF (:duplicate_handling = 'UPDATE') THEN
            MERGE INTO BudgetLineItem AS target
            USING (
                SELECT
                    :target_budget_header_id AS BudgetHeaderID,
                    GLAccountID,
                    CostCenterID,
                    FiscalPeriodID,
                    OriginalAmount,
                    COALESCE(AdjustedAmount, 0) AS AdjustedAmount,
                    SpreadMethodCode,
                    'BULK_IMPORT' AS SourceSystem,
                    CAST(:import_batch_id AS VARCHAR(50)) AS SourceReference,
                    RowID
                FROM t_import_staging
                WHERE IsValid = TRUE
                  AND IsProcessed = FALSE
                ORDER BY RowID
                LIMIT :batch_size
            ) AS source
            ON  target.BudgetHeaderID  = source.BudgetHeaderID
            AND target.GLAccountID     = source.GLAccountID
            AND target.CostCenterID    = source.CostCenterID
            AND target.FiscalPeriodID  = source.FiscalPeriodID
            WHEN MATCHED THEN UPDATE SET
                OriginalAmount       = source.OriginalAmount,
                AdjustedAmount       = source.AdjustedAmount,
                SpreadMethodCode     = source.SpreadMethodCode,
                SourceReference      = source.SourceReference,
                LastModifiedDateTime = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            WHEN NOT MATCHED THEN
                INSERT (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
                        OriginalAmount, AdjustedAmount, SpreadMethodCode,
                        SourceSystem, SourceReference, LastModifiedDateTime)
                VALUES (source.BudgetHeaderID, source.GLAccountID, source.CostCenterID,
                        source.FiscalPeriodID, source.OriginalAmount, source.AdjustedAmount,
                        source.SpreadMethodCode, source.SourceSystem, source.SourceReference,
                        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);

            rows_this_batch := SQLROWCOUNT;
        ELSE
            -- SKIP duplicates or REJECT (rows with existing keys already
            -- flagged IsValid=FALSE by the validator above).
            INSERT INTO BudgetLineItem (
                BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
                OriginalAmount, AdjustedAmount, SpreadMethodCode,
                SourceSystem, SourceReference, ImportBatchID, LastModifiedDateTime
            )
            SELECT
                :target_budget_header_id,
                stg.GLAccountID,
                stg.CostCenterID,
                stg.FiscalPeriodID,
                stg.OriginalAmount,
                COALESCE(stg.AdjustedAmount, 0),
                stg.SpreadMethodCode,
                'BULK_IMPORT',
                CAST(:import_batch_id AS VARCHAR(50)),
                :import_batch_id,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            FROM t_import_staging stg
            WHERE stg.IsValid = TRUE
              AND stg.IsProcessed = FALSE
              AND (:duplicate_handling = 'REJECT'
                   OR NOT EXISTS (
                        SELECT 1 FROM BudgetLineItem bli
                        WHERE bli.BudgetHeaderID = :target_budget_header_id
                          AND bli.GLAccountID    = stg.GLAccountID
                          AND bli.CostCenterID   = stg.CostCenterID
                          AND bli.FiscalPeriodID = stg.FiscalPeriodID
                   ))
            ORDER BY stg.RowID
            LIMIT :batch_size;

            rows_this_batch := SQLROWCOUNT;
        END IF;

        -- Mark processed rows.  Scope-narrowed to the batch we just wrote
        -- by picking the top-N RowIDs from unprocessed valid rows.
        UPDATE t_import_staging stg
        SET IsProcessed = TRUE,
            ProcessedDateTime = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHERE stg.IsValid = TRUE
          AND stg.IsProcessed = FALSE
          AND stg.RowID <= COALESCE((
              SELECT MAX(RowID) FROM (
                  SELECT RowID
                    FROM t_import_staging
                   WHERE IsValid = TRUE AND IsProcessed = FALSE
                   ORDER BY RowID
                   LIMIT :batch_size
              )
          ), 0);

        processed_batches := :processed_batches + 1;
    END WHILE;

    -- Final counts.
    SELECT
        SUM(CASE WHEN IsProcessed = TRUE  THEN 1 ELSE 0 END),
        SUM(CASE WHEN IsValid     = FALSE THEN 1 ELSE 0 END)
      INTO :rows_imported, :rows_rejected
      FROM t_import_staging;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'success',           TRUE,
        'batch_id',          :import_batch_id,
        'source',            :import_source,
        'target_budget_id',  :target_budget_header_id,
        'total_rows',        :total_rows,
        'valid_rows',        :valid_rows,
        'invalid_rows',      :invalid_rows,
        'rows_imported',     :rows_imported,
        'rows_rejected',     :rows_rejected,
        'batches_processed', :processed_batches,
        'batch_size',        :batch_size,
        'duration_ms',       DATEDIFF(MILLISECOND, :start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
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

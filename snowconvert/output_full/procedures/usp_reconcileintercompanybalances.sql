/*
    usp_ReconcileIntercompanyBalances - Complex intercompany reconciliation with XML reporting

    Dependencies:
        - Tables: BudgetLineItem, ConsolidationJournal, ConsolidationJournalLine, GLAccount, CostCenter
        - Views: vw_BudgetConsolidationSummary
        - Functions: fn_GetHierarchyPath

    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. OPENXML and XML DOM operations - Very different in Snowflake
    2. sp_xml_preparedocument / sp_xml_removedocument - No equivalent
    3. FOR XML PATH with complex nesting and attributes
    4. XML namespaces and xpath queries
    5. HASHBYTES for data comparison - Different syntax
    6. Binary data manipulation with CAST to VARBINARY
    7. CLR function references (commented out example)
    8. EVENTDATA() for DDL trigger context
    9. sys.dm_* dynamic management views
    10. DBCC commands embedded in procedures
    11. Extended stored procedures (xp_*)
    12. Linked server queries with OPENQUERY
    ============================================================================
*/
    --** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "OPENXML" **
CREATE OR REPLACE PROCEDURE Planning.usp_ReconcileIntercompanyBalances (BUDGETHEADERID INT, RECONCILIATIONDATE DATE DEFAULT NULL, ENTITYCODES TEXT DEFAULT NULL,         -- List of entities to reconcile
TOLERANCEAMOUNT DECIMAL(19,4) DEFAULT 0.01, TOLERANCEPERCENT DECIMAL(5,4) DEFAULT 0.001, AUTOCREATEADJUSTMENTS BOOLEAN DEFAULT 0, RECONCILIATIONREPORTXML TEXT DEFAULT NULL, UNRECONCILEDCOUNT INT DEFAULT NULL, TOTALVARIANCEAMOUNT DECIMAL(19,4) DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        EFFECTIVEDATE DATE := NVL(:RECONCILIATIONDATE, CURRENT_DATE());
        RECONCILIATIONID VARCHAR := UUID_STRING();
        XMLHANDLE INT;
        ERRORMSG NVARCHAR(4000);
        JOURNALID BIGINT;
        LINENUM INT := 0;
    BEGIN
--        --** SSC-FDM-TS0029 - SET NOCOUNT STATEMENT IS COMMENTED OUT, WHICH IS NOT APPLICABLE IN SNOWFLAKE. **
--        SET NOCOUNT ON;

        -- Tables for reconciliation processing
        CREATE OR REPLACE TEMPORARY TABLE PUBLIC.T_IntercompanyPairs (
            PairID INT IDENTITY(1,1) ORDER PRIMARY KEY,
            Entity1Code VARCHAR(20) NOT NULL,
            Entity2Code VARCHAR(20) NOT NULL,
            GLAccountID INT NOT NULL,
            PartnerAccountID INT NOT NULL,
            Entity1Amount DECIMAL(19, 4) NOT NULL,
            Entity2Amount DECIMAL(19, 4) NOT NULL,
            Variance DECIMAL(19, 4) NOT NULL,
            VariancePercent DECIMAL(8, 6) NULL,
            IsWithinTolerance BOOLEAN NOT NULL,
            ReconciliationStatus VARCHAR(20),
            MatchHash VARBINARY(32)
--                                   ,
--            --** SSC-FDM-0021 - CREATE INDEX IS NOT SUPPORTED BY SNOWFLAKE **
--            INDEX IX_Entities (Entity1Code, Entity2Code)
                                                        
--                                                        ,
--            --** SSC-FDM-0021 - CREATE INDEX IS NOT SUPPORTED BY SNOWFLAKE **
--            INDEX IX_Status (IsWithinTolerance, ReconciliationStatus)
        );
        -- EXACT, PARTIAL, UNMATCHED
        CREATE OR REPLACE TEMPORARY TABLE PUBLIC.T_ReconciliationDetails (
            DetailID INT IDENTITY(1,1) ORDER PRIMARY KEY,
            PairID INT,
            SourceLineItemID BIGINT,
            TargetLineItemID BIGINT,
            MatchType VARCHAR(20),
            MatchScore DECIMAL(5, 4),
            MatchDetails NVARCHAR(500)
        );
        BEGIN
            -- =====================================================================
            -- Parse entity list from XML using OPENXML (legacy pattern)
            -- =====================================================================
            CREATE OR REPLACE TEMPORARY TABLE T_ENTITYLIST (
                EntityCode VARCHAR(20) PRIMARY KEY,
                EntityName NVARCHAR(100),
                IncludeFlag BOOLEAN DEFAULT true
            );
            IF (:ENTITYCODES IS NOT NULL) THEN
                BEGIN
                    -- Prepare XML document handle
                    CALL sp_xml_preparedocument();

                    INSERT INTO T_ENTITYLIST (EntityCode, EntityName, IncludeFlag)
                    SELECT
                        EntityCode,
                        EntityName,
                        NVL(Include, 1)
                    FROM
                        PUBLIC.OPENXML_UDF(:XMLHANDLE, ':Entities:Entity');
                    -- Release XML document
                    CALL sp_xml_removedocument();
                END;
            ELSE
                BEGIN
                    -- Get all distinct entities from budget data
                    INSERT INTO T_ENTITYLIST (EntityCode)
                    SELECT DISTINCT
                        LEFT(cc.CostCenterCode, CHARINDEX('-', cc.CostCenterCode || '-') - 1)
                    FROM
                        Planning.BudgetLineItem bli
                    INNER JOIN
                            Planning.CostCenter cc
                            ON bli.CostCenterID = cc.CostCenterID
                    WHERE
                        bli.BudgetHeaderID = :BUDGETHEADERID;
                END;
            END IF;

            -- =====================================================================
            -- Identify intercompany pairs and calculate variances
            -- =====================================================================
            INSERT INTO PUBLIC.T_IntercompanyPairs (Entity1Code, Entity2Code, GLAccountID, PartnerAccountID, Entity1Amount, Entity2Amount, Variance, VariancePercent, IsWithinTolerance, ReconciliationStatus, MatchHash)
            SELECT
                e1.EntityCode,
                e2.EntityCode,
                bli1.GLAccountID,
                gla1.ConsolidationAccountID,
                SUM(bli1.FinalAmount),
                -SUM(NVL(bli2.FinalAmount, 0)),  -- Opposite sign
                SUM(bli1.FinalAmount) + SUM(NVL(bli2.FinalAmount, 0)),
                CASE
                    WHEN ABS(SUM(bli1.FinalAmount)) > 0
                    THEN (SUM(bli1.FinalAmount) + SUM(NVL(bli2.FinalAmount, 0))) / ABS(SUM(bli1.FinalAmount))
                    ELSE NULL
                END,
                CASE
                    WHEN ABS(SUM(bli1.FinalAmount) + SUM(NVL(bli2.FinalAmount, 0))) <= :TOLERANCEAMOUNT
                    THEN 1
                    WHEN ABS(SUM(bli1.FinalAmount)) > 0
                         AND ABS((SUM(bli1.FinalAmount) + SUM(NVL(bli2.FinalAmount, 0))) / SUM(bli1.FinalAmount)) <= :TOLERANCEPERCENT
                    THEN 1
                    ELSE 0
                END,
                'PENDING',
                -- HASHBYTES for matching - Different in Snowflake
                SHA2(CONCAT(e1.EntityCode, '|', e2.EntityCode, '|',
                           CAST(bli1.GLAccountID AS VARCHAR), '|',
                           CAST(ABS(ROUND(SUM(bli1.FinalAmount), 0)) AS VARCHAR)), 256)
            FROM
                Planning.BudgetLineItem bli1
            INNER JOIN
                    Planning.GLAccount gla1
                    ON bli1.GLAccountID = gla1.GLAccountID
            INNER JOIN
                    Planning.CostCenter cc1
                    ON bli1.CostCenterID = cc1.CostCenterID
                !!!RESOLVE EWI!!! /*** SSC-EWI-TS0082 - CROSS APPLY HAS BEEN CONVERTED TO LEFT OUTER JOIN AND REQUIRES MANUAL VALIDATION. ***/!!!
                LEFT OUTER JOIN
                    (
                               SELECT
                            LEFT(cc1.CostCenterCode, CHARINDEX('-', cc1.CostCenterCode || '-') - 1) AS EntityCode
                           ) e1
            INNER JOIN
                    T_EntityList el1
                    ON e1.EntityCode = el1.EntityCode
                    AND el1.IncludeFlag = 1
            -- Find partner entries
            LEFT JOIN
                    Planning.BudgetLineItem bli2
                ON bli2.BudgetHeaderID = :BUDGETHEADERID
                AND bli2.GLAccountID = gla1.ConsolidationAccountID
            LEFT JOIN
                    Planning.CostCenter cc2
                    ON bli2.CostCenterID = cc2.CostCenterID
                !!!RESOLVE EWI!!! /*** SSC-EWI-TS0082 - CROSS APPLY HAS BEEN CONVERTED TO LEFT OUTER JOIN AND REQUIRES MANUAL VALIDATION. ***/!!!
                LEFT OUTER JOIN
                    (
                               SELECT
                            LEFT(NVL(cc2.CostCenterCode, ''), CHARINDEX('-', NVL(cc2.CostCenterCode, '') || '-') - 1) AS EntityCode
                           ) e2
            LEFT JOIN
                    T_EntityList el2
                    ON e2.EntityCode = el2.EntityCode
            WHERE
                bli1.BudgetHeaderID = :BUDGETHEADERID
              AND gla1.IntercompanyFlag = 1
              AND gla1.ConsolidationAccountID IS NOT NULL
            GROUP BY
                e1.EntityCode,
                e2.EntityCode,
                bli1.GLAccountID,
                gla1.ConsolidationAccountID
            HAVING
                SUM(bli1.FinalAmount) <> 0 OR SUM(NVL(bli2.FinalAmount, 0)) <> 0;

            -- =====================================================================
            -- Perform detailed matching using fuzzy logic
            -- =====================================================================
            INSERT INTO PUBLIC.T_ReconciliationDetails (PairID, SourceLineItemID, TargetLineItemID, MatchType, MatchScore, MatchDetails)
            SELECT
                ip.PairID,
                bli1.BudgetLineItemID,
                bli2.BudgetLineItemID,
                CASE
                    WHEN bli1.FinalAmount = -bli2.FinalAmount
                    THEN 'EXACT'
                    WHEN ABS(bli1.FinalAmount + bli2.FinalAmount) <= :TOLERANCEAMOUNT
                    THEN 'TOLERANCE'
                    WHEN bli2.BudgetLineItemID IS NULL THEN 'UNMATCHED_SOURCE'
                    ELSE 'PARTIAL'
                END,
                CASE
                    WHEN bli1.FinalAmount = -bli2.FinalAmount
                    THEN 1.0
                    WHEN ABS(bli1.FinalAmount) > 0
                    THEN 1.0 - ABS((bli1.FinalAmount + NVL(bli2.FinalAmount, 0)) / bli1.FinalAmount)
                    ELSE 0
                END,
                CONCAT(
                    'Source: ',
                !!!RESOLVE EWI!!! /*** SSC-EWI-0006 - FORMAT: 'N2' MAY FAIL OR MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE.  ***/!!!
                TO_CHAR(bli1.FinalAmount, 'N2'),
                    ' | Target: ',
                !!!RESOLVE EWI!!! /*** SSC-EWI-0006 - FORMAT: 'N2' MAY FAIL OR MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE.  ***/!!!
                TO_CHAR(NVL(bli2.FinalAmount, 0), 'N2'),
                    ' | Diff: ',
                !!!RESOLVE EWI!!! /*** SSC-EWI-0006 - FORMAT: 'N2' MAY FAIL OR MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE.  ***/!!!
                TO_CHAR(bli1.FinalAmount + NVL(bli2.FinalAmount, 0), 'N2')
                )
            FROM
                PUBLIC.T_IntercompanyPairs ip
            INNER JOIN
                    Planning.BudgetLineItem bli1
                ON bli1.BudgetHeaderID = :BUDGETHEADERID
                AND bli1.GLAccountID = ip.GLAccountID
            LEFT JOIN
                    Planning.BudgetLineItem bli2
                ON bli2.BudgetHeaderID = :BUDGETHEADERID
                AND bli2.GLAccountID = ip.PartnerAccountID;

            -- Update reconciliation status
            UPDATE T_IntercompanyPairs ip
            SET
                    ReconciliationStatus =
                CASE
                    WHEN ip.IsWithinTolerance = 1 THEN 'RECONCILED'
                    WHEN EXISTS (
                        SELECT 1 FROM
                                PUBLIC.T_ReconciliationDetails rd
                        WHERE
                                rd.PairID = ip.PairID
                                AND rd.MatchType = 'EXACT'
                    ) THEN 'MATCHED'
                    WHEN EXISTS (
                        SELECT 1 FROM
                                PUBLIC.T_ReconciliationDetails rd
                        WHERE
                                rd.PairID = ip.PairID
                                AND rd.MatchType = 'PARTIAL'
                    ) THEN 'PARTIAL_MATCH'
                    ELSE 'UNRECONCILED'
                END;
            -- =====================================================================
            -- Auto-create adjustment entries if requested
            -- =====================================================================
            IF (:AUTOCREATEADJUSTMENTS = 1) THEN
            BEGIN
                     
                     

                -- Create consolidation journal for adjustments
                INSERT INTO Planning.ConsolidationJournal (JournalNumber, JournalType, BudgetHeaderID, FiscalPeriodID, PostingDate, Description, StatusCode)
                SELECT
                    'ICR-' || TO_CHAR(:EFFECTIVEDATE, 'YYYYMMDD') || '-' || LEFT(CAST(:RECONCILIATIONID AS VARCHAR(36)), 8),
                    'ELIMINATION',
                    :BUDGETHEADERID,
                    --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                    (
                        SELECT TOP 1
                                ANY_VALUE(FiscalPeriodID)
                        FROM
                                Planning.FiscalPeriod
                                         WHERE
                                :EFFECTIVEDATE BETWEEN PeriodStartDate AND PeriodEndDate
                    ),
                    :EFFECTIVEDATE,
                    'Auto-generated intercompany reconciliation adjustment',
                    'DRAFT';
                    LET _scope_identity_query_id VARCHAR := LAST_QUERY_ID();
                    JOURNALID :=
                    SELECT
                        MAX(JournalLineID)
                    FROM
                        Planning.ConsolidationJournalLine AT (STATEMENT => _scope_identity_query_id);

                -- Insert adjustment lines for unreconciled pairs
                INSERT INTO Planning.ConsolidationJournalLine (JournalID, LineNumber, GLAccountID, CostCenterID, DebitAmount, CreditAmount, Description)
                SELECT
                    :JOURNALID,
                    ROW_NUMBER() OVER (ORDER BY ip.PairID),
                    ip.GLAccountID,
                    --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                    (
                        SELECT TOP 1
                                ANY_VALUE(CostCenterID)
                        FROM
                                Planning.CostCenter
                                         WHERE
                                CostCenterCode LIKE ip.Entity1Code || '%'
                    ),
                    CASE WHEN ip.Variance > 0 THEN ip.Variance
                        ELSE 0 END,
                    CASE WHEN ip.Variance < 0 THEN ABS(ip.Variance) ELSE 0 END,
                    CONCAT('IC Adjustment: ', ip.Entity1Code, ' <-> ', ip.Entity2Code)
                FROM
                    PUBLIC.T_IntercompanyPairs ip
                WHERE
                    ip.ReconciliationStatus = 'UNRECONCILED'
                  AND ABS(ip.Variance) > :TOLERANCEAMOUNT;
                    _scope_identity_query_id := LAST_QUERY_ID();
            END;
            END IF;
            -- =====================================================================
            -- Build XML report using FOR XML PATH with complex nesting
            -- =====================================================================
            RECONCILIATIONREPORTXML := (
                           SELECT
                    -- Summary statistics
                    -- Entity summary
                    -- Detailed pairs
                            -- Nested detail lines
                    --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                    PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('ReconciliationID', :RECONCILIATIONID, 'ReconciliationDate', :EFFECTIVEDATE, 'BudgetHeaderID', :BUDGETHEADERID, 'ToleranceAmount', :TOLERANCEAMOUNT, 'TolerancePercent', :TOLERANCEPERCENT), 'ReconciliationReport')
                       );

            -- Set output parameters
            SELECT
                COUNT(*),
                SUM(ABS(Variance))
            INTO
                :UNRECONCILEDCOUNT,
                :TOTALVARIANCEAMOUNT
            FROM
                PUBLIC.T_IntercompanyPairs
            WHERE
                ReconciliationStatus = 'UNRECONCILED';
        EXCEPTION
            WHEN OTHER THEN
                ERRORMSG := SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/;
                -- Build error report as XML
                RECONCILIATIONREPORTXML := (
                               SELECT
                    --** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
                    PUBLIC.FOR_XML_UDF(OBJECT_CONSTRUCT('Status',
                    'ERROR', 'ErrorNumber', SQLCODE /*** SSC-FDM-TS0023 - ERROR NUMBER COULD BE DIFFERENT IN SNOWFLAKE ***/, 'ErrorMessage', :ERRORMSG, 'ErrorLine', ERROR_LINE() !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'ERROR_LINE FUNCTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!!, 'ErrorProcedure', 'Planning.usp_ReconcileIntercompanyBalances' /*** SSC-FDM-TS0023 - ERROR PROCEDURE NAME COULD BE DIFFERENT IN SNOWFLAKE ***/), 'ReconciliationError')
                           );
                LET DECLARED_EXCEPTION EXCEPTION;
                RAISE DECLARED_EXCEPTION;
        END;

        -- Cleanup
        DROP TABLE IF EXISTS PUBLIC.T_IntercompanyPairs;
        DROP TABLE IF EXISTS PUBLIC.T_ReconciliationDetails;
        DROP TABLE T_ENTITYLIST;
    END;
$$;
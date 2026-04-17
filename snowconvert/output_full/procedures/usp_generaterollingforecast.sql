/*
    usp_GenerateRollingForecast - Generates rolling forecast with statistical projections

    Dependencies:
        - Tables: BudgetHeader, BudgetLineItem, FiscalPeriod, CostCenter, GLAccount
        - Functions: fn_GetAllocationFactor, tvf_GetBudgetVariance
        - Views: vw_BudgetConsolidationSummary

    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. PIVOT with dynamic columns - Different syntax in Snowflake
    2. Window functions with ROWS BETWEEN and complex frames
    3. LAG/LEAD with multiple offsets computed dynamically
    4. PERCENTILE_CONT and statistical aggregates
    5. FOR XML PATH string concatenation pattern - Use LISTAGG in Snowflake
    6. Dynamic PIVOT generation with sp_executesql
    7. Global temp tables (##) - Not available in Snowflake
    8. OPENJSON for JSON parsing - Different in Snowflake
    9. Recursive forecast calculation with running totals
    10. COMPUTE BY clause (deprecated but still used)
    11. Complex CASE expressions with subqueries
    12. APPLY with derived tables
    ============================================================================
*/
CREATE OR REPLACE PROCEDURE Planning.usp_GenerateRollingForecast (BASEBUDGETHEADERID INT, HISTORICALPERIODS INT DEFAULT 12,           -- Months of history to analyze
FORECASTPERIODS INT DEFAULT 12,           -- Months to forecast
FORECASTMETHOD STRING DEFAULT 'WEIGHTED_AVERAGE',  -- WEIGHTED_AVERAGE, LINEAR_TREND, EXPONENTIAL, SEASONAL
SEASONALITYJSON STRING DEFAULT NULL,  -- JSON array of seasonal factors
GROWTHRATEOVERRIDE DECIMAL(8,4) DEFAULT NULL, CONFIDENCELEVEL DECIMAL(5,4) DEFAULT 0.95, OUTPUTFORMAT STRING DEFAULT 'DETAIL',  -- DETAIL, SUMMARY, PIVOT
TARGETBUDGETHEADERID INT DEFAULT NULL, FORECASTACCURACYMETRICS STRING DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "transact",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        STARTTIME TIMESTAMP_NTZ(7) := SYSDATE();
        ERRORMESSAGE NVARCHAR(4000);
        SOURCEFISCALYEAR SMALLINT;
        SOURCESTARTPERIOD INT;
        BASELINEENDPERIODID INT;
        -- Dynamic PIVOT - Very different in Snowflake
        PIVOTCOLUMNS NVARCHAR;
        DYNAMICSQL NVARCHAR;
    BEGIN
--        --** SSC-FDM-TS0029 - SET NOCOUNT STATEMENT IS COMMENTED OUT, WHICH IS NOT APPLICABLE IN SNOWFLAKE. **
--        SET NOCOUNT ON;

        -- =========================================================================
        -- Create global temp table for cross-session visibility (debugging)
        -- =========================================================================
        DROP TABLE IF EXISTS PUBLIC.T_ForecastWorkspace;
        -- Relative position
        -- Confidence interval
        --** SSC-FDM-0009 - GLOBAL TEMPORARY TABLE FUNCTIONALITY NOT SUPPORTED. **
        CREATE OR REPLACE TEMPORARY TABLE PUBLIC.T_ForecastWorkspace (
            WorkspaceID INT IDENTITY(1,1) ORDER PRIMARY KEY,
            SessionID INT DEFAULT :SPID,
            GLAccountID INT,
            CostCenterID INT,
            FiscalPeriodID INT,
            PeriodSequence INT,
            ActualAmount DECIMAL(19, 4),
            ForecastAmount DECIMAL(19, 4),
            LowerBound DECIMAL(19, 4),
            UpperBound DECIMAL(19, 4),
            SeasonalFactor DECIMAL(8, 6),
            TrendComponent DECIMAL(19, 4),
            CyclicalComponent DECIMAL(19, 4),
            Residual DECIMAL(19, 4),
            WeightFactor DECIMAL(8, 6),
            IsForecast BOOLEAN DEFAULT false,
            CalculationStep INT
        );
        -- =========================================================================
        -- Parse seasonality JSON using OPENJSON
        -- =========================================================================
        CREATE OR REPLACE TEMPORARY TABLE T_SEASONALFACTORS (
            MonthNumber INT PRIMARY KEY,
            SeasonalFactor DECIMAL(8, 6)
        );
        IF (:SEASONALITYJSON IS NOT NULL) THEN
            BEGIN
                INSERT INTO T_SEASONALFACTORS (MonthNumber, SeasonalFactor)
                SELECT
                    CAST(key AS INT) + 1,  -- Convert 0-based to 1-based month
                    CAST(value AS DECIMAL(8, 6))
                FROM
                    TABLE(PUBLIC.OPENJSON_UDF(:SEASONALITYJSON));  -- OPENJSON not in Snowflake, use PARSE_JSON
            END;
        ELSE
            BEGIN
                -- Default: no seasonality
                INSERT INTO T_SEASONALFACTORS (MonthNumber, SeasonalFactor)
                SELECT
                    n, 1.0
                FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) AS T (
                        n
                    );
            END;
        END IF;
        BEGIN
            -- Get base budget info
            SELECT
                FiscalYear,
                StartPeriodID
            INTO
                :SOURCEFISCALYEAR,
                :SOURCESTARTPERIOD
            FROM
                Planning.BudgetHeader
            WHERE
                BudgetHeaderID = :BASEBUDGETHEADERID;
            IF (:SOURCEFISCALYEAR IS NULL) THEN
                BEGIN
                    ERRORMESSAGE := 'Base budget header not found';
                    CALL PUBLIC.THROW_UDP(50100, :ERRORMESSAGE, 1);
                END;
            END IF;

            -- =====================================================================
            -- Populate historical data with window calculations
            -- =====================================================================
            INSERT INTO PUBLIC.T_ForecastWorkspace (GLAccountID, CostCenterID, FiscalPeriodID, PeriodSequence, ActualAmount, SeasonalFactor, WeightFactor, IsForecast)
            SELECT
                bli.GLAccountID,
                bli.CostCenterID,
                bli.FiscalPeriodID,
                ROW_NUMBER() OVER (
                    PARTITION BY
                    bli.GLAccountID, bli.CostCenterID
                    ORDER BY fp.FiscalYear, fp.FiscalMonth
                ) AS PeriodSequence,
                bli.FinalAmount,
                NVL(sf.SeasonalFactor, 1.0),
                -- Exponential decay weight: more recent = higher weight
                POWER(0.9, :HISTORICALPERIODS - ROW_NUMBER() OVER (
                    PARTITION BY
                    bli.GLAccountID, bli.CostCenterID
                    ORDER BY fp.FiscalYear, fp.FiscalMonth
                )) AS WeightFactor,
                0 AS IsForecast
            FROM
                Planning.BudgetLineItem bli
            INNER JOIN
                    Planning.FiscalPeriod fp
                    ON bli.FiscalPeriodID = fp.FiscalPeriodID
            LEFT JOIN
                    T_SeasonalFactors sf
                    ON fp.FiscalMonth = sf.MonthNumber
            WHERE
                bli.BudgetHeaderID = :BASEBUDGETHEADERID
              AND fp.FiscalYear >= :SOURCEFISCALYEAR - 1; -- Include prior year for trend

            -- =====================================================================
            -- Calculate trend components using window functions
            -- =====================================================================
            LET _scope_identity_query_id VARCHAR := LAST_QUERY_ID();
            UPDATE T_ForecastWorkspace fw
            SET
                    TrendComponent = CASE
                    WHEN tc.N > 1 AND (tc.N * tc.SumXX - tc.SumX * tc.SumX) <> 0
                    THEN (tc.N * tc.SumXY - tc.SumX * tc.SumY) / (tc.N * tc.SumXX - tc.SumX * tc.SumX)
                    ELSE 0
                END,
                    CyclicalComponent = CASE
                    WHEN tc.SameMonthLastYear IS NOT NULL AND tc.SameMonthLastYear <> 0
                    THEN (tc.ActualAmount - tc.SameMonthLastYear) / tc.SameMonthLastYear
                    ELSE 0
                END,
                    Residual = tc.ActualAmount - tc.MA12,
                    LowerBound = tc.LowerPercentile,
                    UpperBound = tc.UpperPercentile
                FROM
                    (
                    WITH TrendCalc AS (
                SELECT
                                WorkspaceID,
                                GLAccountID,
                                CostCenterID,
                                PeriodSequence,
                                ActualAmount,
                                -- Moving average with different windows
                                AVG(ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                        ORDER BY PeriodSequence
                        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                    ) AS MA3,
                                AVG(ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                        ORDER BY PeriodSequence
                        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                    ) AS MA6,
                                AVG(ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                        ORDER BY PeriodSequence
                        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) AS MA12,
                                -- Trend calculation using linear regression components
                                -- Snowflake has different syntax for these aggregations
                                COUNT(*) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                        ORDER BY PeriodSequence
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS N,
                                SUM(PeriodSequence) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS SumX,
                                SUM(ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS SumY,
                                SUM(PeriodSequence * ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS SumXY,
                                SUM(PeriodSequence * PeriodSequence) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS SumXX,
                                -- LAG/LEAD for comparison
                                LAG(ActualAmount, 1, 0) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                        ORDER BY PeriodSequence
                    ) AS PrevAmount1,
                                LAG(ActualAmount, 12, NULL) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                        ORDER BY PeriodSequence
                    ) AS SameMonthLastYear,
                                -- Percentile calculations
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS MedianAmount,
                                PERCENTILE_CONT(:CONFIDENCELEVEL) WITHIN GROUP (ORDER BY ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS UpperPercentile,
                                PERCENTILE_CONT(1 - :CONFIDENCELEVEL) WITHIN GROUP (ORDER BY ActualAmount) OVER (
                        PARTITION BY
                                    GLAccountID, CostCenterID
                    ) AS LowerPercentile
                FROM
                                PUBLIC.T_ForecastWorkspace
                WHERE
                                IsForecast = 0
            )
                    SELECT
                    *
                    FROM
                    TrendCalc
                    ) AS tc
                WHERE
                    fw.WorkspaceID = tc.WorkspaceID;

            -- Generate forecast period skeleton

            -- =====================================================================
            -- Generate forecast periods and calculate forecasts
            -- =====================================================================
            -- Get last actual period
            SELECT
                MAX(FiscalPeriodID)
            INTO
                :BASELINEENDPERIODID
            FROM
                PUBLIC.T_ForecastWorkspace
            WHERE
                IsForecast = 0;
            INSERT INTO PUBLIC.T_ForecastWorkspace (GLAccountID, CostCenterID, FiscalPeriodID, PeriodSequence, ForecastAmount, SeasonalFactor, LowerBound, UpperBound, IsForecast, CalculationStep)
            WITH FuturePeriods AS (
                SELECT
                    fp.FiscalPeriodID,
                    fp.FiscalMonth,
                    ROW_NUMBER() OVER (ORDER BY fp.FiscalYear, fp.FiscalMonth) AS FutureSequence
                FROM
                    Planning.FiscalPeriod fp
                WHERE
                    fp.FiscalPeriodID > :BASELINEENDPERIODID
                  AND fp.IsClosed = 0
            ),
            BaselineStats AS (
                SELECT
                    GLAccountID,
                    CostCenterID,
                    MAX(PeriodSequence) AS LastActualSequence,
                    AVG(ActualAmount * WeightFactor) / NULLIF(AVG(WeightFactor), 0) AS WeightedAvg,
                    AVG(TrendComponent) AS AvgTrend,
                    STDDEV(ActualAmount) AS StdDev
                FROM
                    PUBLIC.T_ForecastWorkspace
                WHERE
                    IsForecast = 0
                GROUP BY
                    GLAccountID,
                    CostCenterID
            )
            SELECT
                bs.GLAccountID,
                bs.CostCenterID,
                fp.FiscalPeriodID,
                bs.LastActualSequence + fp.FutureSequence,
                -- Different forecast methods
                CASE
                    :FORECASTMETHOD
                    WHEN 'WEIGHTED_AVERAGE' THEN bs.WeightedAvg * NVL(sf.SeasonalFactor, 1.0) * POWER(1 + NVL(:GROWTHRATEOVERRIDE, bs.AvgTrend), fp.FutureSequence)
                    WHEN 'LINEAR_TREND' THEN bs.WeightedAvg + (bs.AvgTrend * (bs.LastActualSequence + fp.FutureSequence))
                    WHEN 'EXPONENTIAL' THEN bs.WeightedAvg * POWER(1 + NVL(:GROWTHRATEOVERRIDE, 0.02), fp.FutureSequence)
                    WHEN 'SEASONAL' THEN bs.WeightedAvg * sf.SeasonalFactor *
                        (1 + NVL(:GROWTHRATEOVERRIDE, 0) * fp.FutureSequence / 12)
                    ELSE bs.WeightedAvg
                END,
                NVL(sf.SeasonalFactor, 1.0),
                -- Confidence interval bounds
                CASE
                    :FORECASTMETHOD
                    WHEN 'WEIGHTED_AVERAGE' THEN bs.WeightedAvg - (1.96 * bs.StdDev * SQRT(fp.FutureSequence))
                    ELSE bs.WeightedAvg * 0.8
                END,
                CASE
                    :FORECASTMETHOD
                    WHEN 'WEIGHTED_AVERAGE' THEN bs.WeightedAvg + (1.96 * bs.StdDev * SQRT(fp.FutureSequence))
                    ELSE bs.WeightedAvg * 1.2
                END,
                1 AS IsForecast,
                fp.FutureSequence
            FROM
                BaselineStats AS bs
            CROSS JOIN FuturePeriods AS fp
            LEFT JOIN
                    T_SeasonalFactors sf
                    ON
                    --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                    (
                    SELECT
                    ANY_VALUE(FiscalMonth)
                    FROM
                    Planning.FiscalPeriod
                    WHERE
                    FiscalPeriodID = fp.FiscalPeriodID
                    ) = sf.MonthNumber
            WHERE
                fp.FutureSequence <= :FORECASTPERIODS;

            -- =====================================================================
            -- Create target budget header and insert forecast data
            -- =====================================================================
            INSERT INTO Planning.BudgetHeader (BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear, StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode, Notes)
            SELECT
                BudgetCode || '_FORECAST_' || TO_CHAR(CURRENT_TIMESTAMP() :: TIMESTAMP, 'YYYYMMDD'),
                BudgetName || ' - Rolling Forecast',
                'ROLLING',
                'FORECAST',
                FiscalYear,
                :SOURCESTARTPERIOD,
                (SELECT
                    MAX(FiscalPeriodID) FROM
                    PUBLIC.T_ForecastWorkspace
                    WHERE
                    IsForecast = 1),
                :BASEBUDGETHEADERID,
                'DRAFT',
                CONCAT('Generated by usp_GenerateRollingForecast at ', :STARTTIME,
                       ' using method: ', :FORECASTMETHOD)
            FROM
                Planning.BudgetHeader
            WHERE
                BudgetHeaderID = :BASEBUDGETHEADERID;
            _scope_identity_query_id := LAST_QUERY_ID();
            TARGETBUDGETHEADERID := SCOPE_IDENTITY() !!!RESOLVE EWI!!! /*** SSC-EWI-TS0095 - SNOWCONVERT AI WAS UNABLE TO DETERMINE THE TARGET TABLE FOR SCOPE_IDENTITY(). NO PRECEDING INSERT TO AN IDENTITY TABLE FOUND IN THE SAME SCOPE. ***/!!!;

            -- Insert forecast line items
            INSERT INTO Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem)
            SELECT
                :TARGETBUDGETHEADERID,
                GLAccountID,
                CostCenterID,
                FiscalPeriodID,
                ForecastAmount,
                0,
                :FORECASTMETHOD,
                'ROLLING_FORECAST'
            FROM
                PUBLIC.T_ForecastWorkspace
            WHERE
                IsForecast = 1
              AND ForecastAmount IS NOT NULL;
            _scope_identity_query_id := LAST_QUERY_ID();

            -- =====================================================================
            -- Generate output based on format
            -- =====================================================================
            IF (:OUTPUTFORMAT = 'PIVOT') THEN
                -- Dynamic PIVOT - Very different in Snowflake
            BEGIN
                     
                     

                -- Build column list using FOR XML PATH
                SELECT
                    INSERT((
                    SELECT DISTINCT
                                LISTAGG ( ',' || PUBLIC.QUOTENAME_UDF(fp.PeriodName), '')
                    FROM
                                PUBLIC.T_ForecastWorkspace fw
                    INNER JOIN
                                    Planning.FiscalPeriod fp
                                    ON fw.FiscalPeriodID = fp.FiscalPeriodID
                    WHERE
                                fw.IsForecast = 1
                    ORDER BY ',' || PUBLIC.QUOTENAME_UDF(fp.PeriodName)
                ).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
                    INTO
                    :PIVOTCOLUMNS;
                    DYNAMICSQL := '
                SELECT
                   gla.AccountNumber,
                   gla.AccountName,
                   cc.CostCenterCode,
                   cc.CostCenterName,
                   ' || :PIVOTCOLUMNS || '
                FROM (
                    SELECT
                       fw.GLAccountID,
                       fw.CostCenterID,
                       fp.PeriodName,
                       fw.ForecastAmount
                    FROM
                       PUBLIC.T_ForecastWorkspace fw
                       INNER JOIN
                          Planning.FiscalPeriod fp
                          ON fw.FiscalPeriodID = fp.FiscalPeriodID
                    WHERE
                       fw.IsForecast = 1
                ) AS SourceData
                   PIVOT (SUM(ForecastAmount)
                    FOR PeriodName IN (''' || :PIVOTCOLUMNS || ''')
                ) AS PivotTable
                   INNER JOIN
                    Planning.GLAccount gla
                    ON PivotTable.GLAccountID = gla.GLAccountID
                   INNER JOIN
                    Planning.CostCenter cc
                    ON PivotTable.CostCenterID = cc.CostCenterID
                ORDER BY gla.AccountNumber, cc.CostCenterCode;';
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!
                    EXECUTE IMMEDIATE :DYNAMICSQL;
            END;
            ELSEIF (:OUTPUTFORMAT = 'SUMMARY') THEN
            BEGIN
                SELECT
                    gla.AccountType,
                    fp.FiscalYear,
                    fp.FiscalQuarter,
                    SUM(fw.ForecastAmount) AS TotalForecast,
                    SUM(fw.LowerBound) AS TotalLowerBound,
                    SUM(fw.UpperBound) AS TotalUpperBound
                FROM
                    PUBLIC.T_ForecastWorkspace fw
                INNER JOIN
                    Planning.GLAccount gla
                    ON fw.GLAccountID = gla.GLAccountID
                INNER JOIN
                    Planning.FiscalPeriod fp
                    ON fw.FiscalPeriodID = fp.FiscalPeriodID
                WHERE
                    fw.IsForecast = 1
                GROUP BY
                    gla.AccountType,
                    fp.FiscalYear,
                    fp.FiscalQuarter
                ORDER BY gla.AccountType, fp.FiscalYear, fp.FiscalQuarter;
            END;
            END IF;
            -- =====================================================================
            -- Calculate and return accuracy metrics as JSON
            -- =====================================================================
            FORECASTACCURACYMETRICS := (
                           SELECT
                    :FORECASTMETHOD AS ForecastMethod,
                    :HISTORICALPERIODS AS HistoricalPeriods,
                    :FORECASTPERIODS AS ForecastPeriods,
                               (SELECT
                    COUNT(*) FROM
                    PUBLIC.T_ForecastWorkspace
                    WHERE
                    IsForecast = 0) AS ActualDataPoints,
                               (SELECT
                    COUNT(*) FROM
                    PUBLIC.T_ForecastWorkspace
                    WHERE
                    IsForecast = 1) AS ForecastDataPoints,
                               (SELECT
                    AVG(ABS(Residual)) FROM
                    PUBLIC.T_ForecastWorkspace
                    WHERE
                    IsForecast = 0) AS MeanAbsoluteResidual,
                               (SELECT
                    STDDEV(ActualAmount) FROM
                    PUBLIC.T_ForecastWorkspace
                    WHERE
                    IsForecast = 0) AS HistoricalStdDev,
                    DATEDIFF(MILLISECOND, :STARTTIME, SYSDATE()) AS ExecutionTimeMs
                !!!RESOLVE EWI!!! /*** SSC-EWI-0021 - FOR CLAUSE NOT SUPPORTED IN SNOWFLAKE ***/!!!
                           FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                       );
        EXCEPTION
            WHEN OTHER THEN
                FORECASTACCURACYMETRICS := (
                               SELECT
                                   'ERROR' AS Status,
                    SQLCODE /*** SSC-FDM-TS0023 - ERROR NUMBER COULD BE DIFFERENT IN SNOWFLAKE ***/ AS ErrorNumber,
                    SQLERRM /*** SSC-FDM-TS0023 - ERROR MESSAGE COULD BE DIFFERENT IN SNOWFLAKE ***/ AS ErrorMessage,
                    ERROR_LINE() !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE 'ERROR_LINE FUNCTION' CLAUSE IS NOT SUPPORTED IN SNOWFLAKE ***/!!! AS ErrorLine
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0021 - FOR CLAUSE NOT SUPPORTED IN SNOWFLAKE ***/!!!
                               FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                           );
                LET DECLARED_EXCEPTION EXCEPTION;
                RAISE DECLARED_EXCEPTION;
        END;

        -- Cleanup
        DROP TABLE IF EXISTS PUBLIC.T_ForecastWorkspace;
        DROP TABLE T_SEASONALFACTORS;
    END;
$$;
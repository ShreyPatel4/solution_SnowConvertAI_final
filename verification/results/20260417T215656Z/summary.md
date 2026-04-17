# Proc 1 Migration Verification — run 20260417T215656Z

Source proc: `original/src/StoredProcedures/usp_ProcessBudgetConsolidation.sql`  
SQL Server baseline (patched): `sqlserver/04_procedures.sql`  
Snowflake migration: `snowflake/04_procedures.sql`  

## 1. Re-seed fixtures on both engines
- SQL Server: fixtures reloaded (`sqlserver/10_seed.sql`)
- Snowflake:  fixtures reloaded (`snowflake/10_seed.sql`)

## 2. Reload procedures
- SQL Server: `usp_ProcessBudgetConsolidation` reloaded
- Snowflake:  `usp_ProcessBudgetConsolidation` reloaded

## 3. Invoke proc 1 on both engines (identical inputs)
- SQL Server: target_id=11, rows_processed=23, error=(none)
- Snowflake:  target_id=206, rows_processed=23, inserted=11, elim_updated=1

## 4. Fetch consolidated rows (from the newly-created target header on each side)
- SQL Server consolidated rows: 11
- Snowflake consolidated rows:  11

## 5. Row-by-row diff (natural key + FinalAmount)
- **PASS** — all 11 rows match on (GLAccountID, CostCenterID, FiscalPeriodID, FinalAmount)

## 6. Aggregate diff (SUM, COUNT)
- SQL Server: SUM=52500.0000, COUNT=11
- Snowflake:  SUM=52500.0000, COUNT=11
- **PASS**

## Overall
- **VERIFICATION PASSED**: SQL Server baseline and Snowflake migration produced identical consolidated outputs.


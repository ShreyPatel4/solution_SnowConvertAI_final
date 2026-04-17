# Proc 1 Migration Verification — run 20260417T150151Z

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
- SQL Server: target_id=5, rows_processed=23, error=(none)
- Snowflake:  target_id=2, rows_processed=23, inserted=11, elim_updated=1

## 4. Fetch consolidated rows (from the newly-created target header on each side)
- SQL Server consolidated rows: 11
- Snowflake consolidated rows:  11

## 5. Row-by-row diff (natural key + FinalAmount)
- **FAIL** — differences below:
  - row 9: sqlserver=('4', '5', '1', '1000.0000') / snowflake=('4', '5', '1', '0.0000')
  - row 10: sqlserver=('5', '5', '1', '-2000.0000') / snowflake=('5', '5', '1', '-1000.0000')

## 6. Aggregate diff (SUM, COUNT)
- SQL Server: SUM=52500.0000, COUNT=11
- Snowflake:  SUM=52500.0000, COUNT=11
- **PASS**

## Overall
- **VERIFICATION FAILED** — see row-by-row + aggregate sections above.


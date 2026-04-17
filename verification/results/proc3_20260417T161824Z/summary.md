# Proc 3 Migration Verification — run 20260417T161824Z

Source proc: `original/src/StoredProcedures/usp_ExecuteCostAllocation.sql`  
SQL Server baseline (patched): `sqlserver/05_procedures.sql`  
Snowflake migration:           `snowflake/05_procedures.sql`  

## 1. Re-seed fixtures on both engines
- SQL Server: fixtures reloaded (`sqlserver/10_seed.sql`)
- Snowflake:  fixtures reloaded (`snowflake/10_seed.sql`)

## 2. Reload procedures
- SQL Server: `usp_ExecuteCostAllocation` reloaded
- Snowflake:  `usp_ExecuteCostAllocation` reloaded

## 3. Invoke proc 3 on both engines (identical inputs)
- SQL Server: rows_allocated=6, warning=(none)
- Snowflake:  rows_allocated=6, iteration_count=2, queue_size=4, warning=(none)

## 4. Fetch allocated rows (IsAllocated=1 children inserted by proc 3)
- SQL Server allocated rows: 6
- Snowflake allocated rows:  6

## 5. Row-by-row diff (GL, CC, FP, OriginalAmount, SourceLineID, Pct)
- **PASS** — all 6 rows match

## 6. Aggregate diff (SUM(OriginalAmount), COUNT) on allocated children
- SQL Server: SUM=32600.0000, COUNT=6
- Snowflake:  SUM=32600.0000, COUNT=6
- **PASS**

## Overall
- **VERIFICATION PASSED**: SQL Server baseline and Snowflake migration produced identical allocation outputs.


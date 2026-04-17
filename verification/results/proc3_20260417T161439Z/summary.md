# Proc 3 Migration Verification — run 20260417T161439Z

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
- **Snowflake proc failed**: {'sqlcode': 2031, 'sqlerrm': 'SQL compilation error:\nUnsupported subquery type cannot be evaluated inside Function object: FN_GETALLOCATIONFACTOR', 'sqlstate': '42601', 'success': False}

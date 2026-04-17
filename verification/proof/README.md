# Proof of migration: live Snowflake execution logs

Captured 2026-04-17 after the final commit. Every log in this directory is raw stdout from `pipeline/run_sql.py` against the live Snowflake account (PLANNING_DB.PLANNING, WH_XS).

## Index

| File | What it proves |
|---|---|
| `01_show_procedures.log` | All 6 `usp_*` procedures exist in `PLANNING_DB.PLANNING` with their compiled signatures. Output from `SHOW PROCEDURES LIKE 'usp_%'`. |
| `02_proc1_verify.log` | Proc 1 bit-exact cross-engine run via `pipeline/verify.py`. Tail line: `VERIFICATION PASSED`. |
| `03_proc3_verify.log` | Proc 3 bit-exact cross-engine run via `pipeline/verify_proc3.py`. Tail line: `VERIFICATION PASSED`. |
| `04_proc2_smoke.log` | Proc 2 hybrid-AI smoke-test `CALL`. Return object: `status=COMPLETED`, `exit_code=0`, 3 steps, 0 failed, Lock Period affects 2 rows. |
| `05_proc4_smoke.log` | Proc 4 hybrid-AI smoke-test `CALL`. Return object: `success=true`, 18 historical rows read, **39 forecast rows inserted** across Apr-Jun 2026. |
| `06_proc5_smoke.log` | Proc 5 hybrid-AI smoke-test `CALL` on IC fixture (BudgetHeader 2). Return object: `success=true`, **8 intercompany pairs constructed** (2 RECONCILED within tolerance, 6 MATCHED but out-of-tolerance), TotalVariance=2400. |
| `07_proc6_smoke.log` | Proc 6 hybrid-AI smoke-test `CALL`. Return object: `success=true`, `rows_imported=1`, `invalid_rows=0`, `rows_rejected=0`. |

## Six procedures registered in Snowflake

From `01_show_procedures.log`:

```
USP_BULKIMPORTBUDGETDATA(VARCHAR, ARRAY, NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMBER, BOOLEAN, NUMBER) RETURN VARIANT
USP_EXECUTECOSTALLOCATION(NUMBER, VARCHAR, NUMBER, BOOLEAN, NUMBER, NUMBER, VARCHAR, ARRAY) RETURN VARIANT
USP_GENERATEROLLINGFORECAST(NUMBER, NUMBER, NUMBER, VARCHAR, VARCHAR, NUMBER, NUMBER, VARCHAR) RETURN VARIANT
USP_PERFORMFINANCIALCLOSE(NUMBER, NUMBER, VARCHAR, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, VARCHAR, BOOLEAN) RETURN VARIANT
USP_PROCESSBUDGETCONSOLIDATION(NUMBER, NUMBER, VARCHAR, BOOLEAN, BOOLEAN, VARIANT, NUMBER, BOOLEAN) RETURN VARIANT
USP_RECONCILEINTERCOMPANYBALANCES(NUMBER, DATE, VARIANT, NUMBER, NUMBER, BOOLEAN) RETURN VARIANT
```

## How to reproduce

From the repo root, with a live Snowflake connection configured in `.env`:

```bash
.venv/bin/python pipeline/run_sql.py /tmp/proof_show.sql    # see SHOW PROCEDURES
.venv/bin/python pipeline/verify.py                          # proc 1 (diffs vs SQL Server)
.venv/bin/python pipeline/verify_proc3.py                    # proc 3 (diffs vs SQL Server)
.venv/bin/python pipeline/run_sql.py /tmp/test_proc2.sql     # happy-path CALL
.venv/bin/python pipeline/run_sql.py /tmp/test_proc4.sql
.venv/bin/python pipeline/run_sql.py /tmp/test_proc5.sql
.venv/bin/python pipeline/run_sql.py /tmp/test_proc6.sql
```

The smoke-test `CALL` scripts are inlined in `verification/results/smoke_20260417T204500Z/summary.md`.

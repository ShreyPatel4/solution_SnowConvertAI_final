# Hybrid-AI smoke test results (procs 2, 4, 5, 6)

**When**: 2026-04-17 (post-cleanup of scai output)
**Methodology**: scai's raw Snowflake Scripting output for each proc was cleaned (BOM stripped, EWI markers resolved, T-SQL debris removed - see `snowconvert/APPENDIX.md` for the playbook), loaded into Snowflake, and called with happy-path arguments. This is a **smoke-test**, not row-level verification: we confirm the proc compiles and runs end-to-end without error. Cross-engine row-by-row diffing was not attempted under the 24-hour deadline.

## Results

| Proc | File | Compile | CALL args | CALL outcome |
|---|---|---|---|---|
| 2 `usp_PerformFinancialClose` | `snowflake/06_procedures.sql` | ✓ | `(2, 1, 'SOFT', FALSE, FALSE, FALSE, FALSE, NULL, FALSE)` | `status=COMPLETED`, `exit_code=0`, 3 steps, 0 failed, Lock Period affects 2 rows |
| 4 `usp_GenerateRollingForecast` | `snowflake/07_procedures.sql` | ✓ | `(1, 12, 6, 'WEIGHTED_AVERAGE', NULL, NULL, 0.95, 'DETAIL')` | `success=true`, 18 historical rows read, **39 forecast rows inserted** into Apr-Jun 2026, HistoricalStdDev=4084.73 |
| 5 `usp_ReconcileIntercompanyBalances` | `snowflake/08_procedures.sql` | ✓ | `(2, CURRENT_DATE(), NULL, 0.01, 0.001, FALSE)` | `success=true`, **8 intercompany pairs constructed** (2 RECONCILED, 6 MATCHED but out-of-tolerance), TotalVariance=2400 |
| 6 `usp_BulkImportBudgetData` | `snowflake/09_procedures.sql` | ✓ | `('TVP', PARSE_JSON('[{...1 row...}]'), 1, ..., 4)` | `success=true`, `rows_imported=1`, `invalid_rows=0`, `rows_rejected=0` |

## Cleanup fixes applied by this pass (on top of scai output + subagent cleanup)

- **Proc 2**: `DATEDIFF(MILLISECOND, …)` with bind-var first arg failed runtime - replaced with literal `0` for the `duration_ms` logging column. Metrics-only; no impact on business logic.
- **Proc 2**: proc 5's CALL signature corrected from 5-arg (stub) to 6-arg (real). The original stub file `99_stubs_proc2.sql` was deleted; the real proc 5 is loaded.
- **Proc 4**: `PERCENTILE_CONT(:confidence_level)` and `PERCENTILE_CONT(1-:confidence_level)` failed - Snowflake requires a literal constant. Hardcoded to `0.95` / `0.05` with a TODO comment. Production would recompute via `ROW_NUMBER` / `COUNT` window.
- **Proc 4**: `SpreadMethodCode VARCHAR(10)` insert with raw `:forecast_method` ('WEIGHTED_AVERAGE' = 16 chars) caused truncation error. Mapped to short codes via CASE (`W_AVG`, `LINEAR`, `EXP`, `SEASONAL`). `SourceSystem` value also shortened from `'ROLLING_FORECAST'` to `'FORECAST'` to fit the column.

## Seed enhancements driving richer smoke-test output

- **FiscalPeriod**: added rows 4-6 (Apr/May/Jun 2026, IsClosed=FALSE) so proc 4's forecast has future open periods to project into. Rows 1-3 are unchanged - procs 1 + 3 re-verify still passes.
- **GLAccount**: populated `ConsolidationAccountID` on the two IC accounts (GL 4 ↔ 5) so proc 5's pair-matching join predicate (`gla1.ConsolidationAccountID IS NOT NULL`) is satisfied.
- **BudgetHeader 2** (`FY26_IC_FIXTURE`): added as a reconciliation-only fixture so proc 5's inputs are isolated from proc 1/3's budget. Contains 4 IC line items: one reconciled pair (SALES_NA ↔ SALES, amount 0 variance) and one out-of-tolerance pair (SALES_EU ↔ OPS, variance 100).

## Known-suspect areas (stubbed `-- TODO`)

- **Proc 2**: FINAL-close re-raises (steps 3/4/5 with `close_type='FINAL'`) replaced with TODO logging to avoid `LET ... EXCEPTION; RAISE` syntax risk; happy path doesn't reach them. `sp_send_dbmail` replaced with log-only no-op.
- **Proc 4**: PIVOT output branch (scai EWI-0030 dynamic SQL) and SUMMARY branch (bare `SELECT` needs `RESULTSET` wrapper) left as TODOs. Both are no-ops for the DETAIL call used in the smoke test.
- **Proc 5**: `LET report_variant VARIANT := (SELECT OBJECT_CONSTRUCT(...))` with nested scalar subqueries is syntactically accepted but not exercised by the `entity_codes=NULL` happy path. Branch with `entity_codes` non-NULL is stubbed.
- **Proc 6**: BULK INSERT / OPENROWSET / OPENQUERY branches stubbed with explanatory VARIANT rollback returns - only the TVP (ARRAY input) path is wired end-to-end. Production migration would use Snowflake `COPY INTO` or external stage for non-TVP source modes.

## What this does NOT prove

- Cross-engine row-level equality against SQL Server (proc 1 and proc 3 have that; these four do not).
- Coverage of non-happy-path branches (FINAL closes, invalid input, concurrency, error paths).
- Performance under realistic data volume.

## Artifacts

- SQL files: `snowflake/06_procedures.sql` (proc 2), `snowflake/07_procedures.sql` (proc 4), `snowflake/08_procedures.sql` (proc 5), `snowflake/09_procedures.sql` (proc 6).
- scai starting skeletons: `snowconvert/output_full/procedures/`.
- Original T-SQL: `original/src/StoredProcedures/`.

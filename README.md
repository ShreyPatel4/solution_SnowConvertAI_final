# SQL Server → Snowflake Migration - SnowConvert AI Take-Home

Submission for the SnowConvert AI take-home. Deliverables per the brief: (1) working code for each converted proc, (2) how I verified correctness, (3) how I used AI.

## 1. Problem (restated from the PDF)

The provided `src/` zip is an enterprise financial-planning schema - 8 tables, 3 user-defined table types, 3 functions, 3 views, 6 stored procedures - authored for SQL Server 2022 using a wide surface of SQL Server-specific features (`HIERARCHYID`, XML columns with XQuery, FILESTREAM, `FOR SYSTEM_TIME`, `sp_getapplock`, `sp_executesql`, cursors, temporal tables, persisted computed columns, table-valued parameters).

The task: stand up SQL Server locally, provision a Snowflake account, migrate the schema, and port as many of six named procedures as reasonable. At minimum: proc 1.

## 2. Scope delivered - all 6 procs migrated, two tiers

| # | Stored procedure | Approach | Status |
|---|---|---|---|
| 1 | `usp_ProcessBudgetConsolidation` | **Hand-migrated** | **Value-exact verified** - 11 rows, `SUM(FinalAmount) = 52500.0000` |
| 3 | `usp_ExecuteCostAllocation` | **Hand-migrated** | **Value-exact verified** - 6 rows, `SUM(OriginalAmount) = 32600.0000` |
| 2 | `usp_PerformFinancialClose` | **Hybrid AI** - scai + human EWI cleanup | `status=COMPLETED`, 3 steps, 0 failed, 2 rows locked on Feb 2026 |
| 4 | `usp_GenerateRollingForecast` | **Hybrid AI** - scai + human EWI cleanup | `success=true`, 18 historical rows read + **39 forecast rows inserted** across Apr-Jun 2026 |
| 5 | `usp_ReconcileIntercompanyBalances` | **Hybrid AI** - scai + human EWI cleanup | `success=true`, **8 IC pairs constructed** (2 reconciled, 6 out-of-tolerance), TotalVariance=2400 |
| 6 | `usp_BulkImportBudgetData` | **Hybrid AI** - scai + human EWI cleanup | `success=true`, 1 TVP row imported, 0 invalid, 0 rejected |

Schema (8 tables + 3 UDTTs), the functions + views the converted procs depend on, and identical seed fixtures are loaded on **both engines**.

**Tier definitions**:
- **Value-exact verified**: SQL Server baseline and Snowflake migration re-seeded from identical fixtures, proc called on both sides, output diffed row-by-row on natural keys + aggregate. Zero divergence.
- **Hybrid AI**: scai's raw auto-translation (at `snowconvert/output_full/procedures/`) was cleaned up by hand (BOM stripped, all `!!!RESOLVE EWI!!!` markers resolved or stubbed, T-SQL debris like `@@TRANCOUNT` / `SAVE TRANSACTION` / `OUTPUT INTO` dropped per the playbook documented in `snowconvert/APPENDIX.md`). Smoke-test = proc compiles in Snowflake and runs to completion on happy-path input without error. **Does NOT prove row-level equivalence** to the SQL Server baseline - branch coverage is limited to the happy path and a small number of TODO-stubbed edge branches. See `verification/results/smoke_20260417T204500Z/summary.md` for per-proc smoke-test inputs, outputs, and known-suspect areas.

## 3. How to run

```bash
# SQL Server
docker run -d --name sqlserver -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
sqlcmd -I -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i sqlserver/00_bootstrap.sql
sqlcmd -I -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d Planning -i sqlserver/01_schema.sql

# Snowflake (reads PAT from .env - see .env.example)
python pipeline/run_sql.py snowflake/00_bootstrap.sql
python pipeline/run_sql.py snowflake/01_schema.sql
python pipeline/run_sql.py snowflake/02_functions.sql
python pipeline/run_sql.py snowflake/03_views.sql
python pipeline/run_sql.py snowflake/04_procedures.sql   # proc 1 (hand)
python pipeline/run_sql.py snowflake/05_procedures.sql   # proc 3 (hand)
python pipeline/run_sql.py snowflake/06_procedures.sql   # proc 2 (hybrid AI)
python pipeline/run_sql.py snowflake/07_procedures.sql   # proc 4 (hybrid AI)
python pipeline/run_sql.py snowflake/08_procedures.sql   # proc 5 (hybrid AI)
python pipeline/run_sql.py snowflake/09_procedures.sql   # proc 6 (hybrid AI)
python pipeline/run_sql.py snowflake/10_seed.sql

# Verify (row-level equality, procs 1 + 3)
python pipeline/verify.py
python pipeline/verify_proc3.py

# Smoke-test the 4 hybrid AI procs (happy-path CALL per verification/results/smoke_*/summary.md)
```

## 4. Verification (PDF Q2)

`pipeline/verify.py` / `pipeline/verify_proc3.py` is the authority. Every run:

1. Re-seeds both engines from identical fixture files so surrogate IDs line up by intent (via `IDENTITY_INSERT` on SQL Server; Snowflake `AUTOINCREMENT` accepts explicit values).
2. Reloads the proc - the harness always exercises the committed code, not a stale in-engine copy.
3. Calls the proc on both sides with identical inputs.
4. Pulls the rows the proc inserted/updated and diffs them **by natural key** - `(GLAccountID, CostCenterID, FiscalPeriodID)` + value columns. Surrogate IDs are never compared (IDENTITY vs. AUTOINCREMENT seed independently).
5. Runs an independent aggregate check (`SUM` + `COUNT`) as a second diff surface.
6. Writes a timestamped Markdown report to `verification/results/`.

**Intentionally excluded from the diff**: `RowHash` (SQL Server `HASHBYTES` returns `VARBINARY`; Snowflake `SHA2` returns lowercase hex `VARCHAR` - byte-equality would need canonical DECIMAL string formatting; the value-column diff already covers correctness), wall-clock timestamps, random-UUID columns.

Latest passing runs:
- `verification/results/20260417T212621Z/summary.md` - proc 1 PASSED
- `verification/results/proc3_20260417T212642Z/summary.md` - proc 3 PASSED

Live execution logs for all 6 procs (raw stdout from Snowflake) are in `verification/proof/`. The `SHOW PROCEDURES` log there lists all 6 compiled signatures; per-proc `.log` files contain the actual `CALL` results and VARIANT returns.

## 5. AI usage (PDF Q3)

Three layers, each with a different amount of trust placed in the AI output.

**Layer 1: Snowflake's own `scai` CLI.**
Installed `scai` v2.20.0 (Homebrew cask), ran it on pristine T-SQL, captured raw output under `snowconvert/output/`. Headline numbers from `snowconvert/output/reports/Assessment.csv`: 23 files, 816 LOC, 74 issues, 94.44% CodeCompletenessScore, 3 Critical parse errors on XML-index DDL, 16 `!!!RESOLVE EWI!!!` markers in the proc body. scai emits Snowflake Scripting (not JavaScript UDFs), which is the target I picked by hand. The output is a well-structured skeleton but does not compile as-is: the EWI markers are hand-off signals for a human reviewer. Full construct-by-construct comparison in `snowconvert/APPENDIX.md`.

**Layer 2: hand-authored AI translation pipeline.**
`pipeline/translate.py` reads a Snowflake-Scripting-pinned prompt template, fills `SOURCE_TSQL` / `SCHEMA_CONTEXT` / `DEPENDENCY_CONTEXT` placeholders, calls an LLM, and parses a strict JSON output: `{translated_sql, rationale, confidence, lossy_conversions[], open_questions[]}`. The `lossy_conversions` field is required output: the model must list every construct where semantics don't fully carry over, which forces explicit acknowledgement instead of silent mistranslation.

**Layer 3: verification harness.**
Anything AI produces has to pass the cross-engine row-level diff to count as migrated. AI writes; the harness decides.

**Honest scope**: procs 1 and 3 are hand-translated + value-exact verified (see §6). Procs 2, 4, 5, 6 are **hybrid**: scai produced the first-pass skeleton; I then cleaned up EWI markers and T-SQL debris per the APPENDIX playbook, loaded into Snowflake, and ran a happy-path smoke-test CALL on each. All four return `success: true` (or the equivalent `status: COMPLETED` for proc 2) - see `verification/results/smoke_20260417T204500Z/summary.md`. Row-level cross-engine verification was out of scope for the 24-hour window on these four; their cleanup still includes TODO-stubs on branches the smoke test doesn't exercise.

## 6. What I did manually - and why

For a single well-understood proc, per-statement human reasoning is faster than a pipeline round-trip: cursor → recursive CTE? cursor → `LAG` window? keep it as a Scripting loop? every translation is a judgment call that benefits from context the model can't take in at once. Manual translation also surfaced four real issues:

- *(my bug, caught by `verify.py`)* `SQLROWCOUNT` used as a bare identifier inside a `VALUES` clause - needs DECLARE + assign + `:var` bind.
- *(my bug, caught by `verify.py`)* `LEAD` used where the T-SQL cursor's `FETCH RELATIVE 1`-reassigns-vars semantic actually needed `LAG` - aggregates matched but row-level distribution didn't.
- *(bug in the original T-SQL, preserved in the scai input)* Proc 1 runs dynamic SQL via `sp_executesql` that references `@ConsolidatedAmounts` (a table variable). Dynamic-SQL batches cannot see the caller's table variables - fails at runtime. Patched in `sqlserver/04_procedures.sql` with inline IF branches; Snowflake migration avoids the dynamic SQL entirely.
- *(bug in the original T-SQL)* Proc 1 inserts `'CONSOLIDATED'` (12 chars) into `SpreadMethodCode VARCHAR(10)` - silent truncation. Patched to `'CONSOL'` on both engines with a documenting comment.

Proc 3 (`usp_ExecuteCostAllocation`) surfaced one Snowflake-specific deviation:
- `fn_GetAllocationFactor` (scalar UDF with a subquery) is rejected inside `INSERT` with `"Unsupported subquery type cannot be evaluated inside Function object"`. Inlined as a `LEFT JOIN CostCenter` in the Snowflake migration; functionally equivalent.

## 7. scai vs. hand-crafted - what I observed

| Metric | scai output (proc 1) | Hand-crafted (proc 1) |
|---|---|---|
| Language choice | Snowflake Scripting | Snowflake Scripting |
| Lines of output | 595 | 383 |
| `!!!RESOLVE EWI!!!` markers | 16 | 0 |
| Compiles on Snowflake as-is | No - fails on literal marker | Yes |
| Passes `pipeline/verify.py` | N/A - can't compile | Yes (value-exact on natural key) |
| Latent Bug #1 (dynamic SQL on `@`-table) | Accidentally neutralized - Snowflake temp tables ARE visible to dynamic SQL; no warning emitted | Deliberately unrolled + documented |
| Latent Bug #2 (`'CONSOLIDATED'` into `VARCHAR(10)`) | No - carried through verbatim twice | Yes (`'CONSOL'`) |
| Cursors rewritten to set-based | No - preserved as Scripting loops with PRF-0003 warning | Yes (GROUP BY + `LAG`) |

Full table + 3-paragraph synthesis in `snowconvert/APPENDIX.md`. Install + invocation history in `snowconvert/RUN_LOG.md`.

**Takeaway**: the right production architecture is **both**. scai as Layer 1 (syntactic translation in seconds), an AI pipeline as Layer 2 (semantic review + EWI-marker resolution), `verify.py` as Layer 3 (cross-engine validation).

## 8. Data-engineering remarks

Things a DE would flag to a production migration owner, not covered elsewhere in the repo:

1. **Constraints become suggestions.** Snowflake tracks PK/FK/UNIQUE/CHECK as metadata only - only `NOT NULL` is enforced at runtime. Procs that relied on "the INSERT will fail if the FK is bad" are now silent data-quality risks. The SCOPE_IDENTITY substitute in proc 1 uses `UQ_BudgetHeader_Code_Year` as a lookup - a duplicate would silently return the wrong row. Add app-level validation or dbt tests on every load.
2. **`sp_getapplock` and `WAITFOR DELAY` just disappear.** Snowflake's MVCC + statement isolation removes the need for coarse app-level locks; busy-wait throttles become no-ops. Proc 3 now runs concurrently across scenarios - usually a win, occasionally a correctness bug; when it's the latter, the fix is MERGE on a natural key, not a lock.
3. **Time Travel is not a system-versioned table.** 1-day default, 90-day Enterprise cap. Any proc reading `FOR SYSTEM_TIME AS OF <T>` beyond 90 days breaks. I kept `CostCenterHistory` as a proc-maintained mirror for the durable audit trail.
4. **Cross-engine `SHA2` is a trap.** Two-engine hash equality needs matching DECIMAL CAST-to-string formatting. Don't use RowHash as a migration-correctness signal; use natural-key + aggregate diffs.
5. **Surrogate IDs are not comparable.** IDENTITY vs. AUTOINCREMENT seed independently. The harness enforces that surrogate IDs never enter a diff.
6. **Warehouse sizing is a migration step, not an ops detail.** `WH_XS` is fine for 12-row fixtures; useless for 100M-row production. A real plan sizes per-proc and measures wall time + credits burned, not just correctness.
7. **Schema DDL is the under-weighted hard part.** FILESTREAM → external stage is architectural. HIERARCHYID → materialized path costs every writer a `HierarchyLevel` update. Persisted computed columns become proc-maintained regular columns. These land through migration windows as code changes, not config.

## 9. Repo layout

```
solution_SnowConvertAI_final/
├── README.md                        this file
├── sqlserver/                       source baseline: bootstrap, schema, proc 1 + 3, seed
├── snowflake/                       target migration: bootstrap, schema, functions, views,
│                                    04_procedures (proc 1), 05 (proc 3),
│                                    06 (proc 2 hybrid), 07 (proc 4 hybrid),
│                                    08 (proc 5 hybrid), 09 (proc 6 hybrid), 10_seed
├── pipeline/                        extract.py, translate.py, run_sql.py, verify.py,
│                                    verify_proc3.py, prompts/translate_proc.md
├── snowconvert/                     scai install + run artifacts:
│                                      input/   pristine T-SQL fed to scai
│                                      output/  single-proc run (proc 1 only)
│                                      output_full/  full 5-proc run
│                                      logs/    scai stdout logs (code add + convert)
│                                      APPENDIX.md, RUN_LOG.md, make_loadable.py
├── verification/
│   ├── results/                     timestamped diff reports + smoke-test summary
│   └── proof/                       raw Snowflake stdout: SHOW PROCEDURES + 6 CALL logs
└── original/                        pristine materials as received (PDF + zip + unzipped)
```

## 10. What I'd do with more time

Promote procs 2, 4, 5, 6 from **hybrid smoke-tested** to **value-exact verified** by writing per-proc verification harnesses on the `verify.py` / `verify_proc3.py` pattern. Resolve the TODO-stubbed branches each proc currently skips (FINAL-close re-raises in proc 2; PIVOT + SUMMARY modes in proc 4; `entity_codes` non-NULL branch in proc 5; BULK INSERT / OPENROWSET / OPENQUERY modes in proc 6 - see `verification/results/smoke_*/summary.md` for the exact list). Rewrite proc 4's `PERCENTILE_CONT` via `ROW_NUMBER` / `COUNT` so `confidence_level` can be dynamic again. Wrap `translate.py` in a retry loop that feeds compile/verify errors back into the prompt - right now it's one-shot. Add parameter-branch coverage to the harnesses (currently only happy path - `INCREMENTAL` consolidation, `@IncludeEliminations=0`, proc 3's `EXCLUSIVE` concurrency path are untested). Seed a 100× larger fixture to exercise `@MaxIterations`, rule-dependency cycle detection, and non-trivial reconciliation pairs. Canonicalise DECIMAL formatting so `RowHash` can re-enter the diff.

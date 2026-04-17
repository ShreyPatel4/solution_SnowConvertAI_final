# Appendix: scai vs. Hand-Crafted Migration — Construct-by-Construct Comparison

> **Status: MEASURED (2026-04-17).** scai was installed (`brew install --cask snowconvert-ai`, v2.20.0) and run on the pristine T-SQL under `snowconvert/input/`. Output is preserved verbatim at `snowconvert/output/`; assessment reports at `snowconvert/output/reports/` (`Issues.csv`, `Assessment.csv`, `SqlObjects.csv`).
>
> **Input under test:** `snowconvert/input/procedures/usp_ProcessBudgetConsolidation.sql` (pristine T-SQL — no in-file patches).
> **Hand-crafted reference:** `snowflake/04_procedures.sql` (verified bit-exact on 11 rows + aggregate by `pipeline/verify.py`).
> **scai output (raw):** `snowconvert/output/procedures/usp_processbudgetconsolidation.sql` (595 lines).
> **scai Assessment:** 23 files processed, 816 LOC converted, **74 issues total** (10 unique EWI / 9 unique FDM / 1 unique PRF), `CodeCompletenessScore = 94.44%`, **3 Critical parse errors** on schema DDL files.
>
> **Headline: scai's raw output does not compile on Snowflake as-is.** It fails at the literal string `!!!RESOLVE EWI!!!` on line 39 — scai emits these markers as a mandatory signal that human post-editing is required before load. The hand-crafted version compiles cleanly and passes `pipeline/verify.py`.

---

## Comparison Table — 12 T-SQL Constructs (measured)

| # | T-SQL Construct (input) | scai Output (MEASURED) | Hand-Crafted Output | Delta | Signal Value |
|---|---|---|---|---|---|
| 1 | **Table variables** (`DECLARE @ProcessingLog TABLE(...)`, `@HierarchyNodes`, `@ConsolidatedAmounts`) with inline `INDEX` clauses. | `CREATE OR REPLACE TEMPORARY TABLE T_PROCESSINGLOG / T_HIERARCHYNODES / T_CONSOLIDATEDAMOUNTS` emitted inside the proc body. `INDEX IX_...` clauses dropped with `SSC-FDM-0021` comments. `PRIMARY KEY` preserved. Identifier `@X` → `:X` uniformly. | `CREATE OR REPLACE TEMPORARY TABLE t_processing_log / t_consolidated_amounts` with same intent. Naming prefix `t_` + snake_case is cosmetic choice. | **Converged.** Same Snowflake construct, cosmetic naming difference. | **Low.** Both translators mastered the bread-and-butter idiom. |
| 2 | **FAST_FORWARD cursor** iterating hierarchy bottom-up with per-row `UPDATE` + `MERGE`. | Preserved as Snowflake Scripting `CURSOR` + `WHILE (:FETCH_STATUS = 0) LOOP ... FETCH ... END LOOP`. Per-row `UPDATE`/`MERGE` preserved. Warnings: `SSC-FDM-TS0013` (rows non-modifiable) + `SSC-PRF-0003` (fetch-in-loop is a performance-degrading pattern). | **Set-based**: single `INSERT INTO t_consolidated_amounts SELECT ... GROUP BY GLAccountID, CostCenterID, FiscalPeriodID`. Hierarchy ordering proven irrelevant by inspecting downstream use of `SubtotalAmount` (never flows out). | scai preserved the loop; hand-crafted collapsed to one set-based statement. scai explicitly tagged the loop as performance-degrading — the warning is correct, but scai didn't take the next step of rewriting. | **High.** Defining T-SQL→Snowflake question: does the translator *understand the cursor is an aggregation*? scai: no. Hand-crafted: yes. |
| 3 | **SCROLL KEYSET cursor** with `FETCH RELATIVE 1` + `FETCH PRIOR` + `FOR UPDATE OF` for intercompany elimination. | Preserved the cursor with **two `!!!RESOLVE EWI!!!` markers** (`SSC-EWI-TS0037` non-scrollable + `SSC-EWI-0058` `FOR UPDATE` unsupported). Forces non-compilable output that requires human cleanup. | `UPDATE t_consolidated_amounts ... FROM (SELECT ..., LAG(FinalAmount) OVER (ORDER BY GLAccountID, CostCenterID) AS prev_amt ...) p WHERE p.prev_amt = -p.FinalAmount`. `LAG` (not `LEAD`) replicates the `FETCH RELATIVE 1` + variable-reassignment semantic exactly — caught as a bug during first diff-run. | scai cannot express scroll semantics; emits `!!!RESOLVE EWI!!!` and hands off. Hand-crafted recognized the pattern is a window function. | **Very high.** This construct has no 1:1 rewrite. scai correctly refuses (emits unusable output with markers); hand-crafted found the window-function equivalent. |
| 4 | **Named savepoints** (`SAVE TRANSACTION SavePoint_AfterHeader`; conditional `ROLLBACK TRANSACTION <savepoint>` in CATCH). | Emitted `SAVE TRANSACTION SavePoint_AfterHeader;` and `ROLLBACK SavePoint_AfterHeader` **verbatim**, flagged with `SSC-EWI-TS0106` + `SSC-EWI-0073`. Non-compilable. | Dropped. Rationale documented: intermediate state is built in temp tables, so whole-transaction rollback has the same net effect as savepoint rollback on this proc. | scai flags + emits non-runnable SQL; hand-crafted simplifies away. | **Medium-high.** Silent divergence on error paths would be worse; scai at least makes the non-equivalence visible via `!!!RESOLVE EWI!!!`. |
| 5 | **Dynamic SQL + `sp_executesql` + `@ConsolidatedAmounts` table-variable reference** (Latent Bug #1). | scai translated `@ConsolidatedAmounts` → temp table `T_ConsolidatedAmounts` (see Row 1), which **silently neutralizes the latent bug** — Snowflake temp tables ARE visible to `EXECUTE IMMEDIATE`. scai also generated a helper UDF `PUBLIC.TRANSFORM_SP_EXECUTE_SQL_STRING_UDF` to simulate sp_executesql's parameter-binding, called via `EXECUTE IMMEDIATE PUBLIC.TRANSFORM_SP_EXECUTE_SQL_STRING_UDF(...)`. No warning about the original T-SQL anti-pattern. | Side-stepped by **unrolling the dynamic SQL into static IF branches** on `:rounding_precision` + `:include_zero_balances`. Eliminates the risk by construction and is more readable. The latent bug is explicitly documented in the SQL Server baseline's patch comment. | scai accidentally "fixed" the bug via substrate change — fixed for the wrong reason, without informing the operator. Hand-crafted fixed deliberately and documented. | **Highest.** The outcome differs at the *analysis* level, not the syntax level. scai's silent rescue is precisely the class of behavior that makes tool-only migrations dangerous: the tool handled the happy path by accident and would hand a real bug into production in a non-equivalent case. |
| 6 | **`OUTPUT inserted.X INTO @Table`** (twice). | Preserved `OUTPUT inserted.BudgetHeaderID, inserted.BudgetCode INTO @InsertedHeaders` **verbatim** inside the `INSERT`, flagged `SSC-EWI-0021` (OUTPUT CLAUSE NOT SUPPORTED). Non-compilable. Requires human cleanup. | Both `OUTPUT INTO` blocks removed; downstream code was re-read-by-natural-key (`BudgetCode + '_CONSOL_' + YYYYMMDD`) so capture wasn't needed. Dead-code elimination across the flow. | scai can't do cross-statement dead-code analysis; hand-crafted removed by analysis. | **Medium.** scai emits warning + leaves broken code; human post-edit required before load. |
| 7 | **`CROSS APPLY` to TVF** (`FROM Planning.tvf_ExplodeCostCenterHierarchy(NULL, 10, 0, GETDATE())`). | `FROM TABLE(Planning.tvf_ExplodeCostCenterHierarchy(NULL, 10, 0, CURRENT_TIMESTAMP() :: TIMESTAMP)) h` — clean rewrite. Also emitted `SSC-FDM-0007` (missing dependent) because scai's input pack didn't include our reconstructed TVF. | Same `FROM TABLE(...)` pattern with explicit casts (`NULL::NUMBER`, `FALSE`, `CURRENT_TIMESTAMP()::TIMESTAMP_NTZ`). | **Converged.** Cosmetic casting difference. | **Low.** Well-documented rewrite pattern; both handled cleanly. |
| 8 | **`RAISERROR(msg, 16, 1)` + `THROW 50001, msg, 1` + bare `THROW` re-raise**. | Generated helper UDFs `PUBLIC.RAISERROR_UDF` and `PUBLIC.THROW_UDP`. Calls via `SELECT PUBLIC.RAISERROR_UDF(msg, 16, 1, array_construct())` and `CALL PUBLIC.THROW_UDP(50001, msg, 1)`. Bare `THROW` → `LET DECLARED_EXCEPTION EXCEPTION; RAISE DECLARED_EXCEPTION;`. More faithful to original raise/catch contract. | `RETURN OBJECT_CONSTRUCT('success', FALSE, 'step', ..., 'error', ...)` — deliberately re-shaped output contract to VARIANT return value (no OUTPUT params in Snowflake Scripting anyway). Errors travel in the same channel as successes. | Different output contract by design. scai preserves exception semantics; hand-crafted restructures to a structured return. Neither strictly "more correct". | **Medium.** Highlights: scai makes **mechanical** decisions; hand-crafted makes **API-design** decisions. |
| 9 | **`@@TRANCOUNT`, `XACT_STATE()`, `CURSOR_STATUS()`** in the CATCH block. | Translated `@@TRANCOUNT` → `:TRANCOUNT` (**undefined variable** in Snowflake Scripting); `XACT_STATE()` → `CURRENT_TRANSACTION()` (wrong — function returns a transaction ID, not a state); `CURSOR_STATUS('local', 'X')` left as-is with `SSC-EWI-0073` pending-review marker. **Non-compilable**. | Reduced to a single `ROLLBACK;` in `EXCEPTION WHEN OTHER THEN`. The `@@TRANCOUNT > 0` / `XACT_STATE() = 1` guards become vacuous in Snowflake's single-transaction model. | scai produces literal-translation debris; hand-crafted collapses to the idiomatic equivalent. | **Medium-high.** Shows the limits of syntactic translation — a rewrite needs to understand that Snowflake has no nested transactions. |
| 10 | **System functions: `NEWID()`, `FORMAT(..., 'yyyyMMdd')`, `CONVERT(VARCHAR(30), t, 126)`, `SYSUTCDATETIME()`, `ISNULL`**. | `UUID_STRING()`, `TO_CHAR(CURRENT_TIMESTAMP() :: TIMESTAMP, 'YYYYMMDD')`, `CAST(t AS VARCHAR(30))` (loses ISO-8601 format semantic), `SYSDATE()`, `NVL`. | Same functions except `NVL` → `COALESCE` and kept `::TIMESTAMP_NTZ` explicit. `CONVERT(..., 126)` explicitly handled as ISO-8601 in the hand-crafted path. | Mostly converged. scai loses the `126` format-specifier nuance in `CONVERT`. | **Low.** Expected convergence on syntactic function remapping. |
| 11 | **`SpreadMethodCode VARCHAR(10)` ← `'CONSOLIDATED'` (12 chars)** — Latent Bug #2. | `'CONSOLIDATED'` **passed through verbatim, twice** (lines 198 and 508 of scai's output). No warning. On actual load, Snowflake would either truncate or error depending on account strict-string-truncation setting. | Explicitly `'CONSOL'` with inline comment noting `VARCHAR(10)` constraint. Patched identically on both engines so row-level diff stays exact. | scai cannot read column-width constraints against value literals. Hand-crafted caught it. | **High.** Durable prediction that held: no syntactic translator (AI-layer or otherwise) reads CHECK implications from column widths. This is precisely what a verification harness (`pipeline/verify.py`) is for. |
| 12 | **Target dialect: SQL Scripting vs. JavaScript UDF**. | `LANGUAGE SQL` + `$$ ... $$` + `DECLARE ... BEGIN ... END;` — **Snowflake Scripting**. Aligned with current Snowflake guidance. | Snowflake Scripting — same choice, pinned in `pipeline/prompts/translate_proc.md` as the prompt's role-defining directive. | **Converged.** | **Low-but-validates.** Both agree Scripting is the 2026 idiomatic target. An older tool might still emit JS; scai got this right. |

### Summary counts

| Metric | scai | Hand-crafted |
|---|---|---|
| Lines of output | **595** | **383** |
| `!!!RESOLVE EWI!!!` human-intervention markers | **16** | **0** |
| Compiles on Snowflake as-is | **No** (fails on literal marker line 39) | **Yes** |
| Passes `pipeline/verify.py` bit-exact | **N/A** (can't even compile) | **Yes** (11/11 rows + aggregate) |
| Catches Latent Bug #1 (dynamic-SQL on table-var) | Accidentally neutralized via substrate change — no warning | Explicitly unrolled, documented |
| Catches Latent Bug #2 (`'CONSOLIDATED'` on VARCHAR(10)) | **No** — passed through verbatim twice | **Yes** |
| Cursors → set-based | **No** (preserved as Scripting loops) | **Yes** (GROUP BY + LAG) |
| Language choice | Scripting ✓ | Scripting ✓ |

---

## Synthesis

### Where scai is clearly superior

scai is built by the Snowflake team and its **syntactic rewrite tables are authoritative and exhaustive**. On the classes of construct with a clean 1:1 Snowflake analogue — system functions, `CROSS APPLY → TABLE(...)`, `MERGE` with standard `WHEN MATCHED / NOT MATCHED`, type coercions, `FOR` loop → Scripting cursor — scai produces output that is at parity with a careful human on the first pass, with zero rework. **scai also generates helper UDFs** (`RAISERROR_UDF`, `THROW_UDP`, `TRANSFORM_SP_EXECUTE_SQL_STRING_UDF`, `HIERARCHYID_UDF`) to simulate T-SQL constructs that have no native Snowflake equivalent — a useful backward-compatibility shim the hand-crafted path skipped. Its **assessment report** (`Issues.csv`, `Assessment.csv`) is immediately actionable: 16 unique warning codes catalogued with file/line references — that kind of machine-readable output is what enables large-scale migration triage. For bulk migration of the 80th-percentile proc dominated by syntactic translation, scai is going to be faster and more consistent than any human.

### Where hand-crafted (or a semantically-aware pipeline) wins

The hand-crafted migration wins wherever the translation decision depends on **context invisible at the point of translation**. Three concrete cases in this proc:

1. **The hierarchy-traversal cursor (Row 2) is an aggregation in disguise.** Only reading the whole loop body and the downstream use of `SubtotalAmount` reveals that ordering never flows out, which unlocks the set-based rewrite. scai correctly emitted `SSC-PRF-0003` (fetch-in-loop is a performance-degrading pattern) but did not take the semantic step of rewriting.
2. **The dynamic-SQL block (Row 5) contains a latent bug that scai "fixed" accidentally** via the same table-variable-to-temp-table rewrite it applied elsewhere. Temp tables ARE visible to `EXECUTE IMMEDIATE` in Snowflake, so scai's output runs where the original T-SQL would have failed. The incidental fix is worse than an explicit fix because **the operator gets no warning** that the original had a real bug.
3. **`SpreadMethodCode` truncation (Row 11)** is a data-fidelity issue no syntactic tool will catch — the bug is in the interaction between a column width and a string literal, not in the SQL dialect.

In all three cases, hand-crafted (or an LLM pipeline prompted with the pitfall list) produces measurably better Snowflake code. Hand-crafted also makes every decision **explicit**: the header comment in `snowflake/04_procedures.sql` lists 11 translation decisions with rationale — an audit artifact scai's output doesn't produce.

### The unified story

Neither approach is strictly better; they are **complementary, and the production answer is both**. scai should own the high-volume syntactic rewriting — cursors-to-Scripting-loops, system-function remapping, `MERGE` translations, `TABLE(...)` wrapping, helper-UDF generation — and emit `!!!RESOLVE EWI!!!` markers on every construct that needs human judgment. A pipeline like `pipeline/translate.py`, with its prompt pinned to Snowflake Scripting and armed with a curated pitfall list (this repo's `pipeline/prompts/translate_proc.md`), should take scai's output as input and apply one pass of **semantic** rewrites: collapse cursor-aggregations to `GROUP BY`, neutralize `!!!RESOLVE EWI!!!` markers case by case, and catch data-fidelity issues like `VARCHAR(10) ← 'CONSOLIDATED'`. `pipeline/verify.py` then runs as the final gate to catch what both layers missed. **scai is building the first layer; the customer still needs the second and third layers on top.** The pipeline + verification harness in this repo is a prototype of exactly that.

---

## Verification attempt — loading scai's output

Attempt 1: raw scai output fails with UTF-8 BOM parse error (scai emits a BOM; `run_sql.py` and `snow sql` both trip on it).
Attempt 2: BOM-stripped output still fails — compile error on line 39 at the literal `!!!RESOLVE EWI!!!` marker:

```
001003 (42000): SQL compilation error:
syntax error line 39 at position 8 unexpected '!'.
syntax error line 39 at position 19 unexpected 'EWI'.
```

**scai's output is not intended to be loaded as-is.** The markers are a mandatory signal that 16 specific constructs in the output require human judgment before the proc can run. This is not a scai failure — it's a correct refusal to emit code that would silently diverge from T-SQL semantics. Loading scai's output would require: (a) stripping the BOM, (b) hand-resolving each of the 16 EWI markers, (c) fixing the `:TRANCOUNT` / `CURRENT_TRANSACTION()` translation errors, (d) either dropping the savepoints or rewriting the error handler, (e) dropping the `OUTPUT INTO` clauses or rewriting to temp-table staging, (f) fixing `'CONSOLIDATED'` → `'CONSOL'`. Estimated: ~30 minutes of human work per proc — on top of review time.

---

## Assessment report (scai's own output)

| Field | Value |
|---|---|
| AppType / AppVersion | `cli / 2.20.0` |
| Source dialect | Transact (T-SQL) |
| Target | Snowflake |
| Files Processed | 23 |
| Code Units Converted | 816 LOC |
| Execution Time | 00:00:11 |
| Total Issues | 74 |
| Unique EWI (conversion issues) | 10 |
| Unique FDM (functional differences) | 9 |
| Unique PRF (performance remarks) | 1 |
| **CodeCompletenessScore** | **94.44%** |
| Critical parse errors | 3 (all on `BudgetHeader.sql` + `AllocationRule.sql` at `CREATE XML INDEX` / `CREATE PRIMARY XML INDEX` statements — scai's parser does not recognize XML-index DDL) |

Artifacts preserved:
- `snowconvert/output/procedures/usp_processbudgetconsolidation.sql` — scai output verbatim
- `snowconvert/output/helpers/*.sql` — 4 generated helper UDFs (HIERARCHYID, RAISERROR, THROW, TRANSFORM_SP_EXECUTE_SQL_STRING)
- `snowconvert/output/tables/*.sql` — 8 table DDL files scai emitted
- `snowconvert/output/types/*.sql` — 3 type DDL files scai emitted
- `snowconvert/output/reports/Issues.csv` — full issue log with codes, severities, file/line refs
- `snowconvert/output/reports/Assessment.csv` — full Assessment metrics

---

## Conclusion

The honest assessment: **scai is the right first layer of a migration pipeline, and the hand-crafted + AI-pipeline + verification-harness approach in this repo is the right second and third layers on top**. scai handled 80% of the proc with a high-quality Snowflake Scripting skeleton in 11 seconds; a human or LLM pipeline was required to (1) collapse the cursor-aggregation to set-based, (2) catch the two latent bugs, (3) resolve 16 `!!!RESOLVE EWI!!!` markers, and (4) clean up translation debris (`:TRANCOUNT`, `CURRENT_TRANSACTION()`, `'CONSOLIDATED'`). The combined approach is strictly stronger than either alone. This repo's `pipeline/verify.py` run against scai's output (after human cleanup) would be the natural next experiment — a subject for a second iteration.

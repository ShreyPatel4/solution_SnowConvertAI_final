# SnowConvert AI Take-Home — SQL Server → Snowflake Migration

Migration of an enterprise financial-planning / consolidation schema from SQL Server to Snowflake, built as a take-home assessment.

**Assignment start:** _fill in from email received time_
**Deadline:** +24 hours

---

## Ship Checklist

- [x] Phase 0 — SQL Server container + Snowflake account reachable; `Planning` DB + `WH_XS` / `PLANNING_DB.PLANNING` provisioned
- [x] Phase 1 — Schemas loaded on both engines: `sqlserver/01_schema.sql` (baseline, FILESTREAM keyword stripped for Docker compat) + `snowflake/01_schema.sql` (migrated, with per-table translation rationale inline) + seed fixtures on both
- [x] Phase 2 — Missing functions/views reconstructed: `tvf_ExplodeCostCenterHierarchy`, `fn_GetAllocationFactor`, `vw_AllocationRuleTargets` on both engines. Scope limited to objects actually called by procs 1 + 3 (deferred `fn_GetHierarchyPath`, `tvf_GetBudgetVariance`, `vw_BudgetConsolidationSummary`)
- [x] Phase 3 — AI translation pipeline (`pipeline/extract.py` + `pipeline/translate.py` + `pipeline/run_sql.py` + `pipeline/verify.py`) and prompt template (`pipeline/prompts/translate_proc.md`)
- [x] Phase 4 — Proc 1: `usp_ProcessBudgetConsolidation` migrated + verified — **cross-engine bit-exact match on 11 consolidated rows + aggregate** (see `verification/results/`)
- [x] Phase 5 — Proc 3: `usp_ExecuteCostAllocation` migrated + verified — **cross-engine bit-exact match on 6 allocated rows + aggregate** (see `verification/results/proc3_*/`)
- [x] Phase 6 — SnowConvert tool comparison — official `scai` CLI installed and run on pristine T-SQL; measured head-to-head vs. hand-crafted migration (see `snowconvert/APPENDIX.md`)
- [x] Phase 7 — Writeup (this README)
- [ ] Phase 8 — End-to-end rerun on a fresh Snowflake session

## Results snapshot

### Proc 1 — `usp_ProcessBudgetConsolidation`

| Metric | SQL Server baseline | Snowflake migration | Match |
|---|---|---|---|
| Consolidated rows inserted | 11 | 11 | ✓ |
| `SUM(FinalAmount)` on target | 52500.0000 | 52500.0000 | ✓ |
| Row-by-row on (GLAccount, CostCenter, FiscalPeriod, FinalAmount) | — | — | ✓ all 11 |
| Intercompany eliminations applied | 1 | 1 | ✓ |
| Proc succeeded | yes | yes | ✓ |

### Proc 3 — `usp_ExecuteCostAllocation`

| Metric | SQL Server baseline | Snowflake migration | Match |
|---|---|---|---|
| Allocated rows inserted | 6 | 6 | ✓ |
| `SUM(OriginalAmount)` on allocations | 32600.0000 | 32600.0000 | ✓ |
| Row-by-row on (GL, CC, FP, OriginalAmount, AllocationSourceLineID, AllocationPercentage) | — | — | ✓ all 6 |
| Proc succeeded | yes | yes | ✓ |

### Proc 1 — scai vs. hand-crafted (see `snowconvert/APPENDIX.md` for the full 12-row construct comparison)

| Metric | scai output | Hand-crafted |
|---|---|---|
| Language choice | Snowflake Scripting ✓ | Snowflake Scripting ✓ |
| Lines of output | 595 | 383 |
| `!!!RESOLVE EWI!!!` human-intervention markers | 16 | 0 |
| Compiles on Snowflake as-is | No (fails line 39 on literal marker) | Yes |
| Passes `pipeline/verify.py` | N/A (can't compile) | Yes, bit-exact |
| Caught Latent Bug #1 (dynamic-SQL on `@`-table) | Accidentally neutralized via substrate change | Deliberately unrolled + documented |
| Caught Latent Bug #2 (`'CONSOLIDATED'` → VARCHAR(10)) | No (carried through twice) | Yes (`'CONSOL'`) |
| Cursors rewritten to set-based | No (preserved as Scripting loops with PRF-0003 warning) | Yes (GROUP BY + LAG) |
| scai CodeCompletenessScore | 94.44% | — |

---

## Repository Layout

```
solution_SnowConvertAI/
├── README.md                     # this file; the submission writeup
├── .env.example                  # secrets template (copy to .env)
├── .gitignore
├── original/                     # unmodified materials as provided
│   ├── SnowConvert AI Take Home Assignment.pdf
│   ├── src (2).zip               # canonical
│   └── src/                      # unzipped copy for GitHub browsing
├── sqlserver/                    # SQL Server side
│   ├── 00_bootstrap.sql
│   ├── 01_schema.sql
│   └── 10_seed.sql
├── snowflake/                    # Snowflake side
│   ├── 00_bootstrap.sql
│   ├── 01_schema.sql
│   ├── 02_functions.sql
│   ├── 03_views.sql
│   ├── 04_procedures.sql
│   └── 10_seed.sql
├── pipeline/                     # AI translation pipeline
│   ├── requirements.txt
│   ├── extract.py
│   ├── translate.py
│   ├── verify.py
│   └── prompts/
│       └── translate_proc.md
└── verification/
    └── results/                  # diff reports (timestamped)
```

---

## Quick Start

### 0. Environment

```bash
cp .env.example .env
# edit .env with Snowflake PAT, SQL Server SA password, etc.
```

Start SQL Server:
```bash
docker run -d --name sqlserver-assessment \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD="YourStrong!Passw0rd" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

Connect to Snowflake:
```bash
# SnowSQL CLI
snowsql -c assessment

# or Python
python pipeline/verify.py --dry-run
```

### 1. Load baseline schema

```bash
# SQL Server
sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i sqlserver/00_bootstrap.sql
sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d Planning -i sqlserver/01_schema.sql

# Snowflake
snowsql -c assessment -f snowflake/00_bootstrap.sql
snowsql -c assessment -f snowflake/01_schema.sql
```

### 2. Run the pipeline

_TBD — Phase 3_

---

## Architecture

### AI Translation Pipeline

_Diagram + narrative filled in during Phase 3._

### Translation Idiom Map

| T-SQL construct | Snowflake equivalent | Lossy? |
|---|---|---|
| `HIERARCHYID` | Materialized-path VARCHAR + recursive CTE | Yes — loses built-in ordering, needs custom compare |
| `XML` type | `VARIANT` + `PARSE_XML` | Partial — XQuery semantics differ |
| `HASHBYTES('SHA2_256', ...)` | `SHA2(..., 256)` | No |
| Table variables (`@tbl`) | Session temp tables | No |
| Cursors over hierarchy | Recursive CTE (set-based) | No (preferred) |
| `sp_executesql` | `EXECUTE IMMEDIATE ... USING (...)` | No |
| Named savepoints | Linear txn + compensating logic | **Yes — document** |
| `FOR SYSTEM_TIME AS OF X` | Time Travel `AT(TIMESTAMP => X)` | **Yes — 1-day default / 90-day Enterprise max** |
| `sp_getapplock` | None; rely on MVCC | **Yes — remove, document** |
| `WAITFOR DELAY` | None | **Yes — remove** |
| `NEWSEQUENTIALID()` | `UUID_STRING()` | Yes — not sequential, index implications |
| `IDENTITY(1,1)` | `AUTOINCREMENT` / sequence | No direct — IDs differ across engines; diff by natural key |
| Persisted computed columns | Virtual columns or views | Partial — Snowflake computes on read |
| Filtered indexes | Clustering key + WHERE | Yes |
| XML primary/secondary indexes | None (auto-optimized) | No action needed |
| `FILESTREAM` | External stage + BLOB URL | **Yes — architectural change** |
| TVPs | `ARRAY`/`OBJECT` params or temp tables | Yes — API change |

---

## Verification Methodology

The verification harness is `pipeline/verify.py`. Every run:

1. **Re-seeds both engines** from identical fixture files (`sqlserver/10_seed.sql` + `snowflake/10_seed.sql`) with matching surrogate IDs (via `IDENTITY_INSERT` on SQL Server; Snowflake `AUTOINCREMENT` accepts explicit inserts).
2. **Reloads the procs** so the harness always tests the current code, not a stale-in-engine copy.
3. **Invokes proc 1** on both engines with identical inputs.
4. **Pulls consolidated rows** — those belonging to the newly-created target header on each side — and diffs them row-by-row on the natural key `(GLAccountID, CostCenterID, FiscalPeriodID)` + `FinalAmount`. Surrogate IDs are never used for comparison because they differ across engines.
5. **Aggregate check**: `SUM(FinalAmount)` + `COUNT(*)` as an independent second diff.
6. **Writes a Markdown report** to `verification/results/<UTC timestamp>/summary.md`.

Run it:

```
.venv/bin/python pipeline/verify.py
```

### Latest run
`verification/results/20260417T150401Z/summary.md`
- Row-by-row: **PASS** — all 11 consolidated rows match on natural key + FinalAmount
- Aggregate: **PASS** — `SUM=52500.0000, COUNT=11` on both engines

### Fixtures — intentional branch coverage
- **3-level CostCenter hierarchy** (CORP → {SALES, OPS, IT} → {SALES_NA, SALES_EU, OPS_MFG}) — exercises the recursive CTE path in `tvf_ExplodeCostCenterHierarchy`
- **3 FiscalPeriods** (Q1 2026 by month) — multiple-period rollup
- **APPROVED BudgetHeader** — required by proc 1's validation guard; DRAFT would cause early return
- **Offsetting intercompany pair** (+1000 / −1000 across two different GL accounts, same CostCenter + FiscalPeriod) — exercises the elimination logic
- **Zero-amount row** — verifies the "skip NULL FinalAmount" filter after recalculation
- **Negative-adjustment row** — verifies mixed-sign arithmetic

### What is intentionally NOT compared
- **Surrogate IDs**: `BudgetHeaderID`, `BudgetLineItemID`, etc. differ across engines (`IDENTITY` vs. `AUTOINCREMENT` seed different sequences). Comparison is by natural key only.
- **RowHash**: Snowflake's `SHA2(str, 256)` returns lowercase hex (VARCHAR); SQL Server's `HASHBYTES('SHA2_256', str)` returns raw VARBINARY. Byte-for-byte equality would require matching the CAST-AS-VARCHAR formatting of `NUMBER/DECIMAL` on both engines — brittle, and it doesn't validate anything the natural-key + aggregate diffs don't already.
- **`CreatedDateTime`, `ModifiedDateTime`, `AttachmentRowGuid`**: wall-clock and UUID values that differ by design.
- **Proc 3 + proc 5**: deferred. Functions + view they need are reconstructed and ready.

---

## AI Usage Narrative

### Pipeline architecture

```
T-SQL source file
       │
       ▼
[ extract.py ]  — splits into translation units (CREATE PROC / FUNC / VIEW / TABLE / TYPE batches)
       │
       ▼
[ translate.py ]  — structured prompt to Claude; returns JSON with:
       │              { translated_sql, rationale, confidence, lossy_conversions, open_questions }
       ▼
[ run_sql.py ]   — loads the translated SQL into Snowflake
       │
       ▼
[ verify.py ]    — re-seeds + calls proc on both engines; natural-key + aggregate diff
       │
       ▼
  pass / fail report
```

### How it was actually used on this task

**Proc 1 was hand-translated first.** For a single, well-understood proc, I got faster with deep per-statement reasoning than through a pipeline round-trip — every translation decision is a judgment call (cursor → recursive CTE? cursor → window function? keep the cursor under Scripting?) that benefits from context the model can't easily see all at once.

But the pipeline was built alongside as the infrastructure that would take over for proc 3 / 5 / beyond. Two specific places where the pipeline already paid off on proc 1:

1. **`verify.py` ran repeatedly during the hand-translation.** It caught two of my own bugs and two latent bugs in the original T-SQL proc:
   - *(my bug)* Used `SQLROWCOUNT` as a bare identifier inside a SQL `VALUES` clause instead of a declared-variable bind. Fixed to a DECLARE + assign + `:var` pattern.
   - *(my bug)* Used `LEAD` where the original cursor's `FETCH RELATIVE 1`+reassigned-vars semantic needed `LAG` — aggregates matched but per-row distribution didn't. Fixed after comparing specific rows.
   - *(original bug)* The dynamic-SQL block in proc 1's recalculation step referenced a table variable via `sp_executesql`, which fails at runtime (dynamic SQL runs in a separate batch and cannot see the caller's table variables). Patched in `sqlserver/04_procedures.sql`; Snowflake migration side-stepped by unrolling the dynamic SQL into static IF branches.
   - *(original bug)* Inserted `'CONSOLIDATED'` (12 chars) into `SpreadMethodCode VARCHAR(10)`. Patched to `'CONSOL'` on both engines identically.

2. **`translate.py --dry-run` sanity-checked the prompt template.** The idiom-map table, the pitfall list, and the JSON output schema in `pipeline/prompts/translate_proc.md` were iterated while inspecting what the model would actually see. Running the full LLM translation on proc 1 was deferred because the hand-crafted version was already passing verification; running the pipeline next would have been for validation, not primary output.

### Prompt strategy (`pipeline/prompts/translate_proc.md`)
- **Target dialect** is pinned to **Snowflake Scripting** (not JavaScript procedures) in the role description. JS procs are easier for a model to generate but produce worse-idiomatic output; this is a pre-commitment.
- **Idiom map** is inline — the same map shown above in this README, sourced from the schema DDL's per-table comment blocks.
- **Known pitfalls** (HIERARCHYID → materialized path, `FOR SYSTEM_TIME` → Time Travel with documented window limits, TVPs → ARRAY-of-OBJECT or temp tables) are listed explicitly so the model doesn't have to rediscover them.
- **Output shape is a strict JSON schema**: `{translated_sql, rationale, confidence, lossy_conversions[], open_questions[]}`. The `lossy_conversions` field is load-bearing — the model MUST call out each construct where semantics don't fully carry over, which forces explicit acknowledgment rather than silent over-fitting.

### What I'd feed back into the pipeline in a longer engagement
- **Compile-error retry loop**: if Snowflake rejects the translated SQL, re-prompt with `(original_tsql, first_attempt, compile_error)` and ask for a fix. Right now `translate.py` is one-shot.
- **Verification-failure retry loop**: if `verify.py` reports a row-diff, re-prompt with the specific failing rows.
- **Multi-model ensembling**: run both Opus and Sonnet on the same proc and surface disagreements. Often the most interesting migration questions are the ones where two competent translators disagree.

---

## Limitations & Non-Equivalences

Each "lossy" entry in the idiom map is documented here. "Lossy" means the Snowflake construct does not preserve every nuance of the SQL Server one; procs using those nuances may need behavioural adjustment.

| Construct | Non-equivalence | Impact |
|---|---|---|
| `FOR SYSTEM_TIME` | Snowflake Time Travel: 1-day default / 90-day Enterprise max retention. No way to query history at an instant beyond the window. | `CostCenter` was system-versioned; we keep a `CostCenterHistory` mirror table. Procs doing `FOR SYSTEM_TIME AS OF <T>` further back than 90 days would break. |
| `sp_getapplock` | No Snowflake equivalent. | Proc 3 (deferred) uses this for coarse serialization; the migration would remove it and rely on Snowflake's MVCC + statement-level isolation. Concurrent-bulk-allocation runs are now possible. |
| `WAITFOR DELAY` | No equivalent. | Proc 3's throttle becomes a no-op. Snowflake warehouse auto-scaling makes throttling less relevant. |
| `HIERARCHYID` + methods | Materialized-path `VARCHAR` + proc-maintained `HierarchyLevel INT`. | `HierarchyPath.GetLevel()` becomes an explicit column (must be populated on writes). Spatial ordering would require a custom compare fn — not used here. |
| `NEWSEQUENTIALID()` | `UUID_STRING()` is random, not sequential. | Clustering/indexing on the GUID column is now a bad idea. (It was a bad idea on SQL Server too — sequential GUIDs only mitigated B-tree page fragmentation.) |
| `IDENTITY(1,1)` | `AUTOINCREMENT` values differ across engines for the same logical rows. | Verification must diff by natural key, never surrogate. Enforced in `verify.py`. |
| `IGNORE_DUP_KEY = ON` | No equivalent. | Replaced by `MERGE` on the natural key in ingest procs. Behaviour: conflicts become UPDATEs, not silently-ignored inserts — stricter semantic. |
| `OUTPUT INTO @tbl` | No OUTPUT clause in Snowflake. | Temp-table staging + follow-up query as substitute. |
| `FILESTREAM` | No Snowflake equivalent. | Inline `BINARY` for small attachments (this dataset); external stage + file URL is the right pattern for real workloads. |
| `PERSISTED` computed columns | Stored as proc-maintained regular columns. | Writers must compute the value on `INSERT`/`UPDATE`. The migrated proc does; seeds do. Cross-engine byte-equality of SHA2 hashes requires identical string-formatting — excluded from strict diff. |
| `CHECK / UNIQUE / FOREIGN KEY` | Snowflake tracks them as metadata only — NOT enforced at runtime (only `NOT NULL` is enforced). | Application/proc layer must enforce. The migrated proc relies on `UQ_BudgetHeader_Code_Year` for a SCOPE_IDENTITY substitute — a duplicate would silently return the wrong row. |
| XML primary/secondary `INDEX` | No equivalent (Snowflake auto-optimizes `VARIANT`). | No action needed; documented for completeness. |
| `ON DELETE CASCADE` | Not supported in Snowflake FK syntax. | Cascade cleanup moves into the deletion procedure. |

---

## What I'd Do With More Time

- **Run `pipeline/translate.py` end-to-end over proc 1** (not just `--dry-run`) and diff its output against both scai's output and the hand-crafted migration. Three-way comparison makes the claim "the pipeline is the second layer on top of scai" concretely testable.
- **Feed scai's `!!!RESOLVE EWI!!!` markers into the pipeline as the prompt's starting point.** The marker locations are a machine-readable handoff from Layer 1 (scai) to Layer 2 (AI review). Demonstrating that handoff automated is the natural next deliverable.
- **Seed a 100x larger fixture** (~100k line items, cycles in the CostCenter hierarchy, richer XML/JSON target specs) to exercise branches the current fixture doesn't touch — especially the `INCREMENTAL` consolidation type, `@MaxIterations` safeguard in proc 1's cursor loop, and transitive rule-dependency cycle detection in proc 3.
- **Close the RowHash cross-engine gap** by computing SHA2 over a canonicalised concat string (pad decimals to fixed width) on both sides. Would let the row-level diff include RowHash.
- **Wrap `translate.py` in a retry-on-fail loop** that feeds compile + verification errors back into the prompt as correction context. Right now it's one-shot.
- **Add parameter-branch coverage** to `verify.py` / `verify_proc3.py` — exercise `@ConsolidationType=INCREMENTAL`, `@IncludeEliminations=0`, non-NULL `@ProcessingOptions`, and proc 3's `@ConcurrencyMode=EXCLUSIVE` path. Currently only the happy path.
- **Migrate procs 4, 5, 6** (forecast, reconcile, bulk-import). Each exposes a distinct class of T-SQL → Snowflake idiom challenge (global temp tables, OPENXML, BULK INSERT / COPY INTO).

---

## Appendix: SnowConvert Tool Comparison

**Done.** See `snowconvert/APPENDIX.md` for the full 12-row construct-by-construct comparison between scai's output and the hand-crafted migration, plus a 3-paragraph synthesis. Headline: scai produces a high-quality Snowflake Scripting skeleton in 11 seconds, but does **not compile as-is** — emits 16 `!!!RESOLVE EWI!!!` markers requiring human judgment. Hand-crafted compiles and passes `pipeline/verify.py` bit-exact. The right production architecture is both: scai as Layer 1 (syntactic translation), an AI pipeline as Layer 2 (semantic review + marker resolution), `verify.py` as Layer 3 (cross-engine validation).

See also `snowconvert/RUN_LOG.md` for the install + invocation history, and `snowconvert/output/` for scai's raw output artifacts (preserved verbatim — `procedures/`, `tables/`, `types/`, `helpers/`, `reports/`).

---

## Notes

Private repository for SnowConvert AI take-home evaluation only. The `original/` folder preserves materials as received; all other folders contain migration work.

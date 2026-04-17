# SnowConvert AI Take-Home — SQL Server → Snowflake Migration

Migration of an enterprise financial-planning / consolidation schema from SQL Server to Snowflake, built as a take-home assessment.

**Assignment start:** _fill in from email received time_
**Deadline:** +24 hours

---

## Ship Checklist

- [x] Phase 0 — SQL Server container + Snowflake account reachable; `Planning` DB + `WH_XS` / `PLANNING_DB.PLANNING` provisioned
- [x] Phase 1 — Schemas loaded on both engines: `sqlserver/01_schema.sql` (baseline, FILESTREAM keyword stripped for Docker compat) + `snowflake/01_schema.sql` (migrated, with per-table translation rationale inline)
- [ ] Phase 2 — Missing functions/views reconstructed (`tvf_ExplodeCostCenterHierarchy`, etc.)
- [ ] Phase 3 — AI translation pipeline runnable end-to-end
- [ ] Phase 4 — Proc 1: `usp_ProcessBudgetConsolidation` migrated + verified
- [ ] Phase 5 — Proc 3: `usp_ExecuteCostAllocation` migrated + verified
- [ ] Phase 6 — (Stretch) Proc 5 OR SnowConvert tool comparison
- [ ] Phase 7 — Writeup polished
- [ ] Phase 8 — End-to-end rerun on a fresh Snowflake session

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

_Filled in during Phase 1+. Plan:_

1. **Deterministic seeded fixtures** — 3-level cost-center hierarchy, APPROVED+DRAFT budgets, intercompany-flagged GL accounts, offsetting pairs, NULLs, zero amounts, edge cases.
2. **Identical natural keys** across SQL Server and Snowflake (so surrogate-ID differences don't confuse the diff).
3. **Parallel execution** — same inputs to both engines.
4. **Diff strategy** — row counts, `HASH_AGG` over value columns, spot checks on computed columns (`FinalAmount`, `RowHash`, `HierarchyLevel`).
5. **Parameter-branch coverage** — exercise `@ConsolidationType=FULL`/`INCREMENTAL`, `@IncludeEliminations=0`/`1`.
6. **Honest gap log** — what was not tested.

---

## AI Usage Narrative

_Filled in during Phase 3+._

- Pipeline design
- Prompt strategy (idiom map, structured output, confidence + lossy-conversions list)
- Failure modes observed
- Corrections applied and why

---

## Limitations & Non-Equivalences

_Every lossy row in the idiom map above gets a paragraph here as translations land._

---

## What I'd Do With More Time

_Filled in at Phase 7._

---

## Appendix: SnowConvert Tool Comparison

_If Phase 6 stretch is taken — ran the official SnowConvert tool on Proc 1 and compared output vs. our AI pipeline._

---

## Notes

Private repository for SnowConvert AI take-home evaluation only. The `original/` folder preserves materials as received; all other folders contain migration work.

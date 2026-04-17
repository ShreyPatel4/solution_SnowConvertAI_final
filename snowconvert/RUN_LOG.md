# SnowConvert run log

## What actually ran (2026-04-16)

The install blocker below was resolved by running the installer directly
in Terminal.app (outside Claude Code, where sudo can prompt). The full
sequence that ran end-to-end on this machine:

1. `brew install --cask snowconvert-ai` — run in Terminal.app so the
   `sudo installer` step could receive a password prompt. Installed
   scai 2.20.0.
2. `brew install snowflake-cli` — Homebrew formula (no sudo), installs
   the `snow` CLI that scai uses to resolve Snowflake connections.
3. `snow connection add --connection-name default --account prc57893.us-east-1
   --user SHREYP412 --password <PAT> --role ACCOUNTADMIN --warehouse WH_XS
   --database PLANNING_DB --schema PLANNING --default --no-interactive` —
   non-interactive connection configured with the PAT.
4. `SCAI_ACCEPT_TERMS=true scai terms accept` — env flag required in a
   non-TTY shell.
5. `scai init sc_comparison -l SqlServer` — created project at
   `/tmp/sc_comparison/`.
6. `scai code add -i snowconvert/input` — registered the 12 pristine
   T-SQL files (1 proc + 8 tables + 3 UDTTs).
7. `scai code convert` — succeeded. Output copied to
   `snowconvert/output/` (see file tree there).

**Measured outcome** (from `snowconvert/output/reports/Assessment.csv`
and `Issues.csv`):

| Metric                      | Value                                |
|-----------------------------|--------------------------------------|
| Files emitted               | 23                                   |
| Total LOC of output         | 816                                  |
| Issues surfaced             | 74                                   |
| Conversion completeness     | 94.44%                               |
| Critical parse errors       | 3 (all on the XML-index DDL)         |
| `!!!RESOLVE EWI!!!` markers | 16 (in the converted procedure body) |
| Helper UDFs generated       | 4 (HIERARCHYID_UDF, RAISERROR_UDF, THROW_UDP, TRANSFORM_SP_EXECUTE_SQL_STRING_UDF) |

The output is **not directly loadable**: a BOM prefix and the
`!!!RESOLVE EWI!!!` literals make the file fail compilation as-is. scai
emits these intentionally as hand-off markers for a human reviewer —
they are not a tool bug but a workflow signal. The `make_loadable.py`
post-processor was authored to sand off the mechanical parts (BOM,
`USE` headers, identifier case, trailing semicolon) so the reviewer can
focus on the semantic `EWI` markers.

See `snowconvert/APPENDIX.md` for the construct-by-construct comparison
against the hand-crafted Snowflake translation.

---

## Historical context (pre-unblock)

The section below was written before the install unblocked. Retained
for the record — everything in it has been superseded by the run above.

## Current state (2026-04-16)

**Install: BLOCKED on sudo-TTY.** The Homebrew cask post-install step
(`brew install --cask snowconvert-ai`) runs `/usr/bin/sudo installer` on
a .pkg, which cannot receive a password prompt from a non-interactive
shell. It has to be run by the operator directly in Terminal.app.
`scai` has therefore not yet been run on this machine.

**Everything that does NOT require the binary is prepared and is listed
below.** The first post-install action is a single one-liner (`scai init`
+ `scai code add` + `scai code convert`) that will produce the output
file and unblock the rest of the pipeline.

## Tool identification (once installed)

- Tool: SnowConvert AI CLI (`scai`) — Snowflake's current migration tool
- Version: 2.20.0 (from Homebrew tap `snowflakedb/snowconvert-ai`)
- Distribution: Apple Silicon .pkg, installed via `brew install --cask snowconvert-ai`
- Install location (expected): `/usr/local/snowconvertai/bin/scai`, symlinked
  to `/usr/local/bin/scai`
- Auth: uses Snowflake CLI's default connection (shared with `snow` CLI)

## Tool lineage note

The legacy `snowct` CLI is deprecated. `scai` is the current AI-driven
replacement. This lineage is relevant to the comparison itself: the
architectural question is now "how does Snowflake's in-house AI
translator compare to a purpose-built pipeline with stricter idiom
pinning and a verification-harness feedback loop?" — the exact story
the `pipeline/` directory in this repo is built to answer.

## Inputs staged (ready for scai)

All files are byte-identical to `original/src/` (verified by file size
match on each file):

```
snowconvert/input/procedures/
    usp_ProcessBudgetConsolidation.sql          22,209 bytes  (pristine T-SQL)

snowconvert/input/tables/
    AllocationRule.sql                           2,609 bytes
    BudgetHeader.sql                             2,435 bytes
    BudgetLineItem.sql                           3,403 bytes
    ConsolidationJournal.sql                     2,734 bytes
    ConsolidationJournalLine.sql                 2,441 bytes
    CostCenter.sql                               1,686 bytes
    FiscalPeriod.sql                             1,773 bytes
    GLAccount.sql                                1,991 bytes

snowconvert/input/types/
    AllocationResultTableType.sql                  677 bytes
    BudgetLineItemTableType.sql                    912 bytes
    HierarchyNodeTableType.sql                     511 bytes

snowconvert/input/functions/    (empty — mirrors original/src/Functions/)
snowconvert/input/views/        (empty — mirrors original/src/Views/)
```

**Pristineness guarantee** (why this matters for the comparison):
- No `'CONSOL'` patch (the `VARCHAR(10)` truncation latent bug is present)
- No inlined-UPDATE patch (the `sp_executesql` + `@ConsolidatedAmounts`
  latent bug is present)
- No "PATCH:" marker comments
- `EXEC sp_executesql` still on its original line
- `'CONSOLIDATED'` string literal still appears twice in the file

These must remain *unfixed* in the scai input so we can measure how scai
handles each specific pitfall. The patched versions live in
`sqlserver/04_procedures.sql` (for baseline correctness) and
`snowflake/04_procedures.sql` (hand-crafted Snowflake translation).

## Prepared artifacts

1. **`snowconvert/APPENDIX.md`** — 12-row comparison table pre-filled
   with predictions per construct, plus a 3-paragraph capability-level
   synthesis. Table cells in the scai column are tagged `PREDICTED` and
   will be replaced with measured values post-run. The synthesis is
   written to stand alone even if scai never unblocks on this machine.

2. **`snowconvert/make_loadable.py`** — self-contained (stdlib-only)
   Python script that applies *mechanical* edits (strip `USE` headers,
   fully-qualify CREATE object names, normalize identifier case, ensure
   trailing semicolon) to scai's output and writes
   `snowconvert/output_loadable.sql`. Has a `--dry-run` flag that prints
   every edit as `line N: <before> -> <after>` for reviewer audit. Design
   constraint: no logic edits, no statement-body rewrites — a reviewer
   reading the dry-run log should be able to confirm every edit is
   cosmetic. See the module docstring for the full rule list.

3. **This log (`RUN_LOG.md`)** — captures install blocker, prep status,
   and the exact command sequence to run post-install.

## Run plan: exact commands, once `scai` is installed

```bash
# 1. Accept terms of service (one-time per machine)
scai terms accept

# 2. Initialize a new SnowConvert project in the snowconvert/ dir
#    (cwd when this runs: /Users/shrey/Personal Projects/Assesment/solution_SnowConvertAI/)
scai init sc_comparison -l SqlServer

# 3. Enter the new project dir
cd sc_comparison

# 4. Register the pristine T-SQL input
scai code add -i /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/snowconvert/input

# 5. Run the conversion (writes output into the project's output/ dir)
scai code convert
```

**After the `scai code convert` run:**

```bash
# 6. Copy scai's output file out to our snowconvert/output/ dir
#    (path depends on the sc_comparison project layout — typically
#     sc_comparison/output/Output/procedures/*.sql)
cp sc_comparison/output/Output/procedures/usp_ProcessBudgetConsolidation.sql \
   /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/snowconvert/output/

# 7. Run make_loadable.py as a dry-run first to audit the edits
.venv/bin/python /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/snowconvert/make_loadable.py \
    --input /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/snowconvert/output/usp_ProcessBudgetConsolidation.sql \
    --db PLANNING_DB \
    --schema PLANNING \
    --dry-run

# 8. If the edit log looks right, run it for real
.venv/bin/python /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/snowconvert/make_loadable.py \
    --input /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/snowconvert/output/usp_ProcessBudgetConsolidation.sql \
    --db PLANNING_DB \
    --schema PLANNING

# 9. Load scai's (post-make_loadable) output into Snowflake under a
#    different procedure name so it doesn't collide with the hand-crafted
#    one. Quickest path: open output_loadable.sql, edit
#    "CREATE ... PROCEDURE ..." to "CREATE OR REPLACE PROCEDURE
#    usp_ProcessBudgetConsolidation_scai(...)", then run:
.venv/bin/python /Users/shrey/Personal\ Projects/Assesment/solution_SnowConvertAI/pipeline/run_sql.py \
    snowconvert/output_loadable.sql

# 10. Diff scai's output against the hand-crafted version on the same
#     fixture. Quickest path is to call both procs and compare the 11
#     consolidated output rows — same methodology as pipeline/verify.py.
#     (A dedicated variant of verify.py for this comparison can be added
#     later; for now, a manual two-proc-call + aggregate-diff is enough.)

# 11. Update snowconvert/APPENDIX.md:
#     - Replace every "PREDICTED" cell in the table with the measured
#       value (one pass through the output file).
#     - Revise any synthesis sentence that a measurement contradicts.
#     - Check each of the five "Open Questions" at the end and move them
#       to a "Measured Answers" section.
```

## Run attempts

| Attempt | Date (UTC)          | Outcome                                                                                   |
|---------|---------------------|-------------------------------------------------------------------------------------------|
| 1       | 2026-04-16 (late)   | **Succeeded.** 23 files / 816 LOC / 74 issues / 94.44% complete. 3 Critical parse errors on XML-index DDL. 16 `!!!RESOLVE EWI!!!` markers in proc body. Output in `snowconvert/output/`. |

## Risks / fallbacks

1. **scai install never unblocks** on this machine. Fallback: run scai
   in a Snowflake-provided Docker image or a separate macOS VM, pipe
   the output file back through git. `APPENDIX.md` is authored to stand
   alone on predictions if this happens; no edit to the structure is
   required, only a section-head swap from "PREDICTED" to "PREDICTED —
   scai not run on this machine; predictions retained."
2. **scai changes output format** between versions. Fallback:
   `make_loadable.py` has per-rule toggles (`--rules R1,R2,R3,R4`)
   and a selectable identifier-case mode (`--case upper|strip-quotes|none`)
   so the operator can disable any rule that breaks against a newer
   scai version. All behaviour-changing defaults are documented in the
   script header.
3. **scai produces a malformed or incomplete file**. Fallback:
   `make_loadable.py --dry-run` surfaces this as a "0 edits applied"
   report, which is the signal to re-run scai or file a bug. No silent
   write of a broken file occurs — dry-run is the default first step.

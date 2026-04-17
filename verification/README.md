# Verification

Diff harness for comparing SQL Server baseline vs. Snowflake migration.

Usage (once `pipeline/verify.py` exists):
```
python ../pipeline/verify.py --fixture seed --proc usp_ProcessBudgetConsolidation
```

Results are written to `results/<timestamp>/` with:
- `row_counts.json` — per-table row counts on both engines
- `hash_agg.json` — `HASH_AGG` comparisons on value columns
- `spot_checks.json` — row-level checks on computed columns (FinalAmount, RowHash, HierarchyLevel)
- `summary.md` — human-readable PASS/FAIL report

Local-only artifacts go under `results/local/` (git-ignored).

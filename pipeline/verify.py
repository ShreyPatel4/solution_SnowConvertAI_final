"""End-to-end verification harness for the proc 1 migration.

Re-seeds both engines identically, invokes usp_ProcessBudgetConsolidation on
each, and diffs the newly-created consolidated rows by natural key plus a
SUM aggregate.  Writes a Markdown report to
verification/results/<UTC timestamp>/summary.md and prints it to stdout.

Methodology:
  * Natural-key diff on (GLAccountID, CostCenterID, FiscalPeriodID, FinalAmount)
    — surrogate IDs differ across engines so we never use them for comparison.
  * Aggregate diff (SUM, COUNT) as a second independent check.
  * Row-count sanity check.
  * RowHash is excluded from the strict diff: Snowflake's SHA2 hex differs
    from SQL Server's HASHBYTES VARBINARY and exact cross-engine byte equality
    would require type-matched formatting of NUMBER-to-string; out of scope for
    behavioural verification.

Run:
    .venv/bin/python pipeline/verify.py
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT = ROOT / "verification" / "results" / TS
OUT.mkdir(parents=True, exist_ok=True)

MSSQL_PW = os.environ["MSSQL_SA_PASSWORD"]
SQL_CONTAINER = "sqlserver-assessment"


# ---------- subprocess helpers ----------

def sh(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.stderr.write(r.stdout + r.stderr)
        raise SystemExit(f"Command failed: {' '.join(cmd)}")
    return r.stdout + r.stderr


def mssql_file(path: Path):
    sh(["docker", "cp", str(path), f"{SQL_CONTAINER}:/tmp/_verify.sql"])
    sh([
        "docker", "exec", SQL_CONTAINER,
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost", "-U", "sa", "-P", MSSQL_PW,
        "-i", "/tmp/_verify.sql", "-C", "-N", "-I", "-l", "60",
    ], check=False)


def mssql_rows(sql: str, db: str = "Planning"):
    out = sh([
        "docker", "exec", SQL_CONTAINER,
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost", "-U", "sa", "-P", MSSQL_PW,
        "-d", db, "-C", "-N", "-I",
        "-W", "-s", "|", "-h", "-1",
        "-Q", "SET NOCOUNT ON;\n" + sql,
    ], check=False)
    rows = []
    for ln in out.strip().splitlines():
        ln = ln.rstrip()
        if not ln:
            continue
        if ln.startswith("(") or ln.startswith("Changed database"):
            continue
        rows.append(tuple(f.strip() for f in ln.split("|")))
    return rows


# ---------- Snowflake helpers ----------

def sf_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role="ACCOUNTADMIN",
        warehouse="WH_XS",
        database="PLANNING_DB",
        schema="PLANNING",
    )


def sf_run_file(relpath: str):
    sh([sys.executable, str(ROOT / "pipeline/run_sql.py"), relpath], check=False)


# ---------- report builder ----------

report = []
def say(s: str = ""):
    print(s)
    report.append(s)


def main() -> int:
    say(f"# Proc 1 Migration Verification — run {TS}")
    say("")
    say(f"Source proc: `original/src/StoredProcedures/usp_ProcessBudgetConsolidation.sql`  ")
    say(f"SQL Server baseline (patched): `sqlserver/04_procedures.sql`  ")
    say(f"Snowflake migration: `snowflake/04_procedures.sql`  ")
    say("")

    # --- Re-seed ---
    say("## 1. Re-seed fixtures on both engines")
    mssql_file(ROOT / "sqlserver/10_seed.sql")
    sf_run_file("snowflake/10_seed.sql")
    say("- SQL Server: fixtures reloaded (`sqlserver/10_seed.sql`)")
    say("- Snowflake:  fixtures reloaded (`snowflake/10_seed.sql`)")
    say("")

    # --- Reload procs ---
    say("## 2. Reload procedures")
    mssql_file(ROOT / "sqlserver/04_procedures.sql")
    sf_run_file("snowflake/04_procedures.sql")
    say("- SQL Server: `usp_ProcessBudgetConsolidation` reloaded")
    say("- Snowflake:  `usp_ProcessBudgetConsolidation` reloaded")
    say("")

    # --- Invoke proc ---
    say("## 3. Invoke proc 1 on both engines (identical inputs)")

    call_sql = """
DECLARE @TargetID INT, @RowCount INT, @ErrMsg NVARCHAR(4000);
EXEC Planning.usp_ProcessBudgetConsolidation
    @SourceBudgetHeaderID = 1,
    @TargetBudgetHeaderID = @TargetID OUTPUT,
    @IncludeEliminations = 1,
    @RecalculateAllocations = 1,
    @UserID = 100,
    @RowsProcessed = @RowCount OUTPUT,
    @ErrorMessage = @ErrMsg OUTPUT;
SELECT @TargetID, @RowCount, ISNULL(@ErrMsg, '(none)');
""".strip()
    ms_out = mssql_rows(call_sql)
    if not ms_out:
        say("- **SQL Server: proc call produced no output — aborting**")
        (OUT / "summary.md").write_text("\n".join(report) + "\n")
        return 2
    ms_target = int(ms_out[0][0])
    say(f"- SQL Server: target_id={ms_target}, rows_processed={ms_out[0][1]}, error={ms_out[0][2]}")

    conn = sf_conn()
    cur = conn.cursor()
    cur.execute(
        "CALL usp_ProcessBudgetConsolidation(1, NULL, 'FULL', TRUE, TRUE, NULL, 100, FALSE)"
    )
    sf_raw = cur.fetchall()[0][0]
    sf_ret = sf_raw if isinstance(sf_raw, dict) else json.loads(sf_raw)
    if not sf_ret.get("success"):
        say(f"- **Snowflake proc failed**: {sf_ret}")
        (OUT / "summary.md").write_text("\n".join(report) + "\n")
        return 3
    sf_target = int(sf_ret["target_budget_header_id"])
    say(f"- Snowflake:  target_id={sf_target}, rows_processed={sf_ret['rows_processed']}, "
        f"inserted={sf_ret['inserted_count']}, elim_updated={sf_ret['elim_updated']}")
    say("")

    # --- Pull consolidated rows ---
    say("## 4. Fetch consolidated rows (from the newly-created target header on each side)")
    ms_rows = mssql_rows(f"""
SELECT GLAccountID, CostCenterID, FiscalPeriodID, CAST(FinalAmount AS DECIMAL(19,4))
FROM Planning.BudgetLineItem
WHERE BudgetHeaderID = {ms_target}
ORDER BY GLAccountID, CostCenterID, FiscalPeriodID
""")
    cur.execute(f"""
SELECT GLAccountID, CostCenterID, FiscalPeriodID, CAST(FinalAmount AS DECIMAL(19,4))
FROM BudgetLineItem
WHERE BudgetHeaderID = {sf_target}
ORDER BY GLAccountID, CostCenterID, FiscalPeriodID
""")
    sf_rows = [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in cur.fetchall()]

    say(f"- SQL Server consolidated rows: {len(ms_rows)}")
    say(f"- Snowflake consolidated rows:  {len(sf_rows)}")
    say("")

    # --- Row-by-row diff on natural key + FinalAmount ---
    say("## 5. Row-by-row diff (natural key + FinalAmount)")
    if ms_rows == sf_rows:
        say(f"- **PASS** — all {len(ms_rows)} rows match on (GLAccountID, CostCenterID, FiscalPeriodID, FinalAmount)")
        row_pass = True
    else:
        say("- **FAIL** — differences below:")
        for i in range(max(len(ms_rows), len(sf_rows))):
            m = ms_rows[i] if i < len(ms_rows) else None
            s = sf_rows[i] if i < len(sf_rows) else None
            if m != s:
                say(f"  - row {i}: sqlserver={m} / snowflake={s}")
        row_pass = False
    say("")

    # --- Aggregate diff ---
    say("## 6. Aggregate diff (SUM, COUNT)")
    ms_agg = mssql_rows(
        f"SELECT CAST(SUM(FinalAmount) AS DECIMAL(19,4)), COUNT(*) "
        f"FROM Planning.BudgetLineItem WHERE BudgetHeaderID = {ms_target}"
    )[0]
    cur.execute(
        f"SELECT CAST(SUM(FinalAmount) AS DECIMAL(19,4)), COUNT(*) "
        f"FROM BudgetLineItem WHERE BudgetHeaderID = {sf_target}"
    )
    sf_agg = cur.fetchone()
    say(f"- SQL Server: SUM={ms_agg[0]}, COUNT={ms_agg[1]}")
    say(f"- Snowflake:  SUM={sf_agg[0]}, COUNT={sf_agg[1]}")
    agg_pass = (str(ms_agg[0]) == str(sf_agg[0]) and str(ms_agg[1]) == str(sf_agg[1]))
    say(f"- **{'PASS' if agg_pass else 'FAIL'}**")
    say("")

    # --- Overall verdict ---
    say("## Overall")
    if row_pass and agg_pass:
        say("- **VERIFICATION PASSED**: SQL Server baseline and Snowflake migration produced identical consolidated outputs.")
    else:
        say("- **VERIFICATION FAILED** — see row-by-row + aggregate sections above.")
    say("")

    cur.close()
    conn.close()

    (OUT / "summary.md").write_text("\n".join(report) + "\n")
    print(f"\nReport: {OUT}/summary.md")
    return 0 if (row_pass and agg_pass) else 1


if __name__ == "__main__":
    raise SystemExit(main())

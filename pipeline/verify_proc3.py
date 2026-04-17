"""End-to-end verification harness for the proc 3 migration.

Re-seeds both engines identically, invokes usp_ExecuteCostAllocation on each,
and diffs the newly-inserted allocated rows (IsAllocated=1 children) by natural
key plus a SUM aggregate.  Writes a Markdown report to
verification/results/<UTC timestamp>/summary.md and prints it to stdout.

Methodology:
  * Natural-key diff on (GLAccountID, CostCenterID, FiscalPeriodID,
    AllocationSourceLineID, OriginalAmount, AllocationPercentage) — surrogate
    IDs (BudgetLineItemID) differ across engines so we don't compare them.
  * Aggregate diff (SUM(OriginalAmount), COUNT) as an independent check.
  * Compares OriginalAmount (not FinalAmount) because Snowflake FinalAmount is
    only set explicitly in the migrated proc, while SQL Server's FinalAmount
    is a PERSISTED computed column.  OriginalAmount is written identically
    on both engines.
  * RowHash is excluded: SQL Server HASHBYTES VARBINARY bytes will not match
    Snowflake SHA2 hex for the same formula reasons covered in verify.py.

Run:
    .venv/bin/python pipeline/verify_proc3.py
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
OUT = ROOT / "verification" / "results" / f"proc3_{TS}"
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
    sh(["docker", "cp", str(path), f"{SQL_CONTAINER}:/tmp/_verify3.sql"])
    sh([
        "docker", "exec", SQL_CONTAINER,
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost", "-U", "sa", "-P", MSSQL_PW,
        "-i", "/tmp/_verify3.sql", "-C", "-N", "-I", "-l", "60",
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


def sf_run_sql_file(relpath: str):
    """Run a .sql file via the existing $$-aware run_sql.py helper."""
    sh([sys.executable, str(ROOT / "pipeline/run_sql.py"), relpath], check=False)


# ---------- report builder ----------

report = []
def say(s: str = ""):
    print(s)
    report.append(s)


def main() -> int:
    say(f"# Proc 3 Migration Verification — run {TS}")
    say("")
    say(f"Source proc: `original/src/StoredProcedures/usp_ExecuteCostAllocation.sql`  ")
    say(f"SQL Server baseline (patched): `sqlserver/05_procedures.sql`  ")
    say(f"Snowflake migration:           `snowflake/05_procedures.sql`  ")
    say("")

    # --- Re-seed ---
    say("## 1. Re-seed fixtures on both engines")
    mssql_file(ROOT / "sqlserver/10_seed.sql")
    sf_run_sql_file("snowflake/10_seed.sql")
    say("- SQL Server: fixtures reloaded (`sqlserver/10_seed.sql`)")
    say("- Snowflake:  fixtures reloaded (`snowflake/10_seed.sql`)")
    say("")

    # --- Reload procs ---
    say("## 2. Reload procedures")
    mssql_file(ROOT / "sqlserver/05_procedures.sql")
    sf_run_sql_file("snowflake/05_procedures.sql")
    say("- SQL Server: `usp_ExecuteCostAllocation` reloaded")
    say("- Snowflake:  `usp_ExecuteCostAllocation` reloaded")
    say("")

    # --- Invoke proc ---
    say("## 3. Invoke proc 3 on both engines (identical inputs)")

    # @ConcurrencyMode='NONE' skips sp_getapplock (no advisory lock needed for
    # a single test session).  @AllocationResults receives an empty TVP — the
    # proc never reads that parameter, so this is safe.
    call_sql = """
DECLARE @Dummy Planning.AllocationResultTableType;
DECLARE @Rows INT, @Warn NVARCHAR(MAX);
EXEC Planning.usp_ExecuteCostAllocation
    @BudgetHeaderID = 1,
    @AllocationRuleIDs = NULL,
    @FiscalPeriodID = NULL,
    @DryRun = 0,
    @MaxIterations = 100,
    @ThrottleDelayMS = 0,
    @ConcurrencyMode = 'NONE',
    @AllocationResults = @Dummy,
    @RowsAllocated = @Rows OUTPUT,
    @WarningMessages = @Warn OUTPUT;
SELECT @Rows, ISNULL(@Warn, '(none)');
""".strip()
    ms_out = mssql_rows(call_sql)
    if not ms_out:
        say("- **SQL Server: proc call produced no output — aborting**")
        (OUT / "summary.md").write_text("\n".join(report) + "\n")
        return 2
    say(f"- SQL Server: rows_allocated={ms_out[0][0]}, warning={ms_out[0][1]}")

    conn = sf_conn()
    cur = conn.cursor()
    cur.execute(
        "CALL usp_ExecuteCostAllocation(1, NULL, NULL, FALSE, 100, 0, 'NONE', ARRAY_CONSTRUCT())"
    )
    sf_raw = cur.fetchall()[0][0]
    sf_ret = sf_raw if isinstance(sf_raw, dict) else json.loads(sf_raw)
    if not sf_ret.get("success"):
        say(f"- **Snowflake proc failed**: {sf_ret}")
        (OUT / "summary.md").write_text("\n".join(report) + "\n")
        return 3
    say(f"- Snowflake:  rows_allocated={sf_ret['rows_allocated']}, "
        f"iteration_count={sf_ret.get('iteration_count')}, "
        f"queue_size={sf_ret.get('queue_size')}, "
        f"warning={sf_ret.get('warning_messages') or '(none)'}")
    say("")

    # --- Pull allocated (child) rows from BudgetHeaderID=1 ---
    say("## 4. Fetch allocated rows (IsAllocated=1 children inserted by proc 3)")

    # Natural-key diff columns (surrogates differ).  AllocationSourceLineID points
    # to the source BudgetLineItemID, which was seeded identically on both
    # engines, so it is a valid cross-engine key.
    # SQL Server's sqlcmd renders DECIMAL values with no leading zero
    # (".600000"), while Snowflake returns "0.600000".  Normalize both sides
    # through Decimal so we compare numeric equality, not string format.
    from decimal import Decimal

    def norm(row):
        return (
            row[0], row[1], row[2],
            Decimal(row[3]),
            row[4],
            Decimal(row[5]),
        )

    ms_rows_raw = mssql_rows(f"""
SELECT GLAccountID, CostCenterID, FiscalPeriodID,
       CAST(OriginalAmount AS DECIMAL(19,4)),
       AllocationSourceLineID,
       CAST(AllocationPercentage AS DECIMAL(8,6))
FROM Planning.BudgetLineItem
WHERE BudgetHeaderID = 1
  AND IsAllocated = 1
  AND AllocationSourceLineID IS NOT NULL
ORDER BY GLAccountID, CostCenterID, FiscalPeriodID, AllocationSourceLineID,
         CAST(OriginalAmount AS DECIMAL(19,4));
""")
    ms_rows = [norm(r) for r in ms_rows_raw]

    cur.execute("""
SELECT GLAccountID, CostCenterID, FiscalPeriodID,
       CAST(OriginalAmount AS NUMBER(19,4)),
       AllocationSourceLineID,
       CAST(AllocationPercentage AS NUMBER(8,6))
FROM BudgetLineItem
WHERE BudgetHeaderID = 1
  AND IsAllocated = TRUE
  AND AllocationSourceLineID IS NOT NULL
ORDER BY GLAccountID, CostCenterID, FiscalPeriodID, AllocationSourceLineID,
         CAST(OriginalAmount AS NUMBER(19,4));
""")
    sf_rows = [norm((str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4]), str(r[5])))
               for r in cur.fetchall()]

    say(f"- SQL Server allocated rows: {len(ms_rows)}")
    say(f"- Snowflake allocated rows:  {len(sf_rows)}")
    say("")

    # --- Row-by-row diff on natural key ---
    say("## 5. Row-by-row diff (GL, CC, FP, OriginalAmount, SourceLineID, Pct)")
    if ms_rows == sf_rows:
        say(f"- **PASS** — all {len(ms_rows)} rows match")
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
    say("## 6. Aggregate diff (SUM(OriginalAmount), COUNT) on allocated children")
    ms_agg = mssql_rows(
        "SELECT CAST(SUM(OriginalAmount) AS DECIMAL(19,4)), COUNT(*) "
        "FROM Planning.BudgetLineItem "
        "WHERE BudgetHeaderID = 1 AND IsAllocated = 1 AND AllocationSourceLineID IS NOT NULL"
    )[0]
    cur.execute(
        "SELECT CAST(SUM(OriginalAmount) AS NUMBER(19,4)), COUNT(*) "
        "FROM BudgetLineItem "
        "WHERE BudgetHeaderID = 1 AND IsAllocated = TRUE AND AllocationSourceLineID IS NOT NULL"
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
        say("- **VERIFICATION PASSED**: SQL Server baseline and Snowflake migration produced identical allocation outputs.")
    else:
        say("- **VERIFICATION FAILED** — see sections above.")
    say("")

    cur.close()
    conn.close()

    (OUT / "summary.md").write_text("\n".join(report) + "\n")
    print(f"\nReport: {OUT}/summary.md")
    return 0 if (row_pass and agg_pass) else 1


if __name__ == "__main__":
    raise SystemExit(main())

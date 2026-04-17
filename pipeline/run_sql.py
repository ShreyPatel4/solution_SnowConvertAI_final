"""Run a .sql file on Snowflake using credentials from .env.

Splits on ';' at end-of-line. Not robust to ';' inside strings, but fine for
DDL-heavy scripts. For proc bodies, wrap in EXECUTE IMMEDIATE or use a
JavaScript-style splitter later.

Usage:
    .venv/bin/python pipeline/run_sql.py snowflake/00_bootstrap.sql
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

if len(sys.argv) < 2:
    print("Usage: run_sql.py <path.sql>", file=sys.stderr)
    sys.exit(1)

sql_path = Path(sys.argv[1])
if not sql_path.is_absolute():
    sql_path = ROOT / sql_path
if not sql_path.exists():
    print(f"File not found: {sql_path}", file=sys.stderr)
    sys.exit(1)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PAT"],
    role=os.environ.get("SNOWFLAKE_ROLE") or None,
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE") or None,
)
cur = conn.cursor()

text = sql_path.read_text()
stmts = []
buf: list[str] = []
in_dollar_block = False
for line in text.splitlines():
    stripped = line.strip()
    # Outside a $$...$$ block, skip blank lines and full-line comments.
    if not in_dollar_block:
        if not stripped or stripped.startswith("--"):
            continue
    buf.append(line)
    # A line with an odd number of `$$` tokens flips block state.
    if line.count("$$") % 2 == 1:
        in_dollar_block = not in_dollar_block
    # Statement terminator is `;` at EOL, but only OUTSIDE a $$ block —
    # procedure bodies contain many `;`-terminated inner statements.
    if not in_dollar_block and stripped.rstrip().endswith(";"):
        stmt = "\n".join(buf).rstrip().rstrip(";").strip()
        if stmt:
            stmts.append(stmt)
        buf = []
if buf:
    tail = "\n".join(buf).strip()
    if tail:
        stmts.append(tail)

print(f"Running {len(stmts)} statement(s) from {sql_path.name}")
failed = 0
for i, stmt in enumerate(stmts, 1):
    first = stmt.splitlines()[0][:80]
    print(f"  [{i}/{len(stmts)}] {first}")
    try:
        cur.execute(stmt)
        try:
            rows = cur.fetchall()
            for row in rows[:5]:
                print(f"      -> {row}")
            if len(rows) > 5:
                print(f"      ... ({len(rows) - 5} more rows)")
        except snowflake.connector.errors.InterfaceError:
            pass
    except Exception as e:
        failed += 1
        print(f"      !! {e}")

cur.close()
conn.close()
sys.exit(1 if failed else 0)

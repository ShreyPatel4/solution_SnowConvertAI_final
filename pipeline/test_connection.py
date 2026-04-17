"""Sanity-check Snowflake connectivity using credentials from .env.

Run:
    .venv/bin/python pipeline/test_connection.py
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

account = os.environ.get("SNOWFLAKE_ACCOUNT")
user = os.environ.get("SNOWFLAKE_USER")
pat = os.environ.get("SNOWFLAKE_PAT")
role = os.environ.get("SNOWFLAKE_ROLE") or "ACCOUNTADMIN"
warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE") or None

missing = [k for k, v in {
    "SNOWFLAKE_ACCOUNT": account,
    "SNOWFLAKE_USER": user,
    "SNOWFLAKE_PAT": pat,
}.items() if not v]
if missing:
    print(f"Missing in .env: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

print(f"Connecting to {account} as {user} (role={role})...")

try:
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=pat,
        role=role,
        warehouse=warehouse,
    )
except Exception as e:
    print(f"Connection failed: {e}", file=sys.stderr)
    print(
        "If password auth fails, try: authenticator='PROGRAMMATIC_ACCESS_TOKEN' "
        "or 'oauth' with token=<PAT>.",
        file=sys.stderr,
    )
    sys.exit(2)

cur = conn.cursor()
cur.execute(
    "SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ACCOUNT(), "
    "CURRENT_REGION(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
)
row = cur.fetchone()
print("Connected.")
print(f"  Version:   {row[0]}")
print(f"  User:      {row[1]}")
print(f"  Account:   {row[2]}")
print(f"  Region:    {row[3]}")
print(f"  Role:      {row[4]}")
print(f"  Warehouse: {row[5]}")
cur.close()
conn.close()

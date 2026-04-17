"""Split a T-SQL file into translation units (statements).

Used as the first stage of the AI translation pipeline:
  extract.py  ->  translate.py  ->  load to Snowflake  ->  verify.py

Emits a JSON list of {kind, name, source} records — one per batch.
kind in {procedure, function, view, table, type, batch}.

Usage:
    .venv/bin/python pipeline/extract.py path/to/file.sql
"""
import json
import re
import sys
from pathlib import Path

TYPE_PATTERNS = [
    ("procedure", re.compile(r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+([^\s(]+)", re.IGNORECASE | re.MULTILINE)),
    ("function",  re.compile(r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\s+([^\s(]+)", re.IGNORECASE | re.MULTILINE)),
    ("view",      re.compile(r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?VIEW\s+([^\s(]+)", re.IGNORECASE | re.MULTILINE)),
    ("table",     re.compile(r"^\s*CREATE\s+TABLE\s+([^\s(]+)", re.IGNORECASE | re.MULTILINE)),
    ("type",      re.compile(r"^\s*CREATE\s+TYPE\s+([^\s(]+)", re.IGNORECASE | re.MULTILINE)),
]


def extract(text: str) -> list[dict]:
    units = []
    # T-SQL batches are GO-delimited.  Snowflake input files split on ';' at
    # batch boundary — we treat both here for portability.
    batches = re.split(r"(?:^\s*GO\s*$|\n\n---\n)", text, flags=re.MULTILINE)
    for batch in batches:
        stripped = batch.strip()
        if not stripped:
            continue
        kind = "batch"
        name = None
        for k, pat in TYPE_PATTERNS:
            m = pat.search(stripped)
            if m:
                kind = k
                name = m.group(1)
                break
        units.append({"kind": kind, "name": name, "source": stripped})
    return units


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: extract.py <path.sql>", file=sys.stderr)
        sys.exit(1)
    text = Path(sys.argv[1]).read_text()
    print(json.dumps(extract(text), indent=2))

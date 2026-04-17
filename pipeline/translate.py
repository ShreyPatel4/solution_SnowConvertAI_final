"""T-SQL -> Snowflake Scripting translator using Claude.

Second stage of the AI translation pipeline (extract -> translate -> verify).
Reads a T-SQL source file, builds a structured prompt from
pipeline/prompts/translate_proc.md, invokes Claude, and returns the model's
structured output (translated SQL + rationale + confidence + lossy list).

For this take-home, proc 1 was hand-translated (faster for a single proc that
benefits from deep per-statement reasoning).  This script exists as the
infrastructure for subsequent procs: it would have translated proc 3 / proc 5
en route to shipping them, and it is how the AI-usage narrative in the README
gets "runnable code to point at", not just "I used an LLM".

Usage:
    ANTHROPIC_API_KEY=sk-ant-... .venv/bin/python pipeline/translate.py \\
        original/src/StoredProcedures/usp_ExecuteCostAllocation.sql
"""
import json
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def build_prompt(source_tsql: str) -> str:
    template = (ROOT / "pipeline/prompts/translate_proc.md").read_text()
    schema_ctx = (ROOT / "snowflake/01_schema.sql").read_text()
    dep_fns = (ROOT / "snowflake/02_functions.sql").read_text()
    dep_views = (ROOT / "snowflake/03_views.sql").read_text()
    dep_ctx = dep_fns + "\n\n" + dep_views
    return (
        template
        .replace("{{SOURCE_TSQL}}", source_tsql)
        .replace("{{SCHEMA_CONTEXT}}", schema_ctx)
        .replace("{{DEPENDENCY_CONTEXT}}", dep_ctx)
    )


def extract_json(text: str) -> dict | None:
    # Try to find a JSON object in the response
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: translate.py <path_to_tsql> [--dry-run]", file=sys.stderr)
        return 1

    src = Path(sys.argv[1])
    dry_run = "--dry-run" in sys.argv

    if not src.exists():
        print(f"File not found: {src}", file=sys.stderr)
        return 1

    source_tsql = src.read_text()
    prompt = build_prompt(source_tsql)

    if dry_run:
        print("=== Prompt (dry run) ===")
        print(prompt[:2000])
        print(f"... ({len(prompt)} chars total)")
        return 0

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set in environment or .env", file=sys.stderr)
        print("Re-run with --dry-run to inspect the prompt without an API call.", file=sys.stderr)
        return 2

    try:
        import anthropic
    except ImportError:
        print("pip install anthropic", file=sys.stderr)
        return 3

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text

    parsed = extract_json(text)
    if parsed is None:
        print("=== Raw response (JSON not found) ===")
        print(text)
        return 4

    print("=== Rationale ===")
    print(parsed.get("rationale", "(missing)"))
    print(f"\n=== Confidence: {parsed.get('confidence')} ===")
    print("\n=== Lossy conversions ===")
    for lc in parsed.get("lossy_conversions", []):
        print(f"  - {lc}")
    print("\n=== Open questions ===")
    for q in parsed.get("open_questions", []):
        print(f"  - {q}")
    print("\n=== Translated SQL ===")
    print(parsed.get("translated_sql", "(missing)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

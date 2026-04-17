"""make_loadable.py — post-process scai output into a loadable Snowflake script.

Purpose
-------
The SnowConvert AI CLI (`scai`) emits Snowflake-dialect SQL that is
syntactically valid but not always directly loadable against an arbitrary
target database/schema. This script applies a small, fixed set of
MECHANICAL edits — never logic edits — so the output can be loaded into
PLANNING_DB.PLANNING without hand-editing.

Design constraints
------------------
- Mechanical-only. No statement-body rewrites, no type conversions, no
  keyword rewrites. If a rule would change behaviour, it does not belong here.
- Pattern-based (line-by-line regex). No SQL parser / AST. The rules are
  simple enough that a parser would be overkill and would introduce its own
  risk surface.
- Every edit is logged. `--dry-run` prints `line N: <before>  ->  <after>`
  for each applied rule. A reviewer can read the log and confirm each edit
  is cosmetic.
- Rules are selectable (--rules) so the user can turn off any that don't
  apply to their particular scai output. Defaults are the conservative set.

Rules implemented
-----------------
  R1  strip_use_headers
      Remove `USE DATABASE X;` and `USE SCHEMA Y;` lines. Target DB/schema
      are supplied as CLI args, so embedded USE statements are at best
      redundant and at worst point at the wrong DB/schema.

  R2  qualify_schema
      For unqualified top-level object declarations
      (`CREATE [OR REPLACE] {PROCEDURE|FUNCTION|TABLE|VIEW} <name>`),
      prepend `<target_db>.<target_schema>.` when the name has fewer than
      two dots. Bare `Planning.xxx` style refs are upgraded to
      `<target_db>.<target_schema>.xxx`. No rewriting inside statement bodies
      — only the CREATE header line.

  R3  identifier_case
      Selectable: --case upper | strip-quotes | none.
        upper        : uppercase unquoted identifiers on CREATE headers only.
        strip-quotes : remove `"..."` around identifiers on CREATE headers
                       only, forcing Snowflake's default-fold-to-UPPER.
        none         : no change.
      scai's output casing convention is not yet empirically known on this
      machine (blocked on install — see snowconvert/RUN_LOG.md). Both
      variants are implemented so the operator picks the right one after
      inspecting the first real scai run. Body-line identifiers are NEVER
      touched — case mismatches inside statement bodies are the operator's
      responsibility, because blindly folding them risks breaking
      case-sensitive VARIANT-path references (e.g. `:Options:RoundingPrecision`).

  R4  ensure_trailing_semicolon
      If the last non-empty, non-comment line doesn't end with `;`, append
      one. Snowflake REST endpoints and the Python connector are strict
      about terminators; scai's output is sometimes missing the final one
      when the input was a single CREATE block.

Non-rules (explicit non-goals)
------------------------------
- No DECLARE/variable renames. scai's variable casing is its own output.
- No comment stripping.
- No WHITESPACE-only normalization.
- No `CREATE OR REPLACE` insertion when scai emits `CREATE` (left to operator).
- No schema-qualification INSIDE statement bodies. Doing so requires parsing
  SQL (string literals, window specs, CTEs all look like identifiers to regex).

Usage
-----
    python snowconvert/make_loadable.py \
        --input   path/to/scai_output.sql \
        --db      PLANNING_DB \
        --schema  PLANNING \
        [--output snowconvert/output_loadable.sql] \
        [--rules R1,R2,R3,R4] \
        [--case upper|strip-quotes|none] \
        [--dry-run]

Exit codes
----------
  0  success (or dry-run completed)
  1  invalid input (file missing, malformed args)
  2  no rules applied (unlikely — probably input already loadable)

Author: Shrey Patel — April 2026
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Edit record
# ---------------------------------------------------------------------------

@dataclass
class Edit:
    line_no: int      # 1-based
    rule: str         # rule id (R1..R4)
    before: str
    after: str        # empty string = line removed


@dataclass
class RuleResult:
    edits: List[Edit] = field(default_factory=list)
    new_lines: List[str] = field(default_factory=list)  # not always used — rules mutate in place


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# USE DATABASE / USE SCHEMA / USE WAREHOUSE (case-insensitive, optional trailing ;)
USE_RE = re.compile(r"""^\s*USE\s+(DATABASE|SCHEMA|WAREHOUSE)\b[^;\n]*;?\s*$""",
                    re.IGNORECASE)

# Top-level CREATE header — captures the object type and name. Supports
# optional OR REPLACE, optional TEMPORARY/SECURE, optional PROCEDURE args on
# subsequent lines (handled by anchoring to the start of the CREATE line only).
CREATE_HEADER_RE = re.compile(
    r"""^(\s*CREATE\s+(?:OR\s+REPLACE\s+)?        # CREATE [OR REPLACE]
          (?:TEMPORARY\s+|TEMP\s+|SECURE\s+)?     # optional modifier
          (PROCEDURE|FUNCTION|TABLE|VIEW|SEQUENCE)  # object type (group 2)
          \s+)                                     # whitespace
          ([^\s(]+)                                # object name (group 3) — up to ( or whitespace
          (.*)$                                     # remainder (group 4)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Quoted identifier — "Planning" or "My Schema"
QUOTED_IDENT_RE = re.compile(r'"([^"]+)"')


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------

def rule_strip_use_headers(lines: List[str]) -> Tuple[List[str], List[Edit]]:
    """R1: drop USE DATABASE / SCHEMA / WAREHOUSE lines."""
    out: List[str] = []
    edits: List[Edit] = []
    for i, ln in enumerate(lines, start=1):
        if USE_RE.match(ln):
            edits.append(Edit(i, "R1", ln.rstrip("\n"), ""))
            # drop it — do not append to out
            continue
        out.append(ln)
    return out, edits


def rule_qualify_schema(
    lines: List[str], target_db: str, target_schema: str
) -> Tuple[List[str], List[Edit]]:
    """R2: rewrite CREATE <name> to CREATE <db>.<schema>.<name> when needed.

    Rules:
      - dots(name) == 0 (bare name):            prepend db.schema.
      - dots(name) == 1 (schema-qualified):     prepend db. (replace schema portion)
      - dots(name) >= 2 (already db-qualified): leave alone
    Quoted identifiers are respected — quotes count as part of the name segment.
    """
    out: List[str] = []
    edits: List[Edit] = []
    for i, ln in enumerate(lines, start=1):
        m = CREATE_HEADER_RE.match(ln)
        if not m:
            out.append(ln)
            continue
        prefix, _obj_type, name, remainder = m.group(1), m.group(2), m.group(3), m.group(4)
        dot_count = _count_unquoted_dots(name)
        if dot_count >= 2:
            # already fully qualified — leave alone
            out.append(ln)
            continue
        if dot_count == 1:
            # schema.name — replace schema portion with target_schema, prepend db.
            _orig_schema, name_part = _split_first_unquoted_dot(name)
            new_name = f"{target_db}.{target_schema}.{name_part}"
        else:
            # bare name
            new_name = f"{target_db}.{target_schema}.{name}"
        new_line = prefix + new_name + remainder
        # Preserve trailing newline if present
        if ln.endswith("\n") and not new_line.endswith("\n"):
            new_line += "\n"
        # Skip no-op edits (e.g. already exactly our target qualification)
        if new_line == ln:
            out.append(ln)
            continue
        edits.append(Edit(i, "R2", ln.rstrip("\n"), new_line.rstrip("\n")))
        out.append(new_line)
    return out, edits


def rule_identifier_case(
    lines: List[str], mode: str
) -> Tuple[List[str], List[Edit]]:
    """R3: apply CREATE-header identifier case rule.

    mode:
      'upper'        — uppercase unquoted identifier on CREATE header only
      'strip-quotes' — remove "..." around identifier on CREATE header only
      'none'         — no-op
    """
    if mode == "none":
        return lines, []
    out: List[str] = []
    edits: List[Edit] = []
    for i, ln in enumerate(lines, start=1):
        m = CREATE_HEADER_RE.match(ln)
        if not m:
            out.append(ln)
            continue
        prefix, _obj, name, remainder = m.group(1), m.group(2), m.group(3), m.group(4)
        if mode == "strip-quotes":
            new_name = QUOTED_IDENT_RE.sub(r"\1", name)
        elif mode == "upper":
            new_name = _upper_unquoted_identifier(name)
        else:
            raise SystemExit(f"Invalid --case mode: {mode}")
        if new_name == name:
            out.append(ln)
            continue
        new_line = prefix + new_name + remainder
        if ln.endswith("\n") and not new_line.endswith("\n"):
            new_line += "\n"
        edits.append(Edit(i, "R3", ln.rstrip("\n"), new_line.rstrip("\n")))
        out.append(new_line)
    return out, edits


def rule_ensure_trailing_semicolon(
    lines: List[str],
) -> Tuple[List[str], List[Edit]]:
    """R4: append a trailing semicolon if the final non-empty, non-comment
    line doesn't have one."""
    # find last non-empty non-comment line
    idx = len(lines) - 1
    while idx >= 0:
        stripped = lines[idx].strip()
        if not stripped:
            idx -= 1
            continue
        if stripped.startswith("--") or stripped.startswith("//"):
            idx -= 1
            continue
        break
    if idx < 0:
        return lines, []
    last = lines[idx]
    last_stripped = last.rstrip("\n")
    # The END of a stored-procedure block might be `END;` or `$$;`. Also a
    # bare `$$` with no semicolon happens. Heuristic: if the trimmed line ends
    # with ; or $$ or END (with or without ;), leave alone; else append ;.
    trimmed = last_stripped.rstrip()
    if trimmed.endswith(";"):
        return lines, []
    if trimmed.endswith("$$"):
        # Snowflake is lenient here, but many clients want trailing ;
        new_line = trimmed + ";" + ("\n" if last.endswith("\n") else "")
        edits = [Edit(idx + 1, "R4", last_stripped, new_line.rstrip("\n"))]
        new_lines = list(lines)
        new_lines[idx] = new_line
        return new_lines, edits
    # Generic: append ;
    new_line = trimmed + ";" + ("\n" if last.endswith("\n") else "")
    edits = [Edit(idx + 1, "R4", last_stripped, new_line.rstrip("\n"))]
    new_lines = list(lines)
    new_lines[idx] = new_line
    return new_lines, edits


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_unquoted_dots(s: str) -> int:
    """Count dots in s that are outside of "..." quotes."""
    count = 0
    in_quote = False
    i = 0
    while i < len(s):
        c = s[i]
        if c == '"':
            in_quote = not in_quote
        elif c == "." and not in_quote:
            count += 1
        i += 1
    return count


def _split_first_unquoted_dot(s: str) -> Tuple[str, str]:
    """Split s at the first unquoted dot. Returns (before, after)."""
    in_quote = False
    for i, c in enumerate(s):
        if c == '"':
            in_quote = not in_quote
        elif c == "." and not in_quote:
            return s[:i], s[i + 1 :]
    return s, ""


def _upper_unquoted_identifier(name: str) -> str:
    """Uppercase identifier segments that are NOT double-quoted.

    'Planning.usp_Foo' -> 'PLANNING.USP_FOO'
    '"Planning".usp_Foo' -> '"Planning".USP_FOO'   (quoted part preserved)
    """
    out = []
    in_quote = False
    buf = ""
    for c in name:
        if c == '"':
            if in_quote:
                # closing quote — emit preserved
                buf += c
                out.append(buf)
                buf = ""
                in_quote = False
            else:
                # opening quote — flush buf (unquoted, so upper) and start preserving
                out.append(buf.upper())
                buf = c
                in_quote = True
        else:
            buf += c
    if in_quote:
        # Unterminated quote — leave as-is to avoid corrupting the output
        out.append(buf)
    else:
        out.append(buf.upper())
    return "".join(out)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Apply mechanical edits to scai output to make it loadable.",
    )
    ap.add_argument("--input", required=True, help="Path to scai output .sql file")
    ap.add_argument("--db", required=True, help="Target Snowflake database (e.g. PLANNING_DB)")
    ap.add_argument("--schema", required=True, help="Target Snowflake schema (e.g. PLANNING)")
    ap.add_argument(
        "--output",
        default=None,
        help="Output path (default: snowconvert/output_loadable.sql relative to this script)",
    )
    ap.add_argument(
        "--rules",
        default="R1,R2,R3,R4",
        help="Comma-separated rule IDs to apply. Default: R1,R2,R3,R4.",
    )
    ap.add_argument(
        "--case",
        default="upper",
        choices=["upper", "strip-quotes", "none"],
        help="Identifier case rule mode for R3. Default: upper.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print edits but do not write the output file.",
    )
    return ap.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    in_path = Path(args.input).resolve()
    if not in_path.is_file():
        sys.stderr.write(f"ERROR: input file not found: {in_path}\n")
        return 1

    here = Path(__file__).resolve().parent
    default_output = here / "output_loadable.sql"
    out_path = Path(args.output).resolve() if args.output else default_output

    with in_path.open("r", encoding="utf-8") as f:
        lines = f.readlines()

    rules_requested = [r.strip().upper() for r in args.rules.split(",") if r.strip()]
    valid_rules = {"R1", "R2", "R3", "R4"}
    for r in rules_requested:
        if r not in valid_rules:
            sys.stderr.write(f"ERROR: invalid rule id '{r}' (valid: R1..R4)\n")
            return 1

    all_edits: List[Edit] = []

    # Apply rules in fixed order — later rules see the output of earlier ones.
    # This is intentional: R2 should qualify CREATE headers AFTER R1 has
    # removed stray USE lines (otherwise the indices in the edit log would
    # shift midway through a pass).
    if "R1" in rules_requested:
        lines, edits = rule_strip_use_headers(lines)
        all_edits.extend(edits)
    if "R2" in rules_requested:
        lines, edits = rule_qualify_schema(lines, args.db, args.schema)
        all_edits.extend(edits)
    if "R3" in rules_requested:
        lines, edits = rule_identifier_case(lines, args.case)
        all_edits.extend(edits)
    if "R4" in rules_requested:
        lines, edits = rule_ensure_trailing_semicolon(lines)
        all_edits.extend(edits)

    # Print report
    print(f"make_loadable: {in_path}")
    print(f"               -> {out_path}")
    print(f"               db={args.db} schema={args.schema} rules={','.join(rules_requested)} case={args.case}")
    print(f"               {len(all_edits)} edit(s) applied")
    print("-" * 72)
    for e in all_edits:
        if e.after == "":
            print(f"  line {e.line_no:4d} [{e.rule}] REMOVE: {e.before}")
        else:
            print(f"  line {e.line_no:4d} [{e.rule}] EDIT:   {e.before}")
            print(f"  {'':9}       {'':>4}        ->      {e.after}")
    print("-" * 72)

    if args.dry_run:
        print("DRY RUN — no file written")
        return 0

    if not all_edits:
        sys.stderr.write("WARN: no edits applied. Input may already be loadable.\n")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(lines), encoding="utf-8")
    print(f"wrote: {out_path}  ({out_path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

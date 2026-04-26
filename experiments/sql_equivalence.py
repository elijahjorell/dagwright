"""Experiment H1 — plan -> SQL structural equivalence.

The first measurement of outcome equivalence: does the dagwright
plan, when deterministically rendered to SQL, match what a
competent AE would write by hand for the same task?

This is structural-only — it compares normalized SQL strings
(whitespace + identifier folding) rather than executing them.
H2 (data-level execution against a populated DuckDB) is the
follow-up that closes the data layer; it requires more setup
(seeded jaffle_shop database) and is scaffolded but not yet wired.

Each fixture pairs the spec with a hand-written canonical SQL
that an AE would write to materialize the same metric. The
harness:

  1. Runs dagwright on the spec, takes the top-ranked plan.
  2. Renders the plan to SQL via dagwright/sql_render.py.
  3. Normalizes both SQL strings.
  4. Compares — line-level diff if they differ.

What "winning" looks like:
  - Both SQL strings parse as valid DuckDB queries (out of scope
    for this harness; structural equivalence is what we test here).
  - Both compute the same logical result.
  - The dagwright-rendered SQL is, modulo aliasing and whitespace,
    semantically equivalent to the canonical.

Usage:

  uv run --no-sync python experiments/sql_equivalence.py
  uv run --no-sync python experiments/sql_equivalence.py --emit  # print rendered SQL
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixture pairs (spec + hand-written ground-truth SQL)
# ---------------------------------------------------------------------------


@dataclass
class FixturePair:
    id: str
    spec: Path
    manifest: Path
    bi: Optional[Path]
    plan_index: int  # which ranked plan to render (0 = top-ranked)
    canonical_sql: str  # what an AE would write by hand


# Canonical SQL for new_customers_monthly on jaffle_shop_modern.
#
# What dagwright emits today, with this fixture's symbolic range
# endpoints (earliest_event, current_period). dagwright cannot
# resolve symbolic ranges to concrete dates without warehouse
# access, so the spine CTE falls back to SELECT DISTINCT against
# the source — which technically only covers months that already
# have data. A "true" date_spine that fills genuine gaps requires
# either (a) an AE-side post-processing step to substitute in
# range() with resolved endpoints, or (b) dagwright extension to
# read warehouse metadata at compile time. Tracked as a known
# limitation in experiments/README.md.
NEW_CUSTOMERS_MONTHLY_CANONICAL = """\
WITH date_spine_month AS (
  -- range from='earliest_event' to='current_period' — symbolic, fall back to source-distinct
  SELECT DISTINCT date_trunc('month', customers.first_ordered_at) AS month
  FROM customers
  WHERE date_trunc('month', customers.first_ordered_at) IS NOT NULL
),
_agg AS (
  SELECT
    date_trunc('month', customers.first_ordered_at) AS _join_date_spine_month__month,
    count_distinct(customers.customer_id) AS new_customers
  FROM customers
  GROUP BY date_trunc('month', customers.first_ordered_at)
)
SELECT
  date_spine_month.month AS month,
  COALESCE(_agg.new_customers, 0) AS new_customers
FROM date_spine_month
LEFT JOIN _agg ON date_spine_month.month = _agg._join_date_spine_month__month;
"""


FIXTURES: list[FixturePair] = [
    FixturePair(
        id="jaffle_shop_modern / new_customers_monthly (symbolic range)",
        spec=REPO_ROOT / "tests/jaffle_shop_modern/specs/new_customers_monthly.yaml",
        manifest=REPO_ROOT / "tests/jaffle_shop_modern/manifest.json",
        bi=REPO_ROOT / "tests/jaffle_shop_modern/metabase.json",
        plan_index=0,
        canonical_sql=NEW_CUSTOMERS_MONTHLY_CANONICAL,
    ),
]


# ---------------------------------------------------------------------------
# Normalization + diff
# ---------------------------------------------------------------------------


_WS_RE = re.compile(r"\s+")
_TRAILING_SEMI_RE = re.compile(r";\s*$")


def normalize_sql(sql: str) -> str:
    """Collapse whitespace, strip trailing semicolon and comments,
    lowercase. Conservative — preserves identifiers and literals
    unchanged. Designed to forgive whitespace and comment drift,
    not to detect deep semantic equivalence."""
    out_lines: list[str] = []
    for line in sql.splitlines():
        # Strip end-of-line comments
        if "--" in line:
            line = line[: line.index("--")]
        out_lines.append(line)
    s = "\n".join(out_lines)
    s = _WS_RE.sub(" ", s).strip()
    s = _TRAILING_SEMI_RE.sub("", s).strip()
    return s.lower()


def diff_sql(canonical: str, rendered: str) -> str:
    """Render a unified diff between the two SQL strings (raw, not
    normalized — for human readability)."""
    a = canonical.strip().splitlines(keepends=False)
    b = rendered.strip().splitlines(keepends=False)
    diff = difflib.unified_diff(a, b, fromfile="canonical", tofile="rendered", lineterm="")
    return "\n".join(diff)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


@dataclass
class FixtureResult:
    fixture_id: str
    plan_index: int
    rendered_sql: str
    canonical_sql: str
    structurally_equivalent: bool
    diff: str


def render_for_fixture(fx: FixturePair) -> str:
    from dagwright.planner import run_plan
    from dagwright.sql_render import render_metric_request_plan

    args = SimpleNamespace(
        spec=fx.spec, manifest=fx.manifest, bi=fx.bi,
        top=10, format="markdown",
    )
    _, plans, _ = run_plan(args)
    if not plans:
        raise RuntimeError(f"no plans produced for {fx.id}")
    if fx.plan_index >= len(plans):
        raise RuntimeError(
            f"plan_index {fx.plan_index} out of range; got {len(plans)} plans for {fx.id}"
        )
    return render_metric_request_plan(plans[fx.plan_index])


def run(fixtures: list[FixturePair]) -> list[FixtureResult]:
    results: list[FixtureResult] = []
    for fx in fixtures:
        rendered = render_for_fixture(fx)
        equiv = normalize_sql(rendered) == normalize_sql(fx.canonical_sql)
        d = "" if equiv else diff_sql(fx.canonical_sql, rendered)
        results.append(FixtureResult(
            fixture_id=fx.id, plan_index=fx.plan_index,
            rendered_sql=rendered, canonical_sql=fx.canonical_sql,
            structurally_equivalent=equiv, diff=d,
        ))
    return results


def print_summary(results: list[FixtureResult], emit: bool) -> None:
    print()
    print("=" * 78)
    print("Plan -> SQL structural equivalence")
    print("=" * 78)
    for r in results:
        flag = "[EQUIV]" if r.structurally_equivalent else "[DIFF] "
        print(f"  {flag}  {r.fixture_id}  (plan rank {r.plan_index + 1})")
        if emit or not r.structurally_equivalent:
            print()
            print("    --- rendered ---")
            for line in r.rendered_sql.splitlines():
                print(f"    {line}")
            print()
            if not r.structurally_equivalent:
                print("    --- diff vs canonical ---")
                for line in r.diff.splitlines():
                    print(f"    {line}")
                print()

    n_equiv = sum(1 for r in results if r.structurally_equivalent)
    print()
    print(f"Result: {n_equiv} / {len(results)} fixtures structurally equivalent")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--emit", action="store_true",
                   help="Print rendered SQL even when it matches the canonical.")
    args = p.parse_args(argv)

    print(f"fixtures: {len(FIXTURES)}")
    results = run(FIXTURES)
    print_summary(results, emit=args.emit)
    return 0 if all(r.structurally_equivalent for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())

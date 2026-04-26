"""Experiment C — determinism receipts.

Run dagwright twice on each fixture, hash both outputs, verify
byte-identical results. The trivial slam-dunk: anyone can re-run
this and reproduce the same hashes.

Why this matters: the CHARTER's artifact-property thesis rests on
determinism. "Same spec + same manifest = same plan, every time,
for everyone" is the load-bearing claim that distinguishes
dagwright from any prose-plan workflow. This harness produces a
hashed receipt anyone can independently verify.

The complementary claim — that an LLM run twice on the same prompt
produces different output — is asserted but not measured here.
That's a separate experiment (and costs money). What this proves
is the dagwright half of the asymmetry.

Usage:

  uv run --no-sync python experiments/determinism.py
  uv run --no-sync python experiments/determinism.py --out experiments/results/determinism.csv
  uv run --no-sync python experiments/determinism.py --runs 5  # repeat each fixture 5 times
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import sys
import time
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Fixture:
    id: str
    spec: Path
    manifest: Path
    bi: Optional[Path]


FIXTURES: list[Fixture] = [
    Fixture(
        id="jaffle_shop / new_customers_monthly",
        spec=REPO_ROOT / "tests/jaffle_shop/specs/new_customers_monthly.yaml",
        manifest=REPO_ROOT / "tests/jaffle_shop/manifest.json",
        bi=REPO_ROOT / "tests/jaffle_shop/metabase.json",
    ),
    Fixture(
        id="jaffle_shop_modern / new_customers_monthly",
        spec=REPO_ROOT / "tests/jaffle_shop_modern/specs/new_customers_monthly.yaml",
        manifest=REPO_ROOT / "tests/jaffle_shop_modern/manifest.json",
        bi=REPO_ROOT / "tests/jaffle_shop_modern/metabase.json",
    ),
    Fixture(
        id="jaffle_shop_modern / lifetime_spend_pretax (definitional_change)",
        spec=REPO_ROOT / "tests/jaffle_shop_modern/specs/lifetime_spend_pretax.yaml",
        manifest=REPO_ROOT / "tests/jaffle_shop_modern/manifest.json",
        bi=REPO_ROOT / "tests/jaffle_shop_modern/metabase.json",
    ),
    Fixture(
        id="mattermost / dau_desktop_only (real-world manifest)",
        spec=REPO_ROOT / "tests/mattermost/specs/dau_desktop_only.yaml",
        manifest=REPO_ROOT / "tests/mattermost/manifest.json",
        bi=None,
    ),
]


def render_one(fixture: Fixture, fmt: str) -> tuple[str, float]:
    """Run dagwright on a fixture and capture its rendered output as
    a string. Returns (output, wall_ms). Captures stdout because
    `render_plan_output` prints rather than returns; that's how the
    CLI is wired and we want to hash exactly what a user sees."""
    from dagwright.planner import render_plan_output, run_plan

    args = SimpleNamespace(
        spec=fixture.spec, manifest=fixture.manifest, bi=fixture.bi,
        top=3, format=fmt,
    )
    t0 = time.perf_counter()
    spec, plans, rejections = run_plan(args)
    buf = io.StringIO()
    with redirect_stdout(buf):
        render_plan_output(args, spec, plans, rejections)
    wall_ms = (time.perf_counter() - t0) * 1000
    return buf.getvalue(), wall_ms


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def run(fixtures: list[Fixture], runs_per_fixture: int) -> list[dict]:
    rows: list[dict] = []
    for fx in fixtures:
        for fmt in ("json", "markdown"):
            outputs: list[str] = []
            wall_ms_runs: list[float] = []
            for _ in range(runs_per_fixture):
                out, wall_ms = render_one(fx, fmt)
                outputs.append(out)
                wall_ms_runs.append(wall_ms)
            hashes = [sha256(o) for o in outputs]
            all_match = len(set(hashes)) == 1
            rows.append({
                "fixture": fx.id,
                "format": fmt,
                "runs": runs_per_fixture,
                "all_byte_identical": all_match,
                "sha256": hashes[0],
                "size_bytes": len(outputs[0].encode("utf-8")),
                "mean_wall_ms": sum(wall_ms_runs) / len(wall_ms_runs),
                "min_wall_ms": min(wall_ms_runs),
                "max_wall_ms": max(wall_ms_runs),
            })
    return rows


def print_receipts(rows: list[dict]) -> None:
    print()
    print("=" * 110)
    print("Determinism receipts")
    print("=" * 110)
    header = f"{'fixture':<55}  {'fmt':<8}  {'runs':>4}  {'identical':>9}  {'sha256':>16}  {'mean ms':>7}"
    print(header)
    print("-" * len(header))
    for r in rows:
        flag = "[OK]" if r["all_byte_identical"] else "[FAIL]"
        print(
            f"{r['fixture'][:55]:<55}  {r['format']:<8}  {r['runs']:>4}  "
            f"{flag:>9}  {r['sha256'][:16]}  {r['mean_wall_ms']:>7.1f}"
        )
    print()
    all_ok = all(r["all_byte_identical"] for r in rows)
    if all_ok:
        n_total = sum(r["runs"] for r in rows)
        print(f"  ALL DETERMINISTIC: {len(rows)} (fixture, format) pairs × {rows[0]['runs']} runs each = {n_total} compiles, every run byte-identical.")
    else:
        print("  FAILURES PRESENT — at least one fixture produced non-deterministic output.")


def write_csv(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--runs", type=int, default=3,
                   help="Number of runs per (fixture, format) pair (default: 3).")
    p.add_argument("--out", type=Path,
                   default=REPO_ROOT / "experiments/results/determinism.csv")
    args = p.parse_args(argv)

    if args.runs < 2:
        print("--runs must be at least 2 (need to compare across runs).", file=sys.stderr)
        return 1

    print(f"runs per fixture: {args.runs}")
    print(f"fixtures:         {len(FIXTURES)}")
    print(f"formats:          json, markdown (each fixture rendered both ways)")
    print()

    rows = run(FIXTURES, args.runs)
    print_receipts(rows)
    write_csv(rows, args.out)
    print()
    print(f"Receipts written to {args.out}")

    return 0 if all(r["all_byte_identical"] for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())

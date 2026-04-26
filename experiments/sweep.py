"""Experiment E — sweep / bulk benchmark.

Run dagwright on N specs back-to-back; record per-spec wall-clock
and the projected cost of producing the same N plans via an
LLM-only workflow.

The CHARTER's claim: "Bulk analysis. Generate plans across a
parameter sweep ('what if every active metric became desktop-only').
Seconds for hundreds of plans; would be hours and meaningful dollars
via LLM." This harness produces the dagwright half of that
asymmetry as measured numbers, and the LLM half as a projection
based on a documented per-plan token estimate.

The sweep generates N metric_request specs by varying the spec id
and the consumer artifact along an axis. The plans themselves end
up nearly identical — that's intentional. This is a throughput
test: how fast can dagwright handle N independent compiles, given
realistic input?

Usage:

  uv run --no-sync python experiments/sweep.py
  uv run --no-sync python experiments/sweep.py -n 500
  uv run --no-sync python experiments/sweep.py --manifest mattermost
  uv run --no-sync python experiments/sweep.py --tokens-per-llm-plan 50000
"""

from __future__ import annotations

import argparse
import csv
import io
import statistics
import sys
import tempfile
import time
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixture catalog
# ---------------------------------------------------------------------------


@dataclass
class FixtureSet:
    name: str
    manifest: Path
    bi: Optional[Path]
    base_spec: Path
    label: str  # for output


FIXTURE_SETS: dict[str, FixtureSet] = {
    "jaffle_shop_modern": FixtureSet(
        name="jaffle_shop_modern",
        manifest=REPO_ROOT / "tests/jaffle_shop_modern/manifest.json",
        bi=REPO_ROOT / "tests/jaffle_shop_modern/metabase.json",
        base_spec=REPO_ROOT / "tests/jaffle_shop_modern/specs/new_customers_monthly.yaml",
        label="jaffle_shop_modern (small fixture)",
    ),
    "mattermost": FixtureSet(
        name="mattermost",
        manifest=REPO_ROOT / "tests/mattermost/manifest.json",
        bi=None,
        base_spec=REPO_ROOT / "tests/mattermost/specs/dau_desktop_only.yaml",
        label="mattermost (302 models, real-world manifest)",
    ),
}


# ---------------------------------------------------------------------------
# Spec generation
# ---------------------------------------------------------------------------


def generate_specs(fixture: FixtureSet, n: int, out_dir: Path) -> list[Path]:
    """Generate N variations of the base spec by rewriting the id and
    consumer.artifact fields. The plans themselves end up nearly
    identical — this is throughput, not plan-quality coverage."""
    base = fixture.base_spec.read_text(encoding="utf-8")
    paths: list[Path] = []
    for i in range(n):
        modified = base
        # Rewrite the id line (always present on every spec)
        modified = _rewrite_field(modified, "id", f"sweep_spec_{i:04d}")
        # Rewrite consumer.artifact if the spec has one (metric_request).
        # definitional_change specs use a different shape and don't have
        # this — that's OK, the rewrite is a no-op.
        if "artifact:" in modified:
            modified = _rewrite_field(modified, "artifact", f"sweep_artifact_{i:04d}", indent_required=True)

        path = out_dir / f"sweep_spec_{i:04d}.yaml"
        path.write_text(modified, encoding="utf-8")
        paths.append(path)
    return paths


def _rewrite_field(yaml_text: str, key: str, new_value: str, indent_required: bool = False) -> str:
    """Replace the first line matching `<indent>key: value` with the new
    value, preserving indentation. Brittle but adequate for the simple
    rewrite this sweep needs. Falls back silently if the field isn't
    found."""
    lines = yaml_text.split("\n")
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if not stripped.startswith(f"{key}:"):
            continue
        if indent_required and line == stripped:
            # Caller wanted an indented field; this match is at column 0.
            continue
        indent = line[: len(line) - len(stripped)]
        lines[i] = f"{indent}{key}: {new_value}"
        break
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------


@dataclass
class SweepResult:
    spec_path: Path
    wall_ms: float
    plans_produced: int
    rejections: int
    error: str = ""


def run_one(spec_path: Path, fixture: FixtureSet) -> SweepResult:
    """Run dagwright on one spec; capture (and discard) the rendered
    output so the timing reflects realistic CLI behaviour. Errors are
    captured per-spec so one bad spec doesn't blow up the sweep."""
    from dagwright.loaders import SpecError
    from dagwright.planner import render_plan_output, run_plan

    args = SimpleNamespace(
        spec=spec_path, manifest=fixture.manifest, bi=fixture.bi,
        top=3, format="markdown",
    )
    t0 = time.perf_counter()
    try:
        spec, plans, rejections = run_plan(args)
        # Render to /dev/null so the timing matches what the CLI would
        # do end-to-end.
        with redirect_stdout(io.StringIO()):
            render_plan_output(args, spec, plans, rejections)
        wall_ms = (time.perf_counter() - t0) * 1000
        return SweepResult(
            spec_path=spec_path, wall_ms=wall_ms,
            plans_produced=len(plans), rejections=len(rejections),
        )
    except (SpecError, Exception) as e:
        wall_ms = (time.perf_counter() - t0) * 1000
        return SweepResult(
            spec_path=spec_path, wall_ms=wall_ms,
            plans_produced=0, rejections=0, error=f"{type(e).__name__}: {e}",
        )


def run_sweep(fixture: FixtureSet, n: int) -> tuple[list[SweepResult], float, float]:
    """Returns (results, generation_wall_ms, sweep_wall_ms)."""
    with tempfile.TemporaryDirectory(prefix="dagwright-sweep-") as tmp_str:
        tmp = Path(tmp_str)
        t0 = time.perf_counter()
        spec_paths = generate_specs(fixture, n, tmp)
        gen_ms = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        results = [run_one(p, fixture) for p in spec_paths]
        sweep_ms = (time.perf_counter() - t0) * 1000

    return results, gen_ms, sweep_ms


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_summary(
    fixture: FixtureSet,
    n: int,
    results: list[SweepResult],
    gen_ms: float,
    sweep_ms: float,
    tokens_per_llm_plan: int,
) -> None:
    wall_msks = [r.wall_ms for r in results]
    successes = [r for r in results if not r.error]
    error_count = len(results) - len(successes)
    plans_total = sum(r.plans_produced for r in results)
    rejections_total = sum(r.rejections for r in results)

    print()
    print("=" * 78)
    print(f"Sweep: N = {n} on {fixture.label}")
    print("=" * 78)
    print(f"  spec generation:   {gen_ms:>7.1f} ms  ({gen_ms / n:.2f} ms/spec)")
    print(f"  total compile:     {sweep_ms:>7.1f} ms  ({sweep_ms / 1000:.2f} s)")
    print(f"  plans produced:    {plans_total} across {len(successes)} successful specs")
    print(f"  rejections:        {rejections_total}")
    print(f"  errors:            {error_count}")
    print()
    print("  Per-spec wall-time distribution (ms):")
    print(f"    mean:   {statistics.mean(wall_msks):>7.2f}")
    print(f"    median: {statistics.median(wall_msks):>7.2f}")
    print(f"    p95:    {sorted(wall_msks)[int(0.95 * len(wall_msks))]:>7.2f}")
    print(f"    p99:    {sorted(wall_msks)[int(0.99 * len(wall_msks))]:>7.2f}")
    print(f"    max:    {max(wall_msks):>7.2f}")

    print()
    print(f"  LLM-only projection (assuming {tokens_per_llm_plan:,} tokens/plan):")
    total_tokens = n * tokens_per_llm_plan
    print(f"    tokens to produce {n} plans via prose regeneration: {total_tokens:,}")
    print(f"    dagwright tokens:                                    0")

    # Sanity-check rate quotes against typical Anthropic pricing.
    # Quote both Sonnet and Opus to bracket a "real" cost.
    sonnet_in_per_mtok = 3.0    # USD / Mtok input — adjust as Anthropic prices move
    sonnet_out_per_mtok = 15.0
    opus_in_per_mtok = 15.0
    opus_out_per_mtok = 75.0

    # Assume 70/30 split between input and output for a from-scratch
    # plan generation (conservative — heavy on input context).
    input_tok = int(total_tokens * 0.7)
    output_tok = total_tokens - input_tok

    sonnet_cost = (input_tok * sonnet_in_per_mtok + output_tok * sonnet_out_per_mtok) / 1_000_000
    opus_cost = (input_tok * opus_in_per_mtok + output_tok * opus_out_per_mtok) / 1_000_000

    print(f"    estimated USD via Sonnet 4.6 (rough rates): ${sonnet_cost:>6.2f}")
    print(f"    estimated USD via Opus 4.7 (rough rates):   ${opus_cost:>6.2f}")
    print(f"    dagwright cost:                              $ 0.00")
    print()
    print("  Notes on the projection:")
    print("    - Pricing rates baked in are approximate; verify against")
    print("      current Anthropic published rates before quoting.")
    print("    - Tokens-per-plan default of 38K is the CHARTER's stated")
    print("      figure for a from-scratch prose plan; override with")
    print("      --tokens-per-llm-plan if your harness produces a")
    print("      different empirical number from Experiment B.")


def write_csv(results: list[SweepResult], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["spec_path", "wall_ms", "plans_produced", "rejections", "error"])
        for r in results:
            writer.writerow([
                r.spec_path.name, f"{r.wall_ms:.2f}",
                r.plans_produced, r.rejections, r.error,
            ])


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("-n", "--n-specs", type=int, default=100,
                   help="Number of specs to generate and compile (default: 100).")
    p.add_argument("--manifest", default="jaffle_shop_modern",
                   choices=sorted(FIXTURE_SETS.keys()),
                   help="Which fixture's manifest to sweep against (default: jaffle_shop_modern).")
    p.add_argument("--tokens-per-llm-plan", type=int, default=38_000,
                   help="Estimated tokens to produce one plan via prose regeneration "
                        "(CHARTER's stated estimate is ~38K; override with measured "
                        "value from Experiment B).")
    p.add_argument("--out", type=Path,
                   default=REPO_ROOT / "experiments/results/sweep.csv")
    args = p.parse_args(argv)

    fixture = FIXTURE_SETS[args.manifest]
    print(f"manifest:          {fixture.label}")
    print(f"base spec:         {fixture.base_spec.relative_to(REPO_ROOT)}")
    print(f"N:                 {args.n_specs}")

    results, gen_ms, sweep_ms = run_sweep(fixture, args.n_specs)
    write_csv(results, args.out)
    print_summary(fixture, args.n_specs, results, gen_ms, sweep_ms, args.tokens_per_llm_plan)
    print()
    print(f"Per-spec timings written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

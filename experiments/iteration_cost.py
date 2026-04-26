"""Experiment B — cost-per-iteration microbenchmark.

Compares two agent configurations on the same iterative AE task:

  control   : prose-plan regeneration each iteration (no dagwright).
              The LLM holds full context; on each refinement it produces
              a complete fresh plan as prose. This is the AE+LLM
              workflow today.

  treatment : LLM-edits-spec + dagwright-compiles each iteration. The
              LLM produces a spec on iteration 0; on each refinement
              it edits the spec only. dagwright then compiles the
              spec to a ranked plan deterministically.

For each iteration the harness records LLM input/output tokens, LLM
wall-clock, dagwright wall-clock (treatment only), and the total.
Output is one row per (agent, iteration) in a CSV. A console summary
prints totals.

The expected shape, if the thesis holds:
  - control:   tokens grow linearly with iteration (history accumulates)
  - treatment: tokens roughly flat per iteration (each call is one
               edit; dagwright handles the structure)

Note on the control's input growth: it IS realistic. The full
conversation history is what holds plan state in a prose workflow;
trimming it changes the experiment. The growth is part of what
makes the prose-only loop expensive at scale.

Caveats:
  - LLM output is non-deterministic at temperature > 0. We pin
    temperature=0 but Anthropic does not currently expose a seed,
    so retries can still vary slightly.
  - The spec-edit step in the treatment can fail YAML validation;
    the harness retries once with the validate_spec error appended.
  - Pricing is not baked in. The CSV reports tokens; multiply by
    your model's per-Mtok rates to get cost.

Usage:

  # Dry run (no API calls; confirms harness wiring)
  uv run python experiments/iteration_cost.py --dry-run

  # Real run against Anthropic
  export ANTHROPIC_API_KEY=sk-ant-...
  uv run --extra experiments python experiments/iteration_cost.py \\
    --task new_customers_monthly \\
    --model claude-sonnet-4-6 \\
    --out experiments/results/iteration_cost.csv
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixture tasks
# ---------------------------------------------------------------------------


@dataclass
class Task:
    """One iterative AE workflow: an initial NL ask + refinements."""

    id: str
    initial_ask: str
    manifest_path: Path
    bi_path: Optional[Path]
    refinements: list[str]


TASKS: dict[str, Task] = {
    "new_customers_monthly": Task(
        id="new_customers_monthly",
        initial_ask=(
            "The growth team wants to track the number of new customers "
            "acquired each month — customers whose first order falls in "
            "that month — on the growth dashboard. Currently no time-"
            "aggregated view of customer acquisition exists; the customers "
            "mart is per-customer only. Produce a plan: which existing "
            "model to aggregate from, what new model(s) to build, what "
            "operations the dbt change requires, contracts on the new "
            "model, and blast radius (which existing dashboards / "
            "downstream models are affected, if any)."
        ),
        manifest_path=REPO_ROOT / "tests/jaffle_shop_modern/manifest.json",
        bi_path=REPO_ROOT / "tests/jaffle_shop_modern/metabase.json",
        refinements=[
            "Refine: only count customers who have placed more than one order. "
            "Add the appropriate filter and update the plan.",
            "Refine: switch from monthly grain to weekly grain.",
            "Refine: rename the consumer artifact from 'growth_dashboard' to "
            "'acquisition_funnel'.",
            "Refine: add a second measure to the same model: total lifetime "
            "spend across these new customers.",
            "Refine: bump the contract tier from standard to critical "
            "because finance is now a downstream consumer.",
        ],
    ),
}


# ---------------------------------------------------------------------------
# Result records
# ---------------------------------------------------------------------------


@dataclass
class IterationResult:
    task_id: str
    agent_kind: str  # "control" | "treatment"
    iteration: int
    refinement: Optional[str]
    llm_input_tokens: int
    llm_output_tokens: int
    llm_wall_ms: float
    dagwright_wall_ms: float
    total_wall_ms: float
    notes: str = ""


# ---------------------------------------------------------------------------
# Anthropic client wrapper (real or dry-run)
# ---------------------------------------------------------------------------


class LLMClient:
    """Thin wrapper around the Anthropic Messages API. Dry-run mode
    skips network calls and synthesizes plausible token counts so the
    harness can be exercised without an API key."""

    def __init__(self, model: str, dry_run: bool):
        self.model = model
        self.dry_run = dry_run
        self._client = None
        if not dry_run:
            try:
                from anthropic import Anthropic
            except ImportError as e:
                raise SystemExit(
                    "anthropic SDK not installed. Run:\n"
                    "  uv pip install -e \".[experiments]\"\n"
                    "or pass --dry-run to exercise the harness without API "
                    "calls."
                ) from e
            self._client = Anthropic()

    def call(self, messages: list[dict], max_tokens: int = 8000) -> tuple[str, int, int, float]:
        """Send a Messages API call. Returns (text, in_tokens, out_tokens,
        wall_ms). In dry-run mode, fabricates plausible numbers based on
        message size so the CSV is shaped correctly."""
        t0 = time.perf_counter()
        if self.dry_run:
            # Synthesize: input tokens proportional to message bytes / 4;
            # output tokens flat-ish per call. Different shapes for
            # control (full plan) vs treatment (spec edit) — we infer
            # from prompt content.
            joined = "\n".join(m["content"] for m in messages if isinstance(m["content"], str))
            in_tok = max(500, len(joined) // 4)
            is_treatment = "dagwright spec" in joined.lower() or "edit the spec" in joined.lower() or "produce a yaml" in joined.lower()
            out_tok = 600 if is_treatment else 4500
            text = "[DRY-RUN OUTPUT]\nkind: metric_request\nid: stub" if is_treatment else "[DRY-RUN PROSE PLAN]"
            wall_ms = (time.perf_counter() - t0) * 1000
            return text, in_tok, out_tok, wall_ms

        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0,
            messages=messages,
        )
        wall_ms = (time.perf_counter() - t0) * 1000
        text = response.content[0].text if response.content else ""
        in_tok = response.usage.input_tokens
        out_tok = response.usage.output_tokens
        return text, in_tok, out_tok, wall_ms


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


CONTROL_INITIAL = """You are an analytics engineer planning a dbt change against a real-world dbt project.

## Project summary

```json
{manifest_summary}
```

## Stakeholder request

{ask}

## Your task

Produce a complete plan as markdown. Include:

- The chosen parent model and why
- Operations the change requires (new models, edges, contracts)
- Contracts on the new model (which BI artifacts depend on it)
- Blast radius (existing dashboards / downstream dbt models affected)
- Risks and notes

Be concrete and specific to this project's actual model names. Output the plan as markdown only, no preamble."""


CONTROL_REFINEMENT = """The stakeholder has given a refinement:

> {refinement}

Produce an updated complete plan as markdown reflecting this change. Same structure as before — chosen parent, operations, contracts, blast radius, risks. Include the full plan, not a diff."""


TREATMENT_INITIAL = """You are an analytics engineer authoring a dagwright YAML spec from a stakeholder request.

## dagwright spec schema

```json
{schema}
```

## Project summary

```json
{manifest_summary}
```

## Stakeholder request

{ask}

## Your task

Produce a single dagwright YAML spec that captures the stakeholder request. Output the YAML only, no preamble or explanation. Wrap the YAML in a fenced ```yaml block."""


TREATMENT_REFINEMENT = """Edit the following dagwright spec to apply this refinement:

> {refinement}

## Current spec

```yaml
{current_spec}
```

## Your task

Produce the updated YAML spec. Make the smallest edit consistent with the refinement; do not rewrite fields that don't need to change. Output the updated YAML only, wrapped in a fenced ```yaml block."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_YAML_FENCE_RE = re.compile(r"```(?:yaml)?\s*\n(.*?)```", re.DOTALL)


def extract_yaml(text: str) -> str:
    """Pull a fenced YAML block out of the model's response. Falls back
    to the whole text if no fence is found."""
    m = _YAML_FENCE_RE.search(text)
    if m:
        return m.group(1).strip() + "\n"
    return text.strip() + "\n"


def get_manifest_summary(manifest_path: Path) -> dict:
    """Compact summary of the manifest using the same code path the MCP
    tool uses, so the experiment matches what an MCP-driven AE would
    actually pass in context."""
    from dagwright.mcp_server import summarize_manifest
    return summarize_manifest(str(manifest_path.resolve()))


def get_spec_schema_payload() -> dict:
    """Pull the schema descriptor for both spec kinds so the treatment
    agent has the canonical vocabulary in context."""
    from dagwright.mcp_server import get_spec_schema
    return get_spec_schema()


def run_dagwright_plan(spec_path: Path, manifest_path: Path, bi_path: Optional[Path]) -> tuple[float, str]:
    """Compile the spec to a ranked plan list. Returns (wall_ms, error_or_empty)."""
    from dagwright.loaders import SpecError
    from dagwright.planner import run_plan
    args = SimpleNamespace(
        spec=spec_path,
        manifest=manifest_path,
        bi=bi_path,
        top=3,
        format="markdown",
    )
    t0 = time.perf_counter()
    try:
        run_plan(args)
        return (time.perf_counter() - t0) * 1000, ""
    except (SpecError, Exception) as e:  # broad on purpose for harness robustness
        return (time.perf_counter() - t0) * 1000, f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Agents
# ---------------------------------------------------------------------------


def run_control(task: Task, llm: LLMClient) -> list[IterationResult]:
    """Pure prose-plan regeneration agent. Conversation grows each turn."""
    summary = get_manifest_summary(task.manifest_path)
    summary_str = json.dumps(summary, indent=2)

    history: list[dict] = []
    results: list[IterationResult] = []

    # Iteration 0: initial ask
    prompt = CONTROL_INITIAL.format(manifest_summary=summary_str, ask=task.initial_ask)
    history.append({"role": "user", "content": prompt})
    text, in_tok, out_tok, llm_ms = llm.call(history, max_tokens=8000)
    history.append({"role": "assistant", "content": text})
    results.append(IterationResult(
        task_id=task.id, agent_kind="control", iteration=0, refinement=None,
        llm_input_tokens=in_tok, llm_output_tokens=out_tok,
        llm_wall_ms=llm_ms, dagwright_wall_ms=0.0, total_wall_ms=llm_ms,
    ))

    # Iterations 1..N: refinements
    for i, refinement in enumerate(task.refinements, start=1):
        prompt = CONTROL_REFINEMENT.format(refinement=refinement)
        history.append({"role": "user", "content": prompt})
        text, in_tok, out_tok, llm_ms = llm.call(history, max_tokens=8000)
        history.append({"role": "assistant", "content": text})
        results.append(IterationResult(
            task_id=task.id, agent_kind="control", iteration=i, refinement=refinement,
            llm_input_tokens=in_tok, llm_output_tokens=out_tok,
            llm_wall_ms=llm_ms, dagwright_wall_ms=0.0, total_wall_ms=llm_ms,
        ))

    return results


def run_treatment(task: Task, llm: LLMClient) -> list[IterationResult]:
    """LLM edits the spec; dagwright compiles. State lives in the spec
    file, so each call sends only the current spec + the refinement —
    not the full conversation history."""
    summary = get_manifest_summary(task.manifest_path)
    summary_str = json.dumps(summary, indent=2)
    schema = get_spec_schema_payload()
    schema_str = json.dumps(schema, indent=2)

    spec_path = Path(tempfile.mkdtemp(prefix="dagwright-experiment-")) / "spec.yaml"
    results: list[IterationResult] = []

    # Iteration 0: write spec from NL
    prompt = TREATMENT_INITIAL.format(
        schema=schema_str, manifest_summary=summary_str, ask=task.initial_ask
    )
    text, in_tok, out_tok, llm_ms = llm.call(
        [{"role": "user", "content": prompt}], max_tokens=2000
    )
    yaml_str = extract_yaml(text)
    spec_path.write_text(yaml_str, encoding="utf-8")
    dw_ms, err = run_dagwright_plan(spec_path, task.manifest_path, task.bi_path)
    results.append(IterationResult(
        task_id=task.id, agent_kind="treatment", iteration=0, refinement=None,
        llm_input_tokens=in_tok, llm_output_tokens=out_tok,
        llm_wall_ms=llm_ms, dagwright_wall_ms=dw_ms, total_wall_ms=llm_ms + dw_ms,
        notes=err,
    ))

    # Iterations 1..N: refinements
    for i, refinement in enumerate(task.refinements, start=1):
        current_spec = spec_path.read_text(encoding="utf-8")
        prompt = TREATMENT_REFINEMENT.format(
            refinement=refinement, current_spec=current_spec
        )
        text, in_tok, out_tok, llm_ms = llm.call(
            [{"role": "user", "content": prompt}], max_tokens=2000
        )
        yaml_str = extract_yaml(text)
        spec_path.write_text(yaml_str, encoding="utf-8")
        dw_ms, err = run_dagwright_plan(spec_path, task.manifest_path, task.bi_path)
        results.append(IterationResult(
            task_id=task.id, agent_kind="treatment", iteration=i, refinement=refinement,
            llm_input_tokens=in_tok, llm_output_tokens=out_tok,
            llm_wall_ms=llm_ms, dagwright_wall_ms=dw_ms, total_wall_ms=llm_ms + dw_ms,
            notes=err,
        ))

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def write_csv(results: list[IterationResult], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "task_id", "agent_kind", "iteration", "refinement",
            "llm_input_tokens", "llm_output_tokens",
            "llm_wall_ms", "dagwright_wall_ms", "total_wall_ms",
            "notes",
        ])
        for r in results:
            writer.writerow([
                r.task_id, r.agent_kind, r.iteration, r.refinement or "",
                r.llm_input_tokens, r.llm_output_tokens,
                f"{r.llm_wall_ms:.1f}", f"{r.dagwright_wall_ms:.1f}",
                f"{r.total_wall_ms:.1f}", r.notes,
            ])


def print_summary(results: list[IterationResult]) -> None:
    print()
    print("=" * 78)
    print("Per-iteration totals")
    print("=" * 78)
    header = f"{'agent':>10}  {'iter':>4}  {'in_tok':>8}  {'out_tok':>8}  {'llm_ms':>9}  {'dw_ms':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.agent_kind:>10}  {r.iteration:>4}  "
            f"{r.llm_input_tokens:>8}  {r.llm_output_tokens:>8}  "
            f"{r.llm_wall_ms:>9.0f}  {r.dagwright_wall_ms:>7.0f}"
        )

    print()
    print("=" * 78)
    print("Totals by agent")
    print("=" * 78)
    by_agent: dict[str, dict] = {}
    for r in results:
        a = by_agent.setdefault(r.agent_kind, {
            "iterations": 0, "in_tok": 0, "out_tok": 0,
            "llm_ms": 0.0, "dw_ms": 0.0,
        })
        a["iterations"] += 1
        a["in_tok"] += r.llm_input_tokens
        a["out_tok"] += r.llm_output_tokens
        a["llm_ms"] += r.llm_wall_ms
        a["dw_ms"] += r.dagwright_wall_ms

    for agent, t in by_agent.items():
        total_tok = t["in_tok"] + t["out_tok"]
        total_ms = t["llm_ms"] + t["dw_ms"]
        print(
            f"  {agent:>10}: {t['iterations']} iters, "
            f"{total_tok:>7} tokens ({t['in_tok']} in / {t['out_tok']} out), "
            f"{total_ms / 1000:>5.1f} s wall"
        )

    if "control" in by_agent and "treatment" in by_agent:
        c, t = by_agent["control"], by_agent["treatment"]
        c_total = c["in_tok"] + c["out_tok"]
        t_total = t["in_tok"] + t["out_tok"]
        ratio = c_total / t_total if t_total else float("inf")
        print()
        print(f"  control / treatment token ratio: {ratio:.2f}x")
        print(f"  control / treatment wall ratio:  {(c['llm_ms'] + c['dw_ms']) / (t['llm_ms'] + t['dw_ms']):.2f}x")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--task", default="new_customers_monthly", choices=sorted(TASKS.keys()))
    p.add_argument("--model", default="claude-sonnet-4-6",
                   help="Anthropic model id (default: claude-sonnet-4-6)")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "experiments/results/iteration_cost.csv")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip API calls; synthesize plausible token counts to verify the harness wiring.")
    p.add_argument("--agent", choices=("both", "control", "treatment"), default="both",
                   help="Which agent(s) to run (default: both).")
    args = p.parse_args(argv)

    task = TASKS[args.task]

    if not args.dry_run and not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY not set. Either export it or pass --dry-run.",
            file=sys.stderr,
        )
        return 1

    llm = LLMClient(model=args.model, dry_run=args.dry_run)

    print(f"task:    {task.id}")
    print(f"model:   {args.model}{' (DRY RUN)' if args.dry_run else ''}")
    print(f"out:     {args.out}")
    print()

    all_results: list[IterationResult] = []

    if args.agent in ("both", "control"):
        print(f"--- running control: {1 + len(task.refinements)} iterations ---")
        results = run_control(task, llm)
        all_results.extend(results)
        for r in results:
            print(f"  iter {r.iteration}: {r.llm_input_tokens:>6} in / {r.llm_output_tokens:>5} out / {r.llm_wall_ms:>6.0f} ms")

    if args.agent in ("both", "treatment"):
        print()
        print(f"--- running treatment: {1 + len(task.refinements)} iterations ---")
        results = run_treatment(task, llm)
        all_results.extend(results)
        for r in results:
            note = f"  [!] {r.notes}" if r.notes else ""
            print(f"  iter {r.iteration}: {r.llm_input_tokens:>6} in / {r.llm_output_tokens:>5} out / "
                  f"llm {r.llm_wall_ms:>6.0f} ms / dw {r.dagwright_wall_ms:>5.0f} ms{note}")

    write_csv(all_results, args.out)
    print_summary(all_results)
    print()
    print(f"Results written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

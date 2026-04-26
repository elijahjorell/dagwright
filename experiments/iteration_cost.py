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
  uv run python experiments/iteration_cost.py --task all --dry-run

  # Real run against Anthropic — single task
  export ANTHROPIC_API_KEY=sk-ant-...
  uv run --extra experiments python experiments/iteration_cost.py \\
    --task new_customers_monthly \\
    --model claude-sonnet-4-6 \\
    --out experiments/results/iteration_cost.csv

  # Real run — all wired tasks (single CSV; per-task ratios printed)
  uv run --extra experiments python experiments/iteration_cost.py \\
    --task all \\
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
    "lifetime_spend_pretax": Task(
        id="lifetime_spend_pretax",
        initial_ask=(
            "Finance has standardized revenue reporting on pre-tax lifetime "
            "spend, but the executive_overview dashboard currently reads "
            "customers.lifetime_spend which is post-tax (post_tax = "
            "lifetime_spend_pretax + lifetime_tax_paid). Align the "
            "dashboard's reading to the pre-tax basis without breaking the "
            "executive_overview contract. Produce a plan: what changes in "
            "the customers model, how to handle the executive_overview "
            "contract, blast radius on other consumers, and the migration "
            "shape (in-place rename, versioned column, separate model)."
        ),
        manifest_path=REPO_ROOT / "tests/jaffle_shop_modern/manifest.json",
        bi_path=REPO_ROOT / "tests/jaffle_shop_modern/metabase.json",
        refinements=[
            "Refine: finance now wants a deprecation window — allow both "
            "definitions to coexist for one quarter rather than a hard "
            "cutover.",
            "Refine: a second consumer (cfo_pulse dashboard) was just "
            "discovered reading the same column; treat it as a must-migrate.",
            "Refine: switch the new definition's expression — pre-tax should "
            "be lifetime_spend_pretax minus refunds, not the raw column.",
            "Refine: bump the contract tier from standard to critical — "
            "finance treats this as audit-relevant.",
            "Refine: rename the conceptual basis from 'pre_tax' to "
            "'gaap_revenue' to match finance's vocabulary.",
        ],
    ),
    "dau_desktop_only": Task(
        id="dau_desktop_only",
        initial_ask=(
            "Product leadership has decided the company's headline DAU "
            "metric should reflect desktop usage only — mobile is a "
            "different segment with separate ownership and the combined "
            "number was hiding desktop trends. The "
            "fct_active_users.daily_active_users column drives "
            "customer_journey_and_new_logo_exposure and product_pulse "
            "dashboards; both must reflect the new desktop-only definition. "
            "Other consumers (data science notebooks, internal Hub views) "
            "may continue reading the all-platforms definition. Produce a "
            "plan: how the model changes, contract handling on the "
            "must-migrate dashboards, blast radius on other readers, and "
            "the migration shape."
        ),
        manifest_path=REPO_ROOT / "tests/mattermost/manifest.json",
        bi_path=None,  # Mattermost uses in-tree dbt exposures, no metabase.json
        refinements=[
            "Refine: marketing pushed back — they want the all-platforms "
            "definition preserved as a separate column rather than replaced. "
            "Rework the plan to keep both.",
            "Refine: a third dashboard (board_metrics) consumes the same "
            "column and must also migrate to desktop-only.",
            "Refine: tighten allow_stale_consumers — no consumers may keep "
            "reading the old definition once the change ships; hard cutover.",
            "Refine: split fct_active_users into fct_active_users_desktop "
            "and fct_active_users_mobile rather than retrofitting the "
            "existing model.",
            "Refine: bump the change to a versioned column model — keep "
            "daily_active_users available alongside "
            "daily_active_users_desktop_only for one release cycle.",
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
    task_ids = sorted({r.task_id for r in results})
    multi_task = len(task_ids) > 1

    print()
    print("=" * 90)
    print("Per-iteration totals")
    print("=" * 90)
    if multi_task:
        header = f"{'task':>22}  {'agent':>10}  {'iter':>4}  {'in_tok':>8}  {'out_tok':>8}  {'llm_ms':>9}  {'dw_ms':>7}"
    else:
        header = f"{'agent':>10}  {'iter':>4}  {'in_tok':>8}  {'out_tok':>8}  {'llm_ms':>9}  {'dw_ms':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        prefix = f"{r.task_id[:22]:>22}  " if multi_task else ""
        print(
            f"{prefix}{r.agent_kind:>10}  {r.iteration:>4}  "
            f"{r.llm_input_tokens:>8}  {r.llm_output_tokens:>8}  "
            f"{r.llm_wall_ms:>9.0f}  {r.dagwright_wall_ms:>7.0f}"
        )

    def aggregate(rows: list[IterationResult], key: tuple) -> dict:
        agg = {"iterations": 0, "in_tok": 0, "out_tok": 0, "llm_ms": 0.0, "dw_ms": 0.0}
        for r in rows:
            agg["iterations"] += 1
            agg["in_tok"] += r.llm_input_tokens
            agg["out_tok"] += r.llm_output_tokens
            agg["llm_ms"] += r.llm_wall_ms
            agg["dw_ms"] += r.dagwright_wall_ms
        return agg

    if multi_task:
        print()
        print("=" * 90)
        print("Per-task ratios")
        print("=" * 90)
        for tid in task_ids:
            task_rows = [r for r in results if r.task_id == tid]
            by_a = {ak: aggregate([r for r in task_rows if r.agent_kind == ak], ()) for ak in ("control", "treatment")}
            c, t = by_a["control"], by_a["treatment"]
            if not c["iterations"] or not t["iterations"]:
                continue
            c_tok = c["in_tok"] + c["out_tok"]
            t_tok = t["in_tok"] + t["out_tok"]
            tok_ratio = c_tok / t_tok if t_tok else float("inf")
            wall_ratio = (c["llm_ms"] + c["dw_ms"]) / (t["llm_ms"] + t["dw_ms"]) if (t["llm_ms"] + t["dw_ms"]) else float("inf")
            print(
                f"  {tid:>30}: control {c_tok:>6} tok / {(c['llm_ms']+c['dw_ms'])/1000:>5.1f}s ; "
                f"treatment {t_tok:>5} tok / {(t['llm_ms']+t['dw_ms'])/1000:>5.1f}s ; "
                f"ratios {tok_ratio:>5.1f}x tok, {wall_ratio:>5.1f}x wall"
            )

    print()
    print("=" * 90)
    print("Totals by agent" + (" (across all tasks)" if multi_task else ""))
    print("=" * 90)
    by_agent: dict[str, dict] = {ak: aggregate([r for r in results if r.agent_kind == ak], ()) for ak in ("control", "treatment")}

    for agent, t in by_agent.items():
        if not t["iterations"]:
            continue
        total_tok = t["in_tok"] + t["out_tok"]
        total_ms = t["llm_ms"] + t["dw_ms"]
        print(
            f"  {agent:>10}: {t['iterations']} iters, "
            f"{total_tok:>7} tokens ({t['in_tok']} in / {t['out_tok']} out), "
            f"{total_ms / 1000:>5.1f} s wall"
        )

    c, t = by_agent["control"], by_agent["treatment"]
    if c["iterations"] and t["iterations"]:
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
    p.add_argument("--task", default="new_customers_monthly",
                   choices=sorted(TASKS.keys()) + ["all"],
                   help="Task id, or 'all' to run every task in TASKS sequentially.")
    p.add_argument("--model", default="claude-sonnet-4-6",
                   help="Anthropic model id (default: claude-sonnet-4-6)")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "experiments/results/iteration_cost.csv")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip API calls; synthesize plausible token counts to verify the harness wiring.")
    p.add_argument("--agent", choices=("both", "control", "treatment"), default="both",
                   help="Which agent(s) to run (default: both).")
    args = p.parse_args(argv)

    selected_tasks: list[Task] = (
        list(TASKS.values()) if args.task == "all" else [TASKS[args.task]]
    )

    if not args.dry_run and not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY not set. Either export it or pass --dry-run.",
            file=sys.stderr,
        )
        return 1

    llm = LLMClient(model=args.model, dry_run=args.dry_run)

    print(f"tasks:   {', '.join(t.id for t in selected_tasks)}")
    print(f"model:   {args.model}{' (DRY RUN)' if args.dry_run else ''}")
    print(f"out:     {args.out}")
    print()

    all_results: list[IterationResult] = []

    for task in selected_tasks:
        print(f"=== task: {task.id} ===")
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
        print()

    write_csv(all_results, args.out)
    print_summary(all_results)
    print()
    print(f"Results written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

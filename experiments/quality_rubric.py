"""Experiment A — head-to-head plan-quality rubric.

This is the experiment that asks the question Experiment B cannot
answer on its own: **after iterating to convergence, are dagwright's
plans as useful as the prose plans Claude produces without dagwright?**
B proved the cost-per-iteration ratio (~13×); A tests whether that
saving comes at a quality cost.

## Honest preface — what this experiment is and isn't

**Single judge, same model family.** This harness uses Claude as the
rubric judge. The same model produced the control plans and acted as
the spec-editor in the treatment loop. There is an obvious
self-judgment risk; treat the result as a *signal*, not a definitive
finding. A real quality study uses 2–3 human AEs scoring blinded
samples.

**No format anonymisation.** The control plan is prose markdown with
sections; the treatment plan is dagwright's structured ranked-plans
format with operations / contracts / invariants. The judge can tell
them apart by format alone, so any "blinding" we do is cosmetic.
Disclose this in the writeup; don't oversell a formatting equality
that doesn't exist.

**Single-shot per task, no replication.** The judge runs once per
plan-pair. Re-running gets different scores. Median across 3 runs
would tighten the signal; we leave that for v1 once the rubric design
is validated.

## What the harness does

1. Reads control + treatment plan files dumped by
   ``iteration_cost.py --save-plans``. Plans must already exist;
   this harness does NOT re-run B.
2. For each task, builds a judge prompt containing the rubric, the
   stakeholder ask, and both plans.
3. Calls Claude with ``temperature=0`` and ``max_tokens=8000``.
4. Parses the JSON-shaped scores out of the response.
5. Writes one row per (task, agent, rubric_item) to a CSV plus a
   per-task summary printed to stdout.

## Rubric (1–5 each, 7 items, 35 max per plan)

1. **Addresses stakeholder ask** — does the plan answer what was
   asked, or wander?
2. **Specificity to project** — names actual model / column / dashboard
   names from the manifest, not generic placeholders?
3. **Identifies blast radius** — surfaces which existing dashboards
   or downstream models are affected?
4. **Preserves contracts** — flags contract concerns on existing
   artifacts (BI reads, downstream models)?
5. **Enumerates alternatives** — considers multiple parent models /
   grain resolutions / migration shapes?
6. **Surfaces risks** — explicitly names risks (semantic shifts,
   deprecation impact, contract breaks, data-quality concerns)?
7. **Operationally executable** — could a competent AE turn this
   plan into a PR without going back to ask "but how?"

Run:

  uv run --extra experiments python experiments/quality_rubric.py \\
    --plans-dir experiments/results/plans \\
    --tasks new_customers_monthly \\
    --judge-model claude-sonnet-4-6 \\
    --out experiments/results/quality_rubric.csv

Pass ``--tasks all`` once the per-task results look meaningful.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Rubric definition (kept in sync with the prompt template)
# ---------------------------------------------------------------------------


RUBRIC_ITEMS: list[tuple[str, str]] = [
    (
        "addresses_ask",
        "Does the plan directly answer the stakeholder request, or "
        "wander into adjacent concerns? 1 = misses the ask. 5 = answers "
        "fully and stays on-topic.",
    ),
    (
        "specificity",
        "Does the plan name actual project artifacts (model names, "
        "column names, dashboard names from the manifest)? 1 = generic "
        "placeholders. 5 = concrete, project-specific everywhere.",
    ),
    (
        "blast_radius",
        "Does the plan identify which existing dashboards / downstream "
        "models are affected by the change? 1 = no blast-radius "
        "analysis. 5 = explicit list with reasoning.",
    ),
    (
        "contracts",
        "Does the plan flag contract concerns on existing artifacts "
        "(schema reads, grain assumptions, BI consumers)? 1 = ignores "
        "contracts. 5 = explicit per-consumer contract analysis.",
    ),
    (
        "alternatives",
        "Does the plan enumerate alternative paths (different parent "
        "models, different grain resolutions, different migration "
        "shapes)? 1 = single path only. 5 = multiple alternatives "
        "with tradeoffs.",
    ),
    (
        "risks",
        "Does the plan surface risks (semantic drift, deprecation, "
        "contract breaks, data-quality)? 1 = no risk discussion. 5 = "
        "explicit risk register with mitigation.",
    ),
    (
        "executability",
        "Could a competent AE turn this plan into a PR without coming "
        "back to ask 'but how'? 1 = vague gestures. 5 = concrete steps "
        "with model names, columns, contract terms, materialisation "
        "choices.",
    ),
]


# Human-readable stakeholder ask + refinements for each task. These
# reproduce iteration_cost.py's TASKS dict so the judge sees what the
# agents were asked to do without needing to import the harness.
TASK_ASKS: dict[str, dict[str, list[str] | str]] = {
    "new_customers_monthly": {
        "initial": (
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
        "refinements": [
            "Refine: only count customers who have placed more than one "
            "order. Add the appropriate filter and update the plan.",
            "Refine: switch from monthly grain to weekly grain.",
            "Refine: rename the consumer artifact from 'growth_dashboard' "
            "to 'acquisition_funnel'.",
            "Refine: add a second measure to the same model: total "
            "lifetime spend across these new customers.",
            "Refine: bump the contract tier from standard to critical "
            "because finance is now a downstream consumer.",
        ],
    },
    "lifetime_spend_pretax": {
        "initial": (
            "Finance has standardized revenue reporting on pre-tax lifetime "
            "spend, but the executive_overview dashboard currently reads "
            "customers.lifetime_spend which is post-tax. Align the "
            "dashboard's reading to the pre-tax basis without breaking the "
            "executive_overview contract. Produce a plan."
        ),
        "refinements": [
            "Refine: finance now wants a deprecation window — allow both "
            "definitions to coexist for one quarter rather than a hard "
            "cutover.",
            "Refine: a second consumer (cfo_pulse dashboard) was just "
            "discovered reading the same column; treat it as a must-migrate.",
            "Refine: switch the new definition's expression — pre-tax "
            "should be lifetime_spend_pretax minus refunds, not the raw "
            "column.",
            "Refine: bump the contract tier from standard to critical — "
            "finance treats this as audit-relevant.",
            "Refine: rename the conceptual basis from 'pre_tax' to "
            "'gaap_revenue' to match finance's vocabulary.",
        ],
    },
    "dau_desktop_only": {
        "initial": (
            "Product leadership has decided the company's headline DAU "
            "metric should reflect desktop usage only. The "
            "fct_active_users.daily_active_users column drives "
            "customer_journey_and_new_logo_exposure and product_pulse "
            "dashboards; both must reflect the new desktop-only definition. "
            "Other consumers may continue reading the all-platforms "
            "definition. Produce a plan."
        ),
        "refinements": [
            "Refine: marketing pushed back — they want the all-platforms "
            "definition preserved as a separate column rather than replaced.",
            "Refine: a third dashboard (board_metrics) consumes the same "
            "column and must also migrate to desktop-only.",
            "Refine: tighten allow_stale_consumers — no consumers may keep "
            "reading the old definition; hard cutover.",
            "Refine: split fct_active_users into fct_active_users_desktop "
            "and fct_active_users_mobile.",
            "Refine: bump the change to a versioned column model — keep "
            "daily_active_users alongside daily_active_users_desktop_only.",
        ],
    },
}


# ---------------------------------------------------------------------------
# Judge prompt
# ---------------------------------------------------------------------------


JUDGE_PROMPT = """You are evaluating two analytics-engineering (AE) plans \
side-by-side. Both plans were produced by an AE-agent that received the same \
stakeholder request and the same five refinements over six iterations. Your \
job is to score each plan against a structured rubric.

## Stakeholder request

{initial_ask}

## Refinements applied across iterations

{refinements_block}

## Plan A

```
{plan_a}
```

## Plan B

```
{plan_b}
```

## Rubric (each item scored 1-5 per plan)

{rubric_block}

## Your task

Score each plan on each rubric item. Be strict; reserve 5 for genuinely \
strong work and 1 for genuinely weak. The two plans use different formats \
(one is prose, one is structured ranked plans) — score on substance, not \
form. Do not penalise structure for being structured nor prose for being \
prose; both can score well.

Return your scores as a JSON object inside a ```json fenced block, shape:

{{
  "items": [
    {{
      "id": "addresses_ask",
      "score_a": <int 1-5>,
      "score_b": <int 1-5>,
      "rationale": "<one-line comparison>"
    }},
    ...
  ],
  "overall": "<one-paragraph qualitative summary of how the plans differ>"
}}

Output the JSON block only. No preamble.
"""


# ---------------------------------------------------------------------------
# Result records
# ---------------------------------------------------------------------------


@dataclass
class RubricScore:
    task_id: str
    plan_a_id: str  # which agent is "Plan A" (control or treatment)
    plan_b_id: str
    item_id: str
    score_a: int
    score_b: int
    rationale: str


@dataclass
class JudgeRun:
    task_id: str
    plan_a_id: str
    plan_b_id: str
    model: str
    in_tokens: int
    out_tokens: int
    wall_ms: float
    overall: str
    raw_response: str = ""
    scores: list[RubricScore] = field(default_factory=list)


# ---------------------------------------------------------------------------
# LLM client
# ---------------------------------------------------------------------------


class JudgeClient:
    def __init__(self, model: str):
        self.model = model
        try:
            from anthropic import Anthropic
        except ImportError as e:
            raise SystemExit(
                "anthropic SDK not installed. Run:\n"
                "  uv pip install -e \".[experiments]\""
            ) from e
        self._client = Anthropic()

    def call(self, prompt: str, max_tokens: int = 8000) -> tuple[str, int, int, float]:
        t0 = time.perf_counter()
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        wall_ms = (time.perf_counter() - t0) * 1000
        text = response.content[0].text if response.content else ""
        return text, response.usage.input_tokens, response.usage.output_tokens, wall_ms


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.DOTALL)


def extract_json(text: str) -> Optional[dict]:
    m = _JSON_FENCE_RE.search(text)
    payload = m.group(1) if m else text
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def build_prompt(task_id: str, plan_a: str, plan_b: str) -> str:
    ask = TASK_ASKS[task_id]
    refinements_block = "\n".join(
        f"{i + 1}. {r}" for i, r in enumerate(ask["refinements"])
    )
    rubric_block = "\n".join(
        f"- **{item_id}** — {desc}" for item_id, desc in RUBRIC_ITEMS
    )
    return JUDGE_PROMPT.format(
        initial_ask=ask["initial"],
        refinements_block=refinements_block,
        plan_a=plan_a,
        plan_b=plan_b,
        rubric_block=rubric_block,
    )


def judge_task(
    task_id: str,
    plans_dir: Path,
    judge: JudgeClient,
    swap: bool = False,
) -> JudgeRun:
    """Score one task. ``swap=True`` puts treatment as Plan A so we can
    detect labelling bias on a follow-up run."""
    control_path = plans_dir / f"{task_id}__control.md"
    treatment_path = plans_dir / f"{task_id}__treatment.md"
    if not control_path.exists() or not treatment_path.exists():
        raise SystemExit(
            f"Missing plan files for {task_id} in {plans_dir}. Run "
            f"iteration_cost.py --save-plans {plans_dir} first."
        )
    control_plan = control_path.read_text(encoding="utf-8")
    treatment_plan = treatment_path.read_text(encoding="utf-8")

    if swap:
        plan_a, plan_b = treatment_plan, control_plan
        plan_a_id, plan_b_id = "treatment", "control"
    else:
        plan_a, plan_b = control_plan, treatment_plan
        plan_a_id, plan_b_id = "control", "treatment"

    prompt = build_prompt(task_id, plan_a, plan_b)
    text, in_tok, out_tok, wall_ms = judge.call(prompt)

    parsed = extract_json(text)
    scores: list[RubricScore] = []
    overall = ""
    if parsed:
        overall = str(parsed.get("overall", ""))
        for item in parsed.get("items", []):
            try:
                scores.append(
                    RubricScore(
                        task_id=task_id,
                        plan_a_id=plan_a_id,
                        plan_b_id=plan_b_id,
                        item_id=str(item["id"]),
                        score_a=int(item["score_a"]),
                        score_b=int(item["score_b"]),
                        rationale=str(item.get("rationale", "")),
                    )
                )
            except (KeyError, ValueError, TypeError):
                continue

    return JudgeRun(
        task_id=task_id,
        plan_a_id=plan_a_id,
        plan_b_id=plan_b_id,
        model=judge.model,
        in_tokens=in_tok,
        out_tokens=out_tok,
        wall_ms=wall_ms,
        overall=overall,
        raw_response=text,
        scores=scores,
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def write_csv(runs: list[JudgeRun], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "task_id", "plan_a", "plan_b", "judge_model",
            "rubric_item", "score_a", "score_b", "rationale",
        ])
        for run in runs:
            for s in run.scores:
                writer.writerow([
                    s.task_id, s.plan_a_id, s.plan_b_id, run.model,
                    s.item_id, s.score_a, s.score_b, s.rationale,
                ])


def print_summary(runs: list[JudgeRun]) -> None:
    print()
    print("=" * 80)
    print("Per-task scores")
    print("=" * 80)
    for run in runs:
        print(f"\nTask: {run.task_id}  (Plan A = {run.plan_a_id}, Plan B = {run.plan_b_id})")
        print(f"  Judge: {run.model}  | tokens: {run.in_tokens} in / {run.out_tokens} out  | wall: {run.wall_ms:.0f} ms")
        if not run.scores:
            print("  [no scores parsed from judge response]")
            continue
        a_total = sum(s.score_a for s in run.scores)
        b_total = sum(s.score_b for s in run.scores)
        max_total = 5 * len(run.scores)
        print(f"  Plan A total: {a_total}/{max_total}   |   Plan B total: {b_total}/{max_total}")
        for s in run.scores:
            marker = " "
            if s.score_a > s.score_b:
                marker = "A"
            elif s.score_b > s.score_a:
                marker = "B"
            print(f"  {s.item_id:>16}: A={s.score_a} B={s.score_b} [{marker}]  - {_ascii(s.rationale)[:90]}")
        if run.overall:
            print(f"  overall: {_ascii(run.overall)[:200]}")

    print()
    print("=" * 80)
    print("Aggregate (control vs treatment, accounting for swap)")
    print("=" * 80)
    by_agent_total: dict[str, int] = {"control": 0, "treatment": 0}
    by_agent_max: int = 0
    by_item: dict[str, dict[str, list[int]]] = {}
    for run in runs:
        for s in run.scores:
            by_item.setdefault(s.item_id, {"control": [], "treatment": []})
            if run.plan_a_id == "control":
                by_agent_total["control"] += s.score_a
                by_agent_total["treatment"] += s.score_b
                by_item[s.item_id]["control"].append(s.score_a)
                by_item[s.item_id]["treatment"].append(s.score_b)
            else:
                by_agent_total["control"] += s.score_b
                by_agent_total["treatment"] += s.score_a
                by_item[s.item_id]["control"].append(s.score_b)
                by_item[s.item_id]["treatment"].append(s.score_a)
            by_agent_max += 5
    print(
        f"  control total:   {by_agent_total['control']:>3} / {by_agent_max}\n"
        f"  treatment total: {by_agent_total['treatment']:>3} / {by_agent_max}"
    )
    if by_agent_max:
        diff = by_agent_total["treatment"] - by_agent_total["control"]
        sign = "+" if diff >= 0 else ""
        print(f"  treatment - control: {sign}{diff} ({sign}{diff / by_agent_max * 100:.1f} pp)")

    print()
    print("Per-item averages (mean across tasks):")
    for item_id, _desc in RUBRIC_ITEMS:
        scores = by_item.get(item_id, {})
        c = scores.get("control") or []
        t = scores.get("treatment") or []
        if not c or not t:
            continue
        c_mean = sum(c) / len(c)
        t_mean = sum(t) / len(t)
        diff = t_mean - c_mean
        sign = "+" if diff >= 0 else ""
        print(f"  {item_id:>16}: control {c_mean:.2f}  | treatment {t_mean:.2f}  | t-c {sign}{diff:.2f}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _ascii(s: str) -> str:
    """Strip non-ASCII for stdout printing on Windows cp1252 consoles.
    The CSV always preserves the original UTF-8 content."""
    return s.encode("ascii", "replace").decode("ascii")


def _build_prompt_for(task_id: str, plans_dir: Path, swap: bool) -> tuple[str, str, str]:
    """Returns (prompt, plan_a_id, plan_b_id) for a given (task, swap)
    combo. Used by both API-dispatch and the file-based mode."""
    control_path = plans_dir / f"{task_id}__control.md"
    treatment_path = plans_dir / f"{task_id}__treatment.md"
    if not control_path.exists() or not treatment_path.exists():
        raise SystemExit(
            f"Missing plan files for {task_id} in {plans_dir}. Run "
            f"iteration_cost.py --save-plans {plans_dir} first."
        )
    control_plan = control_path.read_text(encoding="utf-8")
    treatment_plan = treatment_path.read_text(encoding="utf-8")
    if swap:
        return build_prompt(task_id, treatment_plan, control_plan), "treatment", "control"
    return build_prompt(task_id, control_plan, treatment_plan), "control", "treatment"


def _write_prompts(plans_dir: Path, task_ids: list[str], out_dir: Path) -> None:
    """Dump one prompt file per (task, swap) combo. The caller can then
    paste each into any Claude session — including a Claude Code
    subagent dispatched from elsewhere — and save the JSON response
    back to <out_dir>/<task>__<order>.scores.json. Run --scores-from
    once all four files exist."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for task_id in task_ids:
        for swap, label in ((False, "control_A"), (True, "treatment_A")):
            prompt, _, _ = _build_prompt_for(task_id, plans_dir, swap)
            f = out_dir / f"{task_id}__{label}.prompt.txt"
            f.write_text(prompt, encoding="utf-8")
            print(f"  wrote {f}")
    print(
        f"\n{2 * len(task_ids)} prompts written to {out_dir}.\n"
        "Dispatch each prompt to any Claude session (Claude Code subagent, "
        "claude.ai, API, etc.) and save the JSON-fenced response back to:\n"
        f"  {out_dir}/<task>__<order>.scores.json\n"
        "Then run: quality_rubric.py --scores-from <dir>"
    )


def _load_scores_from_files(scores_dir: Path) -> list[JudgeRun]:
    """Read JSON score files dumped by an external judge. File names
    must be <task>__<order>.scores.json where <order> is control_A or
    treatment_A. Each file should contain the JSON shape produced by
    the judge prompt: {"items": [...], "overall": "..."}"""
    runs: list[JudgeRun] = []
    for f in sorted(scores_dir.glob("*.scores.json")):
        stem = f.stem.removesuffix(".scores")  # <task>__<order>
        if "__" not in stem:
            print(f"  skipping {f.name}: name doesn't match <task>__<order>", file=sys.stderr)
            continue
        task_id, order = stem.rsplit("__", 1)
        if order not in ("control_A", "treatment_A"):
            print(f"  skipping {f.name}: order must be control_A or treatment_A", file=sys.stderr)
            continue
        if task_id not in TASK_ASKS:
            print(f"  skipping {f.name}: unknown task {task_id}", file=sys.stderr)
            continue
        plan_a_id = "control" if order == "control_A" else "treatment"
        plan_b_id = "treatment" if order == "control_A" else "control"
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"  skipping {f.name}: JSON parse error: {e}", file=sys.stderr)
            continue
        scores = []
        for item in data.get("items", []):
            try:
                scores.append(RubricScore(
                    task_id=task_id,
                    plan_a_id=plan_a_id,
                    plan_b_id=plan_b_id,
                    item_id=str(item["id"]),
                    score_a=int(item["score_a"]),
                    score_b=int(item["score_b"]),
                    rationale=str(item.get("rationale", "")),
                ))
            except (KeyError, ValueError, TypeError):
                continue
        runs.append(JudgeRun(
            task_id=task_id,
            plan_a_id=plan_a_id,
            plan_b_id=plan_b_id,
            model=str(data.get("_judge_model", "external")),
            in_tokens=int(data.get("_in_tokens", 0)),
            out_tokens=int(data.get("_out_tokens", 0)),
            wall_ms=float(data.get("_wall_ms", 0.0)),
            overall=str(data.get("overall", "")),
            scores=scores,
        ))
    return runs


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument(
        "--plans-dir", type=Path,
        default=REPO_ROOT / "experiments/results/plans",
        help="Directory holding <task>__control.md and <task>__treatment.md files.",
    )
    p.add_argument(
        "--tasks", default="new_customers_monthly",
        help="Comma-separated task ids, or 'all' for every task in TASK_ASKS.",
    )
    p.add_argument(
        "--judge-model", default="claude-sonnet-4-6",
        help="Anthropic model id for the judge. Ignored in --scores-from mode.",
    )
    p.add_argument(
        "--swap", action="store_true",
        help="API-dispatch only: put treatment as Plan A (label-bias check).",
    )
    p.add_argument(
        "--out", type=Path,
        default=REPO_ROOT / "experiments/results/quality_rubric.csv",
    )
    p.add_argument(
        "--write-prompts", type=Path, default=None,
        help="No-API mode: write rubric prompts to this directory and exit. "
             "Dispatch each prompt to any Claude session (Claude Code subagent, "
             "claude.ai, etc.), save the JSON response as "
             "<task>__<order>.scores.json in the same directory, then re-run "
             "with --scores-from.",
    )
    p.add_argument(
        "--scores-from", type=Path, default=None,
        help="No-API mode: load externally-judged scores from this directory "
             "and produce the CSV + summary without calling the API. "
             "Filenames: <task>__<order>.scores.json where <order> is "
             "control_A or treatment_A.",
    )
    args = p.parse_args(argv)

    if args.tasks == "all":
        task_ids = list(TASK_ASKS.keys())
    else:
        task_ids = [t.strip() for t in args.tasks.split(",") if t.strip()]
        for t in task_ids:
            if t not in TASK_ASKS:
                print(f"unknown task: {t}", file=sys.stderr)
                return 1

    # No-API: write prompt files, exit. Caller dispatches externally.
    if args.write_prompts:
        _write_prompts(args.plans_dir, task_ids, args.write_prompts)
        return 0

    # No-API: load externally-judged scores, format and print.
    if args.scores_from:
        runs = _load_scores_from_files(args.scores_from)
        if not runs:
            print(f"No score files found in {args.scores_from}.", file=sys.stderr)
            return 1
        print(f"Loaded {len(runs)} judge runs from {args.scores_from}")
        write_csv(runs, args.out)
        print_summary(runs)
        print(f"\nResults written to {args.out}")
        return 0

    # API path (original behavior)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY not set. For a no-API run, see --write-prompts "
            "and --scores-from.",
            file=sys.stderr,
        )
        return 1

    judge = JudgeClient(model=args.judge_model)
    print(f"judge: {args.judge_model}  |  tasks: {', '.join(task_ids)}  |  "
          f"plan-A label: {'treatment' if args.swap else 'control'}")
    print()

    runs: list[JudgeRun] = []
    for task_id in task_ids:
        print(f"--- judging {task_id} ---")
        run = judge_task(task_id, args.plans_dir, judge, swap=args.swap)
        runs.append(run)
        print(f"  {run.in_tokens} in / {run.out_tokens} out  ({run.wall_ms:.0f} ms)")

    write_csv(runs, args.out)
    print_summary(runs)
    print(f"\nResults written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

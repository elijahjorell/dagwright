# dagwright

A **deterministic, fast, free planning-artifact layer** for analytics
engineers. Convert architectural change requests into ranked,
structured plans annotated with operations, contracts, invariants,
and blast radius — in **milliseconds**, with **zero LLM tokens**, the
**same output every time**.

dagwright doesn't replace your LLM. It's the layer your LLM workflows
don't have today: a place where plans become reusable data instead of
disposable prose.

## Receipts

| | dagwright | AE + Claude as prose |
|---|---|---|
| Latency, real-world manifest (302 models) | ~20 ms | ~150 s |
| Tokens per plan | 0 | ~38,700 |
| Same input → same output | always | no |
| Output as data (diff-able, queryable) | yes | no |
| Plan content quality | comparable | sometimes richer |

The intellectual content of the plan is *not* the differentiator. AE +
Claude with the manifest in context produces plans of comparable or
richer quality than dagwright. What dagwright adds is the artifact
shape — and the speed and cost properties that make new use cases
viable.

## What it's good for

- **CI gates.** Run dagwright in CI to check whether a PR's spec
  satisfies declared contracts. Sub-second; zero token cost.
- **Bulk planning.** Generate plans across many specs (parameter
  sweeps, exploratory analysis). Seconds for hundreds of plans;
  would be hours and meaningful dollars via LLM.
- **Iteration loops.** AE tweaks the spec, sees plans update in
  milliseconds. Different qualitative experience from waiting 1–2
  minutes per try.
- **Audit and replay.** Plans persist as artifacts. Diff plans
  across manifest revisions. Reference plans from PRs / Slack.
  Re-run last year's specs against today's DAG.

## What it's *not*

- **Not a replacement for AE + LLM thinking.** A free-form Claude
  session with the manifest in context produces plans of comparable
  quality to dagwright's, sometimes richer (deeper semantic
  awareness, framing pushback, broader plan-shape coverage).
- **Not a forcing function for considering alternatives.** AEs and
  competent LLMs already consider alternatives. The spec records
  what they thought, but doesn't generate the thought.

See `CHARTER.md` for aim, scope, boundaries, kill criteria, and the
empirical validation that shaped this framing.
See `METRIC.md` for target trajectory.
See `PLANNER_NOTES.md` for the planner's current bounds — read before
trusting a "no plans" output.

## Status

v0: `dagwright plan` produces ranked plans for `metric_request` and
`definitional_change` specs against `jaffle_shop`,
`jaffle_shop_modern`, and the real-world `mattermost-analytics`
manifest (302 models, 12 in-tree dbt exposures used as the BI graph).
Both April 30 and June 30 kill-criteria hit early. Next milestones
(see `METRIC.md`):

- **August 31, 2026** — first domain-scoped plan that an external AE
  has actually used.

The hand-coded planner has narrow bounds. Read `PLANNER_NOTES.md`
before trusting a "no plans" output.

## Install

Requires Python 3.10+ and `uv`.

```bash
git clone https://github.com/elijahjorell/dagwright
cd dagwright
uv venv
uv pip install -e .
```

## Usage

```bash
# Single-spec, jaffle_shop fixture
uv run dagwright plan \
  --spec tests/jaffle_shop/specs/new_customers_monthly.yaml \
  --manifest tests/jaffle_shop/manifest.json \
  --bi tests/jaffle_shop/metabase.json \
  --format markdown

# Real-world manifest with in-tree dbt exposures as the BI graph
# (no --bi needed; reads manifest.exposures directly)
uv run dagwright plan \
  --spec tests/mattermost/specs/dau_desktop_only.yaml \
  --manifest tests/mattermost/manifest.json \
  --format markdown
```

Emits ranked plans annotated with operations, contracts, invariants,
blast radius (BI consumers + dbt downstream models), and tradeoffs.
`--format json` and `--format both` also supported. Output is stable
across runs given identical inputs.

See `specs/schema.md` for the spec shape.

## Structure

- `dagwright/` — the planner package (Python).
- `catalog/` — layers, invariants, contracts, operations, grain
  morphisms. Treated as data, read by the planner.
- `specs/` — spec schema documentation.
- `tests/` — OSS dbt projects as fixtures (`jaffle_shop`,
  `jaffle_shop_modern`, `mattermost`).

## Working name

"dagwright" — skilled maker of DAGs, like playwright or shipwright.
Keep or rename before first public release.

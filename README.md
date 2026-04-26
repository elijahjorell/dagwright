# dagwright

A **fast feedback loop** for AE+LLM architectural change planning.
Splits the work right: the LLM does small targeted spec edits (cheap,
what it's good at); dagwright does the deterministic plan enumeration
(free, what *it's* good at). End-to-end iteration drops from
~30–150 s + ~38K tokens per LLM round-trip to ~5–15 s + ~5–10K tokens
(LLM edit time only), with the dagwright step adding ~20 ms + 0
tokens per re-plan. AEs can afford 5–10 iterations in the time and
cost of one full prose-plan regeneration.

dagwright doesn't replace your LLM. It's the deterministic, structured,
free artifact layer your LLM workflows don't have today — the surface
your LLM edits against, instead of regenerating planning prose
end-to-end.

## Receipts

Per-iteration cost comparison. The "iteration" granularity matters:

| | dagwright + LLM-edits-spec | LLM-only (prose plan, regenerated) |
|---|---|---|
| Time per iteration | ~5–15 s (LLM edit + ~20 ms dagwright) | ~30–150 s |
| Tokens per iteration | ~5–10K (small targeted edit) | ~38K (full plan) |
| Same input → same output | yes (dagwright is deterministic) | no |
| Output as data (diff-able, queryable) | yes | no |
| Plan content quality | comparable | sometimes richer |

**Notes on the comparison.** The dagwright step itself is ~20 ms and
0 tokens; the iteration cost above is the end-to-end loop including
the LLM doing a small spec edit. The intellectual content of the
plan is not the differentiator — Claude with the manifest in context
produces plans of comparable or richer quality. What dagwright adds:
the LLM is freed from re-running the planning step every iteration
(it just edits the spec), the resulting plans are deterministic data,
and the iteration is cheap enough that AEs actually iterate.

## What it's good for

- **Plan iteration during development — the headline.** The cost of
  trying a plan variation drops by roughly an order of magnitude vs.
  full prose-plan regeneration. AEs stop committing to the first
  plausible plan and start exploring: *what if must_migrate excluded
  one consumer? what if I split this into two specs? what if the
  new_definition pointed at a different column?* The LLM does the
  small spec edit (5–15 s, a few thousand tokens), dagwright does
  the deterministic re-plan in milliseconds, the AE reads the new
  plan. **Affordable iteration during plan-shaping changes how AEs
  make architectural decisions.** Everything below is a downstream
  consequence of the same property.
- **CI gates.** `dagwright check` on every PR to verify a spec
  satisfies declared contracts. Sub-second; zero token cost.
- **Bulk planning.** Generate plans across many specs (parameter
  sweeps, exploratory analysis). Seconds for hundreds of plans;
  would be hours and meaningful dollars via LLM.
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

### Watch mode (the iteration-loop UX)

```bash
uv run dagwright watch \
  --spec tests/jaffle_shop_modern/specs/lifetime_spend_pretax.yaml \
  --manifest tests/jaffle_shop_modern/manifest.json \
  --bi tests/jaffle_shop_modern/metabase.json \
  --top 2
```

Re-runs the plan whenever the spec, manifest, or BI graph changes on
disk. Open the spec in your editor in one pane, run watch in another
— save = new plans appear in milliseconds. This is the operational
form of the iteration-during-plan-shaping use case the project is
built around. Invalid YAML mid-edit is caught and reported without
crashing the watcher; `Ctrl+C` exits cleanly.

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

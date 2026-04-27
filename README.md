# dagwright

A **deterministic compiler** in the AE+LLM stack for architectural
change planning. The AE writes and reads at the human-friendly ends;
the spec is machine-readable intermediate representation in the
middle:

```
AE writes NL  →  LLM edits spec (IR)  →  dagwright compiles → plan  →  AE reads plan
   ~instant         ~5–15 s, ~5–10K tokens     ~20 ms, 0 tokens         ~instant
```

The AE iterates on plans, not on YAML. Each round-trip is dominated
by the LLM edit (still small and targeted); dagwright's contribution
is deterministic, near-free, reproducible plan generation. End-to-end
per iteration: ~5–15 s + ~5–10K tokens, vs ~30–150 s + ~38K tokens
for the LLM-only equivalent (regenerating a full prose plan from
scratch each iteration). AEs can afford 5–10 iterations in the time
and cost of one full prose-plan regeneration.

dagwright doesn't replace your LLM. It's the missing compile step
between LLM and human in your AE workflow — the layer that turns
LLM-edited specs into deterministic, structured, reproducible plan
artifacts so the AE can iterate on outputs without re-running plan
reasoning through the LLM each time.

## Receipts

Per-iteration cost comparison. The "iteration" granularity matters.
First two rows are measured (Experiment B, Sonnet 4.6, replicated
across 3 tasks × 6 iterations × 2 agents — 54 API calls total);
see `experiments/README.md` for run conditions, per-task ratios,
and the schema-rejection caveat.

| | dagwright + LLM-edits-spec | LLM-only (prose plan, regenerated) |
|---|---|---|
| Time per iteration (avg) | ~4 s | ~88 s — and growing with history |
| Tokens per iteration (avg) | ~1,400 | ~19,200 — growing through the run |
| Same input → same output | yes (dagwright is deterministic) | no |
| Output as data (diff-able, queryable) | yes | no |
| Plan content quality | not yet rigorously tested | not yet rigorously tested (one informal task: LLM-only richer first plan) |
| SQL / data / decision equivalence | untested | untested |

Aggregate across the 3 tasks: **13.3× total token ratio, 20.3×
total wall-clock ratio.** Per-task ratios range 8.8×–21.6× on
tokens and 10.3×–31.9× on wall-clock. The gap widens with iteration
count because the control re-feeds full conversation history each
turn while the treatment sends only the current spec + the
refinement. **Caveat:** in this run, 15/18 treatment refinement
iterations produced schema-invalid YAML that dagwright rejected;
the cost ratio is for "tokens spent" not "iteration end-to-end."
Wiring a validate-and-retry loop in the harness is the next step
before treating the headline as load-bearing — see
`experiments/README.md` for the full discussion.

**Notes on the comparison.** The dagwright step itself is ~20 ms and
0 tokens; the iteration cost above is the end-to-end loop including
the LLM doing a small spec edit. What dagwright adds: the LLM is
freed from re-running the planning step every iteration (it just
edits the spec), the resulting plans are deterministic data, and
the iteration is cheap enough that AEs actually iterate.

**What we have NOT measured.** Plan content equivalence and outcome
equivalence (does the SQL implied by the plan produce the same data
as what an AE+LLM would write?) are untested. The single data point
we have — the April 25 Mattermost dogfood — actually showed Claude-
in-chat producing a *richer* first plan than dagwright. The
artifact-property pivot rests on the assumption that the gap is
small enough that determinism, persistence, and per-iteration cost
dominate. That assumption is defensible, not demonstrated.
Experiments H1 / H2 (plan→SQL → data execution) are designed to
close the SQL/data layer; longitudinal outcome equivalence requires
external users (post-Aug-31).

## What it's good for

- **Plan iteration during development — the headline.** The cost of
  trying a plan variation drops by roughly an order of magnitude vs.
  full prose-plan regeneration. The AE describes a variation in NL
  ("exclude one consumer," "split into two specs," "point the
  new_definition at a different column"); the LLM edits the spec
  IR; dagwright compiles to a new ranked plan; the AE reads the
  plan. The AE never touches YAML during the inner loop. **Affordable
  iteration during plan-shaping changes how AEs make architectural
  decisions.** Everything below is a downstream consequence of the
  same compile step.
- **CI gates.** `dagwright check` on every PR to verify a spec
  satisfies declared contracts. Sub-second; zero token cost.
- **Bulk planning.** Generate plans across many specs (parameter
  sweeps, exploratory analysis). Seconds for hundreds of plans;
  would be hours and meaningful dollars via LLM.
- **Audit and replay.** Plans persist as artifacts. Diff plans
  across manifest revisions. Reference plans from PRs / Slack.
  Re-run last year's specs against today's DAG.

## What it's *not*

- **Not a replacement for AE + LLM thinking.** The single first-party
  comparison we have (April 25, 2026, Mattermost) showed a free-form
  Claude session with the manifest in context produced a *richer*
  first plan than dagwright — broader alternatives, deeper semantic
  awareness, framing pushback. The thesis is not "plans are
  identical"; it's "plans are good enough that the artifact
  properties are worth the integration cost." The "good enough" floor
  is itself untested.
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

- **August 31, 2026** — first change_bundle plan that an external AE
  has actually used on their own dbt project.

The hand-coded planner has narrow bounds. Read `PLANNER_NOTES.md`
before trusting a "no plans" output.

## Install

Requires Python 3.10+ and [`uv`](https://docs.astral.sh/uv/) (works
on macOS, Linux, and Windows). One command — no clone, no venv:

```
uv tool install git+https://github.com/elijahjorell/dagwright
```

This puts a `dagwright` binary on your PATH. Verify:

```
dagwright --help
```

To upgrade later: `uv tool upgrade dagwright`. To uninstall:
`uv tool uninstall dagwright`.

For local development (clone + editable install) see the contributor
section at the bottom of this README.

## Quickstart — AE workflow via Claude

The headline integration is the MCP server. The AE describes a
change to Claude in plain English; Claude calls dagwright; plans and
diffs render inline. The AE never touches YAML during the inner loop.

**1. Wire dagwright into your MCP-aware client.**

Claude Code (any OS):

```
claude mcp add dagwright dagwright mcp
```

Claude Desktop, Cursor, or anything else that reads an
`mcpServers` JSON block — add this entry to its config file:

```json
{
  "mcpServers": {
    "dagwright": {
      "command": "dagwright",
      "args": ["mcp"]
    }
  }
}
```

Config file locations for Claude Desktop:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- Linux: `~/.config/Claude/claude_desktop_config.json`

Restart the client. The server ships its own usage protocol via the
MCP `instructions` channel, so Claude reads how to use dagwright on
connect — you don't need to memorise the tool list or paste a primer.

**2. Make sure your dbt manifest is compiled.**

dagwright reads `manifest.json`. If you don't have one:

```
dbt compile  # or `dbt parse` for a faster manifest-only pass
```

**3. Ask Claude.**

Open a chat in your dbt project (or any project — paste absolute
paths if you're elsewhere) and say something like:

> Use dagwright to plan a new monthly-active-customers mart. The
> manifest is at `target/manifest.json`. The growth team's Looker
> dashboard is the consumer that matters.

Claude will summarise the manifest, fetch the spec schema, write a
spec, validate it, call `plan`, and render a recommendation. Reply
with refinements ("drop consumer X", "what if the parent were Y")
and Claude will edit the spec and re-plan; each subsequent
recommendation includes a diff vs the previous one.

If `plan` returns zero plans, that means "no plan in the planner's
current slice reaches the desired state" — see `PLANNER_NOTES.md`
for the slice's bounds. Claude will tell you which boundary the
candidates fell outside.

The five MCP tools the server exposes (Claude calls these for you):

- **`plan`** — compile a validated spec into ranked plans, with a
  diff vs the previous call on the same spec.
- **`validate_spec`** — schema-check a spec before planning.
- **`get_spec_schema`** — canonical vocabulary for spec kinds.
- **`discover_specs`** — find existing specs in a directory.
- **`summarize_manifest`** — ~12 KB summary of a multi-MB manifest
  so Claude can orient without ingestion.

## CLI (power users, CI, debugging)

The CLI is the same engine the MCP server wraps. Useful for CI gates,
bulk sweeps, and debugging.

```
dagwright plan \
  --spec path/to/spec.yaml \
  --manifest path/to/manifest.json \
  --format markdown
```

`--bi path/to/bi.json` if you have an out-of-tree BI consumer
graph; otherwise dagwright reads `manifest.exposures` directly.
`--format json` and `--format both` also supported. Output is stable
across runs given identical inputs.

Worked examples against the bundled fixtures (clone the repo for
these — they live under `tests/`):

```
dagwright plan \
  --spec tests/jaffle_shop/specs/new_customers_monthly.yaml \
  --manifest tests/jaffle_shop/manifest.json \
  --bi tests/jaffle_shop/metabase.json \
  --format markdown

dagwright plan \
  --spec tests/mattermost/specs/dau_desktop_only.yaml \
  --manifest tests/mattermost/manifest.json \
  --format markdown
```

See `specs/schema.md` for the spec shape.

### Watch mode (the iteration-loop UX)

```
dagwright watch \
  --spec tests/jaffle_shop_modern/specs/lifetime_spend_pretax.yaml \
  --manifest tests/jaffle_shop_modern/manifest.json \
  --bi tests/jaffle_shop_modern/metabase.json \
  --top 4
```

Re-runs the plan whenever the spec, manifest, or BI graph changes on
disk — whether the change came from the AE editing in another pane,
an LLM tool editing the spec on AE instruction, or a script. Watch
mode is the operational form of the iteration-during-plan-shaping
use case the project is built around.

Each re-run after the first prints a **plan diff** before the full
plan output: which plans appeared or disappeared, which scores or
ranks changed, which contract statuses flipped (definitional_change),
which ops were added or removed, which downstream / parent consumers
shifted. The AE doesn't see the spec edit during iteration, so the
plan-side delta is what bridges "I asked for X" with "here's what
got different in the plan." Diffs are produced for both
`metric_request` and `definitional_change` specs.

Invalid YAML mid-edit is caught and reported without crashing the
watcher; `Ctrl+C` exits cleanly.

## Structure

- `dagwright/` — the planner package (Python).
- `catalog/` — layers, invariants, contracts, operations, grain
  morphisms. Treated as data, read by the planner.
- `specs/` — spec schema documentation.
- `tests/` — OSS dbt projects as fixtures (`jaffle_shop`,
  `jaffle_shop_modern`, `mattermost`).

## Contributing / local development

For an editable install against a clone of the repo:

```
git clone https://github.com/elijahjorell/dagwright
cd dagwright
uv venv
uv pip install -e ".[experiments]"   # omit [experiments] if you don't need the harness
```

Then point your MCP client at the local checkout instead of the
installed binary:

```json
{
  "mcpServers": {
    "dagwright": {
      "command": "uv",
      "args": ["run", "--project", "/absolute/path/to/dagwright", "dagwright", "mcp"]
    }
  }
}
```

The MCP server hot-reloads `planner` / `diff` / `loaders` / `state`
on each tool call, so you can edit those modules without restarting
your client. New tool registrations or edits to `mcp_server.py`
itself still need a restart.

## Working name

"dagwright" — skilled maker of DAGs, like playwright or shipwright.
Keep or rename before first public release.

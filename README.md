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

Per-iteration cost comparison. The "iteration" granularity matters:

| | dagwright + LLM-edits-spec | LLM-only (prose plan, regenerated) |
|---|---|---|
| Time per iteration | ~5–15 s (LLM edit + ~20 ms dagwright) | ~30–150 s |
| Tokens per iteration | ~5–10K (small targeted edit) | ~38K (full plan) |
| Same input → same output | yes (dagwright is deterministic) | no |
| Output as data (diff-able, queryable) | yes | no |
| Plan content quality | not yet rigorously tested | not yet rigorously tested (one informal task: LLM-only richer first plan) |
| SQL / data / decision equivalence | untested | untested |

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

### MCP server (the AE-facing surface)

Real AEs don't run terminal commands. They talk to Claude. dagwright
exposes itself as an MCP (Model Context Protocol) server so any
MCP-aware LLM client — Claude Code, Claude Desktop, Cursor — can
invoke it as a tool. The AE describes a change in chat; Claude edits
the spec and calls dagwright; plans + diff render inline.

Add to your Claude Code MCP config (typically
`~/.claude/claude_desktop_config.json` or your IDE's equivalent):

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

The server exposes five tools:

- **`plan(spec_path, manifest_path, bi_path?, top?)`** — the main
  one. Returns serialized spec, ranked plans, rejections, and (on
  subsequent calls for the same spec) a markdown diff vs the
  previous run.
- **`validate_spec(spec_path)`** — checks whether the spec parses
  cleanly. Useful between an LLM-driven spec edit and the next
  `plan` call so the LLM can self-correct invalid edits without
  round-tripping through a planner failure.
- **`get_spec_schema(kind?)`** — machine-readable schema for one or
  all spec kinds: required/optional fields, enum values, regex
  patterns, shape rules, context lookups, and a canonical example.
  Call once per session before authoring or editing a spec so the
  LLM has the canonical vocabulary in context rather than
  discovering it by reading `specs/schema.md` or by making a wrong
  edit and reading the validate_spec error.
- **`discover_specs(root_path)`** — walks a directory, returns
  spec paths grouped by kind and id. Lets the LLM find specs
  without the AE having to recite paths.
- **`summarize_manifest(manifest_path)`** — compact summary of a dbt
  manifest (project name, dbt version, models by layer, marts list,
  exposures). Lets the LLM orient in a new project without ingesting
  the full multi-MB JSON. ~12 KB summary vs ~6 MB raw manifest on
  the Mattermost fixture.

The CLI commands below remain available for power users, CI, and
debugging — but the MCP server is the headline integration.

### Watch mode (the iteration-loop UX)

```bash
uv run dagwright watch \
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

## Working name

"dagwright" — skilled maker of DAGs, like playwright or shipwright.
Keep or rename before first public release.

# dagwright — Charter

The defining document for this project. Revisit when scope is unclear,
when a feature tempts you, or when progress stalls. If a change
conflicts with this charter, change the charter first (explicitly) or
don't make the change.

## Problem (one paragraph)

When an analytics engineer plans a DAG change today, the planning
happens — but it doesn't survive. AE+LLM workflows produce plans as
prose in chat sessions, in PR descriptions, in Slack threads. Those
plans aren't reproducible (same prompt twice → different plans), aren't
reviewable as artifacts separate from the SQL they produce, can't be
diffed when the underlying DAG changes, and can't be generated at
scale (cost and latency scale linearly per plan against the LLM). The
thinking is fine; the persistence is the problem.

## Aim (one sentence)

Convert AE+LLM-style architectural change planning into a
**deterministic, structured, fast, free artifact layer** — the same
thinking, captured as data that's reproducible, reviewable, diff-able,
and cheap enough to run at scale.

## What dagwright is *not*

Empirically validated April 25, 2026: a fresh Claude given the
Mattermost manifest and a real stakeholder note produced an
objectively richer plan than dagwright did — broader alternatives,
deeper semantic awareness, framing pushback dagwright structurally
can't model. The intellectual content is *not* the differentiator.
Therefore:

- **Not a replacement for AE+LLM thinking.** AE+Claude with the
  manifest in context out-thinks dagwright on plan content.
- **Not a forcing function for considering things.** The spec
  doesn't make AEs think about blast radius or alternatives — they
  already do. The spec records what they thought.
- **Not a "better-plans" engine.** Don't sell it as one; the
  empirical result won't back the claim.

## What dagwright is

The artifact layer that AE+LLM workflows lack. Specifically:

- **Deterministic.** Same spec + same manifest = same ranked plans
  every time, for everyone. Two AEs running the same spec converge
  on the same output.
- **Structured.** Plans are data (JSON / markdown), not prose.
  Diff-able, queryable, programmatically composable.
- **Fast.** Plans run in milliseconds. Mattermost (302 models, 6MB
  manifest): ~20ms.
- **Free.** Zero LLM tokens per plan. Cost scales with manifest
  size, not plan count.

## Use cases the artifact layer unlocks

The properties above unlock workflows AE+LLM-as-prose can't reach:

- **CI gates.** `dagwright check` on every PR — pass/fail on
  declared contracts. Sub-second; zero token cost. LLM-in-CI is
  operationally and financially unattractive.
- **Bulk analysis.** Generate plans across a parameter sweep ("what
  if every 'active' metric became desktop-only"). Seconds for
  hundreds of plans; would be hours and meaningful dollars via LLM.
- **Iteration loops.** AE tweaks the spec, sees plans update in
  milliseconds. Different qualitative experience from waiting 1–2
  minutes per try.
- **Historical replay.** Re-run last year's specs against today's
  manifest. See what plans changed because the DAG changed
  underneath. Free.
- **Audit.** Plans persist as artifacts referenceable from PRs,
  Slack, post-mortems. The decision history survives the moment of
  authorship.

## Problem (preflight)

1. **Who.** Analytics engineers in dbt-using orgs at any scale who
   already work with LLM assistants (Claude Code, Cursor, etc).
   Acute pain: plans live in disposable chat sessions; nothing
   survives the moment of authorship in a reusable form.

2. **Current workaround.** AE+LLM produces plans as prose in chat.
   AE pastes the relevant bits into a PR description. Plan vanishes
   when the PR merges. Cannot be replayed, diffed, or run at scale.

3. **If taken away.** AEs continue using AE+LLM as today —
   thinking still happens, persistence still doesn't.

4. **How they find it.** OSS release on GitHub; integration points
   (Claude Code subagents, dbt Cloud plugin); CI gates that
   substitute for PR-time review costs.

5. **Metric that means it works.** See `METRIC.md`.

## What's in scope (v0)

- Ingest dbt `manifest.json`. Read in-tree dbt exposures as the BI
  consumer graph by default; optional `--bi` for separate exports.
- Accept a dagwright-spec (YAML) describing a change. v0 supports
  `metric_request` and `definitional_change`. Broader kinds in
  `specs/REQUEST_TYPES.md` earn implementation per fixture.
- Deterministic planner produces ranked plans. A plan is an ordered
  sequence of operations from `catalog/operations.yaml`. Each plan
  is annotated with operations, contracts (preserved / broken /
  newly required), invariant checks, blast radius (BI consumers +
  dbt downstream models), effort, tradeoffs.
- Output: JSON (machine-readable) + markdown (human-readable).
  Both stable across runs given identical inputs.
- CLI tool. `dagwright plan --spec foo.yaml --manifest target/manifest.json`.

## What's out of scope (v0)

- **Outperforming AE+LLM intellectually.** Dagwright doesn't promise
  richer plans, broader alternatives, or deeper semantic analysis.
  AE+LLM with the manifest in context already does that well.
- **Replacing the LLM.** Spec authoring (NL → spec) and plan
  interpretation (plan → SQL) remain LLM tasks. Dagwright is the
  middle layer.
- **Executing plans.** Tool emits plans; LLM + human execute.
- **Running SQL or touching data.**
- **Standalone NL-to-spec without LLM assistance.**
- **A UI.** CLI only.
- **Streaming / real-time.**
- **Non-dbt transformation layers.**
- **Multiple BI tools simultaneously.**
- **Multi-repo orchestration.**
- **Warehouse-specific optimizations.**

## Boundaries to protect against sprawl

- The planner reads existing state and emits plans. It does not
  build DAGs, mutate manifests, validate generated SQL, or call
  external LLMs.
- Rule and invariant definitions live in `catalog/` and are treated
  as data, not code. The engine (rule evaluation, plan ranking, and
  the eventual Z3 encoding) is implemented in this repo.
- New inputs require justification. v0 inputs are: dbt manifest, BI
  consumer graph (or in-tree exposures), dagwright-spec.
- Spec fields may grow as needed for correctness — never "just in
  case." Each new field has to earn its place against the
  artifact-property thesis: does it make the artifact more useful
  to consumers downstream?
- **Performance budget.** Plan generation must stay under 1 second
  for typical inputs (200–500 model manifests). Token cost must
  stay zero. If a feature would push past either, it pays the
  budget cost in justification.

## Domain framing (still useful, no longer the headline)

Earlier charter revisions led with "domain is the unit of work."
That's still true — domains are how AEs partition mature DAGs, the
boundary at which one AE owns the work and consumer requirements are
enumerable. The domain envelope (multi-spec input, BI-graph derived
contracts, bundled plan output) is still on the roadmap. But it's
secondary to the artifact-property thesis: dagwright's value is in
*how* it produces plans (deterministic, fast, free, structured), not
*what unit* the plans address. Domains scope the input neatly;
artifact properties are why dagwright exists.

## Kill criteria

The project stops or rescopes if any of these become true:

- **April 30, 2026.** `dagwright plan` cannot produce a useful plan
  on `jaffle_shop` given a simple spec. **HIT April 24, 2026.**
- **June 30, 2026.** Cannot produce plans against a real-world
  manifest (`mattermost-analytics`-shape, 200+ models) at sub-
  second latency, zero token cost, with deterministic output.
  This validates the differentiating claim. **HIT April 25, 2026**
  — Mattermost (302 models, 6MB manifest): ~20ms, 0 tokens, output
  reproducible.
- **August 31, 2026.** No external user has run dagwright in CI or
  on their own real dbt project, and no convincing use case has
  emerged that exploits the artifact properties (CI gates, bulk
  planning, replay). If after four months of artifact-property
  positioning nobody has wired dagwright into a workflow that uses
  those properties, the layer isn't pulling its weight.
- **The artifact-property thesis fails empirically.** AE+LLM
  workflows route around dagwright because the spec authoring step
  costs more than the artifact properties save. Spec-fill cost
  must remain less than what one round-trip of LLM plan generation
  would cost; otherwise the layer is net-negative.
- Three consecutive weeks with zero commits.

## Related repos

- `~/ai-lab/` — research lab; findings inform dagwright's design.
- `~/plaid-finance/` — potential personal test-bed dbt project once
  the Beancount ledger is ported to dbt on DuckDB.

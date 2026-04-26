# Primary metric

**Number of executable plans produced by dagwright that an AE
actually used as the artifact for review, reuse, or audit — not as
the source of richer thinking than they'd have produced anyway.**

A plan counts as "executable + used" when:

- The proposed changes are structurally sound
- Reusable pathways are correctly identified
- Contracts are preserved
- Grain is correct
- The author would copy the plan into a PR (or sequence of PRs)
  without major revision
- **AND** the AE chose to run dagwright because they wanted the
  artifact properties — most centrally, the **fast feedback loop
  during plan-shaping** (sub-second iteration on spec variations,
  zero-token cost). Institutional follow-ons (CI gates, bulk
  generation, audit trail) count too, but the headline use case
  is per-AE iteration.

The "and" clause is critical. April 25, 2026 established empirically
that AE+LLM with the manifest in context produces plans of comparable
or richer content than dagwright. Counting "the plan was correct" as
the metric would over-credit dagwright for thinking that AE+LLM
already does. The metric tracks adoption of the artifact layer —
specifically, whether the speed-and-cost property is enabling the
exploration workflow the project is selling.

## Scope levels

One engine, more of it exercised at each level. The metric tracks
the highest scope reached and how often.

- **n=1 (single-spec).** One requirement against the manifest;
  plan adds or modifies one thing.
- **multi-spec.** Multiple forward requirements within one domain;
  plan must satisfy them coherently.
- **domain-scoped.** Domain spec includes contracts derived from
  real BI consumers; plan can restructure freely as long as
  contracts hold.

## Target trajectory

- **April 30, 2026** — 1 single-spec plan on `jaffle_shop`. **Hit
  April 24, 2026.**
- **April 25, 2026** — first single-spec plan against a real-world
  manifest (`mattermost-analytics`, 302 models, 12 in-tree
  exposures). Plan judged executable. Validation of the
  artifact-property thesis: ~20ms, 0 tokens, reproducible output.
  **Hit.**
- **June 30, 2026** — first multi-spec plan within one domain on a
  real-world manifest. Forward requirements include at least one
  preservation-under-change case.
- **August 31, 2026** — first domain-scoped plan against a
  realistic dbt project that an external AE has actually used.
  External use is the validation that the artifact properties are
  worth the integration cost.

## Current value

| Date       | Highest scope reached | Notes |
|------------|------------------------|-------|
| 2026-04-18 | none                   | Charter written, no code yet. |
| 2026-04-24 | n=1 (single-spec)      | `dagwright plan` produces ranked plans for `new_customers_monthly` against `jaffle_shop`. Plan 1 (parent=customers, grain via first_order) is executable. April 30 kill-criterion hit 6 days early. |
| 2026-04-25 | n=1 (single-spec) on realistic project | `dagwright plan` produces four ranked plans for `dau_desktop_only` (definitional_change kind) against `mattermost-analytics` (302-model real-world manifest, 12 dbt exposures used as BI graph). Plans 1 (replace_in_place) and 2 (consumer_only) are both executable in their respective shapes. ~20ms, 0 tokens, reproducible output — validates the artifact-property thesis. Empirical comparison vs. AE+LLM-as-prose on the same task showed prose Claude produced richer plan content; reframed dagwright's value around artifact properties rather than intellectual quality. |

## Leading indicators (track when relevant)

The artifact-property pivot reshapes what to track alongside the
primary metric:

- **Iteration loops per AE session.** How many plan variations does
  an AE consume before settling on the one they execute? An
  "iteration" is a full NL → plan round-trip: the AE describes a
  variation in NL, the LLM edits the spec (IR), dagwright compiles
  to a new plan, the AE reads it. The AE doesn't touch YAML during
  the inner loop. Each round-trip is ~5–15 s + ~5–10K tokens (the
  LLM edit is the bottleneck; dagwright is ~20 ms + 0 tokens within
  it). If this count is consistently 1, the iteration story isn't
  landing — they're still committing to the first plan, and the
  cost advantage isn't translating into changed behavior. Higher
  numbers mean the workflow shift the project is selling is
  actually happening. Hard to measure without telemetry; worth
  tracking informally during dogfooding.
- **Latency.** Time from CLI invocation to JSON/markdown output.
  Must stay sub-second for typical manifests. Floor: ~milliseconds.
  Ceiling: 1 second. The iteration-loop benefit collapses if
  latency creeps up.
- **Token cost.** Must remain zero. Any future feature that calls
  out to an LLM violates the cost story and re-introduces the
  per-iteration friction the project exists to remove.
- **Determinism check.** Same spec + same manifest run twice →
  byte-identical output (or at minimum: same plan ordering,
  identical operation lists, identical scores).
- **Plan reuse.** How often plans are referenced *after* the
  authoring run — in PRs, Slack, post-mortems, replay scripts.
  If this is zero, the artifact isn't doing its persistence job
  even if the iteration loop is being used.
- **Spec authoring cost.** Time and tokens an LLM spends turning
  a natural-language stakeholder note into a valid dagwright-spec
  *the first time*. Must stay below what one round-trip of LLM
  plan generation would cost; otherwise the layer is net-negative.
  After the initial spec exists, *per-iteration* edit cost (a few
  thousand tokens, 5–15 s) is what's paid as the AE explores
  variations.
- **Sweep capability.** Largest N specs run in one batch (`dagwright
  sweep` or equivalent). Institutional metric, follows from the
  same speed property as iteration loops but at scale.

## Kill-criteria-linked signal

If by August 2026 no external user has run dagwright in CI or on
their own real dbt project, and no convincing use case has emerged
that exploits the artifact properties (CI gates, bulk planning,
replay), the artifact layer isn't pulling its weight. Revisit
`CHARTER.md` and consider whether the project should rescope
toward a different layer of the AE+LLM stack — or stop.

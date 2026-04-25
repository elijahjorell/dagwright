# Primary metric

**Number of executable plans the author would actually run, with the
scope they exercise tracked alongside.**

A plan counts as "executable" when:

- The proposed changes are structurally sound
- Reusable pathways are correctly identified
- Contracts are preserved
- Grain is correct
- The author would copy the plan into a PR (or a sequence of PRs)
  without major revision

## Scope levels

One engine, more of it exercised at each level. The metric tracks
the highest scope reached and how often.

- **n=1 (single-spec).** Existing models pinned by the requirement;
  plan adds one thing. Exercises the planner end-to-end but not its
  distinctive value.
- **multi-spec.** Two or more forward requirements within one
  domain; plan must satisfy them coherently. Exercises preservation
  under change and ranked alternatives.
- **domain-scoped.** Domain spec includes contracts derived from
  real BI consumers; plan can restructure freely as long as
  contracts hold. Exercises the full proposition.

## Target trajectory

- **April 30, 2026** — 1 single-spec plan on `jaffle_shop`. **Hit
  April 24, 2026.**
- **June 30, 2026** — first multi-spec plan within one domain on
  `jaffle_shop` or `jaffle_shop_modern`. Forward requirements include
  at least one preservation-under-change case (e.g. a definitional
  change or deprecation alongside a new metric).
- **August 31, 2026** — first domain-scoped plan against a realistic
  dbt project: at least one full domain in scope, at least one BI
  consumer pinning contracts, plan judged executable.
- **October 31, 2026** — at least one external AE has run a
  domain-scoped plan on their own project.

## Current value

| Date       | Highest scope reached | Notes |
|------------|------------------------|-------|
| 2026-04-18 | none                   | Charter written, no code yet. |
| 2026-04-24 | n=1 (single-spec)      | `dagwright plan` produces ranked plans for `new_customers_monthly` against `jaffle_shop`. Plan 1 (parent=customers, grain via first_order) is executable: correct semantics, all engaged invariants hold, no existing-artifact risk. April 30 kill-criterion hit 6 days early. |
| 2026-04-25 | n=1 (single-spec) on realistic project | `dagwright plan` produces four ranked plans for `dau_desktop_only` (definitional_change kind) against `mattermost-analytics` (302-model real-world manifest, 12 dbt exposures used as BI graph). Plans 1 (replace_in_place) and 2 (consumer_only) are both executable in their respective shapes. Surfaced three product gaps to file: downstream-dbt effects missing from blast radius, ranking undervalues consumer_only, versioned-mart edge typing still rough. Sits between the April n=1 milestone and the June multi-spec milestone — first time dagwright has been validated on a manifest at the charter's target scale. |

## Leading indicators (track when relevant)

- Spec fields correctly filled by the LLM, vs. expected total.
- Contract violations correctly detected in synthetic break tests.
- Invariant violations correctly detected in synthetic break tests.
- Plan runtime vs. manual planning time. If the tool takes longer
  than doing it by hand, it isn't useful.

## Kill-criteria-linked signal

If no multi-spec plan is executable by June 30, 2026, this is a
sprawl/scope signal, not a timing issue. Revisit `CHARTER.md`
before writing more code.

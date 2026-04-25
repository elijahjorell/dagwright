# Primary metric

**Number of real-OSS-dbt-project plans produced that the author would
actually execute.**

Measurement: the author reviews each generated plan. A plan counts as
"executable" when:

- The changes proposed are structurally sound
- Reusable pathways are correctly identified
- Contracts are preserved
- Grain is correct
- The author would copy the plan into a PR without major revision

## Target trajectory

- **April 30, 2026** — 1 executable plan on `jaffle_shop`.
- **May 31, 2026** — 5 executable plans across 3 OSS dbt projects.
- **June 30, 2026** — 15 executable plans across 5 projects, shared
  publicly with at least one external AE running it on their project.

## Current value

_To be updated as the project progresses._

| Date       | Count | Notes |
|------------|-------|-------|
| 2026-04-18 | 0     | Charter written, no code yet. |
| 2026-04-24 | 1     | `dagwright plan` produces ranked plans for `new_customers_monthly` against jaffle_shop. Plan 1 (parent=customers, grain via first_order) is the executable plan: correct semantics for the intent, all engaged invariants hold, no existing-artifact risk. April 30 kill-criterion target hit 6 days early. |

## Leading indicators (track when relevant)

- Spec fields correctly filled by the LLM, vs. expected total.
- Contract violations correctly detected in synthetic break tests.
- Invariant violations correctly detected in synthetic break tests.
- Plan runtime vs. manual planning time. If the tool takes longer
  than doing it by hand, it isn't useful.

## Kill-criteria-linked signal

If the primary metric is still 0 on April 30, 2026, this is a
sprawl/scope signal, not just a timing issue. Revisit `CHARTER.md`
before writing more code.

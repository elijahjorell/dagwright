# dagwright

Architectural change planner for analytics engineers.

Given a unified DAG state (dbt + BI-tool consumers) and a spec
describing a desired DAG change, dagwright returns ranked change
plans that preserve declared contracts and structural invariants.

Does not execute plans. Does not touch data. Emits plans; AE + AI
execute them.

See `CHARTER.md` for aim, scope, boundaries, kill criteria.
See `METRIC.md` for target trajectory.
See `PLANNER_NOTES.md` for the planner's current bounds and planned
widenings — read before trusting a "no plans" output.

## Status

Pre-v0. Target: `dagwright plan` running against `jaffle_shop` by
April 30, 2026.

## Intended structure

- `dagwright/` — the planner (reuses dag-simulator research: rules,
  invariants, contracts).
- `specs/` — example dagwright-specs (YAML).
- `tests/` — OSS dbt projects as fixtures (jaffle_shop first).

## Install / usage

TBD. Python CLI, installable via `uv`.

## Working name

"dagwright" — skilled maker of DAGs, like playwright or shipwright.
Keep or rename before first public release.

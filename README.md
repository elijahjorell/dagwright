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

v0: `dagwright plan` produces ranked plans for `metric_request`
specs against the jaffle_shop fixture. The April 30, 2026
kill-criterion was hit on April 24. Next milestone: 5 executable
plans across 3 OSS dbt projects by May 31, 2026 — see `METRIC.md`.

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
uv run dagwright plan \
  --spec tests/jaffle_shop/specs/new_customers_monthly.yaml \
  --manifest tests/jaffle_shop/manifest.json \
  --bi tests/jaffle_shop/metabase.json \
  --format markdown
```

Emits ranked plans annotated with operations, contracts, invariants,
blast radius, and tradeoffs. `--format json` and `--format both`
also supported.

See `specs/schema.md` for the spec shape.

## Structure

- `dagwright/` — the planner package (Python).
- `catalog/` — vendored from dag-simulator as data: layers,
  invariants, contracts, operations, grain morphisms. No code-level
  dependency on dag-simulator.
- `specs/` — spec schema documentation.
- `tests/` — OSS dbt projects as fixtures (jaffle_shop first).

## Working name

"dagwright" — skilled maker of DAGs, like playwright or shipwright.
Keep or rename before first public release.

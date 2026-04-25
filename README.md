# dagwright

Architectural decisions in analytics DAGs have no separate medium —
they happen implicitly when AEs write SQL — so DAGs decay over
time: numbers stop tying, changes break things silently, refactors
become impossible.

dagwright is the missing medium. Given a domain in an analytics DAG
— its in-scope models, the contracts and consumers that must be
preserved, and the requirements it must satisfy — dagwright returns
ranked architectural plans that satisfy the requirements while
preserving the contracts.

Does not execute plans. Does not touch data. Emits plans; AE + AI
execute them.

See `CHARTER.md` for aim, scope, boundaries, kill criteria.
See `METRIC.md` for target trajectory.
See `PLANNER_NOTES.md` for the planner's current bounds and planned
widenings — read before trusting a "no plans" output.

## Status

v0: `dagwright plan` produces ranked plans for single-spec
(`metric_request`-shaped) requirements against the `jaffle_shop`
fixture — the smallest case (n=1) of the engine. The April 30, 2026
kill-criterion was hit on April 24. Next milestones (see
`METRIC.md`):

- **June 30, 2026** — first multi-spec plan within one domain.
- **August 31, 2026** — first domain-scoped plan against a
  realistic dbt project.

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
- `catalog/` — layers, invariants, contracts, operations, grain
  morphisms. Treated as data, read by the planner.
- `specs/` — spec schema documentation.
- `tests/` — OSS dbt projects as fixtures (jaffle_shop first).

## Working name

"dagwright" — skilled maker of DAGs, like playwright or shipwright.
Keep or rename before first public release.

# dagwright — Charter

The defining document for this project. Revisit when scope is unclear,
when a feature tempts you, or when progress stalls. If a change conflicts
with this charter, change the charter first (explicitly) or don't make
the change.

## Aim (one sentence)

Given a unified DAG state (dbt + BI-tool consumers) and a
dagwright-spec describing a desired metric, produce a ranked set of
architectural change plans that preserve declared contracts and
structural invariants, and return them to the caller (AE + AI) to
execute.

## Problem (preflight)

1. **Who.** Analytics engineers in data stacks with dbt plus one or
   more BI-tool consumers (Looker, Metabase, Tableau), responsible for
   adding or evolving metrics. Starting user: the author, dogfooding
   against OSS dbt projects and personal data (eventual plaid-finance-
   as-dbt). Broader target: senior / staff AEs at Series B–D companies
   with >200 dbt models.

2. **Current workaround.** Manual planning in notebooks or PRDs, or
   Claude-in-plan-mode reading the dbt project. Both fail on contract-
   and invariant-level correctness and produce non-reproducible plans.

3. **If taken away.** Users revert to Claude plan mode plus manual
   review. Acceptable for simple changes; breaks down at scale or
   under strict contracts.

4. **How they find it.** OSS release on GitHub; Show HN post; dbt
   package index listing; eventual integration in common AE tooling
   (Claude Code subagent profiles, dbt Cloud plugin, etc.).

5. **Metric that means it works.** See `METRIC.md`.

## What's in scope (v0)

- Ingest `dbt manifest.json`.
- Ingest one BI tool's consumer graph. Start with Metabase (open source,
  accessible API). One, not N.
- Accept a dagwright-spec (YAML) describing the target metric with
  progressive fields (minimal required, more if ambiguity demands it).
- Expect an LLM to be in the loop on the caller side to fill spec
  fields from AE context (notes, ticket, stakeholder request).
- Deterministic planner produces ranked plans. A plan is an ordered
  sequence of DAG operations drawn from the operation set already
  defined in `~/dag-simulator/` (add / remove / rewire nodes and
  edges, rename, change grain / materialization / layer, add or
  modify tests, add or update contracts, etc. — the full vocabulary,
  not just "add a model"). Each plan is annotated with:
  - operations to apply, in order
  - existing pathways reused vs. new construction
  - contracts affected (preserved, broken, newly required)
  - invariant check results (which rules hold after the plan applies)
  - downstream impact (models and BI-tool consumers affected)
  - estimated effort / complexity
  - tradeoffs vs. alternative plans
- Output: JSON (machine-readable) + markdown (human-readable).
- Distribution: CLI tool. `dagwright plan --spec foo.yaml --manifest target/manifest.json [--bi metabase.json]`.

## What's out of scope (v0)

- Executing plans. The tool emits plans; LLM + human execute.
- Running SQL or touching data.
- Standalone NL-to-spec without LLM assistance. The LLM is assumed.
- A UI. CLI only.
- Streaming / real-time.
- Non-dbt transformation layers.
- Multiple BI tools simultaneously.
- Multi-repo orchestration.
- Warehouse-specific optimizations (Snowflake tuning, BigQuery slots,
  etc.).

## Boundaries to protect against sprawl

- The planner **reads** existing state and **emits** plans. It does
  not build DAGs, mutate manifests, or validate generated SQL.
- Contracts and invariants are **reused from `~/dag-simulator/`**;
  do not re-derive in this repo. Depend on that work or vendor a
  minimal subset; never reimplement.
- New inputs require justification. The accepted inputs are: dbt
  manifest, BI-tool consumer graph, dagwright-spec. Anything else
  (dbt sources, SQL linting, lineage tools) is deferred until a
  concrete planning use case demands it.
- The dagwright-spec may grow as needed for correctness, but every
  new field must have a concrete planning use case — no fields
  "just in case."
- New file types / config formats / DSLs require justification.

## Kill criteria

The project stops or rescopes if any of these become true:

- **April 30, 2026.** `dagwright plan` cannot produce a useful plan on
  `jaffle_shop` given a simple spec. (Useful = the author would
  execute the plan.)
- **July 2026.** No one except the author has run the tool on a real
  dbt project.
- The unified DAG state (dbt + BI) proves unreliable to construct from
  multi-tool inputs. Fallback: dbt-only v0, revisit BI in v1. If
  dbt-only v0 also fails by July, stop.
- The dagwright-spec grows so large that an LLM cannot reliably fill
  it from typical AE context. Means the decomposition is wrong.
- Three consecutive weeks with zero commits after v0 ships.

## Related repos

- `~/dag-simulator/` — source of rules, invariants, contracts,
  manifest loader, Z3 solver. dagwright depends on this research.
- `~/ai-lab/` — research lab; findings inform dagwright's design
  (spec-layer decisions, LLM placement).
- `~/plaid-finance/` — potential personal test-bed dbt project once
  the Beancount ledger is ported to dbt on DuckDB.

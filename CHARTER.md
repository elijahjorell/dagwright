# dagwright — Charter

The defining document for this project. Revisit when scope is unclear,
when a feature tempts you, or when progress stalls. If a change
conflicts with this charter, change the charter first (explicitly) or
don't make the change.

## Problem (one paragraph)

Analytics DAGs in mature data stacks decay as they grow. The data
function — supposed to be a force multiplier — turns into overhead:
numbers don't tie across the org, changes break things silently, every
new request takes longer than the last, refactors are impossible
because nobody can tell what depends on what, and stakeholders route
around the data team. The root cause is that **architectural decisions
in analytics DAGs have no separate medium.** A dbt model is
simultaneously code that runs and structure that constrains everything
downstream — they're the same artifact. So architectural choices
happen implicitly, in the act of writing SQL, by whoever's on the
ticket, with no place where alternatives get compared, contracts get
checked, or downstream impact gets surfaced. Each PR is an
architectural commitment masquerading as a feature. Over time the DAG
accretes choices nobody made deliberately.

## Aim (one sentence)

Given a **domain** within an analytics DAG — its in-scope models, the
contracts and consumers that must be preserved, and the requirements
it must satisfy — produce a ranked set of architectural plans that
satisfy the requirements while preserving the contracts, and return
them to the AE + AI to execute.

## Why "domain" is the unit

Domains (customer, revenue, marketing, product, etc.) are how AEs
already partition mature DAGs. The domain is the boundary at which:

- One AE typically owns the work.
- Consumer-side requirements are coherently enumerable (a finite list
  of dashboards and queries, not the whole org).
- Cleanup can happen without org-wide coordination.

The full-DAG case is too large to plan as a unit; the single-PR case
is too small to surface dagwright's distinctive value (alternatives,
blast radius, contract preservation under change). The domain is the
smallest unit where the architectural-decision step is non-trivial,
and the largest unit where a single AE can execute the result.

## One mode, varying scale

There is one operation: **plan**. Inputs are a dagwright-spec
describing a domain (scope, contracts to preserve, forward
requirements) and the existing DAG state (manifest + BI consumer
graph). The planner ranks plans that satisfy the forward requirements
while preserving the contracts.

Two ends of a spectrum, same engine:

- **Smallest case (n=1).** Forward requirements = current outputs
  plus one new thing. Existing models pinned by the requirements stay
  as-is; the planner proposes minimal additions. This is the shipped
  v0 case (`metric_request` against `jaffle_shop`).
- **Largest case (rebuild).** Forward requirements describe only what
  the domain must support, not constrained to current models. The
  planner can freely restructure the domain as long as contracts
  hold.

Most real use is between these. The same engine handles all of it;
what changes is how much of the existing structure the requirements
pin in place.

## Problem (preflight)

1. **Who.** Senior / staff AEs at Series B–D companies with >200 dbt
   models, owning one domain (customer, revenue, marketing, product,
   etc.) within a larger analytics stack. Starting user: the author,
   dogfooding against OSS dbt projects and personal data (eventual
   plaid-finance-as-dbt). The acute pain is debt the AE has inherited
   in their domain, alongside the need to keep delivering against it.

2. **Current workaround.** Manual planning in notebooks or PRDs,
   Claude-in-plan-mode reading the dbt project, or one-PR-at-a-time
   incremental healing. All fail to enumerate alternatives, check
   contracts, or surface blast radius. None give the architectural
   step its own artifact, so decisions remain implicit and the domain
   continues to decay.

3. **If taken away.** AEs revert to manual planning + Claude.
   Adequate for individual changes; debt continues to accumulate;
   cleanup remains intractable at the domain scale.

4. **How they find it.** OSS release on GitHub; Show HN post; dbt
   package index listing; eventual integration in common AE tooling
   (Claude Code subagent profiles, dbt Cloud plugin, etc.).

5. **Metric that means it works.** See `METRIC.md`.

## What's in scope (v0)

- Ingest `dbt manifest.json`.
- Ingest one BI tool's consumer graph. Start with Metabase (open
  source, accessible API). One, not N.
- Accept a dagwright-spec (YAML) describing the domain: in-scope
  models, contracts to preserve, and forward requirements. Each
  forward requirement uses a `kind` from `specs/REQUEST_TYPES.md`;
  v0 implements `metric_request`-shaped requirements only.
- v0 expects the domain spec to be hand-authored (with LLM help).
  Auto-extraction of domain boundaries or contracts from a real DAG
  is the load-bearing v1 problem.
- Expect an LLM to be in the loop on the caller side to fill spec
  fields from AE context (notes, ticket, stakeholder request, dbt +
  BI exploration).
- Deterministic planner produces ranked plans. A plan is an ordered
  sequence of operations from `catalog/operations.yaml` — add /
  remove / rewire nodes and edges, rename, change grain /
  materialization / layer, add or modify tests, add or update
  contracts, and so on; the full vocabulary, not just "add a model."
  Each plan is annotated with:
  - operations to apply, in order
  - existing pathways reused vs. new construction
  - contracts affected (preserved, broken, newly required)
  - invariant check results (which rules hold after the plan applies)
  - downstream impact (models and BI-tool consumers affected)
  - estimated effort / complexity
  - tradeoffs vs. alternative plans
- Output: JSON (machine-readable) + markdown (human-readable).
- Distribution: CLI tool. `dagwright plan --spec <domain>.yaml --manifest target/manifest.json [--bi metabase.json]`.

## What's out of scope (v0)

- **Full-DAG rebuild.** Domains, not the whole project.
- **Auto-extraction of domain boundaries or contracts** from a real
  DAG. v0 expects the AE to author them (with LLM help).
- **Cross-domain refactors as a first-class feature.** Edges that
  cross domain boundaries are inputs/outputs, not things dagwright
  re-plans.
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

- The planner **reads** existing state (domain spec, manifest, BI
  graph) and **emits** plans. It does not build DAGs, mutate
  manifests, validate generated SQL, or extract anything from
  upstream sources beyond what's declared in inputs.
- Rule and invariant definitions live in `catalog/` and are treated
  as data, not code. The engine (rule evaluation, plan ranking, and
  the eventual Z3 encoding) is implemented in this repo. The catalog
  is the source of truth for *what* the rules are; the engine is the
  source of truth for *how* to execute them.
- New inputs require justification. The accepted inputs are: dbt
  manifest, BI-tool consumer graph, dagwright-spec (domain spec).
  Anything else (dbt sources, SQL linting, lineage tools) is
  deferred until a concrete planning use case demands it.
- The dagwright-spec may grow as needed for correctness, but every
  new field must have a concrete planning use case — no fields
  "just in case."
- New file types / config formats / DSLs require justification.

## Kill criteria

The project stops or rescopes if any of these become true:

- **April 30, 2026.** `dagwright plan` cannot produce a useful plan
  on `jaffle_shop` given a simple spec. (Useful = the author would
  execute the plan.) **HIT April 24, 2026.**
- **July 31, 2026.** `dagwright plan` cannot produce a useful
  multi-spec plan within a single domain. (Useful = the author would
  execute the plan as a sequence of PRs.) Multi-spec = at least two
  forward requirements satisfied coherently against shared scope.
- **September 30, 2026.** `dagwright plan` cannot produce a useful
  domain-scoped plan against a realistic dbt project. Domain-scoped
  = full domain in scope, contracts derived from at least one BI
  consumer, plan judged executable.
- **November 2026.** No one except the author has run a
  domain-scoped plan on a real dbt project.
- The unified DAG state (dbt + BI) proves unreliable to construct
  from multi-tool inputs. Fallback: dbt-only v0, revisit BI in v1.
  If dbt-only also fails by September, stop.
- The dagwright-spec grows so large that an LLM cannot reliably fill
  it from typical AE context. Means the decomposition is wrong.
- Three consecutive weeks with zero commits after the first
  domain-scoped plan ships.

## Related repos

- `~/ai-lab/` — research lab; findings inform dagwright's design
  (spec-layer decisions, LLM placement).
- `~/plaid-finance/` — potential personal test-bed dbt project once
  the Beancount ledger is ported to dbt on DuckDB.

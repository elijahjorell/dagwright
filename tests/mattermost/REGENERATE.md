# Regenerating `manifest.json`

`manifest.json` is checked in as a fixture so dagwright tests are
reproducible without a Mattermost dbt environment. Regenerate when
the upstream project moves materially or schema-version bumps.

## Source

- Repo: https://github.com/mattermost/mattermost-data-warehouse
- Sub-project: `transform/mattermost-analytics`
- Upstream commit: `61af2424d790a8f83267f2b9ee12589fd32bcfa9`
- dbt-core: latest stable (Python 3.12)
- dbt-duckdb: latest stable
- Packages used: `dbt_utils 1.3.0`, `codegen 0.13.1`, `dbt_project_evaluator 0.14.3`
- Manifest schema: v12

## Why mattermost-analytics, not snowflake-dbt

Mattermost ships two dbt projects in this repo. `mattermost-analytics`
uses the modern layered convention (`staging/intermediate/marts/`),
declares 9 exposure files spanning 7 mart domains (`data_eng`,
`marketing`, `product`, `release`, `sales`, `web_app`, plus
sub-areas), and matches the layer model dagwright expects.
`snowflake-dbt` is the older project with business-area folders at
the top level — usable but less idiomatic.

## Steps

```bash
# Sparse clone just the sub-project
mkdir -p ~/dagwright/.tmp && cd ~/dagwright/.tmp
git clone --depth=1 --filter=blob:none --sparse \
  https://github.com/mattermost/mattermost-data-warehouse.git mattermost-src
cd mattermost-src
git sparse-checkout set transform/mattermost-analytics
cd transform/mattermost-analytics

# Drop the Snowflake-only package; project also references its
# macros directly in dbt_project.yml — patch both.
cat > packages.yml <<'EOF'
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: dbt-labs/codegen
    version: 0.13.1
  - package: dbt-labs/dbt_project_evaluator
    version: 0.14.3
EOF

# In dbt_project.yml, remove `dbt_snowflake_query_tags` from the
# dispatch search_order list and replace the query-comment block:
#   query-comment:
#     comment: 'parsed-by-dagwright'
#     append: true

# Stub a DuckDB profile keyed under the project's profile name.
mkdir -p .dagwright_profile
cat > .dagwright_profile/profiles.yml <<'EOF'
config:
    send_anonymous_usage_stats: False
snowflake:
  target: duckdb
  outputs:
    duckdb:
      type: duckdb
      path: ":memory:"
      threads: 4
EOF

# Set up env and parse.
uv venv --python 3.12 .dwvenv
uv pip install --python .dwvenv/Scripts/python.exe dbt-core dbt-duckdb
DBT_PROFILES_DIR="$(pwd)/.dagwright_profile" \
  .dwvenv/Scripts/dbt.exe deps
DBT_PROFILES_DIR="$(pwd)/.dagwright_profile" \
  .dwvenv/Scripts/dbt.exe parse --no-partial-parse

cp target/manifest.json ~/dagwright/tests/mattermost/manifest.json
```

Update the upstream commit SHA above to whatever
`git rev-parse HEAD` reports after the clone.

## What's in this manifest

- 302 models (includes ~40 from the `dbt_project_evaluator` package)
- 7 seeds
- 563 sources
- 12 exposures (dashboards, applications, notebooks) — declared
  inline by the Mattermost data team. These supply the BI consumer
  graph that dagwright consumes via `manifest.exposures`; no
  separate `metabase.json` is needed.

## Why `dbt parse` and not `compile` / `run`

Same reasoning as the jaffle_shop fixtures: `parse` produces
`manifest.json` without compiling SQL or executing against the
warehouse. The Mattermost project targets Snowflake and depends on
real source data we don't have — `parse` only validates structure
and Jinja, not SQL semantics, which is exactly what dagwright needs.

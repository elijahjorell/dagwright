# Regenerating `manifest.json`

`manifest.json` is checked in as a fixture so dagwright tests are
reproducible without a dbt installation. Regenerate when the fixture
needs to track upstream changes or a new dbt version.

## Source

- Repo: https://github.com/dbt-labs/jaffle-shop
- Commit: `7be2c5838dbdeca8e915d4e46db70e910753d7f6`
- dbt-core: latest stable (Python 3.12 — dbt-core 1.10 series does
  not import on Python 3.14 yet, see notes below)
- dbt-duckdb: latest stable
- Packages: dbt_utils 1.3.3, dbt_date 0.17.1, dbt-audit-helper (main)

## Steps

```bash
git clone --depth=1 https://github.com/dbt-labs/jaffle-shop.git ~/jaffle_shop_modern_src
cd ~/jaffle_shop_modern_src
uv venv --python 3.12
uv pip install dbt-core dbt-duckdb

# Minimal duckdb profile so `dbt parse` can resolve the adapter.
cat > profiles.yml <<'EOF'
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: jaffle_shop.duckdb
      threads: 1
EOF

DBT_PROFILES_DIR=. .venv/Scripts/dbt deps
DBT_PROFILES_DIR=. .venv/Scripts/dbt parse
cp target/manifest.json ~/dagwright/tests/jaffle_shop_modern/manifest.json
```

Update the commit SHA above to whatever `git rev-parse HEAD` reports
in `~/jaffle_shop_modern_src` after the clone.

## Why `dbt parse` and not `compile` / `run`

`parse` produces `manifest.json` without compiling SQL or executing
against the warehouse — fastest, no DuckDB tables created. The
manifest is the only artifact dagwright needs.

## Why Python 3.12 specifically

The current dbt-core release fails to import on Python 3.14 with a
mashumaro `UnserializableField` error on `JSONObjectSchema.schema`.
Pinning the venv to Python 3.12 sidesteps the issue. Drop the pin
once dbt-core ships a compatible release.

## Differences from the classic `tests/jaffle_shop/` fixture

- **Sources, not seeds.** Raw tables are declared via `sources:` in
  `models/staging/__sources.yml` and appear in the manifest as
  `source.jaffle_shop.X.raw_X`, not `seed.jaffle_shop.raw_X`. The
  loader's existing `sources` branch handles them as SOURCE-layer
  entries.
- **More marts.** `customers`, `orders`, `order_items`, `products`,
  `locations`, `supplies`, plus `metricflow_time_spine` (a real
  date_spine living in `models/marts/`). The planner currently
  classifies the spine as MART by file path, not by purpose.
- **`_at` / `_ed_at` naming convention.** The customers mart uses
  `first_ordered_at` / `last_ordered_at` instead of the classic
  fixture's `first_order` / `most_recent_order`. The date heuristic
  catches the `_at` suffix directly, no description fallback needed.
- **`lifetime_*` columns.** `count_lifetime_orders`,
  `lifetime_spend`, etc. The naïve substring-matching version of
  the date heuristic (pre-2026-04-25) false-positived these via
  `time` ⊂ `lifetime`. The current heuristic uses token-equality
  for `date|time|timestamp|datetime` and correctly excludes them.

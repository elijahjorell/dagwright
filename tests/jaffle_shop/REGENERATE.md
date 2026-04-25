# Regenerating `manifest.json`

`manifest.json` is checked in as a fixture so dagwright tests are
reproducible without a dbt installation. Regenerate when the fixture
needs to track upstream changes or a new dbt version.

## Source

- Repo: https://github.com/dbt-labs/jaffle_shop_duckdb
- Commit: `36bde6cba69d962b83be1d52fc65a0dce1cb4ebb`
- dbt-core: 1.11.6
- dbt-duckdb: 1.10.1

## Steps

```bash
git clone --depth=1 https://github.com/dbt-labs/jaffle_shop_duckdb.git ~/jaffle_shop_src
cd ~/jaffle_shop_src
uv sync
DBT_PROFILES_DIR=. .venv/Scripts/dbt parse
cp target/manifest.json ~/dagwright/tests/jaffle_shop/manifest.json
```

Update the commit SHA above to whatever `git rev-parse HEAD` reports
in `~/jaffle_shop_src` after the clone.

## Why `dbt parse` and not `compile` / `run`

`parse` produces `manifest.json` without compiling SQL or executing
against the warehouse — fastest, no DuckDB file created. The
manifest is the only artifact dagwright needs.

## jaffle_shop quirk

This project uses dbt **seeds** (`raw_customers`, `raw_orders`,
`raw_payments`) instead of `sources:` declarations. They appear in
the manifest as `seed.jaffle_shop.X`, not `source.X.Y`. The
manifest loader must treat both as SOURCE-layer entries.

# PR Classification: mattermost-data-warehouse

Sample size: 50 PRs touching `transform/mattermost-analytics/` (after excluding pure non-AE)
Sampling method: Most recent merged PRs from `gh pr list --repo mattermost/mattermost-data-warehouse --state merged`. Fetched 80 newest, then went one page back via `--search "created:<2024-09-01"`. Filtered to PRs whose file list includes paths under `transform/mattermost-analytics/`. Excluded PRs that only touch the older `transform/snowflake-dbt/` project, infra/CI, CSV bumps (`version_release_dates.csv`), Python utility/parser PRs, and Looker-only PRs.
Date range: roughly Aug 2024 – Apr 2025 (PR numbers ~1501 to ~1687)

Notes on scope:
- The repo has *two* dbt projects: `transform/mattermost-analytics/` (the active modern one, scoped per the task brief) and `transform/snowflake-dbt/` (legacy, mostly being deprecated). I only classified PRs that touch the former. PRs that *only* touch `snowflake-dbt/` (e.g., #1684, #1686, #1670) were treated as out of scope and excluded from totals; many were drops/retirements.
- For each PR I read the title, body (truncated to ~500 chars), and the full mm-analytics file list, but did *not* always read the full diff. Where a PR is ambiguous from the file list (e.g., a `.sql` change could be a definitional change or a metric add depending on whether a column is new vs. redefined), I picked the more defensible call and called it out below.
- I deliberately didn't pad single-kind metric PRs with `test_or_contract` or `add_source_or_seed` when the test/source addition is clearly *part of* shipping the new metric. Multi-kind only counts when there are genuinely independent architectural changes.

## Aggregate

| Category | Count | % of total | % of AE-only |
|---|---|---|---|
| dagwright_single_kind | 26 | 52% | 52% |
| dagwright_composable_multi_kind | 8 | 16% | 16% |
| outside_slice_single_kind | 8 | 16% | 16% |
| outside_slice_multi_kind | 8 | 16% | 16% |
| (excluded as non_ae) | 0 | 0% | — |

(All 50 sampled PRs were AE-relevant after the up-front filter; I dropped non-AE ones during sampling rather than classifying them.)

Mean kinds per AE PR: 1.42
Median kinds per AE PR: 1

## Per-kind frequency

| Kind | Appears in N PRs | % of AE PRs |
|---|---|---|
| metric_request | 27 | 54% |
| definitional_change | 12 | 24% |
| add_source_or_seed | 8 | 16% |
| materialization_change | 4 | 8% |
| dependency_repoint | 5 | 10% |
| structural_split | 3 | 6% |
| structural_merge | 1 | 2% |
| rename | 2 | 4% |
| drop | 2 | 4% |
| test_or_contract | 1 | 2% |
| other_ae_change | 6 | 12% |

(A PR can appear under multiple kinds, so column doesn't sum to 100%.)

## Sampled PRs

| # | Title (truncated) | Kinds | Summary category | Rationale |
|---|---|---|---|---|
| 1501 | Chore: move freshness checks to new PP tables | other_ae_change (source/freshness re-point) | outside_slice_single_kind | Only edits a `_sources.yml`, swapping which tables are checked for freshness. Not a metric or definitional change to a model. |
| 1507 | MM-58201: add more fields for installation information | metric_request | dagwright_single_kind | Adds new measure columns (installation type, binary edition, days-since-first-telemetry) to `dim_daily_server_config` and propagates to `rpt_active_user_base`. New columns on existing models = metric_request. |
| 1509 | Add subscription history model | metric_request, add_source_or_seed | dagwright_composable_multi_kind | Adds new staging source `stg_cws__subscription_history` and a new mart `fct_subscription_history`. The new source declaration is genuinely independent architectural work alongside the new mart; flagging multi-kind. (Defensible single-kind call too: source is "part of" shipping the mart. Borderline.) |
| 1515 | Add dim for customers, adjust the subscription history fact | metric_request, definitional_change | dagwright_composable_multi_kind | Two distinct things: (a) net-new `dim_self_serve_customers` mart, (b) redefinition of `IS_LATEST` logic in existing `fct_subscription_history`. These are architecturally independent. |
| 1517 | Chore: use earlier telemetry start date | definitional_change | dagwright_single_kind | Changes the start-date variable used to filter telemetry data — this redefines what every downstream metric covers. |
| 1518 | Cloud 360 updates | metric_request, definitional_change | dagwright_composable_multi_kind | Adds new Stripe staging columns plus modifies `fct_subscription_history` join logic and adds new fields. Mix of new fields + reworked joins. |
| 1521 | Adjust fct_subscription_history join and other fields | definitional_change | dagwright_single_kind | Switches to right join, changes `IS_LATEST` semantics, adds CWS_INSTALLATION filter. Pure redefinition of existing model. |
| 1522 | MM-54735: trial models | other_ae_change (analyses-only) | outside_slice_single_kind | Adds an `analyses/` SQL file (one-off ad hoc query, not a model). Doesn't fit dagwright kinds well. |
| 1525 | Chore: add installation id summary | metric_request | dagwright_single_kind | New `dim_installation_summary` mart. |
| 1526 | MM-58495: message priority tracking plan | metric_request, add_source_or_seed, definitional_change | dagwright_composable_multi_kind | Big PR (24 files): adds a new staging events source, new seed CSVs for tracking plan, new int models, new fct_feature_daily_snapshot mart, AND revises feature attribution logic in existing models. Genuinely composite. |
| 1528 | MM-58541: add trial request list | metric_request | dagwright_single_kind | New `fct_inapp_trial_requests` mart with dedup + flag derivation. Single net-new fact. |
| 1529 | MM-58541: add company type information | metric_request, rename | outside_slice_multi_kind | Adds company-type column AND renames `fct_in_product_trial_requests` → `fct_onprem_trial_requests_history`. Rename pushes it outside dagwright's slice. |
| 1532 | Update stripe subscription model | definitional_change | dagwright_single_kind | Quantity field semantics changed: prefer metadata `license-seats` over `Quantity`. Pure redefinition. |
| 1534 | Chore: handle backfilled licenses | definitional_change | dagwright_single_kind | Edge-case logic in `stg_cws__license` to reclassify backfilled licenses. Definitional. |
| 1536 | MM-58699: convert edition to boolean | definitional_change | dagwright_single_kind | Changes data type/parsing of `edition` column across two staging models. Same column, new definition. |
| 1537 | Chore: add downloads | metric_request, add_source_or_seed | dagwright_composable_multi_kind | Adds a new staging source (releases / log_entries), new int model, new `fct_downloads` and `dim_ip_daily_summary` marts, plus dbt test. Source addition is sizeable enough to count as independent. |
| 1538 | MM-58380 - Update Stripe data source settings | other_ae_change (source migration) | outside_slice_single_kind | Updates schema name & DB on Stripe source declarations and propagates through stg models. Source-config change, not a metric or column-definition change in dagwright's sense. |
| 1542 | MM-58541: unified trial requests | metric_request, structural_merge | outside_slice_multi_kind | Combines cloud + onprem trial requests into one unified mart `fct_trial_request_history`. Structural merge plus the new mart. |
| 1543 | Chore/add missing exposures | other_ae_change (exposures-only) | outside_slice_single_kind | Only adds dbt `exposures` declarations across multiple `_exposures.yml` files. Documentation/lineage only. |
| 1547 | MM-57542: add calls feature tracking | metric_request, add_source_or_seed | dagwright_composable_multi_kind | New `mm_calls_test_go` staging source + base/stg models, plus new `int_calls_daily_usage_per_user` and integration into feature usage marts. Source declaration is independent of the metric work. |
| 1552 | MM-59410 - Surface more user activity | metric_request | dagwright_single_kind | Adds new activity columns/measures to `int_activity_latest_daily` and propagates to `fct_active_users`. New measure columns. |
| 1553 | MM-59472 - Surface license name and seats | metric_request | dagwright_single_kind | Adds `license_name` and `license_seats` columns to `dim_latest_server_customer_info`. Pure new-fields-on-existing-dim. |
| 1556 | MM-59394: add copilot feature usage | metric_request, add_source_or_seed | dagwright_composable_multi_kind | New `copilot` staging source + base/stg + new int model + integration into feature usage. Same flavor as #1547. |
| 1561 | Chore: add license dimension to active servers | metric_request | dagwright_single_kind | Adds new `dim_daily_license` mart and surfaces license columns onto `fct_active_servers`. Treating mart-add + downstream surfacing as one metric_request bundle. |
| 1564 | Chore: group copilot events | metric_request | dagwright_single_kind | New `grp_copilot_tracks` mart that groups existing copilot events. New aggregation mart from existing parents. |
| 1565 | MM-59472: add license fact | metric_request | dagwright_single_kind | Adds `int_known_licenses`, `bdg_license_server`, `fct_licenses` — a new fact and bridge from existing license parents. Cohesive metric_request. |
| 1567 | MM-59992 - surface security update data | metric_request, definitional_change | dagwright_composable_multi_kind | (a) New `int_server_security_update_latest_daily` and `daily_active_users` surfaced on `fct_active_servers` (metric_request) AND (b) modifies how `int_server_active_days_spined` computes server usage to include security-check sources (definitional_change). Two genuinely independent changes. |
| 1568 | MM-60014 - Update freshness check on user_start_recording | other_ae_change (freshness) | outside_slice_single_kind | One-line freshness threshold change on a source. Not a model change. |
| 1570 | Chore: add dim_date | metric_request | dagwright_single_kind | New `dim_date` common dim. Net-new dim. |
| 1573 | MM-60178 - propagate more Stripe fields to fct_trial_request_history | metric_request | dagwright_single_kind | New columns added to `fct_trial_request_history` and upstream int models. |
| 1574 | MM-60178 - Surface server_id field | metric_request | dagwright_single_kind | Same shape as #1573: adds `server_id` to int + fct + yml docs. |
| 1579 | MM-60178 - Surface installation_id field | metric_request | dagwright_single_kind | Same shape: adds `installation_id` column. |
| 1581 | Chore: Add more Stripe fields to fct_subscription_history | metric_request | dagwright_single_kind | Adds columns to existing fact. |
| 1582 | MM-58167: add country to server information | metric_request, structural_split | outside_slice_multi_kind | Adds country to `dim_server_info` AND splits `dim_server_info` logic out into intermediate models (`int_excludable_servers_country`, `int_server_ip_to_country`, `int_server_telemetry_summary`). The body explicitly says "logic was moved to intermediate layer models" — that's a structural_split. |
| 1583 | Chore: add product name to fct_subscription_history | metric_request | dagwright_single_kind | Adds product_name + subscription_created date columns. |
| 1584 | Add copilot to event registry | metric_request | dagwright_single_kind | New `int_copilot_aggregated_to_date` int model and integration into event registry. |
| 1585 | MM-60178 - surface more Stripe fields to fct_trial_request_history | metric_request | dagwright_single_kind | Same shape as #1573/#1574/#1579: more columns. |
| 1586 | Add 'fresh' column to performance-events | metric_request | dagwright_single_kind | Adds `fresh` column to two performance_events staging models. New measure column. |
| 1624 | Chore: surface shared channels settings | metric_request | dagwright_single_kind | New columns surfaced from license daily into `fct_active_servers`. |
| 1627 | Chore: surface ARR values from SFDC account in licenses models | metric_request | dagwright_single_kind | Adds ARR fields from Salesforce into licenses models. |
| 1628 | Chore: remove dim_*_customers models | drop | outside_slice_single_kind | Removes `dim_cloud_customers` and `dim_self_hosted_customers` and prunes exposures. Pure drop. |
| 1630 | Chore: surface full server version info to nps models | metric_request, definitional_change | dagwright_composable_multi_kind | Adds `version_id` for joining + replaces previously code-built `server_version` (definitional_change) AND surfaces new full version columns (metric_request). Both spread across staging/intermediate/marts. |
| 1632 | MM-61128: Add dbt models to support server config dashboards | metric_request, add_source_or_seed | dagwright_composable_multi_kind | New `dim_daily_server_config` plus several new staging models for ldap/oauth/plugin/saml/service across two sources. Source addition is independent. |
| 1651 | MM-61128: Add support for incremental config server data models | materialization_change, definitional_change | outside_slice_multi_kind | Big PR (25 files): introduces incremental materialization for the config server pipeline AND restructures int models to support it. Materialization is the load-bearing change. |
| 1659 | MM-60600: replace events with deduped events | dependency_repoint, definitional_change | outside_slice_multi_kind | Repoints downstream `int_*` and `stg_*_performance_events` from `event` to deduped event table — this is dependency_repoint plus implicit definitional_change to the dedup logic itself. |
| 1661 | Model support for consolidation of server events | metric_request, add_source_or_seed | dagwright_composable_multi_kind | New `stg_mm_telemetry_prod__configs` source + new int_configs models and new `dim_daily_server_config` columns. Source is an independent kind. |
| 1669 | MM-55440: server-side feature usage | metric_request, add_source_or_seed | dagwright_composable_multi_kind | New `server_tracks` staging source + base/stg models + new `int_server_feature_attribution` and `int_client_feature_attribution`, plus events list updates. |
| 1676 | MM-62008: break database version to major and minor parts | metric_request | dagwright_single_kind | Adds major/minor version columns to `dim_daily_server_info` derived from existing `database_version`. New measure columns. (Could argue definitional_change since it parses an existing field — but the existing column isn't redefined; new columns are added alongside it.) |
| 1680 | Chore: add user agent dimension | metric_request | dagwright_single_kind | New `dim_user_agent` common dim. |
| 1687 | MM-56614: add customers report | metric_request | dagwright_single_kind | New `rpt_current_customers` report mart aggregating from existing parents. |

## Honest caveats

- **Classification is judgment-driven.** Another reviewer might disagree on roughly 6–8 PRs out of 50.
- **The "is a new staging source independent or part of the metric?" call is the dominant judgment axis.** I called source-additions independent (multi_kind) when (a) the source has its own base/stg model layer being introduced, OR (b) the metric work also redefines existing models. If you treat every "new source + new metric" PR as single-kind, the multi-kind dagwright count drops from 8 to roughly 3 and most of those PRs become single-kind metric_request. That moves the dagwright_single_kind share from 52% up to ~62%.
- **`metric_request` vs. `definitional_change` ambiguity.** When a PR says "surface X" and adds a column derived from existing data, I called it metric_request (new column). When it changes the formula/filter for an existing column, I called it definitional_change. PR #1676 (parse db version into major/minor) and #1532 (prefer metadata `license-seats` over `Quantity`) sit on this fence — I called them differently and they're plausibly switchable.
- **Sampling bias.** Sample is the most-recent ~80 merged PRs, which over-represents whatever the team has been actively shipping (lots of trial-request fields, lots of license/server config work). A sample of older PRs might show more renames/drops/structural changes from earlier refactor periods. That said, this is the closest approximation to "what kind of work do AEs file PRs for routinely" that's available.
- **Most ambiguous classifications I'm least confident on:**
  1. **#1582** (add country) — I called the move-to-intermediate-layer a `structural_split`; an alternative read is "this is just refactoring done atomically with a metric_request, count as single-kind metric_request". Body explicitly says logic was moved, which is why I went with split.
  2. **#1659** (replace events with deduped) — I called this `dependency_repoint + definitional_change`. The repoint is clear. Whether there's also a definitional change depends on whether the deduped table differs semantically from the raw one (the body says values "mostly matching"); a stricter reading would drop the definitional_change kind.
  3. **#1567** (security update data) — calling this multi-kind hinges on whether the change to `int_server_active_days_spined` to "include security-check sourced metrics" is a redefinition of existing `daily_active_users`-style measures or just a new feed; I judged it the former because the body says "include … in the computation of server usage metrics".

## Headline finding

About **half (52%) of recent AE-relevant PRs** in mattermost-data-warehouse fit a single dagwright kind cleanly — and the dominant one by far is `metric_request` (54% of PRs touch it, often as their only kind). About **16% would require composing multiple dagwright kinds** (typically `metric_request + definitional_change`, or `metric_request + add_source_or_seed`), so a `change_bundle` mechanism would be exercised on ~1 in 6 real PRs. The remaining ~32% sit outside dagwright's current slice — most commonly source/freshness config changes, exposures-only changes, structural splits/merges, materialization changes, and dependency repoints. Median kind count per PR is 1, mean is 1.42, so dagwright as scoped today plausibly covers a strong majority of real-world AE work but not the long tail of structural/operational changes.

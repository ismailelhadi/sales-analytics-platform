# Pipeline Runbook

## Schedule
The Airflow DAG `sales_daily_pipeline` runs every day at 06:00. It validates and loads the POS and carrier files, runs `dbt build`, publishes marts to Snowflake, and refreshes the RAG index.

## Data quality checks
Every landed file is checked before it is loaded. Blocking checks:
- file_present: the file arrived for the run date.
- schema: the columns exactly match the source contract. Any added, removed or renamed column is schema drift.
- freshness: every event timestamp in the file falls on the run date. A re-sent file from a previous day fails this check.
- volume: the row count is between 50% and 200% of the median of the last 14 loaded days. This check needs at least 5 days of history.
- unique_key: no duplicate primary keys (`txn_id` for POS, `activation_id` for carrier).
- not_null: key columns have no null values.
Non-blocking check:
- known_store: rows that reference a store not in the store master raise a warning but are still loaded, because reconciliation handles them.

## What happens when a check fails
The file is quarantined: it is copied to `data/quarantine/<date>/`, it is not loaded, a critical alert is written to `ops.alerts`, and the Airflow task fails so the failure is visible. Downstream dbt models still build for the other source. Reconciliation skips any date where one of the two sources is missing, so a quarantined file does not create false discrepancies.

## How to re-run a day
Fix or re-deliver the file, then run `python -m pipeline.extract_load --date YYYY-MM-DD` followed by `dbt build`. Loads are idempotent: re-running a date replaces that date's rows instead of duplicating them.

## Where to look
- `ops.load_log`: one row per source per date with status (loaded or quarantined), row count and duration.
- `ops.dq_results`: the result of every check with its metric.
- `ops.alerts`: every warning and critical alert.

"""Publish the Postgres marts to Snowflake for downstream BI.

Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (and optionally SNOWFLAKE_WAREHOUSE,
SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA) to enable. Without them this is a no-op, so the pipeline
runs fully on local Postgres.

After loading, row counts are compared between Postgres and Snowflake for every table
(a parity check, same idea as validating a migration before decommissioning the old system).

    python -m pipeline.snowflake_publish
"""
from __future__ import annotations

import logging
import os

import pandas as pd

from pipeline import db

log = logging.getLogger("pipeline.snowflake")
MARTS = ["dim_store", "fct_daily_store_sales", "fct_activation_reconciliation", "store_tiering"]


def publish_marts() -> dict | str:
    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        log.info("SNOWFLAKE_ACCOUNT not set; skipping Snowflake publish")
        return "skipped"

    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    sf = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SALES"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "MARTS"),
    )
    parity = {}
    with db.connect() as pg:
        for table in MARTS:
            df = db.read_sql(pg, f"select * from marts.{table}")
            df.columns = [c.upper() for c in df.columns]
            write_pandas(sf, df, table.upper(), auto_create_table=True, overwrite=True)
            sf_rows = sf.cursor().execute(f"select count(*) from {table.upper()}").fetchone()[0]
            parity[table] = {"postgres": len(df), "snowflake": sf_rows, "match": len(df) == sf_rows}
            log.info("published %s: %s", table, parity[table])
    sf.close()
    mismatched = [t for t, p in parity.items() if not p["match"]]
    if mismatched:
        raise RuntimeError(f"row count parity failed for {mismatched}")
    return parity


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(publish_marts())

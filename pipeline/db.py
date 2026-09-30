from contextlib import contextmanager
import io

import pandas as pd
import psycopg

from pipeline import config

DDL = """
create schema if not exists raw;
create schema if not exists ops;

create table if not exists raw.stores (
    store_id text primary key, store_name text, province text, city text,
    channel text, sq_ft int, open_date date, _loaded_at timestamptz default now()
);
create table if not exists raw.pos_transactions (
    txn_id text, txn_ts timestamp, store_id text, sku text, qty int, unit_price numeric(10,2),
    discount_pct numeric(4,2), payment_type text, load_date date not null,
    _loaded_at timestamptz default now()
);
create table if not exists raw.carrier_activations (
    activation_id text, txn_id text, store_id text, carrier text, sku text, activation_ts timestamp,
    load_date date not null, _loaded_at timestamptz default now()
);
-- Indexes for the access patterns that matter: per-day idempotent deletes, store/date analytics
-- queries, and joining activations back to sales.
create index if not exists ix_pos_load_date on raw.pos_transactions (load_date);
create index if not exists ix_pos_store_ts on raw.pos_transactions (store_id, txn_ts);
create index if not exists ix_act_load_date on raw.carrier_activations (load_date);
create index if not exists ix_act_txn on raw.carrier_activations (txn_id);
create table if not exists ops.load_log (
    run_id text, source text, load_date date, status text, rows int, duration_ms int,
    logged_at timestamptz default now()
);
create table if not exists ops.dq_results (
    run_id text, source text, load_date date, check_name text, passed boolean, blocking boolean,
    metric double precision, detail text, logged_at timestamptz default now()
);
create table if not exists ops.alerts (
    run_id text, source text, load_date date, severity text, message text,
    raised_at timestamptz default now()
);
"""


@contextmanager
def connect():
    with psycopg.connect(**config.PG, autocommit=False) as conn:
        yield conn


def init_schema(conn):
    conn.execute(DDL)
    conn.commit()


def copy_df(conn, table: str, df: pd.DataFrame):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ", ".join(df.columns)
    with conn.cursor().copy(f"copy {table} ({cols}) from stdin with (format csv)") as cp:
        cp.write(buf.read())


def read_sql(conn, query: str, params=None) -> pd.DataFrame:
    cur = conn.execute(query, params)
    return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

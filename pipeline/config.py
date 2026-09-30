import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = Path(os.getenv("RAW_DIR", ROOT / "data" / "raw"))
QUARANTINE_DIR = Path(os.getenv("QUARANTINE_DIR", ROOT / "data" / "quarantine"))

# libpq-style connection settings; override with env vars (Docker sets PGHOST=db).
PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "dbname": os.getenv("PGDATABASE", "sales"),
}

# Contract for each landed source. A file that doesn't match this exactly is schema drift.
SOURCES = {
    "pos": {
        "file": "pos_transactions.csv",
        "table": "raw.pos_transactions",
        "columns": ["txn_id", "txn_ts", "store_id", "sku", "qty", "unit_price", "discount_pct", "payment_type"],
        "key": "txn_id",
        "ts_col": "txn_ts",
        "not_null": ["txn_id", "store_id", "sku", "txn_ts"],
    },
    "carrier": {
        "file": "carrier_activations.csv",
        "table": "raw.carrier_activations",
        "columns": ["activation_id", "txn_id", "store_id", "carrier", "sku", "activation_ts"],
        "key": "activation_id",
        "ts_col": "activation_ts",
        "not_null": ["activation_id", "store_id", "activation_ts"],
    },
}

# Volume check: today's rows vs the median of the last N accepted loads for that source.
VOLUME_LOOKBACK_DAYS = 14
VOLUME_MIN_RATIO = 0.5
VOLUME_MAX_RATIO = 2.0
VOLUME_MIN_HISTORY = 5

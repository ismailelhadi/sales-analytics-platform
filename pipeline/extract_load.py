"""Extract landed files, validate them, and load clean batches into Postgres (raw schema).

Usage:
    python -m pipeline.extract_load --date 2026-03-01
    python -m pipeline.extract_load --backfill            # every date folder under RAW_DIR
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
import uuid
from datetime import date

import pandas as pd

from pipeline import config, db
from pipeline.quality import BatchReport, run_checks

log = logging.getLogger("pipeline")


def load_stores(conn) -> set[str]:
    stores = pd.DataFrame(json.loads((config.RAW_DIR / "stores.json").read_text()))
    conn.execute("truncate raw.stores")
    db.copy_df(conn, "raw.stores", stores[["store_id", "store_name", "province", "city", "channel",
                                            "sq_ft", "open_date"]])
    conn.commit()
    return set(stores["store_id"])


def recent_volumes(conn, source: str, run_date: date) -> list[int]:
    rows = conn.execute(
        "select rows from ops.load_log where source=%s and status='loaded' and load_date < %s "
        "order by load_date desc limit %s",
        (source, run_date, config.VOLUME_LOOKBACK_DAYS)).fetchall()
    return [r[0] for r in rows]


def record(conn, run_id: str, report: BatchReport, status: str, ms: int):
    conn.execute("insert into ops.load_log (run_id, source, load_date, status, rows, duration_ms) "
                 "values (%s,%s,%s,%s,%s,%s)", (run_id, report.source, report.run_date, status, report.rows, ms))
    with conn.cursor() as cur:
        cur.executemany(
            "insert into ops.dq_results (run_id, source, load_date, check_name, passed, blocking, metric, detail) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s)",
            [(run_id, report.source, report.run_date, r.check, r.passed, r.blocking, r.metric, r.detail)
             for r in report.results])
    for r in report.results:
        if not r.passed:
            severity = "critical" if r.blocking else "warning"
            conn.execute("insert into ops.alerts (run_id, source, load_date, severity, message) "
                         "values (%s,%s,%s,%s,%s)",
                         (run_id, report.source, report.run_date, severity, f"{r.check}: {r.detail}"))
            (log.error if r.blocking else log.warning)("%s %s %s: %s", report.run_date, report.source,
                                                       r.check, r.detail)


def process_source(conn, source: str, run_date: date, run_id: str, known_stores: set[str]) -> BatchReport:
    spec = config.SOURCES[source]
    t0 = time.perf_counter()
    path = config.RAW_DIR / run_date.isoformat() / spec["file"]
    df = pd.read_csv(path, dtype=str) if path.exists() else None
    report = run_checks(source, df, run_date, recent_volumes(conn, source, run_date), known_stores)

    # Idempotent: a re-run for the same date replaces that date's rows.
    conn.execute(f"delete from {spec['table']} where load_date = %s", (run_date,))
    if report.passed:
        df = df.copy()
        df["load_date"] = run_date.isoformat()
        db.copy_df(conn, spec["table"], df)
        status = "loaded"
    else:
        status = "quarantined"
        if df is not None:
            qdir = config.QUARANTINE_DIR / run_date.isoformat()
            qdir.mkdir(parents=True, exist_ok=True)
            shutil.copy(path, qdir / spec["file"])
    record(conn, run_id, report, status, int((time.perf_counter() - t0) * 1000))
    conn.commit()
    return report


def run(dates: list[date]) -> list[BatchReport]:
    run_id = uuid.uuid4().hex[:12]
    reports = []
    with db.connect() as conn:
        db.init_schema(conn)
        known = load_stores(conn)
        for d in dates:
            for source in config.SOURCES:
                reports.append(process_source(conn, source, d, run_id, known))
    return reports


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--backfill", action="store_true")
    args = ap.parse_args()
    if args.backfill:
        dates = sorted(date.fromisoformat(p.name) for p in config.RAW_DIR.iterdir()
                       if p.is_dir() and p.name[:2] == "20")
    else:
        dates = [date.fromisoformat(args.date)]
    reports = run(dates)
    bad = [r for r in reports if not r.passed]
    log.info("processed %d batches: %d loaded, %d quarantined", len(reports), len(reports) - len(bad), len(bad))


if __name__ == "__main__":
    main()

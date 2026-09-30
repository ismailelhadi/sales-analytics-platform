"""Measure the pipeline against the generator's ground truth and write results/pipeline_eval.json.

  1. Data quality: were all injected faults caught, with no false alarms and no silent failures?
  2. Reconciliation: did the model find the injected orphan / store-mismatch activations?
  3. Tiering: do tiers built on Jan-Apr rank stores by what they actually earned in May-Jun?
  4. Query performance: effect of the (store_id, txn_ts) index on a typical analytics query.

    python -m pipeline.evaluate
"""
from __future__ import annotations

import json
import statistics
import time

import pandas as pd
from scipy.stats import spearmanr

from pipeline import config, db

RESULTS = config.ROOT / "results"


def dq_eval(conn) -> dict:
    truth = json.loads((config.RAW_DIR / "_injected_faults.json").read_text())["faults"]
    status = db.read_sql(conn, """
        select distinct on (source, load_date) source, load_date::text as date, status
        from ops.load_log order by source, load_date, logged_at desc""")
    faulty = {(f["source"], f["date"]) for f in truth}
    status["faulty"] = [(s, d) in faulty for s, d in zip(status.source, status.date)]
    caught = int(((status.status == "quarantined") & status.faulty).sum())
    silent = int(((status.status == "loaded") & status.faulty).sum())
    false_alarms = int(((status.status == "quarantined") & ~status.faulty).sum())
    by_type = {}
    for f in truth:
        by_type.setdefault(f["fault"], 0)
        by_type[f["fault"]] += 1
    return {"batches": len(status), "injected_faults": len(faulty), "fault_types": by_type,
            "caught": caught, "silent_failures": silent, "false_alarms": false_alarms,
            "rows_loaded_pos": conn.execute("select count(*) from raw.pos_transactions").fetchone()[0],
            "rows_loaded_carrier": conn.execute("select count(*) from raw.carrier_activations").fetchone()[0],
            "dq_checks_run": conn.execute("select count(*) from ops.dq_results").fetchone()[0]}


def recon_eval(conn) -> dict:
    truth = json.loads((config.RAW_DIR / "_injected_faults.json").read_text())
    included = db.read_sql(conn, "select load_date from staging.stg_loaded_dates")["load_date"]
    acts = db.read_sql(conn, "select activation_id, load_date from raw.carrier_activations")
    in_scope = set(acts[acts.load_date.isin(set(included))].activation_id)
    found = db.read_sql(conn, "select activation_id, recon_status from marts.fct_activation_reconciliation "
                              "where activation_id is not null and recon_status <> 'matched'")
    out = {}
    for status, key in (("orphan_activation", "orphan_activations"), ("store_mismatch", "store_mismatch_activations")):
        expected = set(truth[key]) & in_scope
        detected = set(found[found.recon_status == status].activation_id)
        tp = len(expected & detected)
        out[status] = {"injected_in_scope": len(expected), "detected": len(detected),
                       "precision": round(tp / len(detected), 3) if detected else None,
                       "recall": round(tp / len(expected), 3) if expected else None}
    counts = db.read_sql(conn, "select recon_status, count(*) n from marts.fct_activation_reconciliation group by 1")
    out["status_counts"] = dict(zip(counts.recon_status, counts.n.astype(int)))
    out["dates_reconciled"] = len(included)
    return out


def tiering_eval(conn) -> dict:
    tiers = db.read_sql(conn, "select store_id, tier, composite_score::float as score, revenue::float as revenue "
                              "from marts.store_tiering")
    future = db.read_sql(conn, """select store_id, sum(net_revenue)::float as future_revenue
                                  from marts.fct_daily_store_sales where txn_date > '2026-04-30' group by 1""")
    df = tiers.merge(future, on="store_id")
    order = ["Platinum", "Gold", "Silver", "Bronze"]
    by_tier = df.groupby("tier").future_revenue.mean().reindex(order)
    return {
        "stores": len(df),
        "holdout_window": "2026-05-01 to 2026-06-29",
        "spearman_composite_vs_future_revenue": round(spearmanr(df.score, df.future_revenue)[0], 3),
        "spearman_revenue_only_vs_future_revenue": round(spearmanr(df.revenue, df.future_revenue)[0], 3),
        "avg_future_revenue_by_tier": {k: round(v) for k, v in by_tier.items()},
        "tiers_monotonic": bool(by_tier.is_monotonic_decreasing),
        "platinum_vs_bronze_ratio": round(by_tier["Platinum"] / by_tier["Bronze"], 2),
        "tier_counts": df.tier.value_counts().reindex(order).astype(int).to_dict(),
    }


def query_perf(conn) -> dict:
    q = ("select sum(qty * unit_price) from raw.pos_transactions "
         "where store_id = 'S014' and txn_ts >= '2026-03-01' and txn_ts < '2026-04-01'")

    def timed(n=7):
        times = []
        for _ in range(n):
            t0 = time.perf_counter()
            conn.execute(q).fetchone()
            times.append((time.perf_counter() - t0) * 1000)
        return round(statistics.median(times), 2)

    conn.execute("drop index if exists raw.ix_pos_store_ts")
    conn.commit()
    conn.execute("analyze raw.pos_transactions")
    before = timed()
    conn.execute("create index ix_pos_store_ts on raw.pos_transactions (store_id, txn_ts)")
    conn.execute("analyze raw.pos_transactions")
    conn.commit()
    after = timed()
    return {"query": q, "median_ms_without_index": before, "median_ms_with_index": after,
            "speedup_x": round(before / after, 1)}


def main():
    RESULTS.mkdir(exist_ok=True)
    with db.connect() as conn:
        out = {"data_quality": dq_eval(conn), "reconciliation": recon_eval(conn),
               "tiering": tiering_eval(conn), "query_performance": query_perf(conn)}
    (RESULTS / "pipeline_eval.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()

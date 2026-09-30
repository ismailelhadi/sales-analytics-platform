"""Build the ground-truth evaluation set for the RAG assistant.

Three kinds of questions:
  - doc questions, written by hand, with the doc that answers them and a reference answer
  - data questions about stores and incidents, generated from the warehouse so the reference
    answers are exact
  - unanswerable questions, where the correct behaviour is to say "I don't know"

Each question is assigned to a dev or test split. Thresholds are tuned on dev only and
results are reported on test.

    python -m rag.eval.build_ground_truth
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pandas as pd

from pipeline import db

OUT = Path(__file__).resolve().parent / "ground_truth.jsonl"

DOC_QUESTIONS = [
    ("How is net revenue calculated?", "metric_definitions", "qty * unit_price * (1 - discount_pct), in CAD, excluding tax"),
    ("Does net revenue include tax?", "metric_definitions", "No, net revenue excludes tax"),
    ("What does an accessory attach rate of 0.30 mean?", "metric_definitions", "30 accessories sold per 100 iPhones"),
    ("How is average basket defined?", "metric_definitions", "Net revenue divided by number of transactions"),
    ("How do you calculate activation rate for a store?", "metric_definitions", "Matched iPhone sales divided by all iPhone sales for the store"),
    ("How is revenue growth measured in the tiering model?", "metric_definitions", "Last 30 days of the scoring window vs first 30 days, minus 1"),
    ("What weight does revenue get in the store tiering score?", "tiering_methodology", "0.35"),
    ("Why is revenue log-transformed before scoring?", "tiering_methodology", "So a few very large flagship stores don't dominate the score"),
    ("What percentage of stores are Platinum?", "tiering_methodology", "The top 10% of stores"),
    ("Which percentile range is the Silver tier?", "tiering_methodology", "35% to 70%"),
    ("What date range is used to score stores?", "tiering_methodology", "2026-01-01 to 2026-04-30"),
    ("How can I explain to a manager why a store got its tier?", "tiering_methodology", "Look at the per-input contribution columns; the largest and negative contributions explain it"),
    ("How are the tiers validated?", "tiering_methodology", "On the period after the scoring window; future revenue should decrease from Platinum to Bronze"),
    ("Why are inputs converted to z-scores?", "tiering_methodology", "So inputs on different scales are comparable before weighting"),
    ("What time does the daily pipeline run?", "pipeline_runbook", "06:00 every day"),
    ("What does the volume check compare against?", "pipeline_runbook", "The median of the last 14 loaded days; must be 50% to 200%"),
    ("What happens when a file fails a data quality check?", "pipeline_runbook", "It is quarantined, not loaded, a critical alert is raised and the Airflow task fails"),
    ("How do I re-run a single day after a bad file?", "pipeline_runbook", "Run python -m pipeline.extract_load --date YYYY-MM-DD then dbt build"),
    ("Is the unknown store check blocking?", "pipeline_runbook", "No, it only raises a warning and the rows are still loaded"),
    ("Where can I see every data quality check result?", "pipeline_runbook", "ops.dq_results"),
    ("What is an orphan activation?", "reconciliation_guide", "A carrier activation whose txn_id doesn't exist in POS"),
    ("What does store_mismatch mean in reconciliation?", "reconciliation_guide", "Sale and activation share a txn_id but name different stores"),
    ("Why are some dates excluded from reconciliation?", "reconciliation_guide", "Dates where either the POS or carrier file was quarantined"),
    ("When does the reconciliation guardrail test fail the build?", "reconciliation_guide", "When fewer than 80% of iPhone sales reconcile as matched"),
    ("What columns are in the carrier activations file?", "data_sources", "activation_id, txn_id, store_id, carrier, sku, activation_ts"),
    ("What does the SKU prefix IPD mean?", "data_sources", "iPad"),
    ("Which provinces are covered by the store data?", "data_sources", "ON, QC, BC, AB, MB, NS"),
    ("What store channels exist?", "data_sources", "Flagship, Retail Partner and Carrier Store"),
]

UNANSWERABLE = [
    "Who is the CEO of the company?",
    "What was total revenue in 2024?",
    "What is the customer return policy?",
    "How many employees work at store S014?",
    "What is the revenue forecast for Q4 2026?",
    "What tier is store S250 in?",
    "What is the price of a MacBook in the United States?",
    "Which marketing campaign drove the most sales?",
    "What is the weather in Toronto today?",
    "How much commission does a sales associate earn per iPhone?",
    "What was the net promoter score for Flagship stores?",
    "Which carrier offers the cheapest data plan?",
    "When is the next product launch?",
    "What is the store manager's email for store S003?",
    "How many visitors walked into store S050 last week?",
    "What is the inventory level of AirPods in Ottawa?",
    "What discount will be offered on Black Friday?",
    "Which competitor stores are near store S021?",
    "What was the data quality incident on 2026-03-15?",
    "How do I reset my Apple ID password?",
]


def data_questions(conn, rng: random.Random) -> list[dict]:
    t = db.read_sql(conn, "select store_id, tier, activation_rate, accessory_attach_rate, revenue_growth "
                    "from marts.store_tiering")
    recon = db.read_sql(conn, """select coalesce(pos_store_id, carrier_store_id) store_id, count(*) n
                           from marts.fct_activation_reconciliation where recon_status='missing_activation'
                           group by 1""").set_index("store_id")["n"]
    qs = []
    for r in t.sample(n=16, random_state=rng.randint(0, 10**6)).itertuples():
        template = rng.choice(["tier", "activation", "attach", "missing", "growth"])
        sid = r.store_id
        if template == "tier":
            q, a = f"What tier is store {sid} in?", r.tier
        elif template == "activation":
            q, a = f"What is the activation rate at store {sid}?", f"{r.activation_rate:.1%}"
        elif template == "attach":
            q, a = f"What is the accessory attach rate for {sid}?", f"{r.accessory_attach_rate:.2f}"
        elif template == "missing":
            q, a = f"How many missing activations does store {sid} have?", str(int(recon.get(sid, 0)))
        else:
            q, a = f"How much did revenue grow at store {sid} during the scoring window?", f"{r.revenue_growth:.1%}"
        qs.append({"question": q, "doc_id": f"store_{sid}", "reference": a, "answerable": True, "kind": "store"})

    alerts = db.read_sql(conn, """select distinct on (source, load_date) source, load_date, message
                            from ops.alerts where severity='critical'
                            order by source, load_date, raised_at desc""")
    for r in alerts.itertuples():
        src = "POS" if r.source == "pos" else "carrier"
        q = rng.choice([f"Why wasn't the {src} file loaded on {r.load_date}?",
                        f"What went wrong with the {src} data on {r.load_date}?"])
        qs.append({"question": q, "doc_id": f"incident_{r.load_date}_{r.source}",
                   "reference": r.message, "answerable": True, "kind": "incident"})
    return qs


def main():
    rng = random.Random(42)
    items = [{"question": q, "doc_id": d, "reference": a, "answerable": True, "kind": "doc"}
             for q, d, a in DOC_QUESTIONS]
    with db.connect() as conn:
        items += data_questions(conn, rng)
    items += [{"question": q, "doc_id": None, "reference": "I don't know", "answerable": False,
               "kind": "unanswerable"} for q in UNANSWERABLE]
    rng.shuffle(items)
    for i, it in enumerate(items):
        it["id"] = f"q{i + 1:03d}"
        it["split"] = "dev" if i % 2 == 0 else "test"
    OUT.write_text("\n".join(json.dumps(it) for it in items) + "\n")
    counts = pd.DataFrame(items).groupby(["split", "kind"]).size().unstack(fill_value=0)
    print(f"wrote {len(items)} questions to {OUT}\n{counts}")


if __name__ == "__main__":
    main()

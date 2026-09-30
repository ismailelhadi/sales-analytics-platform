"""Generate a synthetic, multi-source retail sales dataset with injected data-quality faults.

Three "systems of record" land files daily, like they would from separate vendors:
  - POS system         -> pos_transactions.csv    (every sale)
  - Carrier partner    -> carrier_activations.csv (every iPhone activation)
  - Store master (CRM) -> stores.json             (one-time reference file)

A handful of days get deliberately broken files (schema drift, missing file, volume drop,
duplicates, nulls, stale re-sends). The list of injected faults is written to
data/raw/_injected_faults.json so the pipeline's checks can be scored against ground truth.
The pipeline itself never reads that file.

All data is synthetic. Store names, prices and volumes are invented.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROVINCES = {
    "ON": ["Toronto", "Ottawa", "Mississauga", "Waterloo", "Hamilton", "London"],
    "QC": ["Montreal", "Quebec City", "Laval"],
    "BC": ["Vancouver", "Burnaby", "Victoria"],
    "AB": ["Calgary", "Edmonton"],
    "MB": ["Winnipeg"],
    "NS": ["Halifax"],
}
CHANNELS = ["Flagship", "Retail Partner", "Carrier Store"]
CARRIERS = ["Carrier A", "Carrier B", "Carrier C"]
PRODUCTS = [
    # sku, category, price (CAD)
    ("IPH-PRO", "iPhone", 1449), ("IPH-STD", "iPhone", 1129), ("IPH-SE", "iPhone", 579),
    ("MAC-AIR", "Mac", 1399), ("MAC-PRO", "Mac", 2299),
    ("IPD-AIR", "iPad", 799), ("IPD-STD", "iPad", 499),
    ("WAT-S", "Watch", 549), ("WAT-SE", "Watch", 329),
    ("AIR-PRO", "AirPods", 329), ("AIR-STD", "AirPods", 179),
    ("ACC-CASE", "Accessories", 69), ("ACC-CHG", "Accessories", 39), ("ACC-CBL", "Accessories", 29),
]
PROD_WEIGHTS = np.array([10, 14, 5, 4, 1.5, 4, 5, 5, 4, 7, 7, 18, 12, 10], dtype=float)
PROD_WEIGHTS /= PROD_WEIGHTS.sum()


def make_stores(n: int, rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    for i in range(n):
        prov = rng.choice(list(PROVINCES))
        channel = rng.choice(CHANNELS, p=[0.2, 0.5, 0.3])
        rows.append({
            "store_id": f"S{i + 1:03d}",
            "store_name": f"Store {i + 1:03d}",
            "province": prov,
            "city": rng.choice(PROVINCES[prov]),
            "channel": channel,
            "sq_ft": int(rng.integers(1500, 12000 if channel == "Flagship" else 5000)),
            "open_date": str(date(2015, 1, 1) + timedelta(days=int(rng.integers(0, 3000)))),
        })
    df = pd.DataFrame(rows)
    # Latent traits drive behaviour; hidden from the pipeline.
    df["_base_traffic"] = rng.lognormal(mean=3.2, sigma=0.45, size=n) * df["channel"].map(
        {"Flagship": 2.2, "Retail Partner": 1.0, "Carrier Store": 0.9})
    df["_growth"] = rng.normal(0.0, 0.0025, size=n)            # daily drift in traffic
    df["_attach"] = rng.beta(2, 5, size=n)                      # accessory attach propensity
    df["_activation_rate"] = np.clip(rng.normal(0.9, 0.06, size=n), 0.6, 1.0)
    return df


def day_transactions(d: date, day_idx: int, stores: pd.DataFrame, rng: np.random.Generator, seq: list[int]):
    weekday_mult = [0.85, 0.85, 0.9, 0.95, 1.15, 1.4, 1.1][d.weekday()]
    season = 1.0 + 0.25 * np.exp(-((day_idx - 150) ** 2) / (2 * 12 ** 2))  # a launch-like bump
    pos, act = [], []
    skus = [p[0] for p in PRODUCTS]
    price = {p[0]: p[2] for p in PRODUCTS}
    cat = {p[0]: p[1] for p in PRODUCTS}
    for s in stores.to_dict("records"):
        lam = s["_base_traffic"] * weekday_mult * season * (1 + s["_growth"]) ** day_idx
        n = rng.poisson(max(lam, 0.1))
        for _ in range(n):
            seq[0] += 1
            txn_id = f"T{seq[0]:09d}"
            sku = rng.choice(skus, p=PROD_WEIGHTS)
            ts = datetime.combine(d, datetime.min.time()) + timedelta(minutes=int(rng.integers(9 * 60, 21 * 60)))
            qty = 1 if cat[sku] != "Accessories" else int(rng.integers(1, 3))
            disc = float(rng.choice([0, 0, 0, 0.05, 0.1]))
            pos.append((txn_id, ts.isoformat(), s["store_id"], sku, qty, price[sku], disc,
                        rng.choice(["credit", "debit", "financing", "gift_card"], p=[0.55, 0.2, 0.2, 0.05])))
            if cat[sku] == "iPhone":
                # accessory attach, driven by store trait
                if rng.random() < s["_attach"]:
                    seq[0] += 1
                    acc = rng.choice(["ACC-CASE", "ACC-CHG", "ACC-CBL"])
                    pos.append((f"T{seq[0]:09d}", ts.isoformat(), s["store_id"], acc, 1, price[acc], 0.0, "credit"))
                if rng.random() < s["_activation_rate"]:
                    act_ts = ts + timedelta(minutes=int(rng.integers(5, 90)))
                    act.append((f"A{txn_id[1:]}", txn_id, s["store_id"], rng.choice(CARRIERS), sku, act_ts.isoformat()))
    pos_df = pd.DataFrame(pos, columns=["txn_id", "txn_ts", "store_id", "sku", "qty", "unit_price",
                                        "discount_pct", "payment_type"])
    act_df = pd.DataFrame(act, columns=["activation_id", "txn_id", "store_id", "carrier", "sku", "activation_ts"])
    return pos_df, act_df


def inject_reconciliation_noise(pos_df, act_df, rng, truth):
    """Make the two systems disagree in known ways: orphan activations and wrong store ids."""
    if len(act_df) == 0:
        return act_df
    n_orphans = int(rng.integers(0, 3))
    orphans = act_df.sample(n=min(n_orphans, len(act_df)), random_state=int(rng.integers(1e9))).copy()
    orphans["txn_id"] = [f"TX{rng.integers(1e8):08d}" for _ in range(len(orphans))]
    orphans["activation_id"] = orphans["activation_id"] + "X"
    n_wrong = int(rng.integers(0, 3))
    wrong_idx = act_df.sample(n=min(n_wrong, len(act_df)), random_state=int(rng.integers(1e9))).index
    for i in wrong_idx:
        act_df.loc[i, "store_id"] = act_df.loc[i, "store_id"][:-1] + ("9" if act_df.loc[i, "store_id"][-1] != "9" else "8")
    truth["orphan_activations"] += orphans["activation_id"].tolist()
    truth["store_mismatch_activations"] += act_df.loc[wrong_idx, "activation_id"].tolist()
    return pd.concat([act_df, orphans], ignore_index=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--stores", type=int, default=120)
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    start = date.fromisoformat(args.start)

    stores = make_stores(args.stores, rng)
    public = stores[[c for c in stores.columns if not c.startswith("_")]]
    (out / "stores.json").write_text(public.to_json(orient="records", indent=1))
    stores[["store_id", "_base_traffic", "_growth", "_attach", "_activation_rate"]].to_csv(
        out / "_latent_store_traits.csv", index=False)

    # Fault plan: (day index, source, fault type). Deliberately spread out, never in the first 20 days
    # so the volume baseline has history.
    fault_plan = [
        (34, "pos", "schema_drift"), (47, "carrier", "missing_file"), (61, "pos", "volume_drop"),
        (78, "pos", "duplicates"), (95, "carrier", "null_keys"), (112, "pos", "stale_resend"),
        (128, "carrier", "schema_drift"), (141, "pos", "null_keys"), (157, "carrier", "volume_drop"),
        (170, "pos", "missing_file"),
    ]
    faults = {(d, s): f for d, s, f in fault_plan}
    truth = {"faults": [], "orphan_activations": [], "store_mismatch_activations": []}
    seq = [0]
    prev_pos = None
    total_pos = total_act = 0
    for i in range(args.days):
        d = start + timedelta(days=i)
        pos_df, act_df = day_transactions(d, i, stores, rng, seq)
        act_df = inject_reconciliation_noise(pos_df, act_df, rng, truth)
        clean_pos = pos_df
        day_dir = out / d.isoformat()
        day_dir.mkdir(exist_ok=True)
        for source, df in (("pos", pos_df), ("carrier", act_df)):
            fault = faults.get((i, source))
            fname = "pos_transactions.csv" if source == "pos" else "carrier_activations.csv"
            if fault == "missing_file":
                pass
            else:
                if fault == "schema_drift":
                    df = df.rename(columns={df.columns[5]: df.columns[5] + "_v2"})
                    df["source_system_version"] = "2.0"
                elif fault == "volume_drop":
                    df = df.sample(frac=0.2, random_state=1)
                elif fault == "duplicates":
                    df = pd.concat([df, df.sample(frac=0.06, random_state=1)])
                elif fault == "null_keys":
                    df = df.copy()
                    idx = df.sample(frac=0.04, random_state=1).index
                    df.loc[idx, "store_id"] = None
                elif fault == "stale_resend":
                    df = prev_pos  # yesterday's file sent again under today's date
                df.to_csv(day_dir / fname, index=False)
            if fault:
                truth["faults"].append({"date": d.isoformat(), "source": source, "fault": fault})
        prev_pos = clean_pos
        total_pos += len(pos_df)
        total_act += len(act_df)

    (out / "_injected_faults.json").write_text(json.dumps(truth, indent=1))
    print(f"stores={len(stores)} days={args.days} pos_rows={total_pos} activation_rows={total_act} "
          f"faults={len(truth['faults'])} orphan_acts={len(truth['orphan_activations'])} "
          f"store_mismatch_acts={len(truth['store_mismatch_activations'])}")


if __name__ == "__main__":
    main()

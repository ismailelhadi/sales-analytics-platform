from datetime import date

import pandas as pd

from pipeline import quality
from pipeline.config import SOURCES

RUN = date(2026, 3, 1)
COLS = SOURCES["pos"]["columns"]


def pos_df(n=100, day="2026-03-01"):
    return pd.DataFrame({
        "txn_id": [f"T{i}" for i in range(n)], "txn_ts": [f"{day}T10:00:00"] * n,
        "store_id": ["S001"] * n, "sku": ["IPH-PRO"] * n, "qty": [1] * n,
        "unit_price": [1449] * n, "discount_pct": [0] * n, "payment_type": ["credit"] * n,
    })


def test_clean_batch_passes():
    r = quality.run_checks("pos", pos_df(), RUN, history=[100] * 10, known_stores={"S001"})
    assert r.passed and not r.failures


def test_missing_file_fails():
    r = quality.run_checks("pos", None, RUN, history=[], known_stores=set())
    assert not r.passed and r.failures[0].check == "file_present"


def test_schema_drift_detected():
    df = pos_df().rename(columns={"unit_price": "price"})
    r = quality.run_checks("pos", df, RUN, history=[100] * 10, known_stores={"S001"})
    assert [f.check for f in r.failures] == ["schema"]
    assert "price" in r.failures[0].detail


def test_stale_file_fails_freshness():
    r = quality.run_checks("pos", pos_df(day="2026-02-28"), RUN, history=[100] * 10, known_stores={"S001"})
    assert "freshness" in [f.check for f in r.failures]


def test_volume_drop_and_spike():
    assert not quality.check_volume(40, [100] * 10).passed
    assert not quality.check_volume(250, [100] * 10).passed
    assert quality.check_volume(90, [100] * 10).passed


def test_volume_skipped_without_history():
    res = quality.check_volume(5, [100, 100])
    assert res.passed and not res.blocking


def test_duplicates_and_nulls():
    df = pd.concat([pos_df(), pos_df().head(3)])
    df.iloc[0, df.columns.get_loc("store_id")] = None
    checks = {f.check for f in quality.run_checks("pos", df, RUN, [100] * 10, {"S001"}).failures}
    assert {"unique_key", "not_null"} <= checks


def test_unknown_store_is_warning_only():
    df = pos_df()
    df.loc[0, "store_id"] = "S999"
    r = quality.run_checks("pos", df, RUN, [100] * 10, {"S001"})
    assert r.passed
    assert any(x.check == "known_store" and not x.passed for x in r.results)

"""Pre-load data quality checks.

Every landed file is checked before it can reach the warehouse. A batch that fails any
blocking check is quarantined and raises an alert, so a bad file never silently flows
into the marts. Results for every check are persisted to ops.dq_results for observability.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from statistics import median

import pandas as pd

from pipeline import config


@dataclass
class CheckResult:
    check: str
    passed: bool
    blocking: bool = True
    detail: str = ""
    metric: float | None = None


@dataclass
class BatchReport:
    source: str
    run_date: date
    rows: int = 0
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.blocking)

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.blocking and not r.passed]


def check_schema(df: pd.DataFrame, expected: list[str]) -> CheckResult:
    actual = list(df.columns)
    if actual == expected:
        return CheckResult("schema", True)
    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))
    return CheckResult("schema", False, detail=f"schema drift: added={added} removed={removed}")


def check_freshness(df: pd.DataFrame, ts_col: str, run_date: date) -> CheckResult:
    ts = pd.to_datetime(df[ts_col], errors="coerce")
    dates = ts.dt.date.dropna()
    if dates.empty:
        return CheckResult("freshness", False, detail="no parseable timestamps")
    newest, oldest = max(dates), min(dates)
    lag = (run_date - newest).days
    ok = newest == run_date and oldest >= run_date
    return CheckResult("freshness", ok, metric=lag,
                       detail="" if ok else f"expected events on {run_date}, got {oldest}..{newest}")


def check_volume(rows: int, history: list[int]) -> CheckResult:
    if len(history) < config.VOLUME_MIN_HISTORY:
        return CheckResult("volume", True, blocking=False, detail="not enough history yet", metric=rows)
    base = median(history)
    ratio = rows / base if base else 0
    ok = config.VOLUME_MIN_RATIO <= ratio <= config.VOLUME_MAX_RATIO
    return CheckResult("volume", ok, metric=round(ratio, 3),
                       detail="" if ok else f"{rows} rows is {ratio:.0%} of trailing median {base:.0f}")


def check_unique(df: pd.DataFrame, key: str) -> CheckResult:
    dupes = int(df[key].duplicated().sum()) if key in df else -1
    return CheckResult("unique_key", dupes == 0, metric=dupes,
                       detail="" if dupes == 0 else f"{dupes} duplicate {key} values")


def check_not_null(df: pd.DataFrame, cols: list[str]) -> CheckResult:
    present = [c for c in cols if c in df]
    nulls = int(df[present].isna().sum().sum())
    return CheckResult("not_null", nulls == 0, metric=nulls,
                       detail="" if nulls == 0 else f"{nulls} null values in key columns")


def check_known_stores(df: pd.DataFrame, known: set[str]) -> CheckResult:
    """Non-blocking: unknown stores are reported and reconciled downstream, not rejected."""
    ids = df["store_id"].dropna()
    unknown = int((~ids.isin(known)).sum())
    return CheckResult("known_store", unknown == 0, blocking=False, metric=unknown,
                       detail="" if unknown == 0 else f"{unknown} rows reference unknown stores")


def run_checks(source: str, df: pd.DataFrame | None, run_date: date, history: list[int],
               known_stores: set[str]) -> BatchReport:
    spec = config.SOURCES[source]
    report = BatchReport(source, run_date)
    if df is None:
        report.results.append(CheckResult("file_present", False, detail=f"{spec['file']} did not arrive"))
        return report
    report.rows = len(df)
    report.results.append(CheckResult("file_present", True))
    schema = check_schema(df, spec["columns"])
    report.results.append(schema)
    if not schema.passed:
        return report  # other checks depend on the contract
    report.results += [
        check_freshness(df, spec["ts_col"], run_date),
        check_volume(len(df), history),
        check_unique(df, spec["key"]),
        check_not_null(df, spec["not_null"]),
        check_known_stores(df, known_stores),
    ]
    return report

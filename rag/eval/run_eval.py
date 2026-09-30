"""Retrieval + abstention evaluation against the ground-truth set. Needs no API key.

Reports, on the held-out test split:
  - hit@1, hit@3 and MRR for vector-only, keyword-only and hybrid retrieval
  - the abstention threshold picked on the dev split, and how well it separates answerable
    from unanswerable questions on test

    python -m rag.eval.run_eval
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from rag.retrieve import retrieve

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parents[1] / "results"


def load(split=None):
    items = [json.loads(line) for line in (HERE / "ground_truth.jsonl").read_text().splitlines()]
    return [i for i in items if split is None or i["split"] == split]


def retrieval_metrics(items, mode):
    ranks = []
    for it in items:
        docs = [h.doc_id for h in retrieve(it["question"], k=10, mode=mode)]
        ranks.append(docs.index(it["doc_id"]) + 1 if it["doc_id"] in docs else None)
    n = len(ranks)
    return {
        "n": n,
        "hit@1": round(sum(r == 1 for r in ranks) / n, 3),
        "hit@3": round(sum(r is not None and r <= 3 for r in ranks) / n, 3),
        "mrr": round(sum(1 / r for r in ranks if r) / n, 3),
    }


def abstention_scores(items):
    out = []
    for it in items:
        hits = retrieve(it["question"], k=1)
        out.append((hits[0].coverage if hits else 0.0, it["answerable"]))
    return out


def confusion(scores, thr):
    scores = [(float(s), bool(a)) for s, a in scores]
    tp = sum(s < thr and not a for s, a in scores)     # correctly abstained
    fp = sum(s < thr and a for s, a in scores)         # abstained on an answerable question
    fn = sum(s >= thr and not a for s, a in scores)    # answered an unanswerable question
    tn = sum(s >= thr and a for s, a in scores)
    return {"abstain_correct": tp, "abstain_wrong": fp, "answered_unanswerable": fn, "answered_ok": tn,
            "accuracy": round((tp + tn) / len(scores), 3),
            "unanswerable_caught": round(tp / (tp + fn), 3) if tp + fn else None,
            "answerable_kept": round(tn / (tn + fp), 3) if tn + fp else None}


def main():
    RESULTS.mkdir(exist_ok=True)
    test = load("test")
    answerable = [i for i in test if i["answerable"]]
    out = {"retrieval_test": {m: retrieval_metrics(answerable, m) for m in ("vector", "keyword", "hybrid")}}
    out["retrieval_test_by_kind"] = {
        k: retrieval_metrics([i for i in answerable if i["kind"] == k], "hybrid")
        for k in sorted({i["kind"] for i in answerable})}

    dev_scores = abstention_scores(load("dev"))
    grid = np.round(np.arange(0.1, 1.0, 0.05), 2)
    best = max(grid, key=lambda t: (confusion(dev_scores, t)["accuracy"], -t))
    out["abstention"] = {"threshold_from_dev": float(best),
                         "dev": confusion(dev_scores, best),
                         "test": confusion(abstention_scores(test), best)}
    (RESULTS / "rag_retrieval_eval.json").write_text(json.dumps(out, indent=2, default=lambda o: o.item()))
    print(json.dumps(out, indent=2, default=lambda o: o.item()))


if __name__ == "__main__":
    main()

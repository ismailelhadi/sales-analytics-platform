"""End-to-end answer evaluation with an LLM-as-judge. Needs ANTHROPIC_API_KEY.

Compares two configurations on the test split:
  - baseline: plain prompt, no abstention threshold (the model always answers)
  - guarded:  citation prompt + retrieval-coverage abstention + "I don't know" fallback

A response counts as a hallucination when it states something the sources and reference
don't support, or when it answers an unanswerable question instead of declining.
Every judgement is saved so it can be spot-checked by hand.

    ANTHROPIC_API_KEY=... python -m rag.eval.llm_judge
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import anthropic

from rag import answer as rag_answer
from rag.eval.run_eval import RESULTS, load
from rag.retrieve import retrieve

JUDGE_MODEL = os.getenv("JUDGE_MODEL", "claude-sonnet-4-5")
JUDGE_PROMPT = """You are grading a question-answering system for a sales analytics team.

Question: {question}
Answerable from the knowledge base: {answerable}
Reference answer: {reference}
Sources the system retrieved:
{sources}

System response: {response}

Grade strictly and return only JSON:
{{"correct": true|false, "hallucinated": true|false, "reason": "<one sentence>"}}
- If the question is NOT answerable, the response is correct only if it declines (e.g. "I don't know").
  Any substantive answer to an unanswerable question is a hallucination.
- If the question IS answerable, it is correct if it agrees with the reference answer.
  It is hallucinated if it states any fact not supported by the sources or reference.
- Declining an answerable question is incorrect but NOT a hallucination."""


def judge(client, item, response, hits):
    sources = "\n".join(f"[{i + 1}] {h.content}" for i, h in enumerate(hits)) or "(none)"
    msg = client.messages.create(
        model=JUDGE_MODEL, max_tokens=200, temperature=0,
        messages=[{"role": "user", "content": JUDGE_PROMPT.format(
            question=item["question"], answerable=item["answerable"], reference=item["reference"],
            sources=sources, response=response)}])
    text = msg.content[0].text
    return json.loads(re.search(r"\{.*\}", text, re.S).group(0))


def main():
    if not os.getenv("ANTHROPIC_API_KEY"):
        sys.exit("Set ANTHROPIC_API_KEY to run the LLM-as-judge evaluation.")
    client = anthropic.Anthropic()
    items = load("test")
    rows, summary = [], {}
    for config in ("baseline", "guarded"):
        graded = []
        for it in items:
            res = rag_answer.answer(it["question"], guarded=(config == "guarded"))
            verdict = judge(client, it, res.answer, retrieve(it["question"], k=4))
            graded.append(verdict)
            rows.append({"config": config, "id": it["id"], "question": it["question"],
                         "answerable": it["answerable"], "response": res.answer, "mode": res.mode, **verdict})
            print(f"{config:8s} {it['id']} correct={verdict['correct']!s:5s} "
                  f"halluc={verdict['hallucinated']!s:5s} {it['question'][:60]}")
        n = len(graded)
        summary[config] = {"n": n,
                           "accuracy": round(sum(g["correct"] for g in graded) / n, 3),
                           "hallucination_rate": round(sum(g["hallucinated"] for g in graded) / n, 3)}
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "llm_judge_rows.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    (RESULTS / "llm_judge_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

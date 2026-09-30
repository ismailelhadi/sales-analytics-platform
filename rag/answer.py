"""Answer questions from the knowledge base with guardrails against confident wrong answers.

Guardrails:
  1. Abstain before generation: if the best retrieved chunk covers too few of the question's
     key terms, return "I don't know" instead of letting the model guess.
  2. Grounded prompt: the model may only use the numbered sources and must cite them.
  3. Fallbacks: if no LLM key is set or the call fails/times out, return an extractive answer
     (the best matching passage) instead of an error.
  4. Caching + structured logging for predictable behaviour in production.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from functools import lru_cache

from rag.retrieve import Hit, retrieve

log = logging.getLogger("rag.answer")
IDK = "I don't know. The knowledge base doesn't cover that."
# Tuned on the dev split of the ground-truth set (see rag/eval/run_eval.py).
COVERAGE_THRESHOLD = float(os.getenv("RAG_COVERAGE_THRESHOLD", "0.45"))
LLM_MODEL = os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001")

SYSTEM_PROMPT = """You answer questions for a retail sales analytics team using ONLY the numbered sources provided.
Rules:
- Every factual sentence must cite its source like [1] or [2].
- If the sources do not contain the answer, reply exactly: I don't know.
- Never use outside knowledge, never guess numbers, and keep answers under 80 words."""

BASELINE_PROMPT = "Answer the question using the context below."


@dataclass
class Answer:
    question: str
    answer: str
    mode: str                 # llm | extractive | abstain
    abstained: bool
    sources: list[dict] = field(default_factory=list)
    latency_ms: int = 0
    cached: bool = False


def _format_sources(hits: list[Hit]) -> str:
    return "\n\n".join(f"[{i + 1}] {h.title}\n{h.content}" for i, h in enumerate(hits))


def _call_llm(system: str, question: str, hits: list[Hit]) -> str:
    import anthropic
    client = anthropic.Anthropic(timeout=20.0, max_retries=1)
    msg = client.messages.create(
        model=LLM_MODEL, max_tokens=300, temperature=0, system=system,
        messages=[{"role": "user", "content": f"Sources:\n{_format_sources(hits)}\n\nQuestion: {question}"}],
    )
    return msg.content[0].text.strip()


def _extractive(hits: list[Hit]) -> str:
    return f"{hits[0].content} [1]"


def answer(question: str, k: int = 4, guarded: bool = True) -> Answer:
    t0 = time.perf_counter()
    hits = retrieve(question, k=k)
    sources = [{"n": i + 1, "doc_id": h.doc_id, "title": h.title, "score": round(h.score, 4),
                "coverage": round(h.coverage, 2)} for i, h in enumerate(hits)]
    if guarded and (not hits or hits[0].coverage < COVERAGE_THRESHOLD):
        result = Answer(question, IDK, "abstain", True, sources)
    elif not os.getenv("ANTHROPIC_API_KEY"):
        result = Answer(question, _extractive(hits), "extractive", False, sources)
    else:
        try:
            text = _call_llm(SYSTEM_PROMPT if guarded else BASELINE_PROMPT, question, hits)
            idk = text.lower().startswith("i don't know")
            result = Answer(question, IDK if idk else text, "abstain" if idk else "llm", idk, sources)
        except Exception as exc:  # timeout, rate limit, outage -> degrade gracefully
            log.warning("llm call failed, falling back to extractive: %s", exc)
            result = Answer(question, _extractive(hits), "extractive", False, sources)
    result.latency_ms = int((time.perf_counter() - t0) * 1000)
    log.info(json.dumps({"event": "rag_answer", "mode": result.mode, "abstained": result.abstained,
                         "top_doc": hits[0].doc_id if hits else None,
                         "top_coverage": sources[0]["coverage"] if sources else None,
                         "latency_ms": result.latency_ms}))
    return result


@lru_cache(maxsize=512)
def _cached(question_norm: str, k: int) -> Answer:
    return answer(question_norm, k)


def cached_answer(question: str, k: int = 4) -> Answer:
    norm = " ".join(question.strip().split())
    hits_before = _cached.cache_info().hits
    res = _cached(norm, k)
    out = Answer(**asdict(res))
    out.cached = _cached.cache_info().hits > hits_before
    return out

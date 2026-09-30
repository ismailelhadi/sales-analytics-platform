"""Hybrid retrieval: pgvector cosine search + Postgres full-text search, fused with
Reciprocal Rank Fusion (RRF). Vector search handles paraphrases; keyword search handles exact
identifiers like store ids and dates that embeddings tend to blur."""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache

import joblib
import numpy as np
from pgvector.psycopg import register_vector

from pipeline import db
from rag.index import EMBEDDER_PATH

RRF_K = 60
# Weight of keyword results in the fusion (vector weight is 1). Tuned on the dev split.
KEYWORD_WEIGHT = 1.0
CANDIDATES = 20


@dataclass
class Hit:
    chunk_id: str
    doc_id: str
    title: str
    content: str
    score: float          # fused score used for ranking
    similarity: float     # cosine similarity of this chunk to the query
    coverage: float       # share of the query's key terms that appear in this chunk


@lru_cache(maxsize=1)
def _embedder():
    return joblib.load(EMBEDDER_PATH)


def embed(text: str) -> np.ndarray:
    return _embedder().transform([text])[0].astype(np.float32)


def scope_filter(question: str) -> tuple[str, list[str]]:
    """Metadata filter: store cards are only searched when the question names a store.
    Without this, 120 near-identical store cards crowd out the reference docs on
    general questions like "what is an orphan activation?"."""
    stores = [f"store_{s.upper()}" for s in re.findall(r"\b(s\d{3})\b", question, re.I)]
    return "(doc_id not like 'store\\_%%' or doc_id = any(%s))", stores


def _vector_search(conn, qvec, k, question):
    where, stores = scope_filter(question)
    return conn.execute(
        f"select chunk_id, 1 - (embedding <=> %s) as sim from rag.chunks where {where} "
        "order by embedding <=> %s limit %s", (qvec, stores, qvec, k)).fetchall()


def _keyword_search(conn, question, k):
    where, stores = scope_filter(question)
    # OR the query's lexemes together so partial matches still rank (plainto_tsquery would AND them).
    return conn.execute("""
        with q as (
            select to_tsquery('english', coalesce(nullif(
                array_to_string(tsvector_to_array(to_tsvector('english', %s)), ' | '), ''), 'zzzz')) as tsq
        )
        select chunk_id, ts_rank_cd(tsv, q.tsq) as rank
        from rag.chunks, q where tsv @@ q.tsq and {where}
        order by rank desc limit %s""".format(where=where), (question, stores, k)).fetchall()


def retrieve(question: str, k: int = 4, mode: str = "hybrid", keyword_weight: float | None = None) -> list[Hit]:
    kw_w = KEYWORD_WEIGHT if keyword_weight is None else keyword_weight
    qvec = embed(question)
    with db.connect() as conn:
        register_vector(conn)
        vec = _vector_search(conn, qvec, CANDIDATES, question) if mode in ("hybrid", "vector") else []
        kw = _keyword_search(conn, question, CANDIDATES) if mode in ("hybrid", "keyword") else []
        fused: dict[str, float] = {}
        for ranking, weight in ((vec, 1.0), (kw, kw_w)):
            for rank, (cid, _) in enumerate(ranking):
                fused[cid] = fused.get(cid, 0.0) + weight / (RRF_K + rank + 1)
        top = sorted(fused, key=fused.get, reverse=True)[:k]
        if not top:
            return []
        rows = conn.execute(
            "select chunk_id, doc_id, title, content, 1 - (embedding <=> %s), tsvector_to_array(tsv) "
            "from rag.chunks where chunk_id = any(%s)", (qvec, top)).fetchall()
        q_terms = set(conn.execute("select tsvector_to_array(to_tsvector('english', %s))",
                                   (question,)).fetchone()[0])
    by_id = {r[0]: r for r in rows}

    def coverage(cid):
        return len(q_terms & set(by_id[cid][5])) / len(q_terms) if q_terms else 0.0

    return [Hit(cid, by_id[cid][1], by_id[cid][2], by_id[cid][3], fused[cid], float(by_id[cid][4]),
                coverage(cid)) for cid in top]

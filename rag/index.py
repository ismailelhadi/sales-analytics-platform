"""Build the RAG knowledge base in Postgres + pgvector.

Sources indexed:
  1. Hand-written docs in rag/docs/*.md (metric definitions, tiering method, runbook, ...)
  2. One generated "store card" per store from the warehouse (tier, score drivers, reconciliation)
  3. One generated incident note per data quality alert from ops.alerts

Embeddings are computed locally (TF-IDF -> truncated SVD, i.e. latent semantic analysis) so the
project runs with no API keys. Each chunk is stored with its embedding (pgvector) and a
full-text search vector, which lets retrieval combine semantic and keyword search.

    python -m rag.index
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from pgvector.psycopg import register_vector
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer

from pipeline import db

log = logging.getLogger("rag.index")
HERE = Path(__file__).resolve().parent
DOCS_DIR = HERE / "docs"
EMBEDDER_PATH = HERE / "artifacts" / "embedder.joblib"
DIM = 128


def chunk_markdown(doc_id: str, text: str) -> list[dict]:
    """One chunk per `##` section, prefixed with the document title for context."""
    title = re.search(r"^# (.+)$", text, re.M).group(1).strip()
    parts = re.split(r"^## ", text, flags=re.M)[1:]
    chunks = []
    for i, part in enumerate(parts):
        heading, _, body = part.partition("\n")
        chunks.append({
            "chunk_id": f"{doc_id}#{i}",
            "doc_id": doc_id,
            "title": f"{title} - {heading.strip()}",
            "content": f"{title}: {heading.strip()}\n{body.strip()}",
        })
    return chunks


def store_cards(conn) -> list[dict]:
    t = db.read_sql(conn, """
        select t.*, s.store_name, s.city, s.province, s.channel
        from marts.store_tiering t join marts.dim_store s using (store_id)""")
    recon = db.read_sql(conn, """
        select coalesce(pos_store_id, carrier_store_id) as store_id, recon_status, count(*) n
        from marts.fct_activation_reconciliation where recon_status <> 'matched' group by 1, 2""")
    labels = {"revenue": "revenue", "revenue_growth": "revenue growth",
              "accessory_attach_rate": "accessory attach rate", "activation_rate": "activation rate",
              "avg_basket": "average basket"}
    cards = []
    for r in t.itertuples():
        contribs = {k: getattr(r, f"contrib_{k}") for k in labels}
        ranked = sorted(contribs.items(), key=lambda kv: kv[1], reverse=True)
        issues = recon[recon.store_id == r.store_id].set_index("recon_status")["n"].to_dict()
        text = (
            f"Store {r.store_id} ({r.store_name}) is a {r.channel} store in {r.city}, {r.province}. "
            f"Store {r.store_id} tier: {r.tier}. Composite score {r.composite_score:.2f}, "
            f"ranked in the top {r.score_percentile:.0%} of stores. "
            f"Scoring window revenue: ${r.revenue:,.0f}. Revenue growth: {r.revenue_growth:.1%}. "
            f"Accessory attach rate: {r.accessory_attach_rate:.2f}. Activation rate: {r.activation_rate:.1%}. "
            f"Average basket: ${r.avg_basket:,.0f}. "
            f"Strongest driver of the score: {labels[ranked[0][0]]} ({ranked[0][1]:+.2f}). "
            f"Weakest driver: {labels[ranked[-1][0]]} ({ranked[-1][1]:+.2f}). "
            f"Reconciliation issues: {issues.get('missing_activation', 0)} missing activations, "
            f"{issues.get('orphan_activation', 0)} orphan activations, "
            f"{issues.get('store_mismatch', 0)} store mismatches."
        )
        cards.append({"chunk_id": f"store_{r.store_id}#0", "doc_id": f"store_{r.store_id}",
                      "title": f"Store card {r.store_id}", "content": text})
    return cards


def incident_notes(conn) -> list[dict]:
    a = db.read_sql(conn, """
        select distinct on (source, load_date) source, load_date, message
        from ops.alerts where severity = 'critical' order by source, load_date, raised_at desc""")
    notes = []
    for r in a.itertuples():
        src = "POS transactions" if r.source == "pos" else "carrier activations"
        check, _, detail = r.message.partition(": ")
        notes.append({
            "chunk_id": f"incident_{r.load_date}_{r.source}#0", "doc_id": f"incident_{r.load_date}_{r.source}",
            "title": f"Data quality incident {r.load_date} {r.source}",
            "content": (f"Data quality incident on {r.load_date}: the {src} file for {r.load_date} was quarantined "
                        f"and not loaded. Failed check: {check}. Detail: {detail}. "
                        f"Reconciliation excluded {r.load_date} until the file is re-delivered."),
        })
    return notes


def build_index() -> dict:
    with db.connect() as conn:
        chunks = []
        for p in sorted(DOCS_DIR.glob("*.md")):
            chunks += chunk_markdown(p.stem, p.read_text())
        chunks += store_cards(conn)
        chunks += incident_notes(conn)
        texts = [c["content"] for c in chunks]

        embedder = make_pipeline(
            TfidfVectorizer(ngram_range=(1, 2), sublinear_tf=True, stop_words="english",
                            token_pattern=r"(?u)\b\w+\b"),
            TruncatedSVD(n_components=min(DIM, len(texts) - 1), random_state=0),
            Normalizer(copy=False),
        )
        vecs = embedder.fit_transform(texts).astype(np.float32)
        dim = vecs.shape[1]
        EMBEDDER_PATH.parent.mkdir(exist_ok=True)
        joblib.dump(embedder, EMBEDDER_PATH)

        conn.execute("create extension if not exists vector")
        register_vector(conn)
        conn.execute("create schema if not exists rag")
        conn.execute("drop table if exists rag.chunks")
        conn.execute(f"""
            create table rag.chunks (
                chunk_id text primary key, doc_id text, title text, content text,
                embedding vector({dim}),
                tsv tsvector generated always as (to_tsvector('english', title || ' ' || content)) stored
            )""")
        with conn.cursor() as cur:
            cur.executemany("insert into rag.chunks (chunk_id, doc_id, title, content, embedding) "
                            "values (%s,%s,%s,%s,%s)",
                            [(c["chunk_id"], c["doc_id"], c["title"], c["content"], v)
                             for c, v in zip(chunks, vecs)])
        conn.execute("create index on rag.chunks using gin (tsv)")
        conn.commit()
    stats = {"chunks": len(chunks), "dim": dim,
             "docs": len({c['doc_id'] for c in chunks})}
    log.info("indexed %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(build_index())

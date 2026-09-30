"""FastAPI service for the sales knowledge assistant.

    uvicorn rag.api:app --port 8000
    curl -X POST localhost:8000/ask -H 'content-type: application/json' -d '{"question": "What tier is store S014?"}'
"""
from __future__ import annotations

import logging
from dataclasses import asdict

from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from pipeline import db
from rag.answer import cached_answer

logging.basicConfig(level=logging.INFO, format="%(message)s")
app = FastAPI(title="Sales Knowledge Assistant", version="1.0")


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)
    top_k: int = Field(default=4, ge=1, le=10)


@app.get("/health")
async def health():
    def check():
        with db.connect() as conn:
            return conn.execute("select count(*) from rag.chunks").fetchone()[0]
    return {"status": "ok", "chunks_indexed": await run_in_threadpool(check)}


@app.post("/ask")
async def ask(req: AskRequest):
    return asdict(await run_in_threadpool(cached_answer, req.question, req.top_k))

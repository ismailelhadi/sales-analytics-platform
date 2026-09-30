"""Integration tests against a built warehouse + index. Skipped when Postgres isn't reachable."""
import pytest

psycopg = pytest.importorskip("psycopg")
from pipeline import db  # noqa: E402


def _db_ready():
    try:
        with db.connect() as conn:
            conn.execute("select 1 from rag.chunks limit 1")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _db_ready(), reason="warehouse/index not built")


def test_api_health_and_ask(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from fastapi.testclient import TestClient
    from rag.api import app

    client = TestClient(app)
    assert client.get("/health").json()["chunks_indexed"] > 0
    res = client.post("/ask", json={"question": "What is an orphan activation?"}).json()
    assert "reconciliation_guide" in [s["doc_id"] for s in res["sources"][:3]]
    assert not res["abstained"]


def test_out_of_scope_question_abstains():
    from rag.answer import answer
    assert answer("Who is the CEO of the company?").abstained


def test_no_quarantined_batch_reached_raw():
    with db.connect() as conn:
        leaked = conn.execute("""
            select count(*) from ops.load_log l
            join raw.pos_transactions p on p.load_date = l.load_date
            where l.source = 'pos' and l.status = 'quarantined'""").fetchone()[0]
    assert leaked == 0

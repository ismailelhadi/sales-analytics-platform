from rag.index import chunk_markdown
from rag.retrieve import scope_filter


def test_chunk_markdown_splits_sections():
    chunks = chunk_markdown("doc", "# Title\n\n## A\nalpha\n\n## B\nbeta\n")
    assert [c["chunk_id"] for c in chunks] == ["doc#0", "doc#1"]
    assert chunks[1]["content"].startswith("Title: B")


def test_scope_filter_only_includes_named_stores():
    _, stores = scope_filter("What tier is store s014 vs S020?")
    assert stores == ["store_S014", "store_S020"]
    _, none = scope_filter("What is an orphan activation?")
    assert none == []

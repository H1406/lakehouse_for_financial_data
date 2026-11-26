"""Embeddings, incremental indexing and hybrid search over agent.documents / agent.chunks (pgvector).

Hybrid retrieval = vector top-N (cosine distance, HNSW) + full-text top-N (tsvector, GIN), merged with
reciprocal rank fusion: score(d) = sum over rankings of 1 / (k + rank). RRF needs no score calibration
between the two retrievers, which is why it is the standard way to combine them.
"""

import re
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from agents.core.db import connect
from agents.retrieval.documents import Document, fact_sheet, runbook, write_files
from lakehouse.config import settings

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  # 384 dims, runs on CPU
RRF_K = 60
REPO = Path(__file__).resolve().parents[2]


class Embedder:
    """Lazy sentence-transformers wrapper; normalized vectors so cosine distance == 1 - dot product."""

    def __init__(self, model_name: str = MODEL_NAME):
        self.model_name = model_name
        self._model = None

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        if self._model is None:
            from sentence_transformers import SentenceTransformer  # heavy import, only in the agents image
            self._model = SentenceTransformer(self.model_name)
        return self._model.encode(list(texts), normalize_embeddings=True, batch_size=64).tolist()


def vector_literal(v: Sequence[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def rrf(rankings: Sequence[Sequence[str]], k: int = RRF_K) -> list[tuple[str, float]]:
    """Reciprocal rank fusion of several ranked id lists -> [(id, score)] best first."""
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, item in enumerate(ranking, start=1):
            scores[item] = scores.get(item, 0.0) + 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))


def or_query(text: str) -> str:
    """Natural-language question -> websearch_to_tsquery input with OR semantics (AND would rarely match)."""
    words = re.findall(r"[A-Za-z0-9]{2,}", text)
    return " or ".join(words) or "nothing"


def hybrid_search(query: str, *, embedder: Embedder, k: int = 5, kind: str | None = None,
                  ticker: str | None = None, fiscal_year: int | None = None, fiscal_period: str | None = None,
                  candidates: int = 25) -> list[dict]:
    where, args = [], {}
    for col, val in (("kind", kind), ("ticker", ticker.upper() if ticker else None),
                     ("fiscal_year", fiscal_year), ("fiscal_period", fiscal_period.upper() if fiscal_period else None)):
        if val is not None:
            where.append(f"d.{col} = %({col})s")
            args[col] = val
    filt = (" and " + " and ".join(where)) if where else ""
    args.update(q=or_query(query), vec=vector_literal(embedder.encode([query])[0]), n=candidates)
    base = "from agent.chunks c join agent.documents d using (doc_id) where true" + filt
    with connect("agent_ro") as conn:
        vec_ids = [r["chunk_id"] for r in conn.execute(
            f"select c.chunk_id {base} order by c.embedding <=> %(vec)s::vector limit %(n)s", args)]
        fts_ids = [r["chunk_id"] for r in conn.execute(
            f"select c.chunk_id {base} and c.tsv @@ websearch_to_tsquery('english', %(q)s) "
            f"order by ts_rank_cd(c.tsv, websearch_to_tsquery('english', %(q)s)) desc limit %(n)s", args)]
        fused = rrf([vec_ids, fts_ids])[:k]
        if not fused:
            return []
        rows = {r["chunk_id"]: r for r in conn.execute(
            "select c.chunk_id, c.doc_id, c.section, c.content, d.kind, d.ticker, d.fiscal_year, d.fiscal_period "
            "from agent.chunks c join agent.documents d using (doc_id) where c.chunk_id = any(%s)",
            ([cid for cid, _ in fused],))}
    out = []
    for cid, score in fused:
        r = dict(rows[cid])
        r.update(score=round(score, 5), vector_rank=_rank(vec_ids, cid), text_rank=_rank(fts_ids, cid))
        out.append(r)
    return out


def _rank(ids: list[str], cid: str) -> int | None:
    return ids.index(cid) + 1 if cid in ids else None


def load_documents() -> list[Document]:
    with connect("agent_ro", timeout_ms=30_000) as conn:
        rows = conn.execute(
            "select ticker, cik, name, sector, fiscal_year, fiscal_period, period_end, filed_date, accession_no, "
            "revenue, net_income, operating_income, total_assets, total_liabilities, stockholders_equity, "
            "eps_diluted, revenue_qoq, revenue_yoy, net_income_yoy, net_margin, operating_margin, debt_to_equity, "
            "avg_vol_20d, max_vol_20d, worst_drawdown, trading_days, has_restatement "
            "from marts.mart_fact_sheet_inputs order by ticker, period_end").fetchall()
    docs = [fact_sheet(r) for r in rows]
    docs += [runbook(p) for p in sorted((REPO / "runbooks").glob("*.md"))]
    return docs


def refresh_index(embedder: Embedder, docs: list[Document] | None = None) -> dict:
    """Render all documents, write the files, re-embed only documents whose content_hash changed,
    and delete documents that no longer exist. Idempotent: a second run embeds nothing."""
    docs = docs if docs is not None else load_documents()
    generated_at = datetime.now(UTC).isoformat()
    out_root = Path(settings().data_dir)
    for d in docs:
        if d.kind == "fact_sheet":
            write_files(d, out_root, generated_at)
    with connect("agent_rw", read_only=False, timeout_ms=120_000) as conn:
        existing = {r["doc_id"]: r["content_hash"] for r in conn.execute(
            "select doc_id, content_hash from agent.documents")}
        changed = [d for d in docs if existing.get(d.doc_id) != d.content_hash]
        chunk_texts = [(d, i, sec, txt) for d in changed for i, (sec, txt) in enumerate(d.chunks())]
        vectors = embedder.encode([t for *_, t in chunk_texts]) if chunk_texts else []
        with conn.transaction():
            for d in changed:
                conn.execute(
                    """insert into agent.documents (doc_id, kind, ticker, fiscal_year, fiscal_period, title, path,
                           content_hash, metadata, updated_at)
                       values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                       on conflict (doc_id) do update set title = excluded.title, path = excluded.path,
                           content_hash = excluded.content_hash, metadata = excluded.metadata, updated_at = now()""",
                    (d.doc_id, d.kind, d.ticker, d.fiscal_year, d.fiscal_period, d.title, d.path, d.content_hash,
                     _json(d.metadata)))
                conn.execute("delete from agent.chunks where doc_id = %s", (d.doc_id,))
            with conn.cursor() as cur:
                cur.executemany(
                    "insert into agent.chunks (chunk_id, doc_id, chunk_no, section, content, embedding) "
                    "values (%s, %s, %s, %s, %s, %s::vector)",
                    [(f"{d.doc_id}#{i}", d.doc_id, i, sec, txt, vector_literal(v))
                     for (d, i, sec, txt), v in zip(chunk_texts, vectors, strict=True)])
            stale = sorted(set(existing) - {d.doc_id for d in docs})
            if stale:
                conn.execute("delete from agent.documents where doc_id = any(%s)", (stale,))
    return {"documents": len(docs), "changed": len(changed), "chunks_embedded": len(chunk_texts),
            "deleted": len(stale)}


def _json(obj: dict) -> str:
    import json

    return json.dumps(obj, default=str)

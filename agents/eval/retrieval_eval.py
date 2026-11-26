"""Retrieval evaluation (no LLM): Precision@5 / Recall@5 / MRR of vector-only, text-only and hybrid (RRF)
retrieval over fact sheets, using the labels in qa_questions.yaml. Documents are ranked by the first
appearance of any of their chunks."""

import json
from pathlib import Path

import yaml

from agents.core.db import connect
from agents.retrieval.search import Embedder, or_query, rrf, vector_literal

K, CANDIDATES = 5, 40
QUESTIONS = Path(__file__).with_name("qa_questions.yaml")


def ranked_docs(conn, question: str, vec: list[float], mode: str) -> list[str]:
    base = "from agent.chunks c join agent.documents d using (doc_id) where d.kind = 'fact_sheet'"
    args = {"vec": vector_literal(vec), "q": or_query(question), "n": CANDIDATES}
    v = [r["doc_id"] for r in conn.execute(
        f"select c.doc_id, c.chunk_id {base} order by c.embedding <=> %(vec)s::vector limit %(n)s", args)]
    t = [r["doc_id"] for r in conn.execute(
        f"select c.doc_id {base} and c.tsv @@ websearch_to_tsquery('english', %(q)s) "
        f"order by ts_rank_cd(c.tsv, websearch_to_tsquery('english', %(q)s)) desc limit %(n)s", args)]
    if mode == "vector":
        chunks = v
    elif mode == "text":
        chunks = t
    else:  # fuse at document level: each list collapsed to first-appearance doc ranking
        chunks = [d for d, _ in rrf([dedup(v), dedup(t)])]
    return dedup(chunks)[:K]


def dedup(ids: list[str]) -> list[str]:
    return list(dict.fromkeys(ids))


def evaluate() -> dict:
    items = yaml.safe_load(QUESTIONS.read_text())
    emb = Embedder()
    vecs = emb.encode([i["q"] for i in items])
    report = {}
    with connect("agent_ro", timeout_ms=30_000) as conn:
        for mode in ("vector", "text", "hybrid"):
            p, r, mrr, rows = [], [], [], []
            for item, vec in zip(items, vecs, strict=True):
                got, rel = ranked_docs(conn, item["q"], vec, mode), set(item["relevant"])
                hits = [d for d in got if d in rel]
                p.append(len(hits) / K)
                r.append(len(hits) / len(rel))
                first = next((i for i, d in enumerate(got, 1) if d in rel), None)
                mrr.append(1 / first if first else 0.0)
                rows.append({"q": item["q"], "top5": got, "hits": hits})
            n = len(items)
            report[mode] = {"precision@5": round(sum(p) / n, 3), "recall@5": round(sum(r) / n, 3),
                            "mrr": round(sum(mrr) / n, 3), "per_question": rows}
    return report


if __name__ == "__main__":
    rep = evaluate()
    print(json.dumps({m: {k: v for k, v in r.items() if k != "per_question"} for m, r in rep.items()}, indent=1))
    out = Path("docs/eval")
    out.mkdir(parents=True, exist_ok=True)
    (out / "retrieval_eval.json").write_text(json.dumps(rep, indent=1))

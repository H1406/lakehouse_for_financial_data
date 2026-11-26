"""End-to-end agent evaluation with the live LLM (needs DEEPSEEK_API_KEY).

QA:     every question in qa_questions.yaml through the full agent. Scored on: cites at least one relevant
        fact sheet, no ungrounded citations, expected strings present (where given), guardrail stop reason.
Triage: the incidents created by scripts/inject_fault.py (data/faults/<fault>.json): diagnosed CATEGORY vs the
        expected one, plus time to diagnosis. Bonus: real incidents passed with --extra-incidents (not scored).

    python -m agents.eval.agent_eval [--qa-only | --triage-only] [--extra-incidents 6 18]
"""

import argparse
import json
import statistics
from pathlib import Path

import yaml

from agents.qa.agent import ask
from agents.retrieval.search import Embedder
from agents.triage.agent import triage

ROOT = Path(__file__).resolve().parents[2]
FAULTS = ["api_500", "schema_change", "duplicate_batch", "stream_stop"]


def eval_qa(embedder: Embedder) -> dict:
    items = yaml.safe_load((Path(__file__).with_name("qa_questions.yaml")).read_text())
    rows = []
    for it in items:
        r = ask(it["q"], embedder=embedder)
        relevant = set(it["relevant"])
        rows.append({
            "q": it["q"], "answer": r["answer"], "citations": r["citations"],
            "cites_relevant": bool(relevant & set(r["citations"])),
            "ungrounded": r["ungrounded_citations"],
            "expect_ok": all(e in r["answer"] for e in it.get("expect", [])),
            "stopped_by": r["stopped_by"], "steps": r["steps"], "tool_calls": [c["name"] for c in r["tool_calls"]],
            "seconds": r["seconds"], "tokens": r["usage"]["prompt_tokens"] + r["usage"]["completion_tokens"],
        })
    n = len(rows)
    return {"questions": n,
            "cites_relevant_rate": round(sum(r["cites_relevant"] for r in rows) / n, 3),
            "grounded_rate": round(sum(not r["ungrounded"] for r in rows) / n, 3),
            "expect_ok_rate": round(sum(r["expect_ok"] for r in rows) / n, 3),
            "stopped_final_rate": round(sum(r["stopped_by"] == "final" for r in rows) / n, 3),
            "median_seconds": statistics.median(r["seconds"] for r in rows),
            "median_tokens": statistics.median(r["tokens"] for r in rows), "rows": rows}


def eval_triage(embedder: Embedder, extra: list[int]) -> dict:
    rows = []
    for fault in FAULTS:
        meta_path = ROOT / "data" / "faults" / f"{fault}.json"
        if not meta_path.exists():
            rows.append({"fault": fault, "error": "not injected (run make fault FAULT=...)"})
            continue
        meta = json.loads(meta_path.read_text())
        r = triage(meta["incident_id"], embedder=embedder)
        rows.append({"fault": fault, "incident_id": meta["incident_id"], "expected": meta["expected_category"],
                     "diagnosed": r["category"], "correct": r["category"] == meta["expected_category"],
                     "stopped_by": r["stopped_by"], "seconds": r["seconds"], "tool_calls": r["tool_calls"],
                     "note": f"incidents/{meta['incident_id']}.md"})
    bonus = []
    for inc in extra:
        r = triage(inc, embedder=embedder)
        bonus.append({"incident_id": inc, "diagnosed": r["category"], "stopped_by": r["stopped_by"],
                      "seconds": r["seconds"], "note": f"incidents/{inc}.md"})
    scored = [r for r in rows if "correct" in r]
    return {"correct": sum(r["correct"] for r in scored), "of": len(FAULTS),
            "median_seconds_to_note": statistics.median(r["seconds"] for r in scored) if scored else None,
            "rows": rows, "bonus_real_incidents": bonus}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--qa-only", action="store_true")
    p.add_argument("--triage-only", action="store_true")
    p.add_argument("--extra-incidents", type=int, nargs="*", default=[])
    a = p.parse_args()
    emb = Embedder()
    report = {}
    if not a.triage_only:
        report["qa"] = eval_qa(emb)
    if not a.qa_only:
        report["triage"] = eval_triage(emb, a.extra_incidents)
    out = ROOT / "docs" / "eval" / "agent_eval.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1, default=str))
    print(json.dumps({k: {m: v for m, v in s.items() if m not in ("rows",)} for k, s in report.items()},
                     indent=1, default=str))

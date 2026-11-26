"""Financial QA agent: answers from tested data only, citing fact-sheet ids.

Tools: search_fact_sheets (hybrid pgvector + full-text, RRF), list_tables + run_sql (guarded NL-SQL on marts).
After the run, every cited id is checked against the ids the tools actually returned (grounding check).
"""

import argparse
import json
import re
from datetime import date

from agents.core.llm import DeepSeekClient
from agents.core.loop import Guardrails, run_agent
from agents.core.tools import Tool, params
from agents.core.trace import RunLog
from agents.qa.sql_guard import describe_tables, run_sql
from agents.retrieval.search import Embedder, hybrid_search

CITATION = re.compile(r"\[([A-Z]{1,5}-\d{4}-(?:Q[1-3]|FY))\]")
DOC_ID = re.compile(r"\b([A-Z]{1,5}-\d{4}-(?:Q[1-3]|FY))\b")

SYSTEM = """You are a financial data analyst for a market-volatility lakehouse. Today is {today}.
Answer ONLY from tool results; never use outside knowledge for numbers.

Data you can reach:
- Fact sheets (search_fact_sheets): one per ticker x fiscal period, id like NVDA-2027-Q2. Fiscal years can
  differ from calendar years (e.g. NVDA FY2027 Q2 ended 2026-07-26), so check the period end date.
  Periods are Q1, Q2, Q3 and FY (XBRL has no separate Q4).
- marts tables (list_tables, then run_sql): daily volatility, drawdown, correlation, point-in-time fundamentals,
  1-minute intraday volatility. run_sql runs ONE read-only SELECT on marts, max 100 rows.
Universe: AAPL MSFT NVDA AMZN GOOGL META JPM V XOM JNJ WMT KO (equities), BTCUSDT ETHUSDT SOLUSDT (crypto).

Rules:
- Cite every fact-sheet fact with its id in square brackets, e.g. [NVDA-2027-Q2]. Cite SQL results as [sql].
- Prefer one good search with a ticker filter over many vague ones. Do not repeat an identical call.
- If the data does not contain the answer, say so plainly instead of guessing.
- Final answer: short and direct (a few sentences or a small table), numbers with units and dates."""


def tools(embedder: Embedder) -> list[Tool]:
    def search_fact_sheets(query: str, ticker: str | None = None, fiscal_year: int | None = None,
                           fiscal_period: str | None = None, k: int = 5) -> list[dict]:
        hits = hybrid_search(query, embedder=embedder, k=min(int(k), 8), kind="fact_sheet", ticker=ticker,
                             fiscal_year=fiscal_year, fiscal_period=fiscal_period)
        return [{"doc_id": h["doc_id"], "section": h["section"], "content": h["content"], "score": h["score"]}
                for h in hits]

    return [
        Tool("search_fact_sheets", "Hybrid (semantic + keyword) search over company fact sheets. Filter by ticker "
             "and fiscal year/period when the question names them.",
             params(query={"type": "string", "description": "what to look for"},
                    ticker={"type": "string", "description": "e.g. NVDA"},
                    fiscal_year={"type": "integer", "description": "fiscal year, e.g. 2027"},
                    fiscal_period={"type": "string", "enum": ["Q1", "Q2", "Q3", "FY"]},
                    k={"type": "integer", "description": "results, default 5, max 8"},
                    required=["query"]), search_fact_sheets),
        Tool("list_tables", "Columns of the queryable marts tables.", params(), describe_tables),
        Tool("run_sql", "Run ONE read-only SELECT against marts tables (max 100 rows, 5 s timeout).",
             params(sql={"type": "string", "description": "a single PostgreSQL SELECT"}, required=["sql"]),
             run_sql),
    ]


def ask(question: str, *, llm=None, embedder: Embedder | None = None, guard: Guardrails | None = None,
        log: bool = True) -> dict:
    trace = RunLog("qa", enabled=log)
    result = run_agent(system=SYSTEM.format(today=date.today().isoformat()), user=question,
                       tools=tools(embedder or Embedder()), llm=llm or DeepSeekClient(),
                       guard=guard or Guardrails(max_steps=8, max_tool_calls=10), trace=trace)
    cited = sorted(set(CITATION.findall(result.answer)))
    returned = set(DOC_ID.findall("\n".join(result.tool_outputs)))
    return {
        "question": question, "answer": result.answer, "citations": cited,
        "ungrounded_citations": sorted(set(cited) - returned),  # cited ids no tool returned: flag, never trust
        "stopped_by": result.stopped_by, "steps": result.steps,
        "tool_calls": [{"name": c["name"], "args": c["args"], "blocked": c["blocked"]} for c in result.tool_calls],
        "usage": result.usage, "seconds": result.seconds, "run_id": trace.run_id,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Ask the financial QA agent")
    p.add_argument("question")
    out = ask(p.parse_args().question)
    print(out["answer"], "\n")
    print(json.dumps({k: v for k, v in out.items() if k not in ("answer", "question")}, indent=1, default=str))

"""Agent-ready documents: fact sheets rendered from marts.mart_fact_sheet_inputs, plus the runbooks.

A document is Markdown with `## ` sections. Each section becomes one chunk, prefixed with the document's identity
("NVDA-2027-Q2 · NVIDIA Corp. · FY2027 Q2 · Key financials") so a chunk is self-explanatory when retrieved alone.
`content_hash` covers only the Markdown (no timestamps), so an unchanged sheet is never re-embedded.
"""

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from jinja2 import Environment, StrictUndefined

FACT_SHEET = Environment(undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True).from_string("""\
# {{ r.ticker }} · {{ r.name }} · FY{{ r.fiscal_year }} {{ r.fiscal_period }}

## Company
{{ r.name }} ({{ r.ticker }}), sector {{ r.sector }}, SEC CIK {{ r.cik }}.

## Period and filing
Fiscal year {{ r.fiscal_year }}, period {{ r.fiscal_period }}, period ending {{ r.period_end }}.
Filed with the SEC on {{ r.filed_date }}, accession number {{ r.accession_no }}.
Source: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={{ r.cik }}

## Key financials
- Revenue: {{ money(r.revenue) }}
- Operating income: {{ money(r.operating_income) }}
- Net income: {{ money(r.net_income) }}
- Diluted EPS: {{ num(r.eps_diluted) }}
- Total assets: {{ money(r.total_assets) }}
- Total liabilities: {{ money(r.total_liabilities) }}
- Stockholders' equity: {{ money(r.stockholders_equity) }}

## Growth
- Revenue vs previous reported quarter (QoQ): {{ chg(r.revenue_qoq) }}
- Revenue vs same period last year (YoY): {{ chg(r.revenue_yoy) }}
- Net income vs same period last year (YoY): {{ chg(r.net_income_yoy) }}

## Margins and leverage
- Net margin: {{ pct(r.net_margin) }}
- Operating margin: {{ pct(r.operating_margin) }}
- Debt to equity (total liabilities / equity): {{ num(r.debt_to_equity) }}

## Market risk over the period
Measured over {{ r.trading_days or 0 }} trading days up to {{ r.period_end }}.
- Average 20-day annualized volatility: {{ pct(r.avg_vol_20d) }}
- Peak 20-day annualized volatility: {{ pct(r.max_vol_20d) }}
- Worst drawdown from running high: {{ chg(r.worst_drawdown) }}

## Data quality notes
{% if r.has_restatement %}- Values for this period differ across filings (restated, e.g. by a 10-K/A or 10-Q/A); the latest filing is used.
{% else %}- No restatement detected for this period.
{% endif %}
{% for m in missing %}- {{ m }} is not reported in XBRL for this period.
{% endfor %}
""")

NA = "not reported"


def money(v) -> str:
    if v is None:
        return NA
    v = float(v)
    return f"${v / 1e9:,.2f} billion" if abs(v) >= 1e9 else f"${v / 1e6:,.1f} million"


def pct(v) -> str:
    """A level, e.g. a margin or a volatility."""
    return NA if v is None else f"{float(v) * 100:.1f}%"


def chg(v) -> str:
    """A change, always signed."""
    return NA if v is None else f"{float(v) * 100:+.1f}%"


def num(v) -> str:
    return NA if v is None else f"{float(v):,.2f}"


@dataclass
class Document:
    doc_id: str
    kind: str
    title: str
    path: str
    text: str
    ticker: str | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None
    metadata: dict = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.text.encode()).hexdigest()

    def chunks(self) -> list[tuple[str, str]]:
        """[(section, chunk text)]: one chunk per `## ` section, prefixed with the document identity."""
        parts = re.split(r"^## ", self.text, flags=re.M)
        prefix = self.title
        out = []
        for part in parts[1:]:
            section, _, body = part.partition("\n")
            body = body.strip()
            if body:
                out.append((section.strip(), f"{prefix} · {section.strip()}\n{body}"))
        return out


def fact_sheet(row: dict) -> Document:
    r = dict(row)
    doc_id = f"{r['ticker']}-{r['fiscal_year']}-{r['fiscal_period']}"
    labels = {"revenue": "Revenue", "net_income": "Net income", "operating_income": "Operating income",
              "eps_diluted": "Diluted EPS", "stockholders_equity": "Stockholders' equity"}
    missing = [label for col, label in labels.items() if r.get(col) is None]
    text = FACT_SHEET.render(r=type("Row", (), r), money=money, pct=pct, chg=chg, num=num, missing=missing)
    return Document(
        doc_id=doc_id, kind="fact_sheet", title=f"{doc_id} · {r['name']} · FY{r['fiscal_year']} {r['fiscal_period']}",
        path=f"fact_sheets/{r['ticker']}/{r['fiscal_year']}-{r['fiscal_period']}.md", text=text,
        ticker=r["ticker"], fiscal_year=int(r["fiscal_year"]), fiscal_period=r["fiscal_period"],
        metadata={"cik": r["cik"], "filed_date": str(r["filed_date"]), "period_end": str(r["period_end"]),
                  "accession_no": r["accession_no"],
                  "source_urls": [f"https://data.sec.gov/api/xbrl/companyfacts/CIK{r['cik']}.json"]},
    )


def runbook(path: Path) -> Document:
    name = path.stem
    return Document(doc_id=f"runbook:{name}", kind="runbook", title=f"runbook:{name}",
                    path=f"runbooks/{path.name}", text=path.read_text())


def write_files(doc: Document, root: Path, generated_at: str) -> None:
    """Fact sheet Markdown + JSON sidecar (metadata, content_hash, generated_at) under data/."""
    md = root / doc.path
    md.parent.mkdir(parents=True, exist_ok=True)
    md.write_text(doc.text)
    sidecar = {"doc_id": doc.doc_id, "ticker": doc.ticker, "fiscal_year": doc.fiscal_year,
               "fiscal_period": doc.fiscal_period, **doc.metadata, "content_hash": doc.content_hash,
               "generated_at": generated_at}
    md.with_suffix(".json").write_text(json.dumps(sidecar, indent=1))

from datetime import date

from agents.pii.subscribers import mask_email, subscriber_key
from agents.qa.agent import CITATION
from agents.retrieval.documents import chg, fact_sheet, money, pct
from agents.retrieval.search import or_query, rrf
from agents.triage.agent import parse_category

ROW = {
    "ticker": "NVDA", "cik": "0001045810", "name": "NVIDIA Corp.", "sector": "Information Technology",
    "fiscal_year": 2027, "fiscal_period": "Q2", "period_end": date(2026, 7, 26), "filed_date": date(2026, 8, 26),
    "accession_no": "0001045810-26-000075", "revenue": 96221000000.0, "net_income": 59688000000.0,
    "operating_income": 63734000000.0, "total_assets": 320272000000.0, "total_liabilities": 91288000000.0,
    "stockholders_equity": 228984000000.0, "eps_diluted": 2.46, "revenue_qoq": 0.179, "revenue_yoy": 1.0585114,
    "net_income_yoy": 1.259, "net_margin": 0.62, "operating_margin": 0.662, "debt_to_equity": 0.3987,
    "avg_vol_20d": 0.405, "max_vol_20d": 0.473, "worst_drawdown": -0.182, "trading_days": 62, "has_restatement": False,
}


def test_fact_sheet_render_chunks_and_stable_hash():
    doc = fact_sheet(ROW)
    assert doc.doc_id == "NVDA-2027-Q2" and doc.path == "fact_sheets/NVDA/2027-Q2.md"
    assert "Revenue: $96.22 billion" in doc.text and "(YoY): +105.9%" in doc.text and "Net margin: 62.0%" in doc.text
    chunks = doc.chunks()
    assert [s for s, _ in chunks][:3] == ["Company", "Period and filing", "Key financials"]
    assert all(t.startswith("NVDA-2027-Q2 · NVIDIA Corp. · FY2027 Q2 · ") for _, t in chunks)  # self-contained
    assert fact_sheet(ROW).content_hash == doc.content_hash  # no timestamps in the hashed text
    assert fact_sheet(ROW | {"revenue": 1.0}).content_hash != doc.content_hash


def test_missing_values_are_reported_not_invented():
    doc = fact_sheet(ROW | {"revenue": None, "revenue_yoy": None})
    assert "Revenue: not reported" in doc.text and "Revenue is not reported in XBRL" in doc.text


def test_formatters():
    assert money(1.5e9) == "$1.50 billion" and money(2.5e6) == "$2.5 million" and money(None) == "not reported"
    assert pct(0.405) == "40.5%" and chg(-0.182) == "-18.2%" and chg(0.1) == "+10.0%"


def test_rrf_rewards_agreement_between_retrievers():
    fused = dict(rrf([["a", "b", "c"], ["c", "a", "d"]], k=60))
    assert max(fused, key=fused.get) == "a"  # ranked 1st and 2nd beats a single 1st place
    assert fused["c"] > fused["b"] and "d" in fused


def test_or_query_and_citations():
    assert or_query("NVIDIA's revenue in FY2027?") == "NVIDIA or revenue or in or FY2027"
    assert CITATION.findall("Revenue was $96B [NVDA-2027-Q2], margin [AAPL-2026-FY] [sql]") == \
        ["NVDA-2027-Q2", "AAPL-2026-FY"]


def test_triage_category_parsing():
    assert parse_category("...\nCATEGORY: duplicate_load") == "duplicate_load"
    assert parse_category("CATEGORY: made_up") == "unknown" and parse_category("no line") == "unknown"


def test_pii_key_is_keyed_hmac_and_email_masked():
    assert subscriber_key(1) != subscriber_key(2) and len(subscriber_key(1)) == 64
    assert mask_email("linh.tran7@example.com") == "l***@example.com"

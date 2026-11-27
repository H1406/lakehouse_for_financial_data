import json

from agents.core.trace import scrub_tree


def test_structure_and_ids_survive_scrubbing():
    hits = [{"doc_id": "runbook:api_failure", "section": "Quarantine spike", "score": 0.0328},
            {"doc_id": "NVDA-2027-Q3", "section": "Income statement", "score": 0.0161}]
    out = scrub_tree({"arguments": {"query": "quarantine spike"}, "output": hits})
    assert out["output"][0]["doc_id"] == "runbook:api_failure"
    assert out["output"][1] == hits[1]
    assert json.loads(json.dumps(out)) == out  # still valid, round-trippable JSON


def test_secrets_are_still_masked():
    out = scrub_tree({"api_key": "sk-live-123", "headers": {"Authorization": "Bearer abc"},
                      "text": "password=hunter2 and a token " + "a" * 40})
    assert out["api_key"] == "***" and out["headers"]["Authorization"] == "***"
    assert "hunter2" not in out["text"] and "a" * 40 not in out["text"]


def test_non_json_values_become_strings_and_long_strings_are_cut():
    from datetime import date
    out = scrub_tree({"d": date(2026, 11, 26), "big": "x " * 5000, "n": None, "ok": True})
    assert out["d"] == "2026-11-26" and out["n"] is None and out["ok"] is True
    assert len(out["big"]) <= 4000

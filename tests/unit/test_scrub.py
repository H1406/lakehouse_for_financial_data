from lakehouse.ops import scrub


def test_masks_secrets_quotes_and_digits():
    msg = "auth failed token=abc123 for 'AAPL' user \"bob\" account 1234567890 key ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"
    out = scrub(msg)
    assert "abc123" not in out and "AAPL" not in out and "bob" not in out
    assert "1234567890" not in out and "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345" not in out
    assert out.startswith("auth failed token=***")


def test_none_and_truncation():
    assert scrub(None) is None
    assert len(scrub("x " * 5000)) == 2000

"""Scrub error messages and logs before they are stored in `ops` or shown to an LLM."""

import re

_SECRET_KV = re.compile(r"(?i)\b(token|password|passwd|secret|api[_-]?key|authorization)(\s*[=:]\s*|\s+)(\S+)")
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")
_LONG_TOKEN = re.compile(r"\b[A-Za-z0-9_\-]{32,}\b")
_LONG_DIGITS = re.compile(r"\d{6,}")


def scrub(text: str | None, max_len: int = 2000) -> str | None:
    if text is None:
        return None
    text = _SECRET_KV.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)
    text = _QUOTED.sub("'***'", text)
    text = _LONG_TOKEN.sub("***", text)
    text = _LONG_DIGITS.sub("<digits>", text)
    return text[:max_len]

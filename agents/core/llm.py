"""DeepSeek chat-completions client (OpenAI-compatible API) with tool calls and thinking mode.

Docs (checked 2026-11-26): base URL https://api.deepseek.com, models `deepseek-flash` / `deepseek-v4-pro`, thinking
mode on by default (`thinking: {type: enabled}`, `reasoning_effort: low|high|max`). Rule that shapes the agent loop:
in a conversation that carries `tools`, every assistant `reasoning_content` must be sent back in later requests,
otherwise the API answers 400. Plain `requests` instead of an SDK: one endpoint, nothing hidden.
"""

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from lakehouse.config import settings


class LLMError(Exception):
    pass


class RetryableLLMError(LLMError):
    pass


class DeepSeekClient:
    def __init__(self, *, api_key: str | None = None, model: str | None = None, base_url: str | None = None,
                 reasoning_effort: str | None = None, timeout: float = 180):
        s = settings()
        self.api_key = api_key if api_key is not None else s.deepseek_api_key.get_secret_value()
        self.model = model or s.deepseek_model
        self.base_url = (base_url or s.deepseek_base_url).rstrip("/")
        self.reasoning_effort = reasoning_effort or s.deepseek_reasoning_effort
        self.timeout = timeout

    @retry(retry=retry_if_exception_type((RetryableLLMError, requests.ConnectionError, requests.Timeout)),
           wait=wait_exponential(multiplier=2, max=30), stop=stop_after_attempt(4), reraise=True)
    def chat(self, messages: list[dict], tools: list[dict] | None = None, tool_choice: str | None = None) -> dict:
        """One completion. Returns {"message": {...}, "usage": {...}, "finish_reason": str}."""
        if not self.api_key:
            raise LLMError("DEEPSEEK_API_KEY is not set (add it to .env)")
        body: dict = {"model": self.model, "messages": messages, "thinking": {"type": "enabled"},
                      "reasoning_effort": self.reasoning_effort}
        if tools:
            body["tools"] = tools
        if tool_choice:
            body["tool_choice"] = tool_choice
        resp = requests.post(f"{self.base_url}/chat/completions", json=body, timeout=self.timeout,
                             headers={"Authorization": f"Bearer {self.api_key}"})
        if resp.status_code == 429 or resp.status_code >= 500:
            raise RetryableLLMError(f"DeepSeek HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise LLMError(f"DeepSeek HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        choice = data["choices"][0]
        return {"message": choice["message"], "usage": data.get("usage", {}),
                "finish_reason": choice.get("finish_reason")}

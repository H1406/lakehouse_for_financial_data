"""The agent loop shared by every agent: reason -> call tools -> observe -> repeat -> final answer.

    messages = [system, user]
    repeat:
        reply = llm(messages, tools)                 # DeepSeek thinking mode: reasoning_content + tool_calls
        keep reply (incl. reasoning_content) in messages
        no tool calls?  -> final answer
        else run each tool through the guardrails, append results as role=tool messages

Guardrails (all in `Guardrails`): a step cap, a tool-call cap, repeated identical calls blocked, a cap on
blocked calls, a wall-clock budget, a token budget and truncated tool output. When a limit trips, the loop asks
the model once more for a final answer with tools disabled (`tool_choice="none"`) and reports `stopped_by`.
Tools are read-only, so the worst a runaway loop can do is waste tokens; these limits bound that.
"""

import json
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from agents.core.llm import LLMError
from agents.core.tools import Tool
from lakehouse.ops import scrub


@dataclass(frozen=True)
class Guardrails:
    max_steps: int = 8                # LLM calls in the loop (the forced final answer is one extra)
    max_tool_calls: int = 12          # tool executions per run
    max_repeats: int = 1              # how often the *identical* (tool, arguments) may run
    max_blocked: int = 3              # blocked calls (repeat / budget) before the run is stopped
    max_seconds: float = 180.0        # wall clock for the whole run
    max_total_tokens: int = 250_000   # prompt + completion tokens summed over the run
    max_tool_output_chars: int = 6_000


@dataclass
class AgentResult:
    answer: str
    stopped_by: str                   # final | max_steps | max_seconds | max_tokens | blocked_calls | llm_error
    steps: int
    tool_calls: list[dict] = field(default_factory=list)   # {name, args, ok, blocked}
    tool_outputs: list[str] = field(default_factory=list)  # raw tool results (for citation checks)
    reasoning: list[str] = field(default_factory=list)
    usage: dict = field(default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0})
    run_id: str | None = None
    seconds: float = 0.0


FINAL_PROMPT = ("Guardrail '{reason}' reached, so no more tools can be called. Give your best final answer now, "
                "using only the evidence already gathered, and say clearly what is missing or uncertain.")


def _assistant_message(msg: dict) -> dict:
    """Keep exactly what DeepSeek needs back: content, reasoning_content and tool_calls."""
    out = {"role": "assistant", "content": msg.get("content") or ""}
    if msg.get("reasoning_content"):
        out["reasoning_content"] = msg["reasoning_content"]
    if msg.get("tool_calls"):
        out["tool_calls"] = msg["tool_calls"]
    return out


def _json_or_text(text: str | None) -> object:
    try:
        return json.loads(text or "{}")
    except ValueError:
        return text


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit] + f"\n...(truncated {len(text) - limit} chars)"


def run_agent(*, system: str, user: str, tools: list[Tool], llm, guard: Guardrails = Guardrails(),
              trace: Callable[..., None] | None = None) -> AgentResult:
    trace = trace or (lambda *a, **k: None)
    registry = {t.name: t for t in tools}
    specs = [t.spec() for t in tools]
    messages: list[dict] = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    result = AgentResult(answer="", stopped_by="", steps=0, run_id=getattr(trace, "run_id", None))
    seen: Counter = Counter()
    blocked = 0
    started = time.monotonic()
    trace("start", 0, None, {"user": user, "tools": list(registry), "guardrails": guard.__dict__})

    def tokens() -> int:
        return result.usage["prompt_tokens"] + result.usage["completion_tokens"]

    def call_llm(tool_choice: str | None = None) -> dict:
        reply = llm.chat(messages, tools=specs, tool_choice=tool_choice)
        result.steps += 1
        for k in ("prompt_tokens", "completion_tokens"):
            result.usage[k] += int(reply.get("usage", {}).get(k, 0) or 0)
        msg = reply["message"]
        if msg.get("reasoning_content"):
            result.reasoning.append(msg["reasoning_content"])
        messages.append(_assistant_message(msg))
        trace("llm", result.steps, None, {"content": msg.get("content"), "reasoning": msg.get("reasoning_content"),
                                          "tool_calls": msg.get("tool_calls"), "usage": reply.get("usage")})
        return msg

    def finish(answer: str, stopped_by: str) -> AgentResult:
        result.answer, result.stopped_by = answer, stopped_by
        result.seconds = round(time.monotonic() - started, 2)
        trace("final", result.steps, None, {"answer": answer, "stopped_by": stopped_by, "usage": result.usage,
                                            "seconds": result.seconds})
        return result

    def forced_final(reason: str) -> AgentResult:
        trace("guardrail", result.steps, None, {"reason": reason})
        messages.append({"role": "user", "content": FINAL_PROMPT.format(reason=reason)})
        try:
            msg = call_llm(tool_choice="none")
            return finish(msg.get("content") or "(no final answer)", reason)
        except LLMError as exc:
            return finish(f"(stopped by guardrail '{reason}'; final answer failed: {exc})", reason)

    def execute(call: dict) -> tuple[str, object]:
        """Run one tool call -> (text sent to the model, structured output for the run log)."""
        nonlocal blocked

        def both(message: str) -> tuple[str, str]:
            return message, message
        name = call["function"]["name"]
        try:
            args = json.loads(call["function"].get("arguments") or "{}")
            if not isinstance(args, dict):
                raise ValueError("arguments must be a JSON object")
        except ValueError as exc:
            result.tool_calls.append({"name": name, "args": None, "ok": False, "blocked": False})
            return both(f"ERROR: arguments are not valid JSON ({exc}). Retry with a JSON object.")
        record = {"name": name, "args": args, "ok": False, "blocked": False}
        result.tool_calls.append(record)
        signature = name + json.dumps(args, sort_keys=True, default=str)
        if name not in registry:
            return both(f"ERROR: unknown tool '{name}'. Available: {sorted(registry)}")
        if seen[signature] >= guard.max_repeats:
            blocked += 1
            record["blocked"] = True
            return both("BLOCKED by guardrail: this exact call was already made in this run. Reuse its result, "
                        "change the arguments, or give the final answer.")
        if sum(1 for c in result.tool_calls if not c["blocked"]) > guard.max_tool_calls:
            blocked += 1
            record["blocked"] = True
            return both("BLOCKED by guardrail: tool-call budget exhausted. Give the final answer now.")
        seen[signature] += 1
        try:
            output = registry[name].fn(**args)
            record["ok"] = True
        except Exception as exc:  # noqa: BLE001 - a failing tool is evidence, not a crash
            output = f"ERROR: {type(exc).__name__}: {scrub(str(exc), max_len=500)}"
        text = output if isinstance(output, str) else json.dumps(output, default=str)
        return _truncate(text, guard.max_tool_output_chars), output

    while True:
        if result.steps >= guard.max_steps:
            return forced_final("max_steps")
        if time.monotonic() - started > guard.max_seconds:
            return forced_final("max_seconds")
        if tokens() > guard.max_total_tokens:
            return forced_final("max_tokens")
        try:
            msg = call_llm()
        except LLMError as exc:
            return finish(f"(agent stopped: LLM error: {exc})", "llm_error")
        calls = msg.get("tool_calls") or []
        if not calls:
            return finish(msg.get("content") or "", "final")
        for call in calls:
            output, raw = execute(call)
            result.tool_outputs.append(output)
            # Log the structured output (e.g. the retrieved chunks with doc_id/section/score), not the text, so
            # the run log keeps its shape after scrubbing. Plain-text outputs (errors, blocks) are cut at 4000.
            trace("tool", result.steps, call["function"]["name"],
                  {"arguments": _json_or_text(call["function"].get("arguments")),
                   "output": raw[:4000] if isinstance(raw, str) else raw, "output_chars": len(output)})
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": output})
        if blocked >= guard.max_blocked:
            return forced_final("blocked_calls")

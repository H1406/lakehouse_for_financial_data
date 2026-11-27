import json

from agents.core.llm import LLMError
from agents.core.loop import Guardrails, run_agent
from agents.core.tools import Tool, params


class FakeLLM:
    """Replays scripted assistant messages and records every request it receives."""

    def __init__(self, script):
        self.script = list(script)
        self.requests = []

    def chat(self, messages, tools=None, tool_choice=None):
        self.requests.append({"messages": [dict(m) for m in messages], "tool_choice": tool_choice})
        msg = self.script.pop(0) if self.script else {"content": "fallback final"}
        if isinstance(msg, Exception):
            raise msg
        return {"message": msg, "usage": {"prompt_tokens": 10, "completion_tokens": 5}}


def call(name, args, cid="c1"):
    return {"id": cid, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


def echo_tool(counter):
    def fn(text):
        counter.append(text)
        return {"echo": text}
    return Tool("echo", "echo back", params(text={"type": "string"}, required=["text"]), fn)


def test_tool_then_final_answer_and_reasoning_is_sent_back():
    seen = []
    llm = FakeLLM([
        {"content": "", "reasoning_content": "I should look this up", "tool_calls": [call("echo", {"text": "hi"})]},
        {"content": "The answer is hi [X-1]", "reasoning_content": "done"},
    ])
    r = run_agent(system="s", user="u", tools=[echo_tool(seen)], llm=llm)
    assert r.stopped_by == "final" and r.answer.startswith("The answer is hi")
    assert seen == ["hi"] and r.steps == 2 and r.reasoning == ["I should look this up", "done"]
    # DeepSeek rule: the earlier assistant turn (with reasoning_content and tool_calls) is in the 2nd request
    second = llm.requests[1]["messages"]
    assert second[2]["reasoning_content"] == "I should look this up" and second[2]["tool_calls"]
    assert second[3] == {"role": "tool", "tool_call_id": "c1", "content": json.dumps({"echo": "hi"})}


def test_identical_repeated_calls_are_blocked_and_the_loop_stops():
    seen = []
    looping = [{"content": "", "tool_calls": [call("echo", {"text": "same"}, f"c{i}")]} for i in range(20)]
    llm = FakeLLM([*looping])
    r = run_agent(system="s", user="u", tools=[echo_tool(seen)], llm=llm, guard=Guardrails(max_blocked=3))
    assert seen == ["same"]                       # executed once, every repeat was blocked
    assert r.stopped_by == "blocked_calls"
    assert sum(c["blocked"] for c in r.tool_calls) == 3
    assert llm.requests[-1]["tool_choice"] == "none"  # forced final answer with tools disabled


def test_step_cap_stops_a_model_that_never_finishes():
    seen = []
    llm = FakeLLM([{"content": "", "tool_calls": [call("echo", {"text": str(i)}, f"c{i}")]} for i in range(50)])
    r = run_agent(system="s", user="u", tools=[echo_tool(seen)], llm=llm, guard=Guardrails(max_steps=4))
    assert r.stopped_by == "max_steps" and len(seen) == 4 and r.steps == 5  # 4 loop steps + forced final


def test_tool_errors_and_bad_arguments_become_observations():
    def boom():
        raise RuntimeError("db down password=hunter2")
    llm = FakeLLM([
        {"content": "", "tool_calls": [call("boom", {}), {"id": "c2", "type": "function",
                                                          "function": {"name": "echo", "arguments": "{oops"}}]},
        {"content": "final"},
    ])
    r = run_agent(system="s", user="u", tools=[Tool("boom", "fails", params(), boom), echo_tool([])], llm=llm)
    assert r.stopped_by == "final"
    assert r.tool_outputs[0].startswith("ERROR: RuntimeError") and "hunter2" not in r.tool_outputs[0]
    assert "not valid JSON" in r.tool_outputs[1]


def test_unknown_tool_and_llm_error():
    llm = FakeLLM([{"content": "", "tool_calls": [call("nope", {})]}, LLMError("HTTP 400")])
    r = run_agent(system="s", user="u", tools=[echo_tool([])], llm=llm)
    assert "unknown tool" in r.tool_outputs[0] and r.stopped_by == "llm_error"


def test_tool_output_is_truncated():
    big = Tool("big", "big", params(), lambda: "x" * 10_000)
    llm = FakeLLM([{"content": "", "tool_calls": [call("big", {})]}, {"content": "ok"}])
    r = run_agent(system="s", user="u", tools=[big], llm=llm, guard=Guardrails(max_tool_output_chars=100))
    assert len(r.tool_outputs[0]) < 200 and "truncated" in r.tool_outputs[0]


def test_tool_event_logs_structured_output_for_the_run_log():
    events = []

    def trace(event, step=None, tool_name=None, payload=None):
        events.append((event, tool_name, payload))

    llm = FakeLLM([{"content": "", "tool_calls": [call("echo", {"text": "hi"})]}, {"content": "done"}])
    run_agent(system="s", user="u", tools=[echo_tool([])], llm=llm, trace=trace)
    (_, name, payload), = [e for e in events if e[0] == "tool"]
    assert name == "echo"
    assert payload["arguments"] == {"text": "hi"}          # parsed, not a JSON string
    assert payload["output"] == {"echo": "hi"}             # the tool's own structure, not text
    assert payload["output_chars"] == len('{"echo": "hi"}')

"""A tool = name + JSON-schema parameters + a plain Python function. Every agent tool is read-only."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    parameters: dict  # JSON schema of the keyword arguments
    fn: Callable[..., Any]

    def spec(self) -> dict:
        return {"type": "function",
                "function": {"name": self.name, "description": self.description, "parameters": self.parameters}}


def params(required: list[str] | None = None, **props: dict) -> dict:
    """Small helper: params(query={"type": "string", "description": ...}, required=["query"])."""
    return {"type": "object", "properties": props, "required": required or []}

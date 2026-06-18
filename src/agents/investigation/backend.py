"""Backend protocol — produces an event stream for the orchestrator.

A backend wraps a concrete agent (PER, default, etc.) and translates
its execution into normalized :class:`InvestigationEvent` instances.
Memory writes and response assembly are *not* the backend's concern —
they belong to :class:`~agents.investigation.investigation_agent.InvestigationAgent`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from agents.investigation.events import InvestigationEvent


@dataclass(frozen=True)
class InvestigationContext:
    """Per-run inputs threaded through to the backend.

    Mirrors the request payload sent by the OSD frontend so a backend
    can construct prompts identically to the existing Java agent.
    Optional fields are passed through unchanged when the frontend
    omits them.
    """

    question: str
    context: str = ""
    initial_goal: str | None = None
    prev_content: bool = False
    time_range: dict | None = None
    extra: dict = field(default_factory=dict)


@runtime_checkable
class InvestigationBackend(Protocol):
    """Async-iterable backend contract.

    Implementations MUST yield exactly one :class:`FinalEvent` as the
    terminal item on the success path. They MAY yield zero or more
    :class:`TraceEvent` instances before it. On failure the backend
    raises — the orchestrator's outer ``try/except`` writes the error
    terminal message so frontend polling can stop.
    """

    def run(self, ctx: InvestigationContext) -> AsyncIterator[InvestigationEvent]: ...

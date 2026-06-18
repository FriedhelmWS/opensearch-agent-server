"""Normalized event stream produced by an :class:`InvestigationBackend`.

The orchestrator consumes ``TraceEvent`` and ``FinalEvent`` instances
without caring which backend produced them. PER backends emit one
``TraceEvent`` per plan/execute/reflect phase; future backends (e.g. a
default-agent backend) emit one ``TraceEvent`` per tool call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from agents.investigation.response_schema import PERAgentInvestigationResponse

TraceOrigin = Literal["plan", "execute", "reflect", "tool"]


@dataclass(frozen=True)
class TraceEvent:
    """A single intermediate step the agent took.

    ``origin`` distinguishes phase-shaped backends (PER) from
    tool-shaped ones (default-agent). The orchestrator passes it
    straight through to ``structured_data_blob.origin`` so the frontend
    can group trace messages by phase.
    """

    origin: TraceOrigin
    input: str
    response: str


@dataclass(frozen=True)
class FinalEvent:
    """Investigation complete — carries the structured response.

    The orchestrator serializes ``response`` into the terminal message's
    ``structured_data_blob.response`` field. Backends that cannot produce
    a structured response natively (e.g. a future default-agent backend)
    are responsible for shaping their output into this schema before
    emitting the event.
    """

    response: PERAgentInvestigationResponse


InvestigationEvent = TraceEvent | FinalEvent

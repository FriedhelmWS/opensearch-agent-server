"""Normalized event stream produced by an :class:`InvestigationBackend`.

The orchestrator consumes ``TraceEvent`` and ``FinalEvent`` instances
without caring which backend produced them. PER backends emit
``TraceEvent`` for each step plus one per inner LLM call / tool call
within the step; future backends (e.g. a default-agent backend) emit
one ``TraceEvent`` per tool call.
"""

from __future__ import annotations

from dataclasses import dataclass

from agents.investigation.response_schema import PERAgentInvestigationResponse


@dataclass(frozen=True)
class TraceEvent:
    """A single intermediate event the agent emitted.

    ``origin`` is free-form — it doubles as the
    ``structured_data_blob.origin`` value the frontend reads. Common
    values:
      - ``"execute"`` — a top-level step the user sees in the step list
      - ``"LLM"``     — a sub-agent's LLM call inside a step
      - ``"<tool>"``  — a tool invocation inside a step (e.g.
        ``"SearchIndexTool"``); matches the ml-commons
        ``saveTraceData(origin=lastAction)`` convention so imported
        history renders identically.

    ``step_index`` is non-zero for events that should attach to a
    particular execute step. Inner trace events (LLM / tool) carry
    their parent step's index so the orchestrator can route them
    under the correct step's message_id. ``0`` means "no step parent
    — root-level event"; the orchestrator is free to ignore those.
    """

    origin: str
    input: str
    response: str
    step_index: int = 0


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

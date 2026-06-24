"""``PERBackend`` — adapts :func:`agents.per.per_agent.run_per_pipeline_core`
to the :class:`~agents.investigation.backend.InvestigationBackend` protocol.

The PER pipeline reports progress through an ``on_phase`` async callback
fired after each plan / execute / reflect call. This backend pumps those
phase events into an asyncio queue so the orchestrator can consume them
as a normalized :class:`~agents.investigation.events.InvestigationEvent`
stream while the pipeline is still running.

Terminal handling: the PER pipeline returns a free-text markdown
report. The OSD frontend renders findings/hypotheses/topologies, so a
plain markdown report would render as "No hypotheses". We detect a
JSON payload that already conforms to the structured shape (which is
where this is going once the reflect prompt is migrated), and fall
back to packaging the markdown as a single hypothesis so the frontend
has something to display. Producing rich structured output is the
follow-up — see :class:`PERAgentInvestigationResponse`.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from typing import Any, Final

from pydantic import ValidationError

from agents.investigation.backend import InvestigationContext
from agents.investigation.events import FinalEvent, InvestigationEvent, TraceEvent
from agents.investigation.prompts import build_reflect_overlay
from agents.investigation.response_schema import (
    PERAgentHypothesisItem,
    PERAgentInvestigationResponse,
)
from agents.per.per_agent import PhaseEvent, run_per_pipeline_core
from utils.logging_helpers import get_logger, log_info_event

logger = get_logger(__name__)

# Phases the backend forwards as TraceEvents.
#   ``execute_start`` — step dispatched; written as a step row with
#       empty ``response`` so the OSD step list renders a spinner.
#   ``execute``       — step finished; the backend re-uses the same
#       step row and updates ``response`` to the final result.
#   ``execute_inner`` — LLM / tool inside the step; written as a
#       trace row parented to the step's message_id.
# ``plan`` / ``reflect`` are intentionally NOT forwarded — they are
# planner-internal reasoning that ml-commons also doesn't persist.
# ``final`` is converted into a FinalEvent separately.
_FORWARDED_PHASES: Final[set[str]] = {
    "execute_start",
    "execute",
    "execute_inner",
}

# Sentinel pushed onto the queue by the producer task once the pipeline
# returns (or raises). The consumer drains until it sees this so no
# trace events are lost between the last ``on_phase`` and pipeline exit.
_DONE: Final = object()


class PERBackend:
    """Drive the PER pipeline and yield :class:`InvestigationEvent`s.

    The pipeline runs as a background task. ``on_phase`` callbacks
    enqueue ``PhaseEvent`` objects onto an unbounded ``asyncio.Queue``;
    :meth:`run` dequeues them, translates to ``TraceEvent`` /
    ``FinalEvent``, and yields. Consumer back-pressure is intentionally
    absent — phase events arrive seconds apart at most, and dropping
    one would break the OSD frontend's strictly-ordered trace render.
    """

    def __init__(self) -> None:
        # Stateless — sub-agents and MCP client are pulled from the
        # ``agents.per.sub_agents`` module-level slots that
        # ``create_per_agent`` populates at startup.
        pass

    async def run(
        self, ctx: InvestigationContext
    ) -> AsyncIterator[InvestigationEvent]:
        queue: asyncio.Queue[PhaseEvent | object] = asyncio.Queue()

        async def _on_phase(event: PhaseEvent) -> None:
            await queue.put(event)

        problem = self._build_problem_statement(ctx)
        # Overlay tells the reflect sub-agent that, when it finalizes,
        # the result field must be a JSON-stringified
        # PERAgentInvestigationResponse — not free-form markdown.
        # Generic PER stays generic; the investigation specialization
        # is purely additive prompt content.
        reflect_overlay = build_reflect_overlay(ctx)

        async def _producer() -> str:
            try:
                return await run_per_pipeline_core(
                    problem,
                    on_phase=_on_phase,
                    extra_reflect_system_prompt=reflect_overlay,
                )
            finally:
                # Always sentinel — even on exception — so the consumer
                # loop terminates and the orchestrator's outer try/except
                # can write an error terminal message.
                await queue.put(_DONE)

        task = asyncio.create_task(_producer())

        try:
            while True:
                item = await queue.get()
                if item is _DONE:
                    break
                assert isinstance(item, PhaseEvent)
                if item.phase == "final":
                    # The pipeline is about to return; ignore here and
                    # yield the ``FinalEvent`` after ``_DONE`` so we use
                    # the actual return value (which carries any error
                    # surfacing the pipeline already wrapped into text).
                    continue
                if item.phase not in _FORWARDED_PHASES:
                    # plan / reflect / unknown — drop. Planner reasoning
                    # isn't persisted (matches ml-commons).
                    continue
                if item.phase == "execute_start":
                    # Step row written with response="" — OSD shows a
                    # spinner because last message has empty response.
                    yield TraceEvent(
                        origin="execute_start",
                        input=item.input,
                        response="",
                        step_index=item.step_index,
                    )
                elif item.phase == "execute":
                    # Step finished — update the existing step row's
                    # response so OSD flips the spinner to a green
                    # checkmark.
                    yield TraceEvent(
                        origin="execute",
                        input=item.input,
                        response=item.response,
                        step_index=item.step_index,
                    )
                elif item.phase == "execute_inner":
                    # Carries inner_origin like "LLM" or "tool:get_index".
                    yield TraceEvent(
                        origin=item.inner_origin or "LLM",
                        input=item.input,
                        response=item.response,
                        step_index=item.step_index,
                    )

            # Re-raise pipeline errors so the orchestrator can write an
            # error terminal message. ``task.result()`` is the canonical
            # way to surface the producer's exception.
            final_text = task.result()
            yield FinalEvent(response=_to_response(final_text))
        finally:
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    @staticmethod
    def _build_problem_statement(ctx: InvestigationContext) -> str:
        """Compose the PER pipeline's input string from the context.

        The PER pipeline takes a single string. The frontend sends
        question + context separately; we glue them with a clear
        delimiter so the planner can distinguish symptom from setting.
        Optional fields (``initial_goal``, ``time_range``) are appended
        only when present so the prompt stays compact otherwise.
        """
        parts: list[str] = [ctx.question.strip()]
        if ctx.context:
            parts.append(f"Context:\n{ctx.context.strip()}")
        if ctx.initial_goal:
            parts.append(f"Original goal:\n{ctx.initial_goal.strip()}")
        if ctx.time_range:
            parts.append(f"Time range:\n{ctx.time_range}")
        return "\n\n".join(parts)


_FENCED_JSON_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def _to_response(final_text: str) -> PERAgentInvestigationResponse:
    """Convert the PER pipeline's final string into the frontend shape.

    Two paths:

    1. ``final_text`` already contains a JSON object that matches
       :class:`PERAgentInvestigationResponse` (or wraps one in a
       ```json fence). Parse it.
    2. Otherwise treat it as a markdown RCA report and package it as
       a single hypothesis so the frontend has something to render
       instead of "No hypotheses".

    In both paths the response is finally sanitized — the OSD
    validator rejects ``null`` numbers, so any model that emitted
    ``likelihood: null`` / ``importance: null`` would otherwise trip
    "Invalid per agent response".
    """
    parsed = _try_parse_structured(final_text)
    if parsed is not None:
        log_info_event(
            logger,
            "[per_backend] structured response parsed from PER output",
            "investigation.per_backend.structured_response",
            findings=len(parsed.findings),
            hypotheses=len(parsed.hypotheses),
        )
        return _sanitize_for_osd(parsed)

    # Markdown fallback — wrap the report so the UI shows something.
    name = _first_heading(final_text) or "Investigation report"
    log_info_event(
        logger,
        "[per_backend] PER returned free-text — wrapping as single hypothesis",
        "investigation.per_backend.markdown_fallback",
        chars=len(final_text),
    )
    return _sanitize_for_osd(
        PERAgentInvestigationResponse(
            findings=[],
            hypotheses=[
                PERAgentHypothesisItem(
                    id="H1",
                    title=name,
                    description=final_text,
                    likelihood=None,
                    supporting_findings=[],
                )
            ],
            topologies=[],
            investigationName=name,
        )
    )


def _sanitize_for_osd(
    response: PERAgentInvestigationResponse,
) -> PERAgentInvestigationResponse:
    """Coerce nullable strict fields so OSD validates the response.

    The OSD frontend's ``isValidPERAgentHypothesisItem`` /
    ``isValidPERAgentHypothesisFinding`` validators plus the
    ``/api/investigation/note/updateHypotheses`` route schema require:
        hypothesis.likelihood   : number
        finding.importance      : number
        finding.evidence        : string
        topology.description    : string
        topology.traceId        : string
        topology.nodes[].name        : string
        topology.nodes[].startTime   : string
        topology.nodes[].duration    : string
        topology.nodes[].status      : string
        topology.nodes[].parentId    : string | null

    Models sometimes emit ``null`` for these strict fields. Rather
    than crash the response (which lands the user on "Invalid per
    agent response" / a 400 on updateHypotheses), we default the
    numbers to 0 and the strings to empty.
    """
    for h in response.hypotheses:
        if h.likelihood is None:
            h.likelihood = 0
    for f in response.findings:
        if f.importance is None:
            f.importance = 0
        if f.evidence is None:
            f.evidence = ""
    for t in response.topologies:
        if t.description is None:
            t.description = ""
        if t.traceId is None:
            t.traceId = ""
        for n in t.nodes:
            if n.name is None:
                n.name = ""
            if n.startTime is None:
                n.startTime = ""
            if n.duration is None:
                n.duration = ""
            if n.status is None:
                n.status = ""
            # parentId stays None — OSD schema accepts null for root.
    return response


def _try_parse_structured(text: str) -> PERAgentInvestigationResponse | None:
    """Best-effort: parse ``text`` as PERAgentInvestigationResponse.

    Tolerates ```json fences that the LLM sometimes wraps JSON in.
    Returns ``None`` if the text is not parseable JSON or doesn't have
    at least one of the structured arrays — that's the cue to fall
    back to markdown packaging.
    """
    candidates: list[str] = [text]
    fenced = _FENCED_JSON_RE.search(text)
    if fenced is not None:
        candidates.insert(0, fenced.group(1))

    for candidate in candidates:
        try:
            payload: Any = json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        if not any(k in payload for k in ("findings", "hypotheses", "topologies")):
            continue
        try:
            return PERAgentInvestigationResponse.model_validate(payload)
        except ValidationError:
            continue
    return None


def _first_heading(text: str) -> str | None:
    """Pull the first markdown heading line for use as investigationName."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip() or None
    return None



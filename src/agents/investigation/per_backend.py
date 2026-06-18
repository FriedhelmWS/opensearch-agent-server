"""``PERBackend`` — adapts :func:`agents.per.per_agent.run_per_pipeline_core`
to the :class:`~agents.investigation.backend.InvestigationBackend` protocol.

The PER pipeline reports progress through an ``on_phase`` async callback
fired after each plan / execute / reflect call. This backend pumps those
phase events into an asyncio queue so the orchestrator can consume them
as a normalized :class:`~agents.investigation.events.InvestigationEvent`
stream while the pipeline is still running.

The pipeline's terminal output is a free-text report (mirrors the Java
agent). To satisfy the OSD frontend's
:class:`~agents.investigation.response_schema.PERAgentInvestigationResponse`
contract, the backend wraps that text into a single
``investigationName``-only response by default. Producing rich findings /
hypotheses / topologies is a downstream concern (handled by an upcoming
structured-output reflect step that lands separately).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Final

from agents.investigation.backend import InvestigationContext
from agents.investigation.events import FinalEvent, InvestigationEvent, TraceEvent
from agents.investigation.response_schema import PERAgentInvestigationResponse
from agents.per.per_agent import PhaseEvent, run_per_pipeline_core
from utils.logging_helpers import get_logger, log_warning_event

logger = get_logger(__name__)

# Phase names that map 1:1 to ``TraceOrigin`` values. ``"final"`` is
# excluded — the backend emits a ``FinalEvent`` for that case instead.
_TRACE_PHASES: Final[set[str]] = {"plan", "execute", "reflect"}

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

        async def _producer() -> str:
            try:
                return await run_per_pipeline_core(problem, on_phase=_on_phase)
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
                if item.phase in _TRACE_PHASES:
                    yield TraceEvent(
                        origin=item.phase,  # type: ignore[arg-type]
                        input=item.input,
                        response=item.response,
                    )
                else:  # pragma: no cover — defensive, unknown phase
                    log_warning_event(
                        logger,
                        f"[per_backend] unknown phase {item.phase!r} — dropping",
                        "investigation.per_backend.unknown_phase",
                        phase=item.phase,
                    )

            # Re-raise pipeline errors so the orchestrator can write an
            # error terminal message. ``task.result()`` is the canonical
            # way to surface the producer's exception.
            final_text = task.result()
            yield FinalEvent(
                response=PERAgentInvestigationResponse(
                    findings=[], hypotheses=[], topologies=[],
                    investigationName=final_text,
                )
            )
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

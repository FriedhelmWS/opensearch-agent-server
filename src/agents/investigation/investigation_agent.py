"""``InvestigationAgent`` — backend-agnostic orchestrator.

Consumes :class:`~agents.investigation.events.InvestigationEvent` from a
backend and writes ml-commons-shaped messages to a
:class:`~storage.memory_store.MemoryStore`. Two terminal-message
guarantees:

1. **Success path**: when the backend yields :class:`FinalEvent`, write
   one terminal message whose ``structured_data_blob.response`` is the
   :class:`PERAgentInvestigationResponse` JSON.
2. **Error path**: any exception (backend-raised or while writing
   memory) is caught in the outer ``try/except`` and a terminal message
   carrying a non-empty error response is written, so the OSD frontend's
   polling loop can stop instead of timing out.

Trace messages are written one-per-event in arrival order. The backend
is responsible for serializing parallel work before emitting events
(:class:`~agents.investigation.per_backend.PERBackend` does this).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from agents.investigation.backend import InvestigationBackend, InvestigationContext
from agents.investigation.events import FinalEvent, TraceEvent
from agents.investigation.response_schema import PERAgentInvestigationResponse
from storage.memory_store import MemoryStore
from storage.schemas import Message, MessageMetadata, Namespace, StructuredDataBlob
from utils.logging_helpers import (
    get_logger,
    log_error_event,
    log_info_event,
)

logger = get_logger(__name__)

# Phase values written to the phase row. The OSD frontend maps these
# directly onto its ``InvestigationPhase`` enum so the live phase
# indicator reflects what PER is actually doing.
PHASE_PLANNING = "planning"
PHASE_GATHERING_DATA = "gathering_data"
PHASE_COMPLETED = "completed"


def _phase_message_id(parent_message_id: str) -> str:
    """Stable, predictable id the frontend can poll without an extra search.

    Derived purely from ``parent_message_id`` so the frontend has the
    id available the moment trigger returns — no listing step needed.
    """
    return f"{parent_message_id}_phase"


@dataclass(frozen=True)
class InvestigationHandle:
    """Identifiers needed to point the OSD frontend at this run.

    Mirrors the ml-commons two-memory model:

    * ``parent_session_id`` is the *planner* memory. Only the terminal
      message (parent interaction) lives here. The frontend's
      ``getFinalMessage`` looks it up by ``_id`` so it doesn't need to
      know this session id.
    * ``session_id`` (a.k.a. ``executor_memory_id`` in the trigger
      response) is the *executor* memory. Step rows + inner LLM/tool
      traces live here. The OSD step list polls this session, so
      keeping the terminal out of it prevents the terminal from
      rendering as a fake last step in the UI.

    ``parent_session_id`` defaults to ``session_id`` for backwards
    compatibility — callers who don't allocate a separate planner
    session still get a working investigation, just with the legacy
    "terminal shows up as a step" quirk.
    """

    container_id: str
    session_id: str
    parent_message_id: str
    parent_session_id: str | None = None

    @property
    def planner_session_id(self) -> str:
        return self.parent_session_id or self.session_id


class InvestigationAgent:
    """Run an investigation against a pluggable backend.

    Parameters
    ----------
    backend:
        Any object satisfying :class:`InvestigationBackend`. The
        orchestrator does not branch on backend type — adding a new
        backend (e.g. a default-agent backend) requires zero changes
        here.
    memory_store:
        Where trace + terminal messages are persisted. The store must
        preserve insertion order; see ``SqliteMemoryStore`` for the
        reference implementation.
    handle:
        Pre-allocated container / session / root-message ids. Build
        these at trigger time so the synchronous response can return
        them while the investigation runs in the background.
    user_id:
        Optional ``namespace.user_id`` recorded on every message.
    """

    def __init__(
        self,
        *,
        backend: InvestigationBackend,
        memory_store: MemoryStore,
        handle: InvestigationHandle,
        user_id: str | None = None,
    ) -> None:
        self._backend = backend
        self._mem = memory_store
        self._handle = handle
        self._user_id = user_id
        # Maps PER ``step_index`` → that step's stored message_id.
        # Inner trace events (LLM / tool calls inside a step) arrive
        # tagged with their step_index; we look up the corresponding
        # step message_id and write the trace's
        # ``metadata.parent_message_id`` to it. This is what makes the
        # OSD "Explain this step" flyout return the inner traces:
        # the flyout queries ``metadata.parent_message_id == <step
        # msg_id>`` against the executor session.
        self._step_message_ids: dict[int, str] = {}
        # Inner trace events can arrive BEFORE their parent step is
        # written: PER's executor sub-agent fires Strands hooks
        # mid-flight (each LLM call / tool call), but the step itself
        # is only emitted after the whole step's gather() resolves.
        # We buffer pending inner traces per step_index and flush them
        # the moment the step's message_id is established. Within a
        # step, traces flush in the order received (= chronological).
        self._pending_inner: dict[int, list[tuple[TraceEvent, int]]] = {}
        # Tracks current high-level investigation phase ("planning",
        # "gathering_data", "completed"). Written to a dedicated phase
        # row in the planner session so the OSD frontend can poll a
        # single id and reflect the real PER state instead of staying
        # stuck on "Planning for your investigation...". See
        # ``_emit_phase`` for the persistence path.
        self._current_phase: str | None = None

    async def run(self, ctx: InvestigationContext) -> PERAgentInvestigationResponse:
        """Drive the backend and persist its event stream.

        Returns the structured response on success (also written to
        memory). On failure, persists an error terminal message and
        re-raises so the trigger route can record the run as failed
        — but the frontend always sees a terminal message either way.
        """
        log_info_event(
            logger,
            "[investigation] run start",
            "investigation.run.start",
            container_id=self._handle.container_id,
            session_id=self._handle.session_id,
            parent_message_id=self._handle.parent_message_id,
        )

        # Start the run in PLANNING — the planner sub-agent is the
        # first thing PER does and takes 5-15s before any execute
        # event lands. Without this the frontend would stay on the
        # generic "Planning for your investigation..." with no real
        # signal that anything is happening.
        self._emit_phase(ctx.question, PHASE_PLANNING)

        trace_number = 0
        final_response: PERAgentInvestigationResponse | None = None
        try:
            async for event in self._backend.run(ctx):
                if isinstance(event, TraceEvent):
                    trace_number += 1
                    self._write_trace(event, trace_number)
                    # First step dispatched → executor is now running.
                    # Subsequent execute_start events keep this state.
                    if (
                        event.origin == "execute_start"
                        and self._current_phase != PHASE_GATHERING_DATA
                    ):
                        self._emit_phase(ctx.question, PHASE_GATHERING_DATA)
                elif isinstance(event, FinalEvent):
                    # Drain any inner traces still pending — their step
                    # never emitted (e.g. step failed mid-flight). They
                    # fall back to the investigation root parent so
                    # they're at least retrievable.
                    trace_number = self._flush_orphan_inner_traces(trace_number)
                    final_response = event.response
                    self._write_terminal(ctx.question, event.response)
                    self._emit_phase(ctx.question, PHASE_COMPLETED)
                else:  # pragma: no cover — defensive
                    log_error_event(
                        logger,
                        f"[investigation] unknown event type {type(event).__name__}",
                        "investigation.run.unknown_event",
                    )
        except Exception as exc:
            log_error_event(
                logger,
                "[investigation] backend raised — writing error terminal",
                "investigation.run.backend_error",
                error=exc,
            )
            self._write_error_terminal(ctx.question, exc)
            self._emit_phase(ctx.question, PHASE_COMPLETED)
            raise

        if final_response is None:
            # Backend exited cleanly without emitting a FinalEvent —
            # treat as a contract violation. Surface a terminal
            # message so polling stops, then raise.
            err = RuntimeError(
                "backend completed without emitting a FinalEvent — "
                "this is a contract violation"
            )
            self._write_error_terminal(ctx.question, err)
            self._emit_phase(ctx.question, PHASE_COMPLETED)
            raise err

        log_info_event(
            logger,
            "[investigation] run complete",
            "investigation.run.complete",
            trace_count=trace_number,
            container_id=self._handle.container_id,
            session_id=self._handle.session_id,
        )
        return final_response

    def _write_trace(self, event: TraceEvent, trace_number: int) -> None:
        # ``execute`` is the step *completion* event — the placeholder
        # row was already written by ``execute_start``. Update it
        # in-place so the OSD spinner flips to "done" without adding
        # a duplicate row.
        if event.origin == "execute":
            self._complete_step(event)
            return

        # Inner trace whose step hasn't been written yet → buffer.
        # Strands hooks fire mid-step (each LLM/tool call) while the
        # step row is only created by ``execute_start``, and queue
        # ordering between the two is not guaranteed.
        is_step_start = event.origin == "execute_start"
        if (
            not is_step_start
            and event.step_index
            and event.step_index not in self._step_message_ids
        ):
            self._pending_inner.setdefault(event.step_index, []).append(
                (event, trace_number)
            )
            return
        self._write_trace_now(event, trace_number)
        # If this was the step's placeholder, drain any inner traces
        # that arrived before it.
        if is_step_start and event.step_index in self._pending_inner:
            for buf_event, buf_trace_num in self._pending_inner.pop(event.step_index):
                self._write_trace_now(buf_event, buf_trace_num)

    def _complete_step(self, event: TraceEvent) -> None:
        """Apply the executor's final result to the existing step row.

        The placeholder was written by ``execute_start``; here we just
        update its ``structured_data_blob.response`` so the OSD
        spinner transitions to "done". Falls back to ``append`` (so
        we don't silently drop the result) only if the placeholder
        somehow never landed.
        """
        from storage.schemas import _now_iso

        step_msg_id = self._step_message_ids.get(event.step_index)
        if step_msg_id is None:
            # No placeholder ever written — emit a regular step row so
            # the result is still visible. Should be rare; logged for
            # diagnosis.
            log_error_event(
                logger,
                "[investigation] execute completion has no matching "
                "execute_start — falling back to append",
                "investigation.execute_complete.orphan",
                step_index=event.step_index,
            )
            # Synthesize a placeholder ID + trace_number to keep
            # downstream ordering consistent.
            self._write_trace_now(
                TraceEvent(
                    origin="execute_start",
                    input=event.input,
                    response="",
                    step_index=event.step_index,
                ),
                trace_number=0,
            )
            step_msg_id = self._step_message_ids[event.step_index]
        self._mem.update_message_response(
            container_id=self._handle.container_id,
            message_id=step_msg_id,
            response=event.response,
            last_updated_time=_now_iso(),
        )

    def _flush_orphan_inner_traces(self, last_trace_number: int) -> int:
        """Flush inner traces whose step never emitted — root-parented.

        Returns the new trace_number high-water mark.
        """
        next_n = last_trace_number + 1
        for step_index in list(self._pending_inner.keys()):
            for buf_event, _ in self._pending_inner.pop(step_index):
                self._write_trace_now(buf_event, next_n)
                next_n += 1
        return next_n - 1

    def _write_trace_now(self, event: TraceEvent, trace_number: int) -> None:
        # Two-tier write to match what the OSD frontend's executor
        # message list expects:
        #   - ``execute`` events become ``type="message"`` so they
        #     show up in ``getAllMessagesBySessionIdAndMemoryId`` (the
        #     "step-by-step" panel — the query has
        #     ``must_not: metadata.type=trace``)
        #   - ``plan`` / ``reflect`` are reasoning around steps; they
        #     stay ``type="trace"`` so the UI shows them only when
        #     the user opens the per-step trace flyout (which queries
        #     by ``metadata.parent_message_id``)
        #
        # ml-commons's PER reaches the same end state via a different
        # path: a separate executor sub-agent writes each step as a
        # ``type=message`` to the executor session, and its internal
        # reasoning is emitted as ``type=trace``. We have a single
        # writer, so we discriminate here by phase.
        #
        # trace_number / origin are written into BOTH metadata and
        # structured_data regardless of type — matching Java's
        # AgenticConversationMemory ~L100. trace_number is string in
        # metadata and int in structured_data, also matching Java.
        # ``execute_start`` is the step *placeholder* row (type=message
        # with empty response — OSD renders a spinner). Anything else
        # (LLM / tool inner events) is an inner trace (type=trace)
        # that must attach to its step so the OSD "Explain this step"
        # flyout finds it. ``execute`` (the completion) never reaches
        # here — see _complete_step.
        is_step = event.origin == "execute_start"
        metadata_type = "message" if is_step else "trace"
        message_id = uuid.uuid4().hex
        if is_step:
            parent_for_lineage = self._handle.parent_message_id
        else:
            # Inner trace: route to its step. Falls back to the
            # investigation root if the producer didn't tag the event
            # with a step_index (defensive — current PER backend
            # always sets it for non-execute origins).
            step_msg_id = self._step_message_ids.get(event.step_index)
            parent_for_lineage = step_msg_id or self._handle.parent_message_id
        # The frontend only sees ``origin="execute"`` for step rows —
        # ``execute_start`` is our internal placeholder marker, not a
        # value any consumer should reason about.
        stored_origin = "execute" if is_step else event.origin
        message = Message(
            memory_container_id=self._handle.container_id,
            message_id=message_id,
            structured_data_blob=StructuredDataBlob(
                input=event.input,
                response=event.response,
                parent_message_id=parent_for_lineage,
                trace_number=trace_number,
                origin=stored_origin,
            ),
            namespace=Namespace(
                session_id=self._handle.session_id, user_id=self._user_id
            ),
            metadata=MessageMetadata(
                type=metadata_type,
                parent_message_id=parent_for_lineage,
                trace_number=str(trace_number),
                origin=stored_origin,
            ),
        )
        self._mem.append_message(message)
        if is_step and event.step_index:
            self._step_message_ids[event.step_index] = message_id

    def _emit_phase(self, question: str, phase: str) -> None:
        """Persist the current investigation phase for the frontend.

        The phase row lives in the planner session at a predictable
        id (``f"{parent_message_id}_phase"``) so the OSD frontend can
        fetch it by id without an extra search. The OSD frontend maps
        the string into its ``InvestigationPhase`` enum and renders
        the live phase indicator.

        First call appends the row; subsequent calls update the
        ``response`` in place (no row churn). Idempotent — repeated
        calls with the same value are skipped to keep store traffic
        bounded.
        """
        from storage.schemas import _now_iso

        if self._current_phase == phase:
            return
        phase_id = _phase_message_id(self._handle.parent_message_id)
        now = _now_iso()
        if self._current_phase is None:
            # First time → append. ``response`` carries the phase
            # value so future updates can use ``update_message_response``.
            phase_msg = Message(
                memory_container_id=self._handle.container_id,
                message_id=phase_id,
                structured_data_blob=StructuredDataBlob(
                    input=question,
                    response=phase,
                    final_answer=phase,
                ),
                namespace=Namespace(
                    session_id=self._handle.planner_session_id,
                    user_id=self._user_id,
                ),
                # type="phase" so the OSD step-list polling (which
                # filters by metadata.type != "trace" on the executor
                # session) doesn't render it as a fake step. The row
                # is in the planner session anyway, so it's a
                # belt-and-suspenders guard.
                metadata=MessageMetadata(type="phase"),
            )
            self._mem.append_message(phase_msg)
        else:
            self._mem.update_message_response(
                container_id=self._handle.container_id,
                message_id=phase_id,
                response=phase,
                last_updated_time=now,
            )
        self._current_phase = phase

    def _write_terminal(
        self, question: str, response: PERAgentInvestigationResponse
    ) -> None:
        message = Message(
            memory_container_id=self._handle.container_id,
            message_id=self._handle.parent_message_id,
            structured_data_blob=StructuredDataBlob(
                input=question,
                response=response.model_dump_json(),
                final_answer=response.investigationName or "Investigation complete.",
            ),
            # Terminal goes to the planner session (separate from the
            # executor session). The OSD step list polls only the
            # executor session, so it doesn't render the terminal as
            # a fake last step. ``getFinalMessage`` looks the terminal
            # up by ``_id`` and is unaffected by which session it's in.
            namespace=Namespace(
                session_id=self._handle.planner_session_id,
                user_id=self._user_id,
            ),
            metadata=MessageMetadata(type="message"),
        )
        self._mem.append_message(message)

    def _write_error_terminal(self, question: str, exc: BaseException) -> None:
        """Write an error terminal so frontend polling can stop.

        ``response`` is a plain error string — NOT a serialized
        :class:`PERAgentInvestigationResponse`. The OSD frontend's
        success path (``handlePollingSuccess``) parses ``response`` as
        JSON and validates it as a hypotheses response; when those
        checks throw it falls through to ``handleInvestigationFailure``,
        which is exactly what we want on backend errors. Mirrors the
        ml-commons PER runner, which simply ``onFailure(e)`` and writes
        no terminal at all — clients there learn of the failure via the
        20-minute polling timeout. We do one better by writing a
        terminal so polling stops immediately.

        ``response`` is intentionally a short, user-friendly sentinel
        ("Investigation failed") rather than the raw exception. The OSD
        error modal echoes ``response`` directly via ``error.cause``,
        and surfacing internal exception text there ("An error occurred
        (ExpiredTokenException) when calling the ConverseStream
        operation...") is noisy and confusing for users. The structured
        exception detail stays on ``metadata.error`` for debugging /
        future structured readers.
        """
        message = Message(
            memory_container_id=self._handle.container_id,
            message_id=self._handle.parent_message_id,
            structured_data_blob=StructuredDataBlob(
                input=question,
                response="Investigation failed",
                final_answer="Investigation failed",
            ),
            # Error terminal goes to the planner session (same as the
            # success terminal) so it doesn't pollute the executor
            # step list with a "step" titled with the user's question.
            namespace=Namespace(
                session_id=self._handle.planner_session_id,
                user_id=self._user_id,
            ),
            metadata=MessageMetadata(
                type="message",
                **{
                    "error": json.dumps(
                        {"type": type(exc).__name__, "message": str(exc)}
                    )
                },
            ),
        )
        self._mem.append_message(message)

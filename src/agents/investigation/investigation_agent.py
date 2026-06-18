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


@dataclass(frozen=True)
class InvestigationHandle:
    """Identifiers needed to point the OSD frontend at this run.

    Mirrors the ml-commons trigger response fields that
    ``extractParentInteractionId`` consumes — the trigger HTTP route
    serializes this into the response body.
    """

    container_id: str
    session_id: str
    parent_message_id: str


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

        trace_number = 0
        final_response: PERAgentInvestigationResponse | None = None
        try:
            async for event in self._backend.run(ctx):
                if isinstance(event, TraceEvent):
                    trace_number += 1
                    self._write_trace(event, trace_number)
                elif isinstance(event, FinalEvent):
                    final_response = event.response
                    self._write_terminal(ctx.question, event.response)
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
        # ml-commons writes trace_number + origin into BOTH metadata
        # and structured_data (Java AgenticConversationMemory.java
        # ~L100). Mirror that so any reader — frontend, importer,
        # downstream tooling — finds the same fields in the same
        # places. trace_number is stringified in metadata, int in
        # structured_data, also matching Java.
        message = Message(
            memory_container_id=self._handle.container_id,
            message_id=uuid.uuid4().hex,
            structured_data_blob=StructuredDataBlob(
                input=event.input,
                response=event.response,
                parent_message_id=self._handle.parent_message_id,
                trace_number=trace_number,
                origin=event.origin,
            ),
            namespace=Namespace(
                session_id=self._handle.session_id, user_id=self._user_id
            ),
            metadata=MessageMetadata(
                type="trace",
                parent_message_id=self._handle.parent_message_id,
                trace_number=str(trace_number),
                origin=event.origin,
            ),
        )
        self._mem.append_message(message)

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
            namespace=Namespace(
                session_id=self._handle.session_id, user_id=self._user_id
            ),
            metadata=MessageMetadata(type="message"),
        )
        self._mem.append_message(message)

    def _write_error_terminal(self, question: str, exc: BaseException) -> None:
        """Write an error terminal so frontend polling can stop.

        ``response`` MUST be non-empty — the OSD polling service uses
        ``lastMessage?.response`` as its completion signal. We encode
        the error as a minimal :class:`PERAgentInvestigationResponse`
        with the message in ``investigationName`` so the frontend can
        at least show something.
        """
        err_text = f"Investigation failed: {exc}"
        err_response = PERAgentInvestigationResponse(
            findings=[], hypotheses=[], topologies=[], investigationName=err_text
        )
        message = Message(
            memory_container_id=self._handle.container_id,
            message_id=self._handle.parent_message_id,
            structured_data_blob=StructuredDataBlob(
                input=question,
                response=err_response.model_dump_json(),
                final_answer=err_text,
            ),
            namespace=Namespace(
                session_id=self._handle.session_id, user_id=self._user_id
            ),
            metadata=MessageMetadata(type="message", **{"error": json.dumps({"message": str(exc)})}),
        )
        self._mem.append_message(message)

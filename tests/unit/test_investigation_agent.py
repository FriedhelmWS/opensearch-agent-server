"""Unit tests for the investigation orchestrator + SQLite memory store.

Covers the contracts the OSD frontend depends on:

1. Trace events → trace messages, ordered, with correct lineage.
2. ``FinalEvent`` → terminal message whose ``response`` decodes back
   to ``PERAgentInvestigationResponse``.
3. Exceptions raised by the backend ⇒ error terminal message with a
   non-empty ``response`` field (so polling can stop).
4. Backend completing without a ``FinalEvent`` is a contract violation
   — orchestrator writes an error terminal and raises.
5. SQLite store filters out trace messages and orders by insertion
   when polled the way the frontend polls.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from agents.investigation.backend import InvestigationContext
from agents.investigation.events import FinalEvent, InvestigationEvent, TraceEvent
from agents.investigation.investigation_agent import (
    InvestigationAgent,
    InvestigationHandle,
)
from agents.investigation.response_schema import PERAgentInvestigationResponse
from storage.schemas import Message
from storage.sqlite_store import SqliteMemoryStore

pytestmark = pytest.mark.unit


class _ScriptedBackend:
    """Yields a pre-set list of events, optionally raising at the end."""

    def __init__(
        self,
        events: list[InvestigationEvent],
        *,
        raise_after: Exception | None = None,
    ) -> None:
        self._events = events
        self._raise_after = raise_after

    async def run(
        self, ctx: InvestigationContext
    ) -> AsyncIterator[InvestigationEvent]:
        for ev in self._events:
            yield ev
        if self._raise_after is not None:
            raise self._raise_after


@pytest.fixture
def store() -> SqliteMemoryStore:
    return SqliteMemoryStore.in_memory()


@pytest.fixture
def handle(store: SqliteMemoryStore) -> InvestigationHandle:
    container = store.create_container(name="test")
    # Mirror the real trigger route: two sessions per investigation.
    # planner session owns the terminal + phase rows; executor
    # session owns step rows + per-step inner traces. Tests that
    # ``search_messages(session_id=handle.session_id)`` therefore
    # see only the executor-session messages — same shape the OSD
    # step-list polling sees.
    planner_session = store.create_session(container_id=container.container_id)
    executor_session = store.create_session(container_id=container.container_id)
    return InvestigationHandle(
        container_id=container.container_id,
        session_id=executor_session.session_id,
        parent_message_id="parent-msg-1",
        parent_session_id=planner_session.session_id,
    )


@pytest.fixture
def ctx() -> InvestigationContext:
    return InvestigationContext(question="why is latency high?")


def _final(name: str = "done") -> FinalEvent:
    return FinalEvent(
        response=PERAgentInvestigationResponse(
            findings=[], hypotheses=[], topologies=[], investigationName=name
        )
    )


class TestSuccessPath:
    async def test_step_lifecycle_start_then_inner_then_done(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        # Real PER emits: execute_start (placeholder, empty response)
        # → execute_inner (LLM / tool) → execute (final response,
        # which updates the placeholder row in place).
        backend = _ScriptedBackend(
            [
                TraceEvent(
                    origin="execute_start",
                    input="step 1",
                    response="",
                    step_index=1,
                ),
                TraceEvent(
                    origin="LLM",
                    input="<llm-call>",
                    response="reasoning over step 1",
                    step_index=1,
                ),
                TraceEvent(
                    origin="SearchIndexTool",
                    input='{"index":"foo"}',
                    response="{...result...}",
                    step_index=1,
                ),
                # Completion — updates the existing placeholder, does
                # not add a new row.
                TraceEvent(
                    origin="execute",
                    input="step 1",
                    response="findings 1",
                    step_index=1,
                ),
                _final("Latency analysis"),
            ]
        )
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle, user_id="alice"
        )

        result = await agent.run(ctx)
        assert result.investigationName == "Latency analysis"

        # Executor session: step + 2 inner traces. The terminal
        # lives in the planner session (verified separately below);
        # this is the same shape the OSD step-list polling sees.
        all_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=False,
        )
        assert [m.metadata.type for m in all_msgs] == [
            "message", "trace", "trace",
        ]

        execute_msg = all_msgs[0]
        llm_trace = all_msgs[1]
        tool_trace = all_msgs[2]

        # Update happened in place: response is the final text, not "".
        assert execute_msg.structured_data_blob.response == "findings 1"
        # Externally the step's origin is "execute" — "execute_start"
        # is an internal placeholder marker.
        assert execute_msg.structured_data_blob.origin == "execute"

        # Step rooted at the investigation root.
        assert execute_msg.metadata.parent_message_id == "parent-msg-1"
        # Inner traces attach to the step's message_id so the OSD
        # "Explain this step" flyout finds them.
        assert llm_trace.metadata.parent_message_id == execute_msg.message_id
        assert tool_trace.metadata.parent_message_id == execute_msg.message_id

        # origin preserved for inner events.
        assert llm_trace.structured_data_blob.origin == "LLM"
        assert tool_trace.structured_data_blob.origin == "SearchIndexTool"

        # Terminal lives in the planner session, fetched by id.
        terminal = store.get_message_by_id(
            container_id=handle.container_id, message_id="parent-msg-1"
        )
        assert terminal is not None
        assert terminal.metadata.type == "message"
        assert terminal.structured_data_blob.trace_number is None

        for m in (execute_msg, llm_trace, tool_trace, terminal):
            assert m.namespace.user_id == "alice"

    async def test_inner_traces_arriving_before_step_are_buffered(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        # Real PER fires Strands hooks while the step is still
        # running, so inner LLM/tool traces can race ahead of the
        # step's ``execute_start`` placeholder write. Verify we
        # buffer them and parent them to the step once it arrives.
        backend = _ScriptedBackend(
            [
                # Inner traces arrive first…
                TraceEvent(
                    origin="LLM",
                    input="<llm-call>",
                    response="thinking…",
                    step_index=1,
                ),
                TraceEvent(
                    origin="SearchIndexTool",
                    input='{"index":"foo"}',
                    response="...",
                    step_index=1,
                ),
                # …then the step placeholder…
                TraceEvent(
                    origin="execute_start",
                    input="step 1",
                    response="",
                    step_index=1,
                ),
                # …then the step completion.
                TraceEvent(
                    origin="execute",
                    input="step 1",
                    response="findings 1",
                    step_index=1,
                ),
                _final("done"),
            ]
        )
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle, user_id="alice"
        )
        await agent.run(ctx)

        all_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=False,
        )
        # Executor session: step-first (its inner traces are routed
        # under it), then the two buffered inner traces in arrival
        # order. Terminal lives in the planner session.
        assert [m.metadata.type for m in all_msgs] == [
            "message", "trace", "trace",
        ]
        execute_msg = all_msgs[0]
        assert execute_msg.structured_data_blob.origin == "execute"
        # Step row carries the completion's response (update applied).
        assert execute_msg.structured_data_blob.response == "findings 1"
        # The two buffered inner traces now correctly point at the
        # step's message_id, not the investigation root.
        assert all_msgs[1].metadata.parent_message_id == execute_msg.message_id
        assert all_msgs[2].metadata.parent_message_id == execute_msg.message_id
        assert all_msgs[1].structured_data_blob.origin == "LLM"
        assert all_msgs[2].structured_data_blob.origin == "SearchIndexTool"

    async def test_phase_row_transitions_planning_gathering_completed(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        # Drive a run that reaches every phase: planning (run start) →
        # gathering_data (first execute_start) → completed (final).
        backend = _ScriptedBackend(
            [
                TraceEvent(
                    origin="execute_start",
                    input="step 1",
                    response="",
                    step_index=1,
                ),
                TraceEvent(
                    origin="execute",
                    input="step 1",
                    response="findings",
                    step_index=1,
                ),
                _final("done"),
            ]
        )
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle
        )
        await agent.run(ctx)

        # Phase row id is derivable from parent_message_id, so the
        # frontend can poll by id without needing to know the
        # planner session.
        phase_row = store.get_message_by_id(
            container_id=handle.container_id,
            message_id="parent-msg-1_phase",
        )
        assert phase_row is not None
        # Latest value is "completed" — final's emit_phase wins.
        assert phase_row.structured_data_blob.response == "completed"
        assert phase_row.metadata.type == "phase"
        # Phase row must NOT pollute the executor step list.
        exec_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
        )
        assert all(
            m.message_id != "parent-msg-1_phase" for m in exec_msgs
        )

    async def test_terminal_response_round_trips(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        backend = _ScriptedBackend([_final("final report")])
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle
        )

        await agent.run(ctx)

        # Terminal lives in the planner session; the frontend looks
        # it up by message_id (= parent_interaction_id) via
        # ``getFinalMessage``, which is session-agnostic.
        terminal = store.get_message_by_id(
            container_id=handle.container_id, message_id="parent-msg-1"
        )
        assert terminal is not None
        assert terminal.metadata.type == "message"
        # Round-trip the JSON-encoded response back into the schema.
        decoded = PERAgentInvestigationResponse.model_validate_json(
            terminal.structured_data_blob.response
        )
        assert decoded.investigationName == "final report"
        # The executor session must not have surfaced the terminal as
        # a fake step row — that was the original bug where the
        # user's question appeared as the last step.
        exec_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
        )
        assert all(m.message_id != "parent-msg-1" for m in exec_msgs)


class TestErrorPath:
    async def test_backend_exception_writes_error_terminal(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        boom = RuntimeError("planner explosion")
        backend = _ScriptedBackend(
            [TraceEvent(origin="plan", input="q", response="...")],
            raise_after=boom,
        )
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle
        )

        with pytest.raises(RuntimeError, match="planner explosion"):
            await agent.run(ctx)

        terminal = store.get_message_by_id(
            container_id=handle.container_id, message_id="parent-msg-1"
        )
        assert terminal is not None
        # The frontend uses ``getFinalMessage`` (term lookup by id) to
        # decide completion. ``response`` MUST be non-empty so polling
        # stops. Intentionally a short sentinel ("Investigation
        # failed") — NOT the raw exception — because OSD echoes
        # ``response`` in its error modal and exception strings there
        # are noisy. Structured error detail lives in metadata.
        assert terminal.structured_data_blob.response == "Investigation failed"
        assert terminal.metadata.type == "message"
        assert "planner explosion" in (
            getattr(terminal.metadata, "error", "") or ""
        )

    async def test_missing_final_event_is_contract_violation(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        backend = _ScriptedBackend(
            [TraceEvent(origin="plan", input="q", response="r")]
        )
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle
        )

        with pytest.raises(RuntimeError, match="without emitting a FinalEvent"):
            await agent.run(ctx)

        terminal = store.get_message_by_id(
            container_id=handle.container_id, message_id="parent-msg-1"
        )
        assert terminal is not None
        assert terminal.metadata.type == "message"


class TestSqliteStoreOrdering:
    """Verify the polling-shaped query returns messages in insertion order."""

    def test_exclude_trace_filter_and_pagination(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
    ) -> None:
        # Insert: trace, message, trace, message
        from storage.schemas import (
            MessageMetadata,
            Namespace,
            StructuredDataBlob,
        )

        for i, mtype in enumerate(["trace", "message", "trace", "message"]):
            store.append_message(
                Message(
                    memory_container_id=handle.container_id,
                    message_id=f"m-{i}",
                    structured_data_blob=StructuredDataBlob(
                        input="q", response=f"r{i}"
                    ),
                    namespace=Namespace(session_id=handle.session_id),
                    metadata=MessageMetadata(type=mtype),
                )
            )

        msgs, next_token = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
            size=50,
        )
        assert [m.message_id for m in msgs] == ["m-1", "m-3"]
        assert next_token is None

        # With size=1, we get pagination
        first, token1 = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
            size=1,
        )
        assert [m.message_id for m in first] == ["m-1"]
        assert token1 == 1
        second, token2 = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
            size=1,
            from_offset=token1,
        )
        assert [m.message_id for m in second] == ["m-3"]
        assert token2 is None

    def test_session_isolation(
        self, store: SqliteMemoryStore, handle: InvestigationHandle
    ) -> None:
        from storage.schemas import (
            MessageMetadata,
            Namespace,
            StructuredDataBlob,
        )

        other_session = store.create_session(container_id=handle.container_id)

        for sid in (handle.session_id, other_session.session_id):
            store.append_message(
                Message(
                    memory_container_id=handle.container_id,
                    message_id=f"m-{sid}",
                    structured_data_blob=StructuredDataBlob(input="q", response="r"),
                    namespace=Namespace(session_id=sid),
                    metadata=MessageMetadata(type="message"),
                )
            )

        msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=False,
        )
        assert [m.message_id for m in msgs] == [f"m-{handle.session_id}"]

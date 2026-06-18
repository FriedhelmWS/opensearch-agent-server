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
    session = store.create_session(container_id=container.container_id)
    return InvestigationHandle(
        container_id=container.container_id,
        session_id=session.session_id,
        parent_message_id="parent-msg-1",
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
    async def test_trace_then_final_writes_in_order(
        self,
        store: SqliteMemoryStore,
        handle: InvestigationHandle,
        ctx: InvestigationContext,
    ) -> None:
        backend = _ScriptedBackend(
            [
                TraceEvent(origin="plan", input="q", response="plan output"),
                TraceEvent(origin="execute", input="step 1", response="findings 1"),
                TraceEvent(origin="reflect", input="ctx", response="reflect output"),
                _final("Latency analysis"),
            ]
        )
        agent = InvestigationAgent(
            backend=backend, memory_store=store, handle=handle, user_id="alice"
        )

        result = await agent.run(ctx)

        assert result.investigationName == "Latency analysis"

        all_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=False,
        )
        # 3 trace + 1 terminal
        assert [m.metadata.type for m in all_msgs] == [
            "trace", "trace", "trace", "message",
        ]
        # Trace numbers monotonic
        traces = [m for m in all_msgs if m.metadata.type == "trace"]
        assert [t.structured_data_blob.trace_number for t in traces] == [1, 2, 3]
        # Origins preserved
        assert [t.structured_data_blob.origin for t in traces] == [
            "plan", "execute", "reflect",
        ]
        # All trace messages carry the parent_message_id lineage
        for t in traces:
            assert t.structured_data_blob.parent_message_id == "parent-msg-1"
            assert t.metadata.parent_message_id == "parent-msg-1"
            assert t.namespace.user_id == "alice"

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

        # Frontend polls with exclude_trace=True — make sure that returns
        # exactly the terminal message.
        terminal_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
        )
        assert len(terminal_msgs) == 1
        terminal = terminal_msgs[0]
        assert terminal.metadata.type == "message"
        # The terminal message reuses the parent_message_id as its
        # message_id so the frontend can correlate to the trigger
        # response's parent_interaction_id.
        assert terminal.message_id == "parent-msg-1"
        # response must round-trip into PERAgentInvestigationResponse
        decoded = PERAgentInvestigationResponse.model_validate_json(
            terminal.structured_data_blob.response
        )
        assert decoded.investigationName == "final report"


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

        terminal_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
        )
        assert len(terminal_msgs) == 1
        terminal = terminal_msgs[0]
        # The frontend uses ``lastMessage?.response`` to decide
        # completion. It MUST be non-empty on the error path.
        assert terminal.structured_data_blob.response
        decoded = PERAgentInvestigationResponse.model_validate_json(
            terminal.structured_data_blob.response
        )
        assert "planner explosion" in (decoded.investigationName or "")

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

        terminal_msgs, _ = store.search_messages(
            container_id=handle.container_id,
            session_id=handle.session_id,
            exclude_trace=True,
        )
        assert len(terminal_msgs) == 1


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

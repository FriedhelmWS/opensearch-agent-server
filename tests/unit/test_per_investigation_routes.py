"""Unit tests for ``POST /per/investigations``.

We replace the PER backend with a scripted stand-in so the test exercises
the route ↔ orchestrator ↔ store contract without booting Bedrock /
MCP. The substitution happens by monkeypatching ``PERBackend`` inside
:mod:`server.per_investigation_routes`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agents.investigation.backend import InvestigationContext
from agents.investigation.events import FinalEvent, InvestigationEvent, TraceEvent
from agents.investigation.response_schema import PERAgentInvestigationResponse
from server import per_investigation_routes
from server.per_investigation_routes import router as investigations_router
from storage.sqlite_store import SqliteMemoryStore

pytestmark = pytest.mark.unit


class _ScriptedBackend:
    def __init__(self, events: list[InvestigationEvent]) -> None:
        self._events = events
        # Allow tests to await completion deterministically.
        self.done = asyncio.Event()

    async def run(
        self, ctx: InvestigationContext
    ) -> AsyncIterator[InvestigationEvent]:
        try:
            for ev in self._events:
                yield ev
        finally:
            self.done.set()


@pytest.fixture
def scripted_backend(monkeypatch: pytest.MonkeyPatch) -> _ScriptedBackend:
    backend = _ScriptedBackend(
        [
            TraceEvent(origin="plan", input="q", response="planned"),
            FinalEvent(
                response=PERAgentInvestigationResponse(
                    findings=[],
                    hypotheses=[],
                    topologies=[],
                    investigationName="ok",
                )
            ),
        ]
    )
    monkeypatch.setattr(
        per_investigation_routes, "PERBackend", lambda: backend
    )
    return backend


@pytest.fixture
def client_and_store() -> tuple[TestClient, SqliteMemoryStore]:
    store = SqliteMemoryStore.in_memory()
    app = FastAPI()
    app.state.memory_store = store
    app.include_router(investigations_router)
    return TestClient(app), store


def test_trigger_returns_envelope_with_ids(
    client_and_store: tuple[TestClient, SqliteMemoryStore],
    scripted_backend: _ScriptedBackend,
) -> None:
    client, _ = client_and_store
    resp = client.post(
        "/per/investigations",
        json={"question": "why is latency high?"},
    )
    assert resp.status_code == 200
    body = resp.json()
    # ``extractParentInteractionId`` reads ``response.parent_interaction_id``.
    assert body["response"]["parent_interaction_id"]
    assert body["response"]["memory_id"]
    assert body["response"]["executor_memory_id"]
    # ``runningMemory`` is what the OSD frontend hands to its polling
    # service unchanged.
    assert body["runningMemory"]["memoryContainerId"] == body["response"]["memory_id"]
    assert body["runningMemory"]["parentInteractionId"] == (
        body["response"]["parent_interaction_id"]
    )
    assert body["runningMemory"]["executorMemoryId"] == (
        body["response"]["executor_memory_id"]
    )


def test_background_run_writes_terminal_message(
    client_and_store: tuple[TestClient, SqliteMemoryStore],
    scripted_backend: _ScriptedBackend,
) -> None:
    client, store = client_and_store
    resp = client.post(
        "/per/investigations",
        json={"question": "test"},
    )
    body = resp.json()
    container_id = body["response"]["memory_id"]
    session_id = body["response"]["executor_memory_id"]
    parent_msg_id = body["response"]["parent_interaction_id"]

    # The TestClient runs each request synchronously on a fresh loop, so
    # by the time we reach this point the background task has already
    # been scheduled but may not have run. Drain it by polling the
    # store. A handful of iterations is plenty for the scripted backend.
    import time

    deadline = time.monotonic() + 2.0
    terminal = None
    while time.monotonic() < deadline:
        msgs, _ = store.search_messages(
            container_id=container_id,
            session_id=session_id,
            exclude_trace=True,
        )
        if msgs:
            terminal = msgs[0]
            break
        time.sleep(0.02)

    assert terminal is not None, "background investigation never wrote terminal"
    assert terminal.message_id == parent_msg_id
    decoded = PERAgentInvestigationResponse.model_validate_json(
        terminal.structured_data_blob.response
    )
    assert decoded.investigationName == "ok"


def test_503_when_store_unavailable() -> None:
    app = FastAPI()
    app.state.memory_store = None
    app.include_router(investigations_router)
    client = TestClient(app)
    resp = client.post(
        "/per/investigations", json={"question": "q"}
    )
    assert resp.status_code == 503

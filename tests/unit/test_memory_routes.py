"""Unit tests for the ml-commons-shaped memory_containers HTTP routes.

We mount just the memory router on a stripped-down FastAPI app rather
than spinning up :func:`server.ag_ui_app.create_app` — the full lifespan
boots agent factories (Bedrock, MCP) that are noisy and slow to mock,
and the routes only depend on ``app.state.memory_store``.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.memory_routes import router as memory_router
from storage.schemas import (
    Message,
    MessageMetadata,
    Namespace,
    StructuredDataBlob,
)
from storage.sqlite_store import SqliteMemoryStore

pytestmark = pytest.mark.unit


@pytest.fixture
def app_with_store() -> tuple[FastAPI, SqliteMemoryStore]:
    store = SqliteMemoryStore.in_memory()
    app = FastAPI()
    app.state.memory_store = store
    app.include_router(memory_router)
    return app, store


@pytest.fixture
def client(app_with_store: tuple[FastAPI, SqliteMemoryStore]) -> TestClient:
    return TestClient(app_with_store[0])


class TestContainerAndSession:
    def test_create_container(self, client: TestClient) -> None:
        resp = client.post(
            "/_plugins/_ml/memory_containers/_create",
            json={"name": "investigation"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["memory_container_id"]
        assert body["name"] == "investigation"

    def test_create_session(
        self,
        client: TestClient,
        app_with_store: tuple[FastAPI, SqliteMemoryStore],
    ) -> None:
        _, store = app_with_store
        container = store.create_container(name="x")
        resp = client.post(
            f"/_plugins/_ml/memory_containers/{container.container_id}/memories/sessions",
            json={"summary": "investigation"},
        )
        assert resp.status_code == 200
        assert resp.json()["session_id"]


class TestSearchDispatch:
    def _seed_messages(
        self, store: SqliteMemoryStore, container_id: str, session_id: str
    ) -> tuple[str, str, list[str]]:
        """Seed: 1 terminal + 3 traces. Returns (terminal_id, parent_msg_id, trace_ids)."""
        terminal_id = "terminal-1"
        parent_msg_id = "parent-1"
        trace_ids = ["t-1", "t-2", "t-3"]

        store.append_message(
            Message(
                memory_container_id=container_id,
                message_id=terminal_id,
                structured_data_blob=StructuredDataBlob(
                    input="q", response="final answer"
                ),
                namespace=Namespace(session_id=session_id, user_id="alice"),
                metadata=MessageMetadata(type="message"),
            )
        )
        for i, tid in enumerate(trace_ids):
            store.append_message(
                Message(
                    memory_container_id=container_id,
                    message_id=tid,
                    structured_data_blob=StructuredDataBlob(
                        input=f"step{i}",
                        response=f"r{i}",
                        parent_message_id=parent_msg_id,
                        trace_number=i + 1,
                        origin="execute",
                    ),
                    namespace=Namespace(session_id=session_id),
                    metadata=MessageMetadata(
                        type="trace",
                        parent_message_id=parent_msg_id,
                        trace_number=str(i + 1),
                        origin="execute",
                    ),
                )
            )
        return terminal_id, parent_msg_id, trace_ids

    def test_term_lookup_by_id(
        self,
        client: TestClient,
        app_with_store: tuple[FastAPI, SqliteMemoryStore],
    ) -> None:
        _, store = app_with_store
        container = store.create_container()
        session = store.create_session(container_id=container.container_id)
        terminal_id, _, _ = self._seed_messages(
            store, container.container_id, session.session_id
        )

        resp = client.post(
            f"/_plugins/_ml/memory_containers/{container.container_id}"
            "/memories/working/_search",
            json={"query": {"term": {"_id": terminal_id}}},
        )
        assert resp.status_code == 200
        hits = resp.json()["hits"]["hits"]
        assert len(hits) == 1
        assert hits[0]["_id"] == terminal_id
        assert hits[0]["_source"]["structured_data_blob"]["response"] == (
            "final answer"
        )

    def test_term_lookup_missing_returns_empty(
        self,
        client: TestClient,
        app_with_store: tuple[FastAPI, SqliteMemoryStore],
    ) -> None:
        _, store = app_with_store
        container = store.create_container()
        resp = client.post(
            f"/_plugins/_ml/memory_containers/{container.container_id}"
            "/memories/working/_search",
            json={"query": {"term": {"_id": "does-not-exist"}}},
        )
        assert resp.status_code == 200
        assert resp.json()["hits"]["hits"] == []

    def test_session_listing_excludes_traces(
        self,
        client: TestClient,
        app_with_store: tuple[FastAPI, SqliteMemoryStore],
    ) -> None:
        _, store = app_with_store
        container = store.create_container()
        session = store.create_session(container_id=container.container_id)
        terminal_id, _, _ = self._seed_messages(
            store, container.container_id, session.session_id
        )

        resp = client.post(
            f"/_plugins/_ml/memory_containers/{container.container_id}"
            "/memories/working/_search",
            json={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"namespace.session_id": session.session_id}}
                        ],
                        "must_not": [{"term": {"metadata.type": "trace"}}],
                    }
                },
                "size": 50,
            },
        )
        assert resp.status_code == 200
        hits = resp.json()["hits"]["hits"]
        assert [h["_id"] for h in hits] == [terminal_id]

    def test_trace_enumeration(
        self,
        client: TestClient,
        app_with_store: tuple[FastAPI, SqliteMemoryStore],
    ) -> None:
        _, store = app_with_store
        container = store.create_container()
        session = store.create_session(container_id=container.container_id)
        _, parent_msg_id, trace_ids = self._seed_messages(
            store, container.container_id, session.session_id
        )

        resp = client.post(
            f"/_plugins/_ml/memory_containers/{container.container_id}"
            "/memories/working/_search",
            json={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"metadata.parent_message_id": parent_msg_id}},
                            {"match": {"namespace.session_id": session.session_id}},
                            {"match": {"metadata.type": "trace"}},
                        ]
                    }
                },
                "size": 50,
            },
        )
        assert resp.status_code == 200
        hits = resp.json()["hits"]["hits"]
        assert [h["_id"] for h in hits] == trace_ids
        # Frontend uses last hit's `sort[0]` as the search_after cursor.
        # ml-commons sorts traces by integer trace_number (= the
        # document's `message_id` int field on the Java side).
        assert hits[-1]["sort"] == [3]


class TestStoreUnavailable:
    def test_503_when_store_missing(self) -> None:
        app = FastAPI()
        app.state.memory_store = None
        app.include_router(memory_router)
        client = TestClient(app)
        resp = client.post(
            "/_plugins/_ml/memory_containers/_create",
            json={"name": "x"},
        )
        assert resp.status_code == 503

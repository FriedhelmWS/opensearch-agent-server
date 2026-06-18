"""ml-commons-compatible ``memory_containers`` HTTP routes.

The OSD ``dashboards-investigation`` plugin proxies a small fixed set of
ml-commons endpoints through its own ``/api/notebooks/ml/proxy`` route
(see ``server/routes/notebooks/ml_router.ts``). The open-source
deployment doesn't have ml-commons available, so this module reproduces
just the shapes the frontend actually consumes:

* ``POST /_plugins/_ml/memory_containers/_create``
* ``GET  /_plugins/_ml/memory_containers/{id}``
* ``POST /_plugins/_ml/memory_containers/{id}/memories/sessions``
* ``POST /_plugins/_ml/memory_containers/{id}/memories/working/_search``

The search endpoint is the polling endpoint. The frontend only ever
sends a few hand-coded query DSL bodies — we recognize them by shape
rather than implementing a full DSL parser. See
:func:`_dispatch_search` for the supported shapes.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from storage.memory_store import MemoryStore
from storage.schemas import Message
from utils.logging_helpers import get_logger, log_warning_event

logger = get_logger(__name__)

router = APIRouter(tags=["memory_containers"])

# Mounted under this prefix to mirror ml-commons.
_PREFIX = "/_plugins/_ml/memory_containers"


def _get_store(request: Request) -> MemoryStore:
    store = getattr(request.app.state, "memory_store", None)
    if store is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Investigation memory store unavailable — "
                "set AG_UI_ENABLE_PERSISTENCE=true."
            ),
        )
    return store


class _CreateContainerBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str | None = None
    description: str | None = None


class _CreateSessionBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    summary: str | None = None


@router.post(f"{_PREFIX}/_create")
async def create_container(
    body: _CreateContainerBody, request: Request
) -> dict[str, Any]:
    store = _get_store(request)
    container = store.create_container(
        name=body.name, owner_user_id=None
    )
    # Match ml-commons' shape closely enough for the frontend's
    # `memory_container_id` extractor.
    return {
        "memory_container_id": container.container_id,
        "name": container.name,
        "status": "created",
    }


@router.get(f"{_PREFIX}/{{container_id}}")
async def get_container(container_id: str, request: Request) -> dict[str, Any]:
    # The frontend only uses this endpoint to discover the container's
    # existence — return a minimal echo. Storing-and-fetching containers
    # would require a ``get_container`` on the store; not worth the
    # interface change for this single read site today.
    _get_store(request)
    return {"memory_container_id": container_id}


@router.post(f"{_PREFIX}/{{container_id}}/memories/sessions")
async def create_session(
    container_id: str,
    body: _CreateSessionBody,  # noqa: ARG001 — accepted for parity, not used
    request: Request,
) -> dict[str, Any]:
    store = _get_store(request)
    session = store.create_session(container_id=container_id)
    return {"session_id": session.session_id}


@router.post(f"{_PREFIX}/{{container_id}}/memories/working/_search")
async def search_memories(
    container_id: str, body: dict[str, Any], request: Request
) -> dict[str, Any]:
    store = _get_store(request)
    return _dispatch_search(store, container_id, body)


def _dispatch_search(
    store: MemoryStore, container_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    """Map the small set of DSL bodies the frontend sends to store calls.

    The frontend's three call sites are in ``ml_commons_apis.ts``:

    1. ``executeMLCommonsAgenticMessage``: ``query.term._id == messageId``.
       Used by polling to fetch the terminal message — we look up by id.

    2. ``getMLCommonsAgenticMemoryMessages``: bool query with
       ``namespace.session_id`` ``must`` and ``metadata.type == "trace"``
       ``must_not``, paginated via ``from``. Used to enumerate non-trace
       (terminal) messages in a session.

    3. ``getMLCommonsAgenticTracesMessages``: bool query with
       ``metadata.parent_message_id``, ``namespace.session_id``, and
       ``metadata.type == "trace"`` all in ``must``, paginated via
       ``search_after`` on ``message_id``.

    Anything else returns an empty hits page so the frontend's polling
    doesn't crash on a unrecognized shape — but we log it so we notice.
    """
    query = body.get("query") or {}
    # --- Shape 1: term lookup by _id -----------------------------------
    term = query.get("term")
    if isinstance(term, dict) and "_id" in term:
        message = store.get_message_by_id(
            container_id=container_id, message_id=term["_id"]
        )
        return _hits_envelope([message] if message else [], next_token=None)

    bool_q = query.get("bool")
    if not isinstance(bool_q, dict):
        log_warning_event(
            logger,
            "[memory_routes] unrecognized search body shape — returning empty",
            "memory_routes.search.unrecognized_shape",
            keys=list(query.keys()),
        )
        return _hits_envelope([], next_token=None)

    must = bool_q.get("must") or []
    must_not = bool_q.get("must_not") or []

    session_id = _extract_term_value(must, ("namespace.session_id",))
    parent_message_id = _extract_term_value(
        must, ("metadata.parent_message_id",)
    )
    has_must_trace = (
        _extract_term_value(must, ("metadata.type",)) == "trace"
    )
    has_must_not_trace = (
        _extract_term_value(must_not, ("metadata.type",)) == "trace"
    )

    if not session_id:
        log_warning_event(
            logger,
            "[memory_routes] bool query without namespace.session_id — empty",
            "memory_routes.search.missing_session_id",
        )
        return _hits_envelope([], next_token=None)

    size = int(body.get("size") or 50)

    # --- Shape 3: trace enumeration ------------------------------------
    if parent_message_id and has_must_trace:
        search_after = body.get("search_after")
        after = (
            search_after[0]
            if isinstance(search_after, list) and search_after
            else None
        )
        messages, next_token = store.search_traces(
            container_id=container_id,
            session_id=session_id,
            parent_message_id=parent_message_id,
            after_message_id=after,
            size=size,
        )
        return _hits_envelope(messages, next_token=next_token)

    # --- Shape 2: terminal-message enumeration -------------------------
    from_offset = int(body.get("from") or 0)
    messages, next_token = store.search_messages(
        container_id=container_id,
        session_id=session_id,
        exclude_trace=has_must_not_trace,
        from_offset=from_offset,
        size=size,
    )
    return _hits_envelope(messages, next_token=next_token)


def _extract_term_value(
    clauses: list[Any], field_names: tuple[str, ...]
) -> str | None:
    """Find a leaf ``term``/``match`` value for any of ``field_names``.

    The frontend mixes ``term`` and ``match`` clauses in different call
    sites (see ``ml_commons_apis.ts`` lines 178-184 vs 263-279). We
    accept either to keep the dispatch lenient.
    """
    for clause in clauses:
        if not isinstance(clause, dict):
            continue
        for key in ("term", "match"):
            leaf = clause.get(key)
            if isinstance(leaf, dict):
                for name in field_names:
                    if name in leaf:
                        value = leaf[name]
                        return value if isinstance(value, str) else str(value)
    return None


def _hits_envelope(
    messages: list[Message], *, next_token: int | str | None
) -> dict[str, Any]:
    """Wrap messages in the OpenSearch ``hits.hits[]._source`` shape.

    ``utils.ts:73`` reads ``hit._source.structured_data_blob`` and
    ``utils.ts:107`` reads ``lastHit.sort?.[0]`` for trace pagination
    (passed back unchanged as ``search_after`` in the next request).

    For traces we expose ``trace_number`` (int) as the sort key — same
    semantics as the integer ``message_id`` ml-commons writes (Java
    ``AgenticConversationMemory`` sets the document's ``message_id``
    to the trace number). For terminal messages there's no pagination
    cursor; ``_id`` is used as a stable placeholder.
    """
    hits = []
    for msg in messages:
        source = msg.model_dump()
        sort_key: int | str
        if (
            msg.metadata.type == "trace"
            and msg.structured_data_blob.trace_number is not None
        ):
            sort_key = msg.structured_data_blob.trace_number
        else:
            sort_key = msg.message_id
        hit = {
            "_id": msg.message_id,
            "_source": source,
            "sort": [sort_key],
        }
        hits.append(hit)
    envelope: dict[str, Any] = {
        "hits": {"total": {"value": len(hits)}, "hits": hits},
    }
    if next_token is not None:
        envelope["next_token"] = next_token
    return envelope

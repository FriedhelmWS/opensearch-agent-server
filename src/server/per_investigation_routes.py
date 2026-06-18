"""``POST /per/investigations`` — trigger a PER-backed investigation.

The OSD frontend calls this endpoint, gets the trigger envelope back
synchronously, and then polls
``/_plugins/_ml/memory_containers/{id}/memories/working/_search`` until a
terminal message appears (see :mod:`server.memory_routes`).

The investigation runs as a background task on the server's event loop.
``InvestigationAgent`` writes trace + terminal messages to the same
SQLite memory store that the polling endpoint reads from, so the
frontend never sees the agent directly.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from agents.investigation.backend import InvestigationContext
from agents.investigation.investigation_agent import (
    InvestigationAgent,
    InvestigationHandle,
)
from agents.investigation.per_backend import PERBackend
from server.utils import get_user_id_from_request
from storage.memory_store import MemoryStore
from utils.logging_helpers import get_logger, log_error_event, log_info_event

logger = get_logger(__name__)

router = APIRouter(tags=["investigations"])


class _TriggerBody(BaseModel):
    """Request body — mirrors the parameters block the OSD ``executeMLCommonsAgent``
    call sends (``use_investigation.ts:535``).
    """

    model_config = ConfigDict(extra="allow")

    question: str = Field(..., min_length=1)
    context: str = ""
    initial_goal: str | None = None
    prev_content: bool = False
    time_range: dict[str, Any] | None = None
    # Pre-allocated container/session ids let the caller tie the trigger
    # to a notebook on its side. Unused for now — the route allocates
    # fresh ids — but kept on the body for forward-compat.
    memory_container_id: str | None = None
    executor_memory_id: str | None = None


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


@router.post("/per/investigations")
async def trigger_investigation(
    body: _TriggerBody, request: Request
) -> dict[str, Any]:
    store = _get_store(request)

    container_id = body.memory_container_id
    if container_id is None:
        container_id = store.create_container(
            name="investigation"
        ).container_id

    session_id = body.executor_memory_id
    if session_id is None:
        session_id = store.create_session(container_id=container_id).session_id

    parent_message_id = uuid.uuid4().hex

    handle = InvestigationHandle(
        container_id=container_id,
        session_id=session_id,
        parent_message_id=parent_message_id,
    )
    ctx = InvestigationContext(
        question=body.question,
        context=body.context,
        initial_goal=body.initial_goal,
        prev_content=body.prev_content,
        time_range=body.time_range,
    )

    user_id: str | None
    try:
        user_id = get_user_id_from_request(request)
    except Exception:
        # The helper raises when auth is strict + missing; for trigger
        # we already passed middleware, but be defensive in tests.
        user_id = None

    agent = InvestigationAgent(
        backend=PERBackend(),
        memory_store=store,
        handle=handle,
        user_id=user_id,
    )

    log_info_event(
        logger,
        "[per_investigations] trigger accepted",
        "per_investigations.trigger.accepted",
        container_id=container_id,
        session_id=session_id,
        parent_message_id=parent_message_id,
    )

    asyncio.create_task(
        _run_investigation_safely(agent, ctx, parent_message_id)
    )

    # Mirrors the ml-commons trigger response shape that
    # ``extractParentInteractionId`` (common/utils/task.ts) expects, plus
    # ``runningMemory`` so the OSD frontend can hand it directly to the
    # polling service.
    return {
        "response": {
            "parent_interaction_id": parent_message_id,
            "memory_id": container_id,
            "executor_memory_id": session_id,
        },
        "runningMemory": {
            "memoryContainerId": container_id,
            "parentInteractionId": parent_message_id,
            "executorMemoryId": session_id,
        },
    }


async def _run_investigation_safely(
    agent: InvestigationAgent,
    ctx: InvestigationContext,
    parent_message_id: str,
) -> None:
    """Background runner — swallow exceptions after they're recorded.

    ``InvestigationAgent.run`` already writes an error terminal message
    on any exception. Re-raising into the asyncio.create_task task would
    only produce an unhandled-exception log on shutdown — the frontend
    has already been signaled via the terminal message it polls for.
    """
    try:
        await agent.run(ctx)
    except Exception as exc:  # noqa: BLE001 — terminal in-task boundary
        log_error_event(
            logger,
            "[per_investigations] background run failed",
            "per_investigations.background.failed",
            error=exc,
            parent_message_id=parent_message_id,
        )

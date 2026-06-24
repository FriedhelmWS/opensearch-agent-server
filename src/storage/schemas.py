"""Pydantic models mirroring the ml-commons ``memory_containers`` JSON shape.

Field names and nesting MUST match what the Java
``RemoteAgenticConversationMemory`` (ml-commons) writes — the OSD frontend
expects these exact keys when it polls
``/_plugins/_ml/memory_containers/{id}/memories/working/_search`` and
parses the ``hits.hits[]._source`` payloads.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


def _now_iso() -> str:
    """ISO-8601 UTC timestamp matching the Java ``Instant.now().toString()`` format."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class StructuredDataBlob(BaseModel):
    """Free-form payload describing what the agent did or produced.

    For trace messages (``metadata.type == "trace"``):
      - ``input``: the prompt the sub-agent received
      - ``response``: the sub-agent's raw output
      - ``parent_message_id`` / ``trace_number`` / ``origin``: trace lineage

    For terminal messages (``metadata.type == "message"``):
      - ``input``: the original question
      - ``response``: a JSON string of ``PERAgentInvestigationResponse``
      - ``final_answer``: optional human-readable summary
    """

    model_config = ConfigDict(extra="allow")

    input: str
    response: str
    create_time: str = Field(default_factory=_now_iso)
    updated_time: str = Field(default_factory=_now_iso)

    parent_message_id: str | None = None
    trace_number: int | None = None
    origin: str | None = None
    final_answer: str | None = None


class Namespace(BaseModel):
    """Scopes a message to a session and (optionally) a user."""

    session_id: str
    user_id: str | None = None


class MessageMetadata(BaseModel):
    """Trace vs terminal discriminator + lineage pointer.

    Java ml-commons (``AgenticConversationMemory.java`` / ``RemoteAgenticConversationMemory.java``)
    writes ``trace_number`` and ``origin`` into both ``metadata`` and
    ``structured_data`` for trace messages. We mirror that so the same
    document round-trips through any code that reads from either spot.
    """

    model_config = ConfigDict(extra="allow")

    # ``"trace"`` / ``"message"`` follow ml-commons; ``"phase"`` is
    # an agent-server addition for the high-level run-state row that
    # the OSD frontend polls by id (``_phase_message_id``) to render
    # the live investigation phase indicator. Kept out of step-list
    # / trace-flyout polling shapes by virtue of the dedicated id.
    type: Literal["trace", "message", "phase"]
    parent_message_id: str | None = None
    trace_number: str | None = None
    origin: str | None = None


class Message(BaseModel):
    """A single ml-commons-shaped memory message.

    ``created_time`` and ``last_updated_time`` are top-level — matching
    the Java ``MLWorkingMemory`` ``toXContent`` shape and what
    ``utils.ts:79-80`` reads on the OSD frontend.
    """

    model_config = ConfigDict(extra="allow")

    memory_container_id: str
    message_id: str
    structured_data_blob: StructuredDataBlob
    namespace: Namespace
    metadata: MessageMetadata
    infer: bool = False
    created_time: str = Field(default_factory=_now_iso)
    last_updated_time: str = Field(default_factory=_now_iso)


class MemoryContainer(BaseModel):
    """Top-level container — one per investigation."""

    container_id: str
    name: str | None = None
    description: str | None = None
    owner_user_id: str | None = None
    created_time: str = Field(default_factory=_now_iso)


class Session(BaseModel):
    """Executor session inside a container.

    The frontend polls by ``namespace.session_id`` (see
    ``per_agent_memory_service.ts``). Each PER investigation gets its
    own session so concurrent investigations on the same container
    do not bleed into each other.
    """

    session_id: str
    container_id: str
    created_time: str = Field(default_factory=_now_iso)

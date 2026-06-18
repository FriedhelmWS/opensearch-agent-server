"""Abstract memory store contract.

A store knows how to create containers / sessions and append messages
in :mod:`storage.schemas` shape. Read access is exposed as a single
``search_messages`` method that maps the small subset of OpenSearch
query DSL the OSD frontend actually sends — see the docstring on
:meth:`MemoryStore.search_messages` for the supported filter shape.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from storage.schemas import MemoryContainer, Message, Session


@runtime_checkable
class MemoryStore(Protocol):
    """Protocol every memory backend must implement.

    All methods are sync — SQLite is fast enough that an async layer
    would only add complexity. The orchestrator runs writes inside the
    PER pipeline's existing event loop without back-pressure issues
    (one write per phase, ~3 writes per investigation step).
    """

    def create_container(
        self, *, name: str | None = None, owner_user_id: str | None = None
    ) -> MemoryContainer: ...

    def create_session(self, *, container_id: str) -> Session: ...

    def append_message(self, message: Message) -> None:
        """Persist a message. Must preserve insertion order (sort key:
        ``created_time``) — the frontend renders trace steps strictly
        by ``created_time asc``.
        """

    def search_messages(
        self,
        *,
        container_id: str,
        session_id: str,
        exclude_trace: bool = True,
        from_offset: int = 0,
        size: int = 50,
    ) -> tuple[list[Message], int | None]:
        """Return ``(messages, next_token)``.

        Implements the filter the OSD frontend sends to ml-commons:
        ``namespace.session_id == session_id`` AND
        (``exclude_trace`` ? ``metadata.type != "trace"`` : no extra filter),
        sorted by ``created_time asc``, paginated.
        """

    def get_message_by_id(
        self, *, container_id: str, message_id: str
    ) -> Message | None:
        """Return the single message with ``message_id`` in this container,
        or ``None`` if not found. Backs the frontend's
        ``executeMLCommonsAgenticMessage`` (terminal-message lookup by
        ``parent_interaction_id``).
        """

    def search_traces(
        self,
        *,
        container_id: str,
        session_id: str,
        parent_message_id: str,
        after_message_id: int | str | None = None,
        size: int = 50,
    ) -> tuple[list[Message], int | None]:
        """Return trace messages for a parent message, ordered by
        ``trace_number asc`` (matches ml-commons, where the integer
        ``message_id`` field equals the trace number). The
        ``after_message_id`` parameter name is preserved for callers
        that pass through the OpenSearch ``search_after`` cursor; it
        accepts an int or numeric string. ``next_token`` is the last
        ``trace_number`` in the page when more pages exist, else
        ``None``.
        """

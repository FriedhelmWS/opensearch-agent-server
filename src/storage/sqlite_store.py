"""SQLAlchemy-backed implementation of :class:`~storage.memory_store.MemoryStore`.

Shares the engine and ``Base.metadata`` with
:class:`~utils.persistence.AGUIPersistence` so investigation memory lives
in the same SQLite file as AG-UI threads / runs / messages. The ORM
models (:class:`~utils.persistence.MemoryContainerRow` and friends) are
defined alongside the existing AG-UI models so a single
``Base.metadata.create_all(engine)`` covers both feature areas.

Construct via :meth:`SqliteMemoryStore.from_engine` to bind to an
already-initialized :class:`sqlalchemy.engine.Engine` (the typical path
during server startup), or :meth:`SqliteMemoryStore.in_memory` for tests.
"""

from __future__ import annotations

import json
import threading
import uuid

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from storage.memory_store import MemoryStore
from storage.schemas import (
    MemoryContainer,
    Message,
    MessageMetadata,
    Namespace,
    Session,
    StructuredDataBlob,
)
from utils.persistence import (
    Base,
    MemoryContainerRow,
    MemoryMessageRow,
    MemorySessionRow,
)


class SqliteMemoryStore(MemoryStore):
    """ORM-backed memory store sharing the AG-UI engine."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        # Idempotent — picks up the new ``memory_*`` tables on first run
        # and is a no-op afterwards. Existing AG-UI tables are unaffected.
        Base.metadata.create_all(engine)
        self._SessionLocal = sessionmaker(bind=engine)
        # Insert sequence is process-local. Restored from the DB on
        # startup so cross-restart ordering is preserved within a
        # session (re-attaching to an investigation in flight is not
        # supported by the open-source memory anyway, but keeping the
        # counter monotonic across restarts costs nothing).
        self._lock = threading.Lock()
        self._seq = self._load_seq()

    @classmethod
    def from_engine(cls, engine: Engine) -> SqliteMemoryStore:
        return cls(engine)

    @classmethod
    def in_memory(cls) -> SqliteMemoryStore:
        """For tests — owns its own ephemeral SQLite engine.

        Uses ``StaticPool`` so every session checked out shares the same
        underlying connection. Default ``QueuePool`` would create a new
        connection per checkout, and each connection on
        ``:memory:`` sees its own (empty) database.
        """
        return cls(
            create_engine(
                "sqlite:///:memory:",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        )

    def _load_seq(self) -> int:
        session = self._SessionLocal()
        try:
            row = (
                session.query(MemoryMessageRow.insert_seq)
                .order_by(MemoryMessageRow.insert_seq.desc())
                .first()
            )
            return int(row[0]) if row else 0
        finally:
            session.close()

    def create_container(
        self, *, name: str | None = None, owner_user_id: str | None = None
    ) -> MemoryContainer:
        container = MemoryContainer(
            container_id=uuid.uuid4().hex, name=name, owner_user_id=owner_user_id
        )
        session = self._SessionLocal()
        try:
            session.add(
                MemoryContainerRow(
                    container_id=container.container_id,
                    name=container.name,
                    description=container.description,
                    owner_user_id=container.owner_user_id,
                    created_time=container.created_time,
                )
            )
            session.commit()
        finally:
            session.close()
        return container

    def create_session(self, *, container_id: str) -> Session:
        new_session = Session(
            session_id=uuid.uuid4().hex, container_id=container_id
        )
        session = self._SessionLocal()
        try:
            session.add(
                MemorySessionRow(
                    session_id=new_session.session_id,
                    container_id=new_session.container_id,
                    created_time=new_session.created_time,
                )
            )
            session.commit()
        finally:
            session.close()
        return new_session

    def append_message(self, message: Message) -> None:
        with self._lock:
            self._seq += 1
            seq = self._seq
        # Promote trace_number to a column so trace pagination orders
        # by it (matches ml-commons, where the analogous integer
        # message_id column is the sort key). Source of truth stays in
        # structured_data_blob; null on terminal messages.
        trace_number = message.structured_data_blob.trace_number
        session = self._SessionLocal()
        try:
            session.add(
                MemoryMessageRow(
                    message_id=message.message_id,
                    memory_container_id=message.memory_container_id,
                    session_id=message.namespace.session_id,
                    metadata_type=message.metadata.type,
                    structured_data_blob=message.structured_data_blob.model_dump_json(),
                    namespace=message.namespace.model_dump_json(),
                    metadata_json=message.metadata.model_dump_json(),
                    infer=message.infer,
                    created_time=message.created_time,
                    last_updated_time=message.last_updated_time,
                    insert_seq=seq,
                    trace_number=trace_number,
                )
            )
            session.commit()
        finally:
            session.close()

    def search_messages(
        self,
        *,
        container_id: str,
        session_id: str,
        exclude_trace: bool = True,
        from_offset: int = 0,
        size: int = 50,
    ) -> tuple[list[Message], int | None]:
        session = self._SessionLocal()
        try:
            query = session.query(MemoryMessageRow).filter(
                MemoryMessageRow.memory_container_id == container_id,
                MemoryMessageRow.session_id == session_id,
            )
            if exclude_trace:
                query = query.filter(MemoryMessageRow.metadata_type != "trace")
            rows = (
                query.order_by(
                    MemoryMessageRow.created_time.asc(),
                    MemoryMessageRow.insert_seq.asc(),
                )
                .limit(size + 1)  # +1 to detect "has more"
                .offset(from_offset)
                .all()
            )
        finally:
            session.close()

        has_more = len(rows) > size
        rows = rows[:size]
        messages = [self._row_to_message(row) for row in rows]
        next_token = from_offset + size if has_more else None
        return messages, next_token

    def get_message_by_id(
        self, *, container_id: str, message_id: str
    ) -> Message | None:
        session = self._SessionLocal()
        try:
            row = (
                session.query(MemoryMessageRow)
                .filter(
                    MemoryMessageRow.memory_container_id == container_id,
                    MemoryMessageRow.message_id == message_id,
                )
                .first()
            )
        finally:
            session.close()
        return self._row_to_message(row) if row else None

    def search_traces(
        self,
        *,
        container_id: str,
        session_id: str,
        parent_message_id: str,
        after_message_id: int | str | None = None,
        size: int = 50,
    ) -> tuple[list[Message], int | None]:
        """Return traces ordered by ``trace_number`` ASC.

        Mirrors ml-commons: in the Java write path ``message_id`` on the
        document is the integer ``trace_number`` (see
        ``AgenticConversationMemory.java`` ``MLAddMemoriesInput
        .builder()...messageId(traceNum)``). The OSD frontend sorts by
        that field and uses the last hit's ``sort[0]`` as a numeric
        ``search_after`` cursor — so ``next_token`` is the last
        ``trace_number`` in the page when more remain.

        ``after_message_id`` accepts the legacy parameter name plus
        either int or string for compatibility with the cursor as it
        comes back through HTTP / JSON.
        """
        cursor: int | None = None
        if after_message_id is not None:
            try:
                cursor = int(after_message_id)
            except (TypeError, ValueError):
                cursor = None

        session = self._SessionLocal()
        try:
            query = session.query(MemoryMessageRow).filter(
                MemoryMessageRow.memory_container_id == container_id,
                MemoryMessageRow.session_id == session_id,
                MemoryMessageRow.metadata_type == "trace",
                MemoryMessageRow.trace_number.isnot(None),
            )
            if cursor is not None:
                query = query.filter(MemoryMessageRow.trace_number > cursor)
            rows = (
                query.order_by(MemoryMessageRow.trace_number.asc())
                .limit(size + 1)
                .all()
            )
        finally:
            session.close()

        # parent_message_id lives in the JSON-encoded metadata blob
        # rather than a column — filter post-fetch. Trace counts per
        # parent are small (≈ #plan-execute-reflect rounds × steps) so a
        # post-filter is fine.
        filtered = [
            r
            for r in rows
            if json.loads(r.metadata_json).get("parent_message_id")
            == parent_message_id
        ]
        has_more = len(filtered) > size
        filtered = filtered[:size]
        messages = [self._row_to_message(r) for r in filtered]
        next_token = (
            filtered[-1].trace_number if has_more and filtered else None
        )
        return messages, next_token

    @staticmethod
    def _row_to_message(row: MemoryMessageRow) -> Message:
        return Message(
            message_id=row.message_id,
            memory_container_id=row.memory_container_id,
            structured_data_blob=StructuredDataBlob(
                **json.loads(row.structured_data_blob)
            ),
            namespace=Namespace(**json.loads(row.namespace)),
            metadata=MessageMetadata(**json.loads(row.metadata_json)),
            infer=bool(row.infer),
            created_time=row.created_time,
            last_updated_time=row.last_updated_time,
        )

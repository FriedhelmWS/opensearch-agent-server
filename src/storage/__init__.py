"""Memory store — local persistence for agent intermediate output.

The store mirrors the ml-commons ``memory_containers`` JSON shape so the
OSD ``dashboards-investigation`` frontend can poll it via the same
client code used against ml-commons. Two layers:

- :class:`~storage.memory_store.MemoryStore` — abstract Protocol.
- :class:`~storage.sqlite_store.SqliteMemoryStore` — local SQLite-backed
  default implementation.
"""

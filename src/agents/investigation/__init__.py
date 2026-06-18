"""Investigation agent — backend-pluggable orchestrator over a memory store.

The orchestrator (:class:`InvestigationAgent`) consumes a normalized
``InvestigationEvent`` stream from any :class:`InvestigationBackend`
implementation and writes ml-commons-compatible memory messages to a
:class:`~storage.memory_store.MemoryStore`. Backends only know how to
produce events; memory writes and final-response assembly live in the
orchestrator so a future ``DefaultBackend`` plugs in without touching
either layer.
"""

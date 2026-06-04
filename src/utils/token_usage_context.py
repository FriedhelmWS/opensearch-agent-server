"""Per-run token-usage handoff between PER pipeline and the orchestrator.

The default agent's spend is observable via the per-thread Strands agent's
``event_loop_metrics.accumulated_usage`` after the run. The PER agent's
internal plan→execute→reflect sub-agent calls run inside a single
``run_per_pipeline`` tool call, so their Bedrock usage never surfaces on
the outer agent's metrics. This module bridges that gap: the orchestrator
seeds an empty dict before the run, the pipeline mutates it as sub-agent
calls complete, and the orchestrator reads the final dict afterwards to
emit a ``CustomEvent(name="token_usage")`` right before ``RUN_FINISHED``.

Why a shared mutable dict (not ``ContextVar.set``)
==================================================

Strands dispatches tool calls inside a fresh ``contextvars.Context`` (it
runs ``copy_context().run(...)`` to isolate per-tool state). Inside that
copy, calling ``ContextVar.set`` rebinds the name **only within the child
context** — the parent's binding is unchanged, so the orchestrator sees
the original value (``None``). Earlier versions of this module hit
exactly that bug: PER would emit its trailer fine, but the
orchestrator's CustomEvent showed ``inner = {0,0,0,0}`` because the
child-context write was discarded on context exit.

The fix: store a single mutable container and have the pipeline
*mutate* it (``container["input"] += ...``) rather than rebind it.
Mutation is visible across context copies because both the parent and
child reference the same dict object.

Threading note: Strands' MCP background thread is a separate thread, but
the PER pipeline itself runs on the orchestrator's event loop (Strands
awaits the tool coroutine), so the same ContextVar instance is
accessible without thread-safety concerns. Mutation is thread-safe
under CPython for simple ``+=`` on ints inside a dict due to the GIL,
which is sufficient here — the only writer is the pipeline coroutine.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# The container is a dict-of-dicts the orchestrator pre-creates and the
# pipeline fills in. Parent and child contexts share the same object,
# so writes are observable in both directions.
_inner_usage: ContextVar[dict[str, Any] | None] = ContextVar(
    "agent_server_inner_usage", default=None
)


def _empty_container() -> dict[str, Any]:
    """Build the canonical zero-state container.

    Kept private so call sites can't accidentally invent a different
    layout that ``publish_inner_usage`` then fails to extend.
    """
    return {
        "schema_version": 1,
        "input": 0,
        "output": 0,
        "cache_read": 0,
        "cache_write": 0,
        "phase_calls": 0,
        "by_phase": {},  # filled lazily as phases run
        "_present": False,  # flipped True on first publish_inner_usage
    }


def install_inner_usage_container() -> Token:
    """Install a fresh container in the current ``contextvars.Context``.

    Returned token must be passed back to :func:`uninstall_inner_usage_container`
    in a ``finally`` block so the slot resets between requests. Call this
    in the orchestrator BEFORE running the agent — it sets the binding in
    the parent context, and any child context (Strands' tool dispatch)
    inherits a reference to the same mutable dict.
    """
    return _inner_usage.set(_empty_container())


def uninstall_inner_usage_container(token: Token) -> None:
    """Restore the previous binding (paired with ``install_inner_usage_container``)."""
    _inner_usage.reset(token)


def get_inner_usage() -> dict[str, Any] | None:
    """Return the current container, or ``None`` if no run is in progress."""
    return _inner_usage.get()


def publish_inner_usage(usage: dict[str, Any]) -> None:
    """Merge a pipeline-side usage payload into the orchestrator's container.

    Mutates the container in place — does NOT call ``ContextVar.set``,
    because that would rebind the name only in the calling (child)
    context and leave the orchestrator's binding untouched. See module
    docstring.

    The merge replaces top-level scalars (``input`` / ``output`` /
    ``cache_read`` / ``cache_write`` / ``phase_calls`` / ``cache_hit_ratio``)
    and *replaces* the ``by_phase`` dict because the pipeline already
    accumulates it locally — we just want the latest snapshot. Any extra
    keys (e.g. ``per_round``) are passed through verbatim.

    Silently no-ops when no container is installed (e.g. unit tests
    instantiating PER directly without going through the orchestrator).
    """
    container = _inner_usage.get()
    if container is None:
        return
    for key in ("input", "output", "cache_read", "cache_write", "phase_calls"):
        if key in usage:
            container[key] = int(usage[key])
    if "by_phase" in usage and isinstance(usage["by_phase"], dict):
        container["by_phase"] = usage["by_phase"]
    for key in ("cache_hit_ratio", "per_round"):
        if key in usage:
            container[key] = usage[key]
    container["_present"] = True


def inner_usage_is_present() -> bool:
    """Whether the pipeline ever called :func:`publish_inner_usage` this run."""
    container = _inner_usage.get()
    return bool(container and container.get("_present"))

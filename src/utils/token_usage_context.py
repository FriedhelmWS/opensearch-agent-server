"""Per-run token-usage accumulator shared across the agent server.

The default agent's token usage is observable via
``Strands.Agent.event_loop_metrics.accumulated_usage`` after the run.
The PER agent, however, runs an internal plan→execute→reflect loop as
a single ``run_per_pipeline`` tool call — its sub-agent invocations
issue their own Bedrock calls whose usage is *invisible* to the outer
orchestrator agent's ``event_loop_metrics``.

This module provides a tiny ContextVar-based handoff so the PER pipeline
can publish its accumulated usage and the agent_orchestrator can pick
it up after the run finishes — emitting a single uniform AG-UI
``CustomEvent`` (``name="token_usage"``) for downstream consumers
(benchmark runner, billing).

Usage:
    # At the very start of a request, before ``agui_agent.run()``:
    token = reset_inner_usage()

    # Inside per_agent.py, when the pipeline is done:
    set_inner_usage({"input": ..., "output": ..., ...})

    # After ``agui_agent.run()`` finishes, in the orchestrator:
    inner = get_inner_usage()
    reset_inner_usage_token(token)
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# A run-scoped slot holding the inner usage dict published by the PER
# pipeline. ContextVar is sufficient because the pipeline runs in the
# same asyncio task as the orchestrator (Strands invokes the tool with
# ``await`` on the same loop), so the var propagates without
# threading concerns. Reset between requests via ``reset_inner_usage``
# so a stale value never leaks across runs.
_inner_usage: ContextVar[dict[str, Any] | None] = ContextVar(
    "agent_server_inner_usage", default=None
)


def set_inner_usage(usage: dict[str, Any]) -> None:
    """Publish the inner-pipeline usage dict for the orchestrator to pick up."""
    _inner_usage.set(usage)


def get_inner_usage() -> dict[str, Any] | None:
    """Return the inner-pipeline usage dict, or None if not published."""
    return _inner_usage.get()


def reset_inner_usage() -> Token:
    """Clear the slot at the start of a run; returns a token for restore."""
    return _inner_usage.set(None)


def reset_inner_usage_token(token: Token) -> None:
    """Restore the previous ContextVar state (paired with ``reset_inner_usage``)."""
    _inner_usage.reset(token)

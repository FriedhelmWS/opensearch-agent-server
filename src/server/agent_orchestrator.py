"""Agent Orchestrator — routes requests to AG-UI Strands agent wrappers.

``ag_ui_strands.StrandsAgent`` instances are created once per agent name and
then cached so that the per-thread ``StrandsAgentCore`` (and its
``ConversationManager``) survives across requests, giving the agent persistent
conversation memory.  Authentication is handled by :class:`~utils.obo_context.OboAuth`
instances stored on each agent's httpx client — the orchestrator calls
``set_token()`` before each run to inject fresh credentials.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from typing import Any

from ag_ui.core import CustomEvent, EventType, RunAgentInput
from ag_ui_strands import StrandsAgent as AGUIStrandsAgent
from ag_ui_strands.config import StrandsAgentConfig
from strands import Agent as StrandsAgentCore

from orchestrator.router import PageContextRouter
from utils.logging_helpers import get_logger, log_debug_event, log_info_event
from utils.obo_context import OboAuth
from utils.token_usage_context import (
    get_inner_usage,
    inner_usage_is_present,
    install_inner_usage_container,
    uninstall_inner_usage_container,
)

logger = get_logger(__name__)

# A factory callable that returns a pre-configured Strands Agent.
# Headers are no longer passed to the factory — OboAuth handles auth.
AgentFactory = Callable[[], StrandsAgentCore]


def _extract_app_id_from_context(context: list) -> str | None:
    """Extract appId from the AG-UI context array.

    OpenSearch Dashboards sends page context as a Context entry with a JSON
    value containing ``appId`` (e.g. "discover", "explore", "home").
    This function finds the first entry whose value contains an appId.

    Args:
        context: List of AG-UI Context objects (description + value).

    Returns:
        The appId string, or None if not found.
    """
    for ctx in context:
        try:
            value = ctx.value if isinstance(ctx.value, dict) else json.loads(ctx.value)
            if isinstance(value, dict) and "appId" in value:
                app_id = value["appId"]
                log_debug_event(
                    logger,
                    f"Extracted appId='{app_id}' from AG-UI context",
                    "orchestrator.context_app_id",
                    app_id=app_id,
                )
                return app_id
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    return None


def _extract_page_context(input_data: RunAgentInput) -> str | None:
    """Extract page_context from RunAgentInput.

    Strategy:
      1. Check forwardedProps.page_context (direct override, useful for curl testing)
      2. Check AG-UI context array for page context with appId (sent by Dashboard)

    Args:
        input_data: AG-UI RunAgentInput.

    Returns:
        page_context string or None.
    """
    page_context = None
    if hasattr(input_data, "forwarded_props") and input_data.forwarded_props:
        page_context = input_data.forwarded_props.get("page_context")

    if not page_context and hasattr(input_data, "context") and input_data.context:
        page_context = _extract_app_id_from_context(input_data.context)

    return page_context


def _extract_bearer_token(headers: dict[str, str] | None) -> str | None:
    """Extract the Bearer token from an Authorization header dict.

    Args:
        headers: HTTP headers dict (may contain "authorization" key).

    Returns:
        The raw JWT token string, or None.
    """
    if not headers:
        return None
    auth = headers.get("authorization") or headers.get("Authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:]
    return auth  # non-Bearer value — pass through as-is


class AgentOrchestrator:
    """Routes AG-UI requests to the appropriate ``ag_ui_strands.StrandsAgent``.

    Instead of holding pre-created agents, the orchestrator stores *factory*
    functions.  Each factory returns a ``StrandsAgentCore`` with an httpx
    client configured with :class:`~utils.obo_context.OboAuth`.

    Before each ``run()`` the orchestrator calls ``OboAuth.set_token()`` on
    the agent's auth instance.  The token is stored behind a
    ``threading.Lock``, so it is visible to the MCP client's background
    thread where httpx requests are actually executed.
    """

    def __init__(self, router: PageContextRouter) -> None:
        self._agent_factories: dict[str, dict[str, Any]] = {}
        self._cached_agui_agents: dict[str, AGUIStrandsAgent] = {}
        self._router = router

    def register_agent_factory(
        self,
        name: str,
        factory: AgentFactory,
        description: str = "",
        config: StrandsAgentConfig | None = None,
    ) -> None:
        """Register an agent factory for on-demand agent creation.

        Args:
            name: Unique agent name (must match registry name).
            factory: Callable that returns a pre-configured Strands Agent.
            description: Human-readable description.
            config: Optional tool-behavior configuration.
        """
        self._agent_factories[name] = {
            "factory": factory,
            "description": description,
            "config": config,
        }
        log_info_event(
            logger,
            f"Registered agent factory '{name}' in orchestrator",
            "orchestrator.agent_factory_registered",
            agent_name=name,
        )

    async def run(
        self,
        input_data: RunAgentInput,
        agent_name: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[Any]:
        """Yield AG-UI events for *input_data*.

        If *agent_name* is ``None`` the orchestrator extracts ``page_context``
        from *input_data* and uses :class:`PageContextRouter` to resolve the
        target agent.

        Before yielding events the OBO token from *headers* is set on the
        agent's :class:`~utils.obo_context.OboAuth` instance via
        ``set_token()``.  The token is stored behind a ``threading.Lock`` so
        it is accessible from the MCP client's background thread where httpx
        requests are executed.

        Args:
            input_data: AG-UI ``RunAgentInput``.
            agent_name: Explicit agent name (skips routing).
            headers: Optional HTTP headers forwarded from the Dashboards
                request (e.g. ``Authorization: Bearer <obo-token>``).

        Yields:
            AG-UI protocol events.
        """
        if agent_name is None:
            page_context = _extract_page_context(input_data)
            registration = self._router.route(page_context)
            agent_name = registration.name
            log_debug_event(
                logger,
                f"Routed page_context='{page_context}' -> agent='{agent_name}'",
                "orchestrator.routed",
                page_context=page_context,
                agent_name=agent_name,
            )

        factory_info = self._agent_factories.get(agent_name)
        if factory_info is None:
            raise RuntimeError(
                f"No agent factory registered with name '{agent_name}'. "
                f"Available: {list(self._agent_factories)}"
            )

        # Reuse a cached AGUIStrandsAgent so that its _agents_by_thread dict
        # (and the Strands ConversationManager inside each per-thread agent)
        # persists across requests — giving the agent conversation memory.
        agui_agent = self._cached_agui_agents.get(agent_name)
        if agui_agent is None:
            strands_agent = factory_info["factory"]()
            agui_agent = AGUIStrandsAgent(
                agent=strands_agent,
                name=agent_name,
                description=factory_info["description"],
                config=factory_info["config"],
            )
            # Keep the MCP client reference on the wrapper to prevent GC
            # from closing the MCP session.
            mcp_ref = getattr(strands_agent, "_mcp_client", None)
            if mcp_ref is not None:
                agui_agent._mcp_client = mcp_ref
            # Keep the OboAuth instance so we can call set_token() on
            # subsequent requests.
            obo_auth = getattr(strands_agent, "_obo_auth", None)
            if obo_auth is not None:
                agui_agent._obo_auth = obo_auth
            self._cached_agui_agents[agent_name] = agui_agent
            log_debug_event(
                logger,
                f"Created and cached agent '{agent_name}'",
                "orchestrator.agent_created",
                agent_name=agent_name,
            )
        else:
            log_debug_event(
                logger,
                f"Reusing cached agent '{agent_name}'",
                "orchestrator.agent_reused",
                agent_name=agent_name,
            )

        # Set the OBO token on the agent's OboAuth instance.  This is
        # thread-safe (lock-protected) and visible to the MCP client's
        # background thread where httpx requests are actually executed.
        token = _extract_bearer_token(headers)
        obo_auth = getattr(agui_agent, "_obo_auth", None)
        if obo_auth is not None:
            obo_auth.set_token(token)

        # Install a SHARED MUTABLE container for the inner-pipeline
        # usage. The pipeline (running inside Strands' per-tool context
        # copy) mutates this dict instead of rebinding the ContextVar —
        # rebinding inside a child context would not propagate back to
        # us. See ``utils.token_usage_context`` for the full rationale.
        usage_token = install_inner_usage_container()
        outer_before = _outer_accumulated_usage(agui_agent, input_data.thread_id)
        try:
            # Hold the terminal RunFinishedEvent so we can splice in a
            # ``CustomEvent`` carrying the run's full token usage right
            # before the run closes — that ordering lets clients receive
            # usage in-band on the same SSE stream without a second
            # request.
            pending_finished = None
            async for event in agui_agent.run(input_data):
                if getattr(event, "type", None) == EventType.RUN_FINISHED:
                    pending_finished = event
                    continue
                yield event
            outer_after = _outer_accumulated_usage(agui_agent, input_data.thread_id)
            inner_payload = get_inner_usage() if inner_usage_is_present() else None
            token_usage_event = _build_token_usage_event(
                agent_name=agent_name,
                outer_before=outer_before,
                outer_after=outer_after,
                inner=inner_payload,
            )
            if token_usage_event is not None:
                yield token_usage_event
            if pending_finished is not None:
                yield pending_finished
        finally:
            uninstall_inner_usage_container(usage_token)


def _outer_accumulated_usage(
    agui_agent: AGUIStrandsAgent, thread_id: str
) -> dict[str, int]:
    """Snapshot the outer Strands agent's accumulated token usage.

    Each thread keeps its own ``StrandsAgentCore`` inside
    ``ag_ui_strands.StrandsAgent``; that core's
    ``event_loop_metrics.accumulated_usage`` accumulates across every
    request on the thread. Snapshot before / after the run and subtract
    to attribute usage to *this* run only.

    Returns zeros if the agent or its metrics are not yet materialized
    (the per-thread agent is created lazily on first request, so the
    "before" snapshot of a brand-new thread has nothing to read).
    """
    agents_by_thread = getattr(agui_agent, "_agents_by_thread", {}) or {}
    core = agents_by_thread.get(thread_id)
    metrics = getattr(core, "event_loop_metrics", None) if core is not None else None
    usage = getattr(metrics, "accumulated_usage", None) if metrics is not None else None
    if not usage:
        return {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
    return {
        "input": int(usage.get("inputTokens", 0)),
        "output": int(usage.get("outputTokens", 0)),
        "cache_read": int(usage.get("cacheReadInputTokens", 0)),
        "cache_write": int(usage.get("cacheWriteInputTokens", 0)),
    }


def _build_token_usage_event(
    *,
    agent_name: str,
    outer_before: dict[str, int],
    outer_after: dict[str, int],
    inner: dict[str, Any] | None,
) -> CustomEvent | None:
    """Build the ``token_usage`` CustomEvent emitted before RUN_FINISHED.

    The outer agent's contribution is the delta of its accumulated
    counters across this run. The inner contribution (currently only
    published by the PER pipeline) is added on top so the event reports
    every Bedrock token spent on behalf of the user — including
    sub-agent calls that never surface as outer SSE events.

    Returns ``None`` only when no metrics were captured at all (e.g. an
    agent type that hasn't run any LLM calls yet); callers fall back to
    plain RUN_FINISHED in that case.
    """
    outer_delta = {
        k: max(0, outer_after.get(k, 0) - outer_before.get(k, 0))
        for k in ("input", "output", "cache_read", "cache_write")
    }
    inner_normalized = {
        "input": int((inner or {}).get("input", 0)),
        "output": int((inner or {}).get("output", 0)),
        "cache_read": int((inner or {}).get("cache_read", 0)),
        "cache_write": int((inner or {}).get("cache_write", 0)),
    }
    total = {
        k: outer_delta[k] + inner_normalized[k]
        for k in ("input", "output", "cache_read", "cache_write")
    }
    if all(v == 0 for v in total.values()):
        return None
    value: dict[str, Any] = {
        "schema_version": 1,
        "agent": agent_name,
        "input": total["input"],
        "output": total["output"],
        "cache_read": total["cache_read"],
        "cache_write": total["cache_write"],
        "outer": outer_delta,
        "inner": inner_normalized,
    }
    # Forward the PER pipeline's per-phase / cache-hit breakdown when
    # present — runners that only want the headline four numbers can
    # ignore these, while ones that care about plan-vs-execute cost get
    # the full picture without scraping HTML comments out of the report.
    if inner:
        for key in ("by_phase", "phase_calls", "cache_hit_ratio", "per_round"):
            if key in inner:
                value[key] = inner[key]
    return CustomEvent(type=EventType.CUSTOM, name="token_usage", value=value)

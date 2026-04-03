"""Default Agent — General OpenSearch Assistant.

A simple Strands agent with all OpenSearch MCP Server tools.
Handles general queries when no specialized sub-agent matches the page context.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from mcp.client.streamable_http import streamable_http_client
from strands import Agent
from strands.tools.mcp import MCPClient

from server.constants import DEFAULT_MCP_SERVER_URL
from utils.logging_helpers import get_logger, log_info_event

logger = get_logger(__name__)

DEFAULT_SYSTEM_PROMPT = """You are a helpful OpenSearch assistant. You help users understand
and manage their OpenSearch clusters.

You have access to OpenSearch tools via the MCP Server. Use them to answer questions about:
- Cluster health and status
- Index management (list, create, delete, mappings)
- Searching and querying indices
- Cluster settings and configuration
- Node and shard information

When answering:
- Use the available tools to fetch real data from OpenSearch
- Present results clearly and concisely
- If a tool call fails, explain what went wrong and suggest alternatives
- If you don't have the right tool for a request, explain what's available
"""


class _MutableHeaders:
    """A mutable container for HTTP headers that can patch a live httpx client.

    The MCPClient captures a transport factory via closure.  By referencing
    this mutable object instead of a plain dict, the orchestrator can swap
    in fresh credentials (e.g. a new OBO token) on every request.

    When ``httpx_client`` is set (after the first MCP session starts), the
    ``update()`` method patches the live client's default headers in-place —
    no MCP restart required.
    """

    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = dict(headers) if headers else {}
        self.httpx_client: Any = None  # set after MCP session starts

    def update(self, headers: dict[str, str] | None) -> None:
        new = dict(headers) if headers else {}
        self.headers = new
        # Patch the live httpx client so the current MCP session picks up
        # the new token immediately — no stop/start cycle needed.
        if self.httpx_client is not None:
            for key, value in new.items():
                self.httpx_client.headers[key] = value


def create_default_agent(
    opensearch_url: str, headers: dict[str, str] | None = None
) -> Agent:
    """Create the default agent with all OpenSearch MCP tools.

    Connects to the OpenSearch MCP server via Streamable HTTP transport.
    The server URL defaults to ``http://localhost:3001/mcp`` and can be
    overridden with the ``MCP_SERVER_URL`` environment variable.

    Args:
        opensearch_url: OpenSearch cluster URL (informational — the MCP
            server is assumed to already be configured for this cluster).
        headers: Optional HTTP headers to forward to the MCP server
            (e.g. Authorization for OpenSearch authentication).

    Returns:
        Configured Strands Agent with MCP tools.
    """
    mcp_server_url = os.getenv("MCP_SERVER_URL", DEFAULT_MCP_SERVER_URL)

    # Use a mutable header container so the orchestrator can update
    # credentials (e.g. OBO token) on every request without recreating
    # the agent or MCP client.
    mutable_headers = _MutableHeaders(headers)

    # Create the httpx client ourselves so we can patch its default headers
    # in-place when the OBO token is refreshed.  This avoids restarting the
    # MCP session on every request.  Using ``streamable_http_client`` (not
    # the deprecated ``streamablehttp_client``) and passing ``http_client``
    # externally means the MCP SDK won't close it when the session ends.
    http_client = httpx.AsyncClient(
        headers=mutable_headers.headers or {},
        timeout=httpx.Timeout(30, read=300),
        verify=False,
        follow_redirects=True,
    )
    mutable_headers.httpx_client = http_client

    mcp_client = MCPClient(
        lambda: streamable_http_client(mcp_server_url, http_client=http_client)
    )
    mcp_client.start()

    tools = list(mcp_client.list_tools_sync())

    agent = Agent(
        system_prompt=DEFAULT_SYSTEM_PROMPT,
        tools=tools,
    )

    # Keep a reference to the MCPClient on the agent to prevent garbage
    # collection from closing the session.  When MCPClient is passed
    # directly to Agent(tools=...) it is registered as a ToolProvider with
    # consumer tracking; the AGUIStrandsAgent wrapper later extracts
    # resolved tools and the original Agent may be GC'd, triggering
    # remove_consumer → stop() which kills the MCP session for subsequent
    # runs.  By starting the client manually and passing resolved tools we
    # avoid the ToolProvider lifecycle entirely.
    agent._mcp_client = mcp_client  # prevent GC
    agent._mutable_headers = mutable_headers  # expose for header refresh

    tool_count = len(agent.tool_registry.registry)
    log_info_event(
        logger,
        f"Default agent initialized with {tool_count} MCP tools "
        f"(server={mcp_server_url}).",
        "default_agent.initialized",
        tool_count=tool_count,
        mcp_server_url=mcp_server_url,
        opensearch_url=opensearch_url,
    )

    return agent

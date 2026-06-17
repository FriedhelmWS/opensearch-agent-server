"""Default Agent — General OpenSearch Assistant.

A simple Strands agent with all OpenSearch MCP Server tools.
Handles general queries when no specialized sub-agent matches the page context.
"""

from __future__ import annotations

import os

import boto3
import httpx
from mcp.client.streamable_http import streamable_http_client
from strands import Agent
from strands.models.bedrock import BedrockModel
from strands.models.model import CacheConfig
from strands.tools.mcp import MCPClient

from agents.skills_loader import (
    LoggingAgentSkills,
    load_all_skills as _load_all_skills,
)
from server.constants import DEFAULT_MCP_SERVER_URL
from utils.logging_helpers import get_logger, log_info_event
from utils.obo_context import OboAuth

# Fallback model when ``BEDROCK_DEFAULT_AGENT_MODEL_ARN`` is unset.
# Mirrors the PER agent's fallback so a misconfigured deployment
# behaves consistently across agents.
_DEFAULT_BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

logger = get_logger(__name__)


DEFAULT_SYSTEM_PROMPT = """You are a helpful OpenSearch assistant. You help users understand
and manage their OpenSearch clusters.

You have access to OpenSearch tools via the MCP Server. Use them to answer questions about:
- Cluster health and status
- Index management (list, create, delete, mappings)
- Searching and querying indices
- Cluster settings and configuration
- Node and shard information

You also have access to domain-specific skills that provide reference documentation
and guidance for specialized tasks. Consult available skills when users need help
with specific OpenSearch features or query languages.

When answering:
- Use the available tools to fetch real data from OpenSearch
- Present results clearly and concisely
- If a tool call fails, explain what went wrong and suggest alternatives
- If you don't have the right tool for a request, explain what's available
- Consult available skills for specialized guidance and reference documentation
"""


def create_default_agent(opensearch_url: str) -> Agent:
    """Create the default agent with all OpenSearch MCP tools and skills.

    Connects to the OpenSearch MCP server via Streamable HTTP transport.
    The server URL defaults to ``http://localhost:3001/mcp`` and can be
    overridden with the ``MCP_SERVER_URL`` environment variable.

    Auto-discovers and loads all skills from the ``skills/`` directory.
    Each subdirectory with a ``SKILL.md`` file is loaded as a skill using
    the Strands SDK ``AgentSkills`` plugin.

    Authentication is handled by :class:`~utils.obo_context.OboAuth`.
    The orchestrator calls ``obo_auth.set_token()`` before each run to
    inject the OBO token.  The token is stored behind a threading lock
    so it is accessible from the MCP client's background thread.

    Args:
        opensearch_url: OpenSearch cluster URL (informational — the MCP
            server is assumed to already be configured for this cluster).

    Returns:
        Configured Strands Agent with MCP tools and skills.
    """
    mcp_server_url = os.getenv("MCP_SERVER_URL", DEFAULT_MCP_SERVER_URL)

    # OboAuth injects the OBO token into every outgoing httpx request.
    # The token is set by the orchestrator before each agent run via
    # set_token() and stored behind a threading.Lock — so the MCP
    # client's background thread can read it safely.
    obo_auth = OboAuth()
    http_client = httpx.AsyncClient(
        auth=obo_auth,
        timeout=httpx.Timeout(30, read=300),
        verify=False,
        follow_redirects=True,
    )

    mcp_client = MCPClient(
        lambda: streamable_http_client(mcp_server_url, http_client=http_client)
    )
    mcp_client.start()

    tools = list(mcp_client.list_tools_sync())

    # Auto-discover and load all skills from skills/ directory
    skills = _load_all_skills(caller="default_agent")

    # Prepare plugins list with AgentSkills if skills are available
    plugins = []
    if skills:
        agent_skills_plugin = LoggingAgentSkills(caller="default_agent", skills=skills)
        plugins.append(agent_skills_plugin)
        log_info_event(
            logger,
            f"Registering {len(skills)} skill(s) with default agent",
            "default_agent.skills_registered",
            skill_count=len(skills),
            skill_names=[s.name for s in skills],
        )

    # Bedrock model with prompt caching enabled. ``cache_tools="default"``
    # injects a cache breakpoint right after the tool schema, which
    # implicitly covers the system prompt (everything before the
    # breakpoint becomes the cached prefix). ``cache_config(strategy=
    # "auto")`` lets Strands also place breakpoints inside the
    # conversation so large tool results — e.g. an OpenSearch
    # ``SearchIndexTool`` response of ~250K tokens — are paid for once
    # and read from cache on every subsequent turn at ~10% of the input
    # price. Without this, a 22-turn investigation re-bills every prior
    # tool result on every turn; we observed cases consuming >1M input
    # tokens for that reason.
    #
    # Model ID resolution: read ``BEDROCK_INFERENCE_PROFILE_ARN`` —
    # the same env var the PER planner / reflector reads — so both
    # agents run on the same inference profile by default. Falls back
    # to the bundled default when the env is unset.
    # ``temperature`` is deliberately omitted — newer Claude inference
    # profiles on Bedrock reject the parameter.
    model_id = os.getenv("BEDROCK_INFERENCE_PROFILE_ARN") or _DEFAULT_BEDROCK_MODEL_ID
    bedrock_model = BedrockModel(
        model_id=model_id,
        boto_session=boto3.Session(),
        streaming=True,
        max_tokens=32768,
        cache_tools="default",
        cache_config=CacheConfig(strategy="auto"),
    )

    # Create agent with MCP tools and skills plugin
    agent = Agent(
        model=bedrock_model,
        system_prompt=DEFAULT_SYSTEM_PROMPT,
        tools=tools,
        plugins=plugins,
    )

    # Keep references to prevent GC from closing the MCP session and
    # to allow the orchestrator to set tokens on subsequent requests.
    agent._mcp_client = mcp_client  # prevent GC
    agent._obo_auth = obo_auth  # expose for token refresh

    tool_count = len(agent.tool_registry.registry)
    log_info_event(
        logger,
        f"Default agent initialized with {tool_count} MCP tools "
        f"(server={mcp_server_url}, model={model_id}, prompt_cache=on).",
        "default_agent.initialized",
        tool_count=tool_count,
        mcp_server_url=mcp_server_url,
        opensearch_url=opensearch_url,
        model_id=model_id,
        prompt_cache=True,
    )

    return agent

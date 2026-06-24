"""PER Agent — Plan / Execute / Reflect root-cause analysis for OpenSearch.

Behavioral parity with ml-commons ``MLPlanExecuteAndReflectAgentRunner``:
  - planner / reflector emit ``{"steps": [...], "result": "..."}`` JSON;
    a non-empty ``result`` field signals completion (loop terminates),
    otherwise the first step in ``steps`` is dispatched to the executor;
  - executor system prompt mirrors ``EXECUTOR_RESPONSIBILITY``;
  - default ``max_steps`` matches the Java default (``20``).

Loop topology (transparent Python orchestration — no opaque Graph):

    plan ──► execute ──► reflect ──┐
              ▲                    │
              └────────────────────┘  (while result == "")

Each phase is invoked directly via ``agent.invoke_async()`` so we control:
  - what context is fed to the reflector (compact summaries of older
    steps + full findings of the latest step — see ``ArtifactStore``);
  - per-phase logging of timing and token usage (observability).

Each executor finding is captured in an :class:`ArtifactStore` so subsequent
reflect prompts stay bounded as the loop iterates — older artifacts collapse
to ``[id, intent, summary]`` rows while the latest is shown in full.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

import boto3
import httpx
from mcp.client.streamable_http import streamable_http_client
from strands import Agent
from strands.models.bedrock import BedrockModel
from strands.models.model import CacheConfig
from strands.tools.mcp import MCPClient

from agents.per.artifact_store import ArtifactStore
from agents.per.sub_agents import (
    build_execute_agent,
    build_plan_agent,
    build_reflect_agent,
    set_mcp_client,
    set_skills,
)
from agents.skills_loader import load_all_skills
from server.constants import DEFAULT_MCP_SERVER_URL
from utils.logging_helpers import get_logger, log_info_event, log_warning_event
from utils.monitored_tool import monitored_tool
from utils.obo_context import OboAuth
from utils.token_usage_context import publish_inner_usage

logger = get_logger(__name__)

_DEFAULT_BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Mirrors DEFAULT_MAX_STEPS_EXECUTED in MLPlanExecuteAndReflectAgentRunner.java.
_MAX_STEPS = 20


@dataclass(frozen=True)
class PhaseEvent:
    """One observation point in the PER pipeline.

    Emitted to a caller-supplied ``on_phase`` callback after each
    plan / execute / reflect sub-agent invocation, plus a final
    ``"final"`` event carrying the pipeline's return string. Pure data
    — the callback decides what (if anything) to persist.

    ``input`` and ``response`` are the prompt and raw text the sub-agent
    saw / produced. For ``phase == "final"``, ``input`` is the original
    problem statement and ``response`` is the pipeline return value.

    ``step_index`` is 0 for plan, 1..N for each execute step, and the
    matching reflect call uses the same index as the last execute step
    in its batch (mirrors the existing ``_run_phase`` numbering).
    """

    phase: Literal[
        "plan",
        "execute_start",  # step dispatched, executor sub-agent now running
        "execute",        # step finished — final response from executor
        "execute_inner",  # LLM call / tool call inside an execute step
        "reflect",
        "final",
    ]
    step_index: int
    input: str
    response: str
    # For ``execute_inner`` events, names what the inner activity was:
    #   - ``"llm"``                        a model call
    #   - ``"tool:<tool_name>"``           a tool invocation
    # ``None`` for the outer phases (plan / execute / reflect / final).
    inner_origin: str | None = None


OnPhase = Callable[[PhaseEvent], Awaitable[None]]
"""Optional async hook fired after each PER phase. ``None`` = no-op."""

ORCHESTRATOR_SYSTEM_PROMPT = """You are an OpenSearch root-cause-analysis assistant
for the Explore page.

When the user reports an OpenSearch observability issue (slow query, high
latency, red cluster, ingestion lag, error spikes, log/trace anomalies,
etc.), call the `run_per_pipeline` tool ONCE with a concise problem
statement that captures:
  - the symptom
  - the affected index / component (if known)
  - the time window (if known)

The tool returns a comprehensive RCA report. Return that report to the
user verbatim — do NOT rewrite, summarize, paraphrase, condense, or
reformat it. Do NOT prepend or append commentary, mitigation suggestions,
or follow-up questions. The pipeline's report is already the deliverable;
re-processing it doubles token consumption and risks truncating the
output. Your only job is to faithfully relay the tool's return value.

If the tool returns an error message (e.g. "Max Steps Limit Reached" or
"Planner produced no actionable plan"), surface that message verbatim as
well so the user can see what happened.
"""


def _extract_json_blob(text: str) -> dict | None:
    """Best-effort extraction of the last JSON object in a string.

    Mirrors ``extractJsonFromMarkdown`` in the Java runner: tolerates
    ```json fences and unwrapped JSON, and falls back to substring between
    the first ``{`` and the last ``}``.
    """
    if not text:
        return None
    fence_match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence_match:
        try:
            return json.loads(fence_match.group(1))
        except json.JSONDecodeError:
            pass
    matches = list(re.finditer(r"\{.*\}", text, re.DOTALL))
    for match in reversed(matches):
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
    return None


def _parse_planner_output(text: str) -> tuple[list[str], str]:
    """Return ``(steps, result)`` from a planner JSON response.

    Falls back to ``([], "")`` on unparseable output so the loop can decide
    to terminate fail-closed rather than spin on garbage.
    """
    parsed = _extract_json_blob(text)
    if not parsed:
        return [], ""
    steps_raw = parsed.get("steps") or []
    steps = [str(s) for s in steps_raw if isinstance(s, str)]
    result = parsed.get("result")
    result = result.strip() if isinstance(result, str) else ""
    return steps, result


def _parse_reflect_output(text: str) -> tuple[list[str], str]:
    """Return ``(next_steps, result)`` from a reflect JSON response.

    The reflect schema is a diff: a list of next steps to dispatch
    (``len > 1`` means independent and OK to fan out in parallel), or a
    populated ``result`` to terminate. Returns ``([], "")`` on unparseable
    output so the caller can decide to abort.

    Tolerates a legacy ``next_step`` (singular string) field for forward
    compatibility — earlier reflect prompts emitted that shape.
    """
    parsed = _extract_json_blob(text)
    if not parsed:
        return [], ""
    raw_steps = parsed.get("next_steps")
    steps: list[str] = []
    if isinstance(raw_steps, list):
        steps = [s.strip() for s in raw_steps if isinstance(s, str) and s.strip()]
    elif isinstance(raw_steps, str) and raw_steps.strip():
        steps = [raw_steps.strip()]
    else:
        legacy = parsed.get("next_step")
        if isinstance(legacy, str) and legacy.strip():
            steps = [legacy.strip()]
    result = parsed.get("result")
    result = result.strip() if isinstance(result, str) else ""
    return steps, result


_FACTS_HEADER_RE = re.compile(r"(?im)^\s*KNOWN_FACTS\s*:\s*$")
_QUERIES_HEADER_RE = re.compile(r"(?im)^\s*QUERIES_EXECUTED\s*:\s*$")


def _parse_bullet_section(text: str, start: int) -> list[str]:
    """Read consecutive bullet lines starting at ``text[start:]``.

    Bullets may begin with ``-`` or ``*``. Empty lines are skipped. The
    first non-bullet, non-empty line ends the section. Lines reading
    ``- (none)`` are treated as an explicit "no entries" marker.
    """
    items: list[str] = []
    for raw in text[start:].splitlines():
        line = raw.strip()
        if not line:
            continue
        if line[:1] not in {"-", "*"}:
            break
        item = line.lstrip("-* ").strip()
        if not item or item.lower() == "(none)":
            continue
        items.append(item)
    return items


def _extract_sections(findings: str) -> tuple[str, list[str], list[str]]:
    """Split executor output into ``(findings_head, queries, facts)``.

    Both ``QUERIES_EXECUTED:`` and ``KNOWN_FACTS:`` are required by the
    executor system prompt. Whichever appears first marks the end of the
    free-text findings; subsequent bullet sections are parsed in place.
    Missing headers degrade gracefully — the corresponding list is empty
    and the full text is preserved as findings.
    """
    if not findings:
        return findings, [], []
    queries_match = _QUERIES_HEADER_RE.search(findings)
    facts_match = _FACTS_HEADER_RE.search(findings)
    head_end = len(findings)
    if queries_match:
        head_end = min(head_end, queries_match.start())
    if facts_match:
        head_end = min(head_end, facts_match.start())
    head = findings[:head_end].rstrip()
    queries = _parse_bullet_section(findings, queries_match.end()) if queries_match else []
    facts = _parse_bullet_section(findings, facts_match.end()) if facts_match else []
    return head, queries, facts


def _extract_facts(findings: str) -> tuple[str, list[str]]:
    """Backwards-compatible shim returning just findings + facts."""
    head, _queries, facts = _extract_sections(findings)
    return head, facts


_QUOTED_TOKEN_RE = re.compile(r"['\"`]([^'\"`\n]{2,})['\"`]")
_BARE_IDENT_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9_.-]{2,})\b")
# Generic words that recur in plan steps but don't distinguish what the
# step is *about*. These would produce false positives if treated as
# coverage signals (they appear in nearly every step or finding).
_COVERAGE_STOPWORDS = frozenset(
    {
        # articles / pronouns / connectives
        "the", "and", "for", "with", "from", "into", "this", "that",
        "these", "those", "their", "each", "all", "any", "some", "such",
        "than", "then", "between", "before", "after", "across", "based",
        # generic verbs
        "use", "using", "used", "run", "running", "execute", "executed",
        "perform", "performs", "performed", "include", "includes",
        "compute", "identify", "discover", "confirm", "check", "ensure",
        "verify", "match", "matches", "matched", "find", "finds",
        "found", "return", "returns", "returned", "reason", "rank",
        "compile", "summarize", "summarise", "produce", "generate",
        "retrieve", "retrieves", "fetch", "fetches", "show", "list",
        "lists", "inspect", "examine", "analyze", "analyse", "compare",
        "over", "above", "below",
        # generic nouns about plan structure / pipeline
        "step", "steps", "tool", "tools", "plan", "task", "tasks",
        "objective", "input", "inputs", "output", "outputs", "context",
        "request", "response", "report", "reports", "summary",
        "summaries", "conclusion", "final",
        # generic data-shape nouns
        "data", "source", "sources", "field", "fields", "document",
        "documents", "result", "results", "value", "values", "name",
        "names", "schema", "mapping", "mappings", "structure", "format",
        "type", "types", "size", "count", "counts", "total", "totals",
        "service", "services", "entity", "entities", "object", "objects",
        # generic action / aggregation nouns
        "query", "queries", "search", "searches", "filter", "filters",
        "aggregate", "aggregates", "aggregation", "aggregations",
        "sample", "samples", "buckets", "bucket", "histogram",
        # generic time / scope nouns
        "window", "windows", "range", "ranges", "interval", "period",
        "first", "next", "last", "previous",
        # generic ordering / superlatives
        "top", "highest", "lowest", "more", "most", "less", "least",
        # misc
        "high", "low", "level", "levels",
    }
)


def _coverage_tokens(text: str) -> set[str]:
    """Extract distinguishing tokens from a plan step.

    Returns a lower-cased set of tokens likely to identify what the step
    is *specifically about* (which index, which field, which metric, etc.).
    Quoted tokens are always kept (plan steps tend to put concrete
    identifiers in quotes). Bare identifiers are kept if they aren't in
    the stopword list and are at least 3 characters — generic English
    plumbing words are filtered out so that incidental mentions of
    "search", "index", "tool", etc. don't satisfy coverage.

    Steps consisting entirely of generic action verbs (e.g.
    ``"Reason over results and rank services"``) yield an empty set, in
    which case the caller treats the step as un-checkable rather than
    flagging it.
    """
    tokens: set[str] = set()
    if not text:
        return tokens
    for match in _QUOTED_TOKEN_RE.finditer(text):
        token = match.group(1).strip().lower()
        if len(token) >= 3:
            tokens.add(token)
    for match in _BARE_IDENT_RE.finditer(text):
        token = match.group(1).strip().lower()
        if len(token) < 3:
            continue
        if token in _COVERAGE_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


_PLAN_COVERAGE_THRESHOLD = 0.6


def _untouched_plan_steps(
    plan_steps: list[str], artifacts: ArtifactStore
) -> list[tuple[int, str, set[str]]]:
    """Return plan steps whose distinctive tokens have not been queried.

    For each plan step we extract ``coverage_tokens`` and compute the
    fraction that appears in the artifact store's QUERIES_EXECUTED rows
    (preferred) and step intents. A step is "untouched" if fewer than
    ``_PLAN_COVERAGE_THRESHOLD`` of its distinctive tokens are present.

    Important: we deliberately do NOT include free-text findings or
    KNOWN_FACTS in the coverage haystack. Those text blobs reflect what
    was *discovered* (e.g., listing every metric field name during
    schema inspection) rather than what was *queried*, and including
    them produces false negatives where a downstream step's identifiers
    happen to appear in an upstream step's schema dump. The
    QUERIES_EXECUTED section is the executor's structured record of
    what it actually probed, which is the right signal here.

    Returns a list of ``(plan_index_1based, step_text, missing_tokens)``.
    Steps with no distinctive tokens (purely abstract, e.g. "Reason over
    results") are skipped — we cannot deterministically tell whether they
    were addressed, so we err toward NOT flagging them.
    """
    if not plan_steps:
        return []
    haystack_parts: list[str] = []
    for artifact in artifacts:
        haystack_parts.append(artifact.step_intent or "")
        haystack_parts.extend(artifact.queries)
    haystack = "\n".join(haystack_parts).lower()
    missing: list[tuple[int, str, set[str]]] = []
    for idx, step in enumerate(plan_steps, start=1):
        tokens = _coverage_tokens(step)
        if not tokens:
            continue
        present = {t for t in tokens if t in haystack}
        coverage = len(present) / len(tokens)
        if coverage < _PLAN_COVERAGE_THRESHOLD:
            missing.append((idx, step, tokens - present))
    return missing


def _usage_tokens(result) -> tuple[int, int]:
    """Pull ``(input, output)`` token counts off an ``AgentResult``.

    Returns ``(0, 0)`` if metrics are unavailable so the caller never has
    to defend against missing telemetry.
    """
    usage = getattr(getattr(result, "metrics", None), "accumulated_usage", None)
    if not usage:
        return 0, 0
    return int(usage.get("inputTokens", 0)), int(usage.get("outputTokens", 0))


def _full_usage(result) -> dict[str, int]:
    """Pull the full Bedrock usage record off an ``AgentResult``.

    Returns input / output plus cache read / write tokens. The cache
    fields are produced by Bedrock when prompt caching is in effect (PER
    sub-agents enable it via ``cache_tools`` / ``cache_config``). Without
    them, downstream token accounting under-counts cached prefixes.
    """
    usage = getattr(getattr(result, "metrics", None), "accumulated_usage", None)
    if not usage:
        return {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
    return {
        "input": int(usage.get("inputTokens", 0)),
        "output": int(usage.get("outputTokens", 0)),
        "cache_read": int(usage.get("cacheReadInputTokens", 0)),
        "cache_write": int(usage.get("cacheWriteInputTokens", 0)),
    }


def _build_reflect_input(
    objective: str,
    plan_steps: list[str],
    artifacts: ArtifactStore,
    latest_n: int = 1,
) -> str:
    """Assemble the reflect-phase prompt with bounded context.

    Sections:
      - ``Objective`` and the original plan (informational only).
      - ``Completed steps``: ``[id] (step N) intent :: summary`` rows.
      - ``KNOWN_FACTS`` (full fidelity): the durable structured facts each
        executor recorded — never truncated, since this is the persistent
        memory that prevents re-running discovery.
      - ``Most recent step (full findings)``: untruncated tail for the last
        artifact only.
    """
    sections = [f"Objective:\n{objective}"]

    if plan_steps:
        plan_block = "\n".join(f"- {step}" for step in plan_steps)
        sections.append(
            f"Original plan (for context only — do not echo or restate):\n{plan_block}"
        )

    compact = artifacts.compact_table()
    if compact:
        sections.append(f"Completed steps (summary):\n{compact}")

    queries = artifacts.all_queries()
    if queries:
        sections.append(
            "QUERIES_EXECUTED (what was ACTUALLY probed — use this to detect "
            "scope-narrowing, not free-text findings):\n" + queries
        )

    facts = artifacts.all_facts()
    if facts:
        sections.append(f"KNOWN_FACTS (durable findings — do NOT rediscover):\n{facts}")

    latest = artifacts.full_findings(last_n=latest_n)
    if latest:
        header = (
            "Most recent step (full findings):"
            if latest_n == 1
            else f"Most recent {latest_n} steps (full findings — completed as a parallel batch):"
        )
        sections.append(f"{header}\n{latest}")

    untouched = _untouched_plan_steps(plan_steps, artifacts)
    if untouched:
        lines = [f"- (plan step {idx}) {step}" for idx, step, _ in untouched]
        sections.append(
            "PLAN COVERAGE WARNING — the following original plan step(s) "
            "have NOT been touched by any completed artifact (none of their "
            "distinctive identifiers appears in any prior step's intent, "
            "findings, or KNOWN_FACTS). Per critical rule 5, you may NOT "
            "finalize while these remain unless KNOWN_FACTS explicitly "
            "invalidates them. Either dispatch them (preferably in parallel "
            "if independent) or, in a future reflect call, justify skipping "
            "by citing the specific KNOWN_FACT that invalidates each.\n"
            + "\n".join(lines)
        )

    sections.append(
        "Output the next step(s) to execute in `next_steps` (a list — "
        "prefer parallel dispatch when steps target independent data "
        "sources; otherwise return a single-element list), or finalize "
        "with a comprehensive report in `result`. Never repeat a completed "
        "step. Never propose work that contradicts KNOWN_FACTS. Honor the "
        "PLAN COVERAGE WARNING above if present."
    )
    return "\n\n".join(sections)


def _build_execute_input(step: str, artifacts: ArtifactStore) -> str:
    """Assemble the execute-phase prompt.

    The executor is rebuilt fresh each iteration (no cross-step conversation
    memory), so KNOWN_FACTS and QUERIES_EXECUTED established by prior steps
    must be reintroduced explicitly. Without this, the executor reflexively
    re-runs index/field discovery it has no way of knowing was already done.
    """
    sections = [f"Step to execute:\n{step}"]
    facts = artifacts.all_facts()
    if facts:
        sections.append(
            "KNOWN_FACTS established by prior steps (do NOT rediscover these — "
            "use them directly):\n" + facts
        )
    queries = artifacts.all_queries()
    if queries:
        sections.append(
            "QUERIES_EXECUTED by prior steps (do NOT re-run these unless this "
            "step explicitly requires it):\n" + queries
        )
    sections.append(
        "Execute this single step and return your findings as plain text, "
        "ending with the required `QUERIES_EXECUTED:` and `KNOWN_FACTS:` sections."
    )
    return "\n\n".join(sections)


async def _run_phase(agent: Agent, prompt: str, phase: str, step_index: int):
    """Invoke a sub-agent and log timing + token usage for the phase.

    The fifth return slot is the full usage dict (input / output /
    cache_read / cache_write). The first four slots stay
    ``(result, elapsed_ms, tokens_in, tokens_out)`` so existing call
    sites that destructure the tuple keep working — only the pipeline's
    accumulator looks at slot 5.
    """
    started = time.perf_counter()
    result = await agent.invoke_async(prompt)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    usage = _full_usage(result)
    tokens_in = usage["input"]
    tokens_out = usage["output"]
    log_info_event(
        logger,
        f"[per] {phase} phase complete (step {step_index})",
        f"per.phase.{phase}",
        phase=phase,
        step_index=step_index,
        elapsed_ms=elapsed_ms,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        cache_read_tokens=usage["cache_read"],
        cache_write_tokens=usage["cache_write"],
    )
    return result, elapsed_ms, tokens_in, tokens_out, usage


async def run_per_pipeline_core(
    problem_statement: str,
    *,
    on_phase: OnPhase | None = None,
    extra_reflect_system_prompt: str | None = None,
) -> str:
    """Run the plan→execute→reflect loop and return the final report.

    Module-level entry point shared by the orchestrator's MCP tool
    (:func:`create_per_agent`) and the investigation
    :class:`~agents.investigation.per_backend.PERBackend`. Behaviour is
    identical when ``on_phase`` is ``None`` — the parameter is the only
    new surface.

    Pre-conditions: :func:`~agents.per.sub_agents.set_mcp_client` and
    :func:`~agents.per.sub_agents.set_skills` have been called (the PER
    sub-agent builders read from those module-level slots). ``create_per_agent``
    handles this during startup; tests may stub them directly.

    Args:
        problem_statement: User-supplied root-cause-analysis problem.
        on_phase: Optional async callback fired after each plan/execute/
            reflect sub-agent invocation, plus a terminal ``"final"``
            event carrying the pipeline's return string. Exceptions
            raised here are intentionally NOT caught — the caller (e.g.
            an orchestrator that needs to write a memory message)
            decides whether a hook failure should abort the pipeline.
        extra_reflect_system_prompt: Optional string appended to the
            reflect sub-agent's system prompt. Lets a caller inject
            domain-specific output requirements (e.g. structured final
            result schema for the investigation backend) without
            modifying the generic PER framework. Plan and execute
            sub-agents are unaffected.
    """
    artifacts = ArtifactStore()
    pipeline_started = time.perf_counter()

    async def _emit(
        phase: str,
        step_index: int,
        input_: str,
        response: str,
        inner_origin: str | None = None,
    ) -> None:
        if on_phase is not None:
            await on_phase(
                PhaseEvent(
                    phase=phase,  # type: ignore[arg-type]
                    step_index=step_index,
                    input=input_,
                    response=response,
                    inner_origin=inner_origin,
                )
            )

    # Bridge from Strands' Agent hooks (called synchronously, possibly
    # from the MCP background thread) to ``on_phase`` (async, runs on
    # the pipeline's event loop). We capture a reference to the
    # running loop and use ``asyncio.run_coroutine_threadsafe`` so
    # tool-call hooks fired off-thread land back on the pipeline's
    # loop. Each hook call is fire-and-forget; we don't wait for the
    # write to land before returning to Strands' tool driver.
    bridge_loop = asyncio.get_running_loop()

    def _make_inner_hook_provider(step_index: int) -> "HookProvider":
        from strands.hooks import HookProvider, HookRegistry
        from strands.hooks.events import (
            AfterModelCallEvent,
            AfterToolCallEvent,
        )

        def _schedule(coro: "asyncio.coroutines.Coroutine") -> None:
            try:
                asyncio.run_coroutine_threadsafe(coro, bridge_loop)
            except RuntimeError:
                # Loop closed — pipeline is shutting down. Drop event.
                pass

        def _on_after_model(event: AfterModelCallEvent) -> None:
            stop = event.stop_response
            if stop is None or stop.message is None:
                return
            # Concatenate text blocks of the assistant message; ignore
            # tool_use blocks (they will be reflected in the
            # AfterToolCallEvent below).
            content = stop.message.get("content") or []
            text_parts = [
                blk.get("text", "")
                for blk in content
                if isinstance(blk, dict) and "text" in blk
            ]
            response_text = "\n".join(t for t in text_parts if t)
            if not response_text:
                return
            _schedule(
                _emit(
                    "execute_inner",
                    step_index,
                    "<llm-call>",
                    response_text,
                    inner_origin="LLM",
                )
            )

        def _on_after_tool(event: AfterToolCallEvent) -> None:
            tool_name = event.tool_use.get("name", "tool")
            tool_input = event.tool_use.get("input", {})
            try:
                input_str = json.dumps(tool_input, ensure_ascii=False, default=str)
            except Exception:
                input_str = str(tool_input)
            # ToolResult.content is a list of content blocks; flatten
            # the text-bearing ones for the trace.
            result_content = event.result.get("content") or []
            output_parts = [
                blk.get("text", "")
                for blk in result_content
                if isinstance(blk, dict) and "text" in blk
            ]
            output_str = "\n".join(p for p in output_parts if p) or json.dumps(
                event.result.get("status", "ok")
            )
            _schedule(
                _emit(
                    "execute_inner",
                    step_index,
                    input_str,
                    output_str,
                    inner_origin=tool_name,
                )
            )

        class _Bridge(HookProvider):
            def register_hooks(self, registry: HookRegistry, **_: Any) -> None:
                registry.add_callback(AfterModelCallEvent, _on_after_model)
                registry.add_callback(AfterToolCallEvent, _on_after_tool)

        return _Bridge()

    usage_totals = {
        "input": 0,
        "output": 0,
        "cache_read": 0,
        "cache_write": 0,
        "phase_calls": 0,
        "by_phase": {
            "plan": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "calls": 0},
            "execute": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "calls": 0},
            "reflect": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "calls": 0},
        },
    }

    def _accumulate(usage: dict, phase: str) -> None:
        for k in ("input", "output", "cache_read", "cache_write"):
            usage_totals[k] += int(usage.get(k, 0))
        usage_totals["phase_calls"] += 1
        bucket = usage_totals["by_phase"].setdefault(
            phase, {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "calls": 0},
        )
        for k in ("input", "output", "cache_read", "cache_write"):
            bucket[k] += int(usage.get(k, 0))
        bucket["calls"] += 1

    def _publish() -> None:
        denom = usage_totals["input"] + usage_totals["cache_read"] + usage_totals["cache_write"]
        cache_hit_ratio = (
            round(usage_totals["cache_read"] / denom, 4) if denom else 0.0
        )
        publish_inner_usage(
            {
                "schema_version": 1,
                "input": usage_totals["input"],
                "output": usage_totals["output"],
                "cache_read": usage_totals["cache_read"],
                "cache_write": usage_totals["cache_write"],
                "cache_hit_ratio": cache_hit_ratio,
                "phase_calls": usage_totals["phase_calls"],
                "by_phase": usage_totals["by_phase"],
            }
        )

    log_info_event(
        logger,
        "[per] pipeline start",
        "per.pipeline.start",
        problem_statement=problem_statement[:200],
    )

    plan_agent = build_plan_agent()
    plan_result, _, _, _, plan_usage = await _run_phase(
        plan_agent, problem_statement, "plan", step_index=0
    )
    _accumulate(plan_usage, "plan")
    plan_text = str(plan_result)
    await _emit("plan", 0, problem_statement, plan_text)
    plan_steps, plan_final = _parse_planner_output(plan_text)

    if plan_final:
        log_info_event(
            logger,
            "[per] planner returned final result without execution",
            "per.pipeline.early_finish",
            phase="plan",
        )
        _publish()
        await _emit("final", 0, problem_statement, plan_final)
        return plan_final

    if not plan_steps:
        log_warning_event(
            logger,
            "[per] planner emitted no steps and no result; aborting",
            "per.pipeline.empty_plan",
        )
        _publish()
        msg = f"Planner produced no actionable plan.\n\nRaw output:\n{plan_result}"
        await _emit("final", 0, problem_statement, msg)
        return msg

    pending_steps: list[str] = [plan_steps[0]]
    last_reflect_text = ""
    steps_executed = 0

    while pending_steps and steps_executed < _MAX_STEPS:
        batch = pending_steps[: max(1, _MAX_STEPS - steps_executed)]
        batch_size = len(batch)
        batch_started = time.perf_counter()
        execute_prompts = [_build_execute_input(s, artifacts) for s in batch]
        # One hook per step so the hook callback can hard-code the
        # right step_index — Strands' hook events don't carry it.
        execute_agents = [
            build_execute_agent(
                hooks=[_make_inner_hook_provider(steps_executed + 1 + i)]
            )
            for i in range(batch_size)
        ]

        log_info_event(
            logger,
            f"[per] dispatching execute batch of {batch_size}",
            "per.pipeline.execute_batch_dispatch",
            batch_size=batch_size,
            first_step_index=steps_executed + 1,
        )

        # Emit a placeholder per step BEFORE awaiting the gather so the
        # OSD step list can show each step as "running" (spinner +
        # ``response`` empty). The real ``execute`` event below updates
        # the same row with the result and flips the UI to "done".
        for i, step_text in enumerate(batch):
            await _emit(
                "execute_start",
                steps_executed + 1 + i,
                step_text,
                "",
            )

        # Emit each step's result as soon as it returns, instead of
        # waiting for the whole batch to gather. Without this, the
        # frontend's step list jumps from 0 → batch_size in one beat
        # — visually "a bunch of steps appeared at once" even though
        # they finished at different times.
        #
        # We still run the LLM calls in parallel; only the on_phase
        # emit (and the artifacts/state mutation it depends on) is
        # serialized. ``asyncio.as_completed`` returns futures in
        # finish order; the post-processing must still write artifacts
        # in plan-step order so reflect/next-batch sees a coherent
        # state, so we collect by step_index and flush in order as
        # contiguous prefixes complete.
        emit_lock = asyncio.Lock()

        async def _execute_one(
            agent: Agent, prompt: str, step_text: str, step_index: int
        ) -> tuple[int, str, Any, int, int, int, Any]:
            outcome = await _run_phase(agent, prompt, "execute", step_index)
            return (step_index, step_text, *outcome)

        coros = [
            _execute_one(agent, prompt, step_text, steps_executed + 1 + i)
            for i, (agent, prompt, step_text) in enumerate(
                zip(execute_agents, execute_prompts, batch)
            )
        ]

        # Buffer out-of-order completions; flush in plan-step order.
        completed: dict[int, tuple] = {}
        next_to_flush = steps_executed + 1
        for fut in asyncio.as_completed(coros):
            step_index, step_text, exec_result, exec_ms, exec_in, exec_out, exec_usage = (
                await fut
            )
            completed[step_index] = (
                step_text,
                exec_result,
                exec_ms,
                exec_in,
                exec_out,
                exec_usage,
            )
            async with emit_lock:
                while next_to_flush in completed:
                    (
                        st_text,
                        ex_res,
                        ex_ms,
                        ex_in,
                        ex_out,
                        ex_usage,
                    ) = completed.pop(next_to_flush)
                    _accumulate(ex_usage, "execute")
                    raw_findings = str(ex_res)
                    findings, queries, facts = _extract_sections(raw_findings)
                    artifacts.add(
                        step_index=next_to_flush,
                        step_intent=st_text,
                        findings=findings,
                        facts=facts,
                        queries=queries,
                        tokens_in=ex_in,
                        tokens_out=ex_out,
                        elapsed_ms=ex_ms,
                    )
                    log_info_event(
                        logger,
                        f"[per] captured {len(facts)} fact(s) and {len(queries)} query(s) from step {next_to_flush}",
                        "per.pipeline.facts_captured",
                        step_index=next_to_flush,
                        fact_count=len(facts),
                        query_count=len(queries),
                    )
                    await _emit("execute", next_to_flush, st_text, raw_findings)
                    next_to_flush += 1

        steps_executed += batch_size
        batch_elapsed_ms = int((time.perf_counter() - batch_started) * 1000)
        log_info_event(
            logger,
            f"[per] execute batch complete ({batch_size} step(s))",
            "per.pipeline.execute_batch_complete",
            batch_size=batch_size,
            batch_elapsed_ms=batch_elapsed_ms,
        )

        reflect_agent = build_reflect_agent(
            extra_system_prompt=extra_reflect_system_prompt,
        )
        reflect_prompt = _build_reflect_input(
            objective=problem_statement,
            plan_steps=plan_steps,
            artifacts=artifacts,
            latest_n=batch_size,
        )
        reflect_result, _, _, _, reflect_usage = await _run_phase(
            reflect_agent, reflect_prompt, "reflect", steps_executed
        )
        _accumulate(reflect_usage, "reflect")
        last_reflect_text = str(reflect_result)
        await _emit("reflect", steps_executed, reflect_prompt, last_reflect_text)
        next_steps, final_result = _parse_reflect_output(last_reflect_text)

        if final_result:
            total_ms = int((time.perf_counter() - pipeline_started) * 1000)
            log_info_event(
                logger,
                "[per] pipeline complete",
                "per.pipeline.finish",
                steps_executed=steps_executed,
                total_elapsed_ms=total_ms,
            )
            _publish()
            await _emit("final", steps_executed, problem_statement, final_result)
            return final_result

        if not next_steps:
            log_warning_event(
                logger,
                "[per] reflector returned no next_steps and no result",
                "per.pipeline.unparseable_reflect",
                steps_executed=steps_executed,
            )
            break

        pending_steps = next_steps

    log_warning_event(
        logger,
        "[per] max steps reached without final result",
        "per.pipeline.max_steps",
        max_steps=_MAX_STEPS,
    )
    _publish()
    msg = (
        f"Max Steps Limit ({_MAX_STEPS}) Reached. Use the same conversation "
        f"to continue.\n\nLast reflection:\n{last_reflect_text or '(no reflection)'}"
    )
    await _emit("final", steps_executed, problem_statement, msg)
    return msg


_per_globals_initialized = False
_per_obo_auth: OboAuth | None = None


def init_per_globals() -> OboAuth:
    """Idempotently populate the sub_agents module-level slots.

    The plan / execute / reflect builders read MCP tools and skills
    from ``agents.per.sub_agents`` (set via ``set_mcp_client`` /
    ``set_skills``). Both the orchestrator's lazy ``create_per_agent``
    factory AND the standalone ``/per/investigations`` route need
    those slots populated — but the route bypasses the orchestrator,
    so we can't rely on ``create_per_agent`` being the trigger.

    Call this once at lifespan startup. Returns the shared
    :class:`OboAuth` so the orchestrator can attach it to the
    top-level Agent it creates later (token / header injection).
    """
    global _per_globals_initialized, _per_obo_auth
    if _per_globals_initialized:
        return _per_obo_auth  # type: ignore[return-value]

    if not os.getenv("BEDROCK_INFERENCE_PROFILE_ARN"):
        os.environ["BEDROCK_INFERENCE_PROFILE_ARN"] = _DEFAULT_BEDROCK_MODEL_ID
        log_info_event(
            logger,
            f"BEDROCK_INFERENCE_PROFILE_ARN not set, defaulting to {_DEFAULT_BEDROCK_MODEL_ID}",
            "per_agent.default_model",
            model_id=_DEFAULT_BEDROCK_MODEL_ID,
        )

    mcp_server_url = os.getenv("MCP_SERVER_URL", DEFAULT_MCP_SERVER_URL)

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
    set_mcp_client(mcp_client)

    log_info_event(
        logger,
        f"[per] MCP client created for {mcp_server_url}",
        "per_agent.mcp_created",
        mcp_server_url=mcp_server_url,
    )

    # Load skills from ``skills/`` once at startup. The PER loop rebuilds
    # plan/execute/reflect sub-agents on every iteration; ``set_skills``
    # lets each builder attach a fresh ``AgentSkills`` plugin from this
    # shared list without re-scanning the filesystem each time.
    set_skills(load_all_skills(caller="per"))

    _per_obo_auth = obo_auth
    _per_globals_initialized = True
    return obo_auth


def create_per_agent(opensearch_url: str) -> Agent:
    """Create the PER orchestrator agent.

    Initializes a single MCP connection (shared with the executor sub-agent),
    and returns the top-level orchestrator Agent. The PER pipeline itself
    is exposed as the ``run_per_pipeline`` tool, which runs a transparent
    plan → execute → reflect Python loop.

    Authentication uses :class:`~utils.obo_context.OboAuth` — the
    :class:`~server.agent_orchestrator.AgentOrchestrator` calls
    ``set_token()`` on the orchestrator's ``_obo_auth`` before each run.
    """
    log_info_event(
        logger,
        f"Initializing PER agent with OpenSearch at {opensearch_url}",
        "per_agent.initializing",
        opensearch_url=opensearch_url,
    )

    obo_auth = init_per_globals()

    @monitored_tool(
        name="run_per_pipeline",
        description=(
            "Run the plan→execute→reflect root-cause-analysis pipeline for an "
            "OpenSearch observability issue. Pass a concise problem statement "
            "(symptom, affected index/component, time window). Returns a "
            "comprehensive RCA report describing every step, findings, and "
            "the final conclusion."
        ),
    )
    async def run_per_pipeline(problem_statement: str) -> str:
        return await run_per_pipeline_core(problem_statement)

    orchestrator_model = BedrockModel(
        model_id=os.environ["BEDROCK_INFERENCE_PROFILE_ARN"],
        boto_session=boto3.Session(),
        streaming=True,
        max_tokens=32768,
        # ``cache_tools`` injects a cache point after the tool schema; the
        # cached prefix covers the system prompt automatically. We avoid
        # the deprecated ``cache_prompt`` here for the same reason.
        cache_tools="default",
        cache_config=CacheConfig(strategy="auto"),
    )

    orchestrator = Agent(
        model=orchestrator_model,
        system_prompt=ORCHESTRATOR_SYSTEM_PROMPT,
        tools=[run_per_pipeline],
        name="per_orchestrator",
    )

    orchestrator._mcp_client = mcp_client
    orchestrator._obo_auth = obo_auth

    log_info_event(
        logger,
        "PER agent initialized successfully",
        "per_agent.initialized",
    )

    return orchestrator

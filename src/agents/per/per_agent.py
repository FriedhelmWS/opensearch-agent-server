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
import re
import os
import time

import boto3
import httpx
from mcp.client.streamable_http import streamable_http_client
from strands import Agent
from strands.models.bedrock import BedrockModel
from strands.tools.mcp import MCPClient

from agents.per.artifact_store import ArtifactStore
from agents.per.sub_agents import (
    build_execute_agent,
    build_plan_agent,
    build_reflect_agent,
    set_mcp_client,
)
from server.constants import DEFAULT_MCP_SERVER_URL
from utils.logging_helpers import get_logger, log_info_event, log_warning_event
from utils.monitored_tool import monitored_tool
from utils.obo_context import OboAuth
from utils.token_usage_context import publish_inner_usage

logger = get_logger(__name__)

_DEFAULT_BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Mirrors DEFAULT_MAX_STEPS_EXECUTED in MLPlanExecuteAndReflectAgentRunner.java.
_MAX_STEPS = 20

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
    if not os.getenv("BEDROCK_INFERENCE_PROFILE_ARN"):
        os.environ["BEDROCK_INFERENCE_PROFILE_ARN"] = _DEFAULT_BEDROCK_MODEL_ID
        log_info_event(
            logger,
            f"BEDROCK_INFERENCE_PROFILE_ARN not set, defaulting to {_DEFAULT_BEDROCK_MODEL_ID}",
            "per_agent.default_model",
            model_id=_DEFAULT_BEDROCK_MODEL_ID,
        )

    log_info_event(
        logger,
        f"Initializing PER agent with OpenSearch at {opensearch_url}",
        "per_agent.initializing",
        opensearch_url=opensearch_url,
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
        artifacts = ArtifactStore()
        pipeline_started = time.perf_counter()

        # Accumulator for Bedrock-reported token usage across every
        # sub-agent call this pipeline makes (plan + each reflect + each
        # executor sub-agent + their nested tool-use turns). Sub-agent
        # traffic is invisible to the outer orchestrator's
        # ``event_loop_metrics.accumulated_usage``; without this the
        # CustomEvent emitted by the orchestrator under-counts the PER
        # pipeline by ~40×. Two views are maintained:
        #   - top-level totals (input / output / cache_read / cache_write)
        #   - by_phase breakdown (plan / execute / reflect, with call counts)
        # ``_publish`` is wired into every return path so the
        # ContextVar handoff to the orchestrator never gets skipped.
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
            # ``publish_inner_usage`` MUTATES the orchestrator-installed
            # container in place — calling ``ContextVar.set`` here would
            # rebind only inside Strands' per-tool context copy and the
            # orchestrator would never see it (this is the bug that made
            # earlier runs report inner={0,0,0,0} despite the pipeline
            # actually spending 5K+ tokens per request).
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

        # ---- Plan phase --------------------------------------------------
        plan_agent = build_plan_agent()
        plan_result, _, _, _, plan_usage = await _run_phase(
            plan_agent, problem_statement, "plan", step_index=0
        )
        _accumulate(plan_usage, "plan")
        plan_steps, plan_final = _parse_planner_output(str(plan_result))

        if plan_final:
            # Planner answered directly without needing execution.
            log_info_event(
                logger,
                "[per] planner returned final result without execution",
                "per.pipeline.early_finish",
                phase="plan",
            )
            _publish()
            return plan_final

        if not plan_steps:
            log_warning_event(
                logger,
                "[per] planner emitted no steps and no result; aborting",
                "per.pipeline.empty_plan",
            )
            _publish()
            return f"Planner produced no actionable plan.\n\nRaw output:\n{plan_result}"

        # ---- Execute / Reflect loop -------------------------------------
        # The executor and reflector are rebuilt fresh each iteration so
        # their conversation history does not carry across steps. Cross-step
        # state is reintroduced explicitly via ``ArtifactStore``:
        #   - ``compact_table()`` for the (truncated) intent/summary trail,
        #   - ``all_facts()`` for full-fidelity KNOWN_FACTS that the executor
        #     and reflector both rely on to avoid rediscovering schema or
        #     re-issuing ruled-out hypotheses.
        # The reflector emits ``next_steps`` (a list — diff-only) instead of
        # rewriting the entire remaining plan each iteration, which is what
        # keeps reflect-output token cost bounded. When the reflector returns
        # ``len(next_steps) > 1`` it has decided those steps are independent,
        # and we fan them out via ``asyncio.gather`` to compress wall-clock.
        pending_steps: list[str] = [plan_steps[0]]
        last_reflect_text = ""
        steps_executed = 0

        while pending_steps and steps_executed < _MAX_STEPS:
            batch = pending_steps[: max(1, _MAX_STEPS - steps_executed)]
            batch_size = len(batch)
            batch_started = time.perf_counter()
            # Snapshot the artifact-derived KNOWN_FACTS once and reuse it for
            # every sibling in the batch. They share state at dispatch time
            # by design — they're declared independent.
            execute_prompts = [_build_execute_input(s, artifacts) for s in batch]
            execute_agents = [build_execute_agent() for _ in batch]

            log_info_event(
                logger,
                f"[per] dispatching execute batch of {batch_size}",
                "per.pipeline.execute_batch_dispatch",
                batch_size=batch_size,
                first_step_index=steps_executed + 1,
            )

            exec_outcomes = await asyncio.gather(
                *(
                    _run_phase(agent, prompt, "execute", steps_executed + 1 + i)
                    for i, (agent, prompt) in enumerate(zip(execute_agents, execute_prompts))
                )
            )

            for i, (step_text, outcome) in enumerate(zip(batch, exec_outcomes)):
                exec_result, exec_ms, exec_in, exec_out, exec_usage = outcome
                _accumulate(exec_usage, "execute")
                step_index = steps_executed + 1 + i
                raw_findings = str(exec_result)
                findings, queries, facts = _extract_sections(raw_findings)
                artifacts.add(
                    step_index=step_index,
                    step_intent=step_text,
                    findings=findings,
                    facts=facts,
                    queries=queries,
                    tokens_in=exec_in,
                    tokens_out=exec_out,
                    elapsed_ms=exec_ms,
                )
                log_info_event(
                    logger,
                    f"[per] captured {len(facts)} fact(s) and {len(queries)} query(s) from step {step_index}",
                    "per.pipeline.facts_captured",
                    step_index=step_index,
                    fact_count=len(facts),
                    query_count=len(queries),
                )

            steps_executed += batch_size
            batch_elapsed_ms = int((time.perf_counter() - batch_started) * 1000)
            log_info_event(
                logger,
                f"[per] execute batch complete ({batch_size} step(s))",
                "per.pipeline.execute_batch_complete",
                batch_size=batch_size,
                batch_elapsed_ms=batch_elapsed_ms,
            )

            reflect_agent = build_reflect_agent()
            # Show the reflector full findings for every artifact in the
            # batch we just completed, not just the most recent one — they
            # were dispatched as a unit and must be reasoned about as a unit.
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

        # Loop exhausted without a final result — mirror the Java runner's
        # "Max Steps Limit" message and surface the last reflector output.
        log_warning_event(
            logger,
            "[per] max steps reached without final result",
            "per.pipeline.max_steps",
            max_steps=_MAX_STEPS,
        )
        _publish()
        return (
            f"Max Steps Limit ({_MAX_STEPS}) Reached. Use the same conversation "
            f"to continue.\n\nLast reflection:\n{last_reflect_text or '(no reflection)'}"
        )

    # Prompt caching disabled — cache_tools / cache_config omitted so
    # Bedrock does not place cache breakpoints.
    orchestrator_model = BedrockModel(
        model_id=os.environ["BEDROCK_INFERENCE_PROFILE_ARN"],
        boto_session=boto3.Session(),
        streaming=True,
        max_tokens=32768,
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

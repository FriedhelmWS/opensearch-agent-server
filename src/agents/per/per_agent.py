"""PER Agent — generic Plan / Execute / Reflect investigation pipeline.

Domain methodology (root-cause analysis, security forensics, performance
regression bisect, etc.) is supplied at runtime by skills loaded from
``skills/`` (see ``_load_skills`` in ``sub_agents``). The framework
itself is domain-neutral; sub-agent prompts intentionally avoid
hard-coding observability/RCA terminology beyond the structural
contracts (evidence-tag syntax, JSON response shape, finalize-gate
semantics) that the orchestrator code parses.

Behavioral parity with ml-commons ``MLPlanExecuteAndReflectAgentRunner``:
  - planner / reflector emit JSON whose ``result`` field signals
    completion (non-empty terminates the loop) and whose remaining
    steps are dispatched to the executor;
  - executor system prompt extends ``EXECUTOR_RESPONSIBILITY`` with
    framework-specific output sections (QUERIES_EXECUTED, KNOWN_FACTS);
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
from dataclasses import dataclass, field

import boto3
import httpx
from mcp.client.streamable_http import streamable_http_client
from strands import Agent
from strands.models.bedrock import BedrockModel
from strands.models.model import CacheConfig
from strands.tools.mcp import MCPClient

from agents.per.artifact_store import ArtifactStore
from agents.per.sub_agents import (
    PlanOutput,
    ReflectOutput,
    build_execute_agent,
    build_plan_agent,
    build_reflect_agent,
    set_mcp_client,
)
from server.constants import DEFAULT_MCP_SERVER_URL
from utils.logging_helpers import get_logger, log_info_event, log_warning_event
from utils.monitored_tool import monitored_tool
from utils.obo_context import OboAuth

logger = get_logger(__name__)

_DEFAULT_BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Default model for the executor sub-agent. Executor work (issue queries,
# transcribe results into KNOWN_FACTS) is mechanical and does not benefit
# from the heavier reasoning model the planner / reflector use, so we
# default it to the same Sonnet ID as the orchestrator fallback. Override
# per-deployment by setting ``BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN`` to
# any inference-profile ARN in the environment — Sonnet 4.6 is a typical
# choice when ``BEDROCK_INFERENCE_PROFILE_ARN`` points at Opus.
_DEFAULT_EXECUTOR_BEDROCK_MODEL_ID = _DEFAULT_BEDROCK_MODEL_ID

# Mirrors DEFAULT_MAX_STEPS_EXECUTED in MLPlanExecuteAndReflectAgentRunner.java.
_MAX_STEPS = 20

# After this many steps, finalize gates STOP rejecting the reflector's
# result. The gates are designed to catch premature finalization; once the
# investigation has run this long, the bigger risk is unbounded looping
# (gate keeps adding new outstanding indicators that produce more findings
# that produce more outstanding indicators). Gate rejection is a hint, not
# a contract — past this threshold we accept whatever conclusion the
# reflector reaches and surface gate violations as caveats in the log.
_FINALIZE_GATE_SOFT_CAP = 8

# Hard fallback step count. If we hit this without converging, force
# finalize regardless of state. This is a safety net for cases where
# the convergence-based triggers below somehow fail.
_FORCE_FINALIZE_STEPS = 12

# Number of consecutive reflect rounds with no new [direct] or [deviation]
# fact at which we treat the investigation as stagnant and switch to the
# force-finalize prompt. Investigations that produce evidence are allowed
# to run; investigations that don't, aren't. This handles Run-6-style
# "reflector voluntarily keeps adding probes" failure where the reflector
# never tries to finalize on its own — once new high-grade evidence
# stops arriving, further probing is unlikely to change the conclusion.
_STAGNATION_ROUNDS_FORCE = 2

# Number of consecutive reflect rounds during which the count of
# untouched original plan steps does NOT decrease, after which the
# reflect prompt gets a PLAN STAGNATION nudge. Captures the failure
# mode where the reflector keeps producing high-grade facts (so
# stagnation doesn't fire) but does so by exploring side-paths that
# bypass the original plan entirely — its candidate set drifts off the
# investigation's intended scope. Detection is deterministic: it reuses
# the existing ``_untouched_plan_steps`` token-coverage check.
_PLAN_STAGNATION_ROUNDS_NUDGE = 3

ORCHESTRATOR_SYSTEM_PROMPT = """You are an investigation orchestrator. Your role
is to dispatch the user's investigation question to the plan→execute→
reflect pipeline and faithfully relay the result.

When the user reports a problem that calls for a structured investigation
(an incident, anomaly, regression, outage, or any "why is this
happening" question that requires evidence-based reasoning across
available data), call the `run_per_pipeline` tool ONCE with a concise
problem statement that captures:
  - what is wrong (the symptom or observation)
  - what entity / scope is affected (if known)
  - the relevant time window (if known)

The pipeline returns a comprehensive investigation report. Return that
report to the user verbatim — do NOT rewrite, summarize, paraphrase,
condense, or reformat it. Do NOT prepend or append commentary,
mitigation suggestions, or follow-up questions. The pipeline's report is
already the deliverable; re-processing it doubles token consumption and
risks truncating the output. Your only job is to faithfully relay the
tool's return value.

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
    """Legacy fallback: regex-extract a JSON blob from raw planner text.

    Used only if the structured-output tool call did NOT land (network
    blip, model went off-protocol, etc.). Normal path is
    :func:`_planner_decision_from_result` which reads the validated
    Pydantic instance off the AgentResult.
    """
    parsed = _extract_json_blob(text)
    if not parsed:
        return [], ""
    steps_raw = parsed.get("steps") or []
    steps = [str(s) for s in steps_raw if isinstance(s, str)]
    result = parsed.get("result")
    result = result.strip() if isinstance(result, str) else ""
    return steps, result


def _planner_decision_from_result(result) -> tuple[list[str], str]:
    """Read planner decision off an AgentResult.

    Strands' ``structured_output_model`` parameter forces the model to
    invoke a generated tool whose input matches :class:`PlanOutput`; the
    validated instance is exposed at ``result.structured_output``. We
    prefer that path because the schema is enforced server-side; if it's
    missing (extremely rare — model went off-protocol despite forcing),
    fall back to the legacy regex extractor on the raw text.
    """
    so = getattr(result, "structured_output", None)
    if isinstance(so, PlanOutput):
        steps = [s.strip() for s in so.steps if isinstance(s, str) and s.strip()]
        return steps, (so.result or "").strip()
    return _parse_planner_output(str(result))


@dataclass
class ReflectDecision:
    """Structured view of a reflect-phase JSON response."""

    next_steps: list[str]
    result: str
    leading_candidate: str = ""
    candidate_reason: str = ""
    outlier_candidate: str = ""
    outlier_reason: str = ""
    direct_indicators_outstanding: list[str] = field(default_factory=list)
    parked_symptoms_outstanding: list[str] = field(default_factory=list)


def _reflect_decision_from_result(result) -> ReflectDecision:
    """Read reflector decision off an AgentResult.

    Same contract as :func:`_planner_decision_from_result`: prefer the
    validated Pydantic instance (:class:`ReflectOutput`) attached by
    Strands' structured-output mechanism, fall back to the legacy regex
    parser only when the tool call somehow did not happen.
    """
    so = getattr(result, "structured_output", None)
    if isinstance(so, ReflectOutput):
        return ReflectDecision(
            next_steps=[s.strip() for s in so.next_steps if isinstance(s, str) and s.strip()],
            result=(so.result or "").strip(),
            leading_candidate=(so.leading_candidate or "").strip(),
            candidate_reason=(so.candidate_reason or "").strip(),
            outlier_candidate=(so.outlier_candidate or "").strip(),
            outlier_reason=(so.outlier_reason or "").strip(),
            direct_indicators_outstanding=[
                s.strip() for s in so.direct_indicators_outstanding if isinstance(s, str) and s.strip()
            ],
            parked_symptoms_outstanding=[
                s.strip() for s in so.parked_symptoms_outstanding if isinstance(s, str) and s.strip()
            ],
        )
    return _parse_reflect_output(str(result))


def _parse_reflect_output(text: str) -> ReflectDecision:
    """Legacy fallback: regex-extract a JSON blob from raw reflector text.

    The reflect schema is a diff: a list of next steps to dispatch
    (``len > 1`` means independent and OK to fan out in parallel), or a
    populated ``result`` to terminate. The schema also exposes the
    leading candidate, the criterion that placed it there, and the list
    of direct indicators that still need querying — these are used by
    the orchestrator to enforce finalize gates without trusting the
    reflector to self-police.

    Returns an empty ``ReflectDecision`` on unparseable output so the
    caller can decide to abort. Tolerates a legacy ``next_step``
    (singular string) field for forward compatibility — earlier reflect
    prompts emitted that shape.
    """
    parsed = _extract_json_blob(text)
    if not parsed:
        return ReflectDecision(next_steps=[], result="")
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
    leading = parsed.get("leading_candidate")
    leading = leading.strip() if isinstance(leading, str) else ""
    reason = parsed.get("candidate_reason")
    reason = reason.strip() if isinstance(reason, str) else ""
    outlier = parsed.get("outlier_candidate")
    outlier = outlier.strip() if isinstance(outlier, str) else ""
    outlier_reason_raw = parsed.get("outlier_reason")
    outlier_reason = (
        outlier_reason_raw.strip() if isinstance(outlier_reason_raw, str) else ""
    )

    def _str_list(key: str) -> list[str]:
        raw = parsed.get(key)
        if isinstance(raw, list):
            return [s.strip() for s in raw if isinstance(s, str) and s.strip()]
        return []

    indicators = _str_list("direct_indicators_outstanding")
    parked = _str_list("parked_symptoms_outstanding")
    return ReflectDecision(
        next_steps=steps,
        result=result,
        leading_candidate=leading,
        candidate_reason=reason,
        outlier_candidate=outlier,
        outlier_reason=outlier_reason,
        direct_indicators_outstanding=indicators,
        parked_symptoms_outstanding=parked,
    )


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


def _finalize_gate_violations(
    decision: ReflectDecision, artifacts: ArtifactStore
) -> list[str]:
    """Deterministic checks that must pass before ``result`` is accepted.

    The reflector's prompt asks it to self-police several conditions
    (direct evidence required, causal-direction walk recorded, no outstanding
    indicators). These checks are duplicated in code so a reflector that
    finalizes prematurely is overruled rather than trusted. Returns a
    list of human-readable violation messages — empty list means the
    finalization is allowed to proceed.
    """
    if not decision.result:
        return []
    violations: list[str] = []
    candidate = decision.leading_candidate
    if not candidate:
        violations.append(
            "leading_candidate is empty but result is populated — finalization "
            "requires naming the entity the conclusion attributes the cause to."
        )
    elif not artifacts.has_direct_fact_for(candidate):
        violations.append(
            f"No KNOWN_FACTS bullet tagged [direct] mentions '{candidate}'. "
            "Per critical rule 9, finalization requires at least one direct "
            "fact for the named candidate; symptom-only attribution is forbidden."
        )
    if decision.direct_indicators_outstanding:
        outstanding = ", ".join(decision.direct_indicators_outstanding)
        violations.append(
            "direct_indicators_outstanding is non-empty: "
            f"{outstanding}. Per critical rule 9, query these before finalizing."
        )
    # Parked symptoms only block finalization when they touch the leading
    # candidate. Symptoms about other entities are allowed to remain parked
    # — the report can mention them as unresolved without blocking. This
    # prevents the recursive-expansion failure mode where each new probe
    # surfaces new entities, each of which produces new symptoms.
    candidate_lc = candidate.lower() if candidate else ""
    blocking_parked = [
        s for s in decision.parked_symptoms_outstanding
        if candidate_lc and candidate_lc in s.lower()
    ]
    if blocking_parked:
        parked = "; ".join(blocking_parked)
        violations.append(
            "parked_symptoms_outstanding contains entries about the leading "
            f"candidate: {parked}. Per critical rule 12, symptoms about the "
            "leading candidate must be promoted to [direct]/[deviation] or "
            "explicitly invalidated before finalizing."
        )
    reason_lc = decision.candidate_reason.lower()
    if candidate and not any(
        marker in reason_lc
        for marker in (
            "walk",
            "outbound",
            "downstream",
            "upstream",
            "neighbor",
            "neighbour",
            "dependency",
        )
    ):
        violations.append(
            "candidate_reason does not record a causal-direction walk "
            "outcome. Per critical rule 8, the walk is mandatory before "
            "finalizing — record in candidate_reason that you examined "
            "the candidate's neighbors / dependencies and either confirm "
            "no promotion or promote one of them."
        )
    if candidate and not any(
        marker in reason_lc
        for marker in (
            "baseline",
            "deviation",
            "× ",
            "x over",
            "x baseline",
            "% over",
            "fold",
        )
    ):
        violations.append(
            "candidate_reason does not cite a relative deviation against "
            "baseline. Per critical rule 11, ranking by absolute magnitude "
            "is forbidden — candidate_reason must include a baseline "
            "comparison (e.g., '+47× over baseline 0.3', '+3% over "
            "baseline 13.8'). Query baseline values for the leading "
            "candidate before finalizing."
        )
    return violations


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


def _build_reflect_input(
    objective: str,
    plan_steps: list[str],
    artifacts: ArtifactStore,
    latest_n: int = 1,
    plan_stagnation_rounds: int = 0,
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

    deviation_facts = artifacts.all_deviation_facts()
    if deviation_facts:
        sections.append(
            "KNOWN_FACTS — [deviation] evidence (incident vs baseline "
            "comparisons — the ONLY evidence eligible for ranking "
            "candidates; rank by RELATIVE deviation here, never by "
            "absolute magnitude):\n" + deviation_facts
        )

    direct_facts = artifacts.all_direct_facts()
    if direct_facts:
        sections.append(
            "KNOWN_FACTS — [direct] evidence (these CAN support a final "
            "attribution on their own; combine with [deviation] above "
            "for ranking):\n" + direct_facts
        )

    symptom_facts = artifacts.all_symptom_facts()
    if symptom_facts:
        sections.append(
            "KNOWN_FACTS — [symptom] evidence (PARKED HYPOTHESES — context "
            "only, CANNOT name a mode by itself; each entry below MUST "
            "appear in `parked_symptoms_outstanding` until it is either "
            "promoted to [direct]/[deviation] by a follow-up query or "
            "explicitly invalidated by another fact):\n" + symptom_facts
        )

    facts = artifacts.all_facts()
    schema_facts: list[str] = []
    for line in facts.splitlines():
        lc = line.lower()
        if (
            "[direct]" in lc
            or "[symptom]" in lc
            or "[deviation]" in lc
        ):
            continue
        schema_facts.append(line)
    schema_block = "\n".join(schema_facts).strip()
    if schema_block:
        sections.append(
            "KNOWN_FACTS — [schema] / unclassified (operational discoveries; "
            "do NOT rediscover):\n" + schema_block
        )

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

    if untouched and plan_stagnation_rounds >= _PLAN_STAGNATION_ROUNDS_NUDGE:
        # Untouched-step count has not decreased for several reflect
        # rounds. Even if the investigation is producing high-grade
        # facts, it's exploring side-paths instead of completing the
        # planned scope. Strong-arm the reflector back onto the plan.
        sections.append(
            f"PLAN STAGNATION — the count of untouched plan steps "
            f"({len(untouched)}) has NOT decreased for "
            f"{plan_stagnation_rounds} consecutive reflect rounds. The "
            "investigation is producing facts but not completing the "
            "originally planned scope. Your NEXT `next_steps` MUST "
            "either (a) dispatch one or more of the untouched plan "
            "steps listed above, OR (b) cite the specific KNOWN_FACT "
            "that invalidates each remaining untouched step. Continuing "
            "to fan out new probes outside the plan without making "
            "progress on the planned scope is not allowed."
        )

    sections.append(
        "Output the next step(s) to execute in `next_steps` (a list — "
        "prefer parallel dispatch when steps target independent data "
        "sources; otherwise return a single-element list), or finalize "
        "with a comprehensive report in `result`. Always populate "
        "`leading_candidate`, `candidate_reason`, `outlier_candidate`, "
        "`outlier_reason`, `direct_indicators_outstanding`, and "
        "`parked_symptoms_outstanding` so the orchestrator can verify "
        "finalize gates. Never repeat a completed step. Never propose "
        "work that contradicts KNOWN_FACTS. Honor the PLAN COVERAGE "
        "WARNING above if present. Finalization will be REJECTED by the "
        "orchestrator if ANY of: (a) the leading_candidate has no "
        "[direct] evidence, (b) direct_indicators_outstanding is "
        "non-empty, (c) candidate_reason does not record a causal-"
        "direction walk outcome, (d) candidate_reason does not cite a "
        "RELATIVE deviation against a comparable reference (absolute "
        "magnitude is forbidden), or (e) parked_symptoms_outstanding "
        "is non-empty. Use the `outlier_candidate` slot every turn to "
        "keep the second-most-anomalous entity visible — confirmation-"
        "bias collapse onto the leading candidate is a recurring miss "
        "path."
    )
    return "\n\n".join(sections)


def _build_force_finalize_input(
    objective: str,
    plan_steps: list[str],
    artifacts: ArtifactStore,
) -> str:
    """Reflect prompt that forces the model to produce a final report now.

    Used after the investigation has run long enough that further probing
    is more likely to be confirmation-driven scope expansion than to
    surface new evidence. Differs from the normal reflect prompt in three
    ways:
      - it tells the model that next_steps will be IGNORED;
      - it explicitly suspends critical rules 5–13 and the finalize gates;
      - it allows the model to acknowledge unknowns and outstanding
        questions inside the `result` text rather than dispatching more
        steps to chase them.
    """
    sections = [f"Objective:\n{objective}"]

    if plan_steps:
        plan_block = "\n".join(f"- {step}" for step in plan_steps)
        sections.append(
            f"Original plan (for context only):\n{plan_block}"
        )

    queries = artifacts.all_queries()
    if queries:
        sections.append(
            "QUERIES_EXECUTED so far:\n" + queries
        )

    deviation_facts = artifacts.all_deviation_facts()
    if deviation_facts:
        sections.append(
            "KNOWN_FACTS — [deviation] (incident vs baseline ratios):\n"
            + deviation_facts
        )

    direct_facts = artifacts.all_direct_facts()
    if direct_facts:
        sections.append(
            "KNOWN_FACTS — [direct] evidence:\n" + direct_facts
        )

    symptom_facts = artifacts.all_symptom_facts()
    if symptom_facts:
        sections.append(
            "KNOWN_FACTS — [symptom] evidence (context only):\n"
            + symptom_facts
        )

    facts = artifacts.all_facts()
    schema_lines = [
        line for line in facts.splitlines()
        if "[direct]" not in line.lower()
        and "[symptom]" not in line.lower()
        and "[deviation]" not in line.lower()
    ]
    schema_block = "\n".join(schema_lines).strip()
    if schema_block:
        sections.append(
            "KNOWN_FACTS — [schema] / unclassified:\n" + schema_block
        )

    sections.append(
        "FORCE FINALIZE: the investigation has run long enough that "
        "further probing is unlikely to change the conclusion more than "
        "it lengthens the run. You MUST now produce your best `result` "
        "from the evidence above. Critical rules 5–13 and the normal "
        "finalize gates are suspended for this turn — your `next_steps` "
        "will be IGNORED. Do not propose more probes.\n\n"
        "In `result`, write a comprehensive report that:\n"
        "1. Names your best top candidate(s) with the relative-deviation "
        "evidence that places them.\n"
        "2. Cites the specific [deviation] / [direct] facts that ground "
        "each named candidate.\n"
        "3. Acknowledges what could not be determined from available "
        "evidence (unprobed dimensions, missing baselines, etc.) — these "
        "are caveats inside the report, NOT reasons to keep investigating.\n"
        "4. Distinguishes the entity that is the actual cause from "
        "entities where the symptom merely surfaces, per the evidence "
        "at hand; if the distinction is genuinely undetermined, say so.\n\n"
        "Populate `leading_candidate`, `candidate_reason`, "
        "`outlier_candidate`, `outlier_reason` for the audit log, and "
        "leave `next_steps` and `direct_indicators_outstanding` and "
        "`parked_symptoms_outstanding` empty. The result you produce now "
        "is what will be returned to the user."
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
    """Invoke a sub-agent and log timing + token usage for the phase."""
    started = time.perf_counter()
    result = await agent.invoke_async(prompt)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    tokens_in, tokens_out = _usage_tokens(result)
    log_info_event(
        logger,
        f"[per] {phase} phase complete (step {step_index})",
        f"per.phase.{phase}",
        phase=phase,
        step_index=step_index,
        elapsed_ms=elapsed_ms,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
    )
    return result, elapsed_ms, tokens_in, tokens_out


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

    if not os.getenv("BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN"):
        os.environ["BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN"] = _DEFAULT_EXECUTOR_BEDROCK_MODEL_ID
        log_info_event(
            logger,
            f"BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN not set, defaulting to "
            f"{_DEFAULT_EXECUTOR_BEDROCK_MODEL_ID}",
            "per_agent.default_executor_model",
            model_id=_DEFAULT_EXECUTOR_BEDROCK_MODEL_ID,
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
            "Run the plan→execute→reflect investigation pipeline. Pass a "
            "concise problem statement (what is wrong, what entity/scope "
            "is affected, time window). Returns a comprehensive "
            "investigation report describing every step, findings, and "
            "the final conclusion. Domain methodology (e.g. for root-cause "
            "analysis, security forensics, performance regression) is "
            "loaded from the agent's installed skills at runtime."
        ),
    )
    async def run_per_pipeline(problem_statement: str) -> str:
        artifacts = ArtifactStore()
        pipeline_started = time.perf_counter()

        log_info_event(
            logger,
            "[per] pipeline start",
            "per.pipeline.start",
            problem_statement=problem_statement[:200],
        )

        # ---- Plan phase --------------------------------------------------
        plan_agent = build_plan_agent()
        plan_result, _, _, _ = await _run_phase(
            plan_agent, problem_statement, "plan", step_index=0
        )
        plan_steps, plan_final = _planner_decision_from_result(plan_result)

        if plan_final:
            # Planner answered directly without needing execution.
            log_info_event(
                logger,
                "[per] planner returned final result without execution",
                "per.pipeline.early_finish",
                phase="plan",
            )
            return plan_final

        if not plan_steps:
            log_warning_event(
                logger,
                "[per] planner emitted no steps and no result; aborting",
                "per.pipeline.empty_plan",
            )
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
        # Convergence tracking: counts the cumulative number of high-grade
        # facts ([direct] or [deviation]) recorded so far. Each reflect
        # round compares the current count against this snapshot — if no
        # new high-grade evidence has arrived for `_STAGNATION_ROUNDS_FORCE`
        # consecutive rounds we treat the investigation as having
        # converged on whatever it has now and switch to the force-
        # finalize prompt. This is more discriminating than a fixed step
        # threshold: investigations that keep producing evidence are
        # allowed to run longer; investigations that don't are stopped
        # earlier. Tracks `[symptom]` and `[schema]` separately is NOT
        # done because those tags don't change the conclusion (symptoms
        # are unresolved hypotheses, schema discoveries are operational).
        last_high_grade_count = 0
        stagnant_rounds = 0
        # Plan-stagnation tracking: count consecutive reflect rounds in
        # which the number of untouched original plan steps did not
        # decrease. Independent from `stagnant_rounds`: the reflector can
        # be advancing on high-grade evidence (no stagnation) while still
        # ignoring the original plan entirely (plan stagnation).
        last_untouched_plan_count: int | None = None
        plan_stagnant_rounds = 0

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
                exec_result, exec_ms, exec_in, exec_out = outcome
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

            # Convergence check: did this batch produce any new [direct]
            # or [deviation] fact? If not, increment the stagnation
            # counter; if it did, reset. The stagnation counter is the
            # primary trigger for force-finalize — see comment at the
            # top of the loop.
            current_high_grade_count = sum(
                1
                for art in artifacts
                for fact in art.facts
                if fact.lstrip().lower().startswith(("[direct]", "[deviation]"))
            )
            new_evidence = current_high_grade_count - last_high_grade_count
            # Stagnation is only meaningful AFTER the investigation has
            # produced at least one [direct] or [deviation] fact. Schema
            # discovery / signal-family inventory phases produce only
            # [schema] facts by design, and a strict "no high-grade for
            # 2 rounds" rule would force-finalize before any per-entity
            # comparison runs. Hold the counter at 0 until the first
            # high-grade fact lands, then start monitoring stagnation.
            if current_high_grade_count == 0:
                stagnant_rounds = 0
                phase = "discovery"
            elif new_evidence > 0:
                stagnant_rounds = 0
                phase = "advancing"
            else:
                stagnant_rounds += 1
                phase = "stagnant"
            last_high_grade_count = current_high_grade_count

            # Plan-stagnation: did the count of untouched original plan
            # steps shrink this round? Done after stagnation update so a
            # round can be both 'advancing' (new high-grade) and
            # plan-stagnant (didn't reduce untouched plan count). Skip
            # entirely if the original plan was empty.
            current_untouched = len(_untouched_plan_steps(plan_steps, artifacts)) if plan_steps else 0
            if not plan_steps:
                plan_stagnant_rounds = 0
            elif last_untouched_plan_count is None:
                plan_stagnant_rounds = 0
            elif current_untouched < last_untouched_plan_count:
                plan_stagnant_rounds = 0
            else:
                plan_stagnant_rounds += 1
            last_untouched_plan_count = current_untouched

            log_info_event(
                logger,
                f"[per] convergence: phase={phase} new={new_evidence} "
                f"stagnant_rounds={stagnant_rounds} "
                f"high_grade_total={current_high_grade_count} "
                f"untouched_plan={current_untouched} "
                f"plan_stagnant_rounds={plan_stagnant_rounds}",
                "per.pipeline.convergence",
                steps_executed=steps_executed,
                phase=phase,
                new_high_grade=new_evidence,
                stagnant_rounds=stagnant_rounds,
                high_grade_total=current_high_grade_count,
                untouched_plan_count=current_untouched,
                plan_stagnant_rounds=plan_stagnant_rounds,
            )

            reflect_agent = build_reflect_agent()
            stagnant_force = stagnant_rounds >= _STAGNATION_ROUNDS_FORCE
            step_force = steps_executed >= _FORCE_FINALIZE_STEPS
            force_finalize = stagnant_force or step_force
            if force_finalize:
                log_warning_event(
                    logger,
                    f"[per] force-finalize triggered at step {steps_executed} "
                    f"(stagnant={stagnant_force}, step_cap={step_force})",
                    "per.pipeline.force_finalize",
                    steps_executed=steps_executed,
                    stagnant_rounds=stagnant_rounds,
                    stagnation_threshold=_STAGNATION_ROUNDS_FORCE,
                    step_threshold=_FORCE_FINALIZE_STEPS,
                    triggered_by="stagnation" if stagnant_force else "step_cap",
                )
                reflect_prompt = _build_force_finalize_input(
                    objective=problem_statement,
                    plan_steps=plan_steps,
                    artifacts=artifacts,
                )
            else:
                # Show the reflector full findings for every artifact in
                # the batch we just completed, not just the most recent
                # one — they were dispatched as a unit and must be
                # reasoned about as a unit.
                reflect_prompt = _build_reflect_input(
                    objective=problem_statement,
                    plan_steps=plan_steps,
                    artifacts=artifacts,
                    latest_n=batch_size,
                    plan_stagnation_rounds=plan_stagnant_rounds,
                )
            reflect_result, _, _, _ = await _run_phase(
                reflect_agent, reflect_prompt, "reflect", steps_executed
            )
            last_reflect_text = str(reflect_result)
            decision = _reflect_decision_from_result(reflect_result)

            # Under force-finalize, whatever the model returns is
            # accepted as the final report — gates are suspended and
            # next_steps are ignored. If the model returned an empty
            # `result` despite the explicit instruction, fall back to
            # surfacing the raw text so the user gets something.
            if force_finalize:
                final_text = decision.result or last_reflect_text or "(empty force-finalize response)"
                total_ms = int((time.perf_counter() - pipeline_started) * 1000)
                log_info_event(
                    logger,
                    "[per] pipeline complete (force-finalized)",
                    "per.pipeline.finish",
                    steps_executed=steps_executed,
                    total_elapsed_ms=total_ms,
                    leading_candidate=decision.leading_candidate,
                    forced=True,
                )
                return final_text

            if decision.result:
                violations = _finalize_gate_violations(decision, artifacts)
                # Past the soft cap, accept the reflector's finalization
                # even if gates would reject it. Gate rejection is meant to
                # catch premature finalization (one or two cycles in); past
                # ~8 steps the bigger risk is unbounded looping where each
                # gate-driven follow-up produces new outstanding indicators
                # that produce more follow-ups. Surface the unmet gates as
                # warnings in the log rather than blocking the user.
                if violations and steps_executed >= _FINALIZE_GATE_SOFT_CAP:
                    log_warning_event(
                        logger,
                        "[per] finalize gates not satisfied but soft cap "
                        "reached; accepting result with caveats",
                        "per.pipeline.finalize_soft_cap",
                        steps_executed=steps_executed,
                        violation_count=len(violations),
                        violations=violations,
                        leading_candidate=decision.leading_candidate,
                    )
                    violations = []
                if violations:
                    log_warning_event(
                        logger,
                        "[per] finalize gates rejected reflector's result; "
                        "forcing continued investigation",
                        "per.pipeline.finalize_rejected",
                        steps_executed=steps_executed,
                        violation_count=len(violations),
                        leading_candidate=decision.leading_candidate,
                    )
                    # Synthesize follow-up steps. We can have multiple
                    # categories of binding violation at once — outstanding
                    # indicators, parked symptoms, missing baseline / walk.
                    # Prefer dispatching the structured items first, fall
                    # back to a coaching step if none apply.
                    forced: list[str] = []
                    for indicator in decision.direct_indicators_outstanding:
                        forced.append(
                            "Query the following outstanding direct indicator "
                            f"for the leading candidate "
                            f"'{decision.leading_candidate}': {indicator}"
                        )
                    for symptom in decision.parked_symptoms_outstanding:
                        forced.append(
                            "Resolve this parked symptom — either query for "
                            "evidence that promotes it to [direct] / "
                            "[deviation], or query for evidence that "
                            f"explicitly invalidates it. Symptom: {symptom}"
                        )
                    if forced:
                        pending_steps = forced
                    else:
                        # The reflector wanted to finalize but the violation
                        # is in `candidate_reason` shape (missing causal
                        # walk or missing baseline citation). Coach it
                        # explicitly without prescribing domain specifics —
                        # the relevant skill (if any) carries those.
                        pending_steps = [
                            "The previous reflect attempt to finalize was "
                            "rejected by the finalize gate. Violations:\n- "
                            + "\n- ".join(violations)
                            + "\n\nProbe the leading candidate "
                            f"'{decision.leading_candidate or '(none named)'}' "
                            "directly to produce [direct] / [deviation] "
                            "evidence: query the candidate's own signals "
                            "with baseline comparison, examine its "
                            "dependencies / neighbors for the causal-"
                            "direction walk, and ensure the next "
                            "candidate_reason cites a RELATIVE deviation "
                            "against baseline (not an absolute value). "
                            "If a domain skill applies to this "
                            "investigation, follow its guidance for what "
                            "counts as direct evidence and what neighbors "
                            "to walk."
                        ]
                    continue

                total_ms = int((time.perf_counter() - pipeline_started) * 1000)
                log_info_event(
                    logger,
                    "[per] pipeline complete",
                    "per.pipeline.finish",
                    steps_executed=steps_executed,
                    total_elapsed_ms=total_ms,
                    leading_candidate=decision.leading_candidate,
                )
                return decision.result

            if not decision.next_steps:
                log_warning_event(
                    logger,
                    "[per] reflector returned no next_steps and no result",
                    "per.pipeline.unparseable_reflect",
                    steps_executed=steps_executed,
                )
                break

            pending_steps = decision.next_steps

        # Loop exhausted without a final result — mirror the Java runner's
        # "Max Steps Limit" message and surface the last reflector output.
        log_warning_event(
            logger,
            "[per] max steps reached without final result",
            "per.pipeline.max_steps",
            max_steps=_MAX_STEPS,
        )
        return (
            f"Max Steps Limit ({_MAX_STEPS}) Reached. Use the same conversation "
            f"to continue.\n\nLast reflection:\n{last_reflect_text or '(no reflection)'}"
        )

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

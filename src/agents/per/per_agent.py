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

# Default model for both planner/reflector AND executor when their env
# vars are unset. Override per-deployment via ``BEDROCK_INFERENCE_PROFILE_ARN``
# (planner/reflector) and ``BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN`` (executor);
# typical config sets the former to Opus and the latter to Sonnet 4.6.
_DEFAULT_BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

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

# Wall-clock budget for an entire PER pipeline run, in seconds. Once
# the elapsed time crosses this, the NEXT reflect phase switches to
# force-finalize regardless of step count or convergence state. This
# is the hard guarantee that pipeline runs cannot drag past ~12
# minutes — independent of how cheaply or expensively each step
# happens to behave on a given run. Force-finalize itself adds another
# 30-60s for the final reflect, plus output streaming, so the
# end-to-end ceiling is roughly budget + ~120s.
_PIPELINE_WALL_CLOCK_BUDGET_SECONDS = 720

# Maximum number of executor sub-agents we will run concurrently in a single
# fan-out batch. The reflector is allowed to declare more independent steps
# than this — we just slice them across multiple loop iterations. Three is
# the sweet spot for OTel-style triple-source investigations (logs + traces
# + metrics) where all three are commonly independent at the same step.
# Going higher (4+) reproduces the context-overflow retry storm we hit
# previously: each executor inherits the full ArtifactStore-derived
# KNOWN_FACTS block plus its own multi-tool-call conversation history, and
# 4+ of those in flight push Bedrock's accumulated payload past the model's
# context window.
_MAX_PARALLEL_EXECUTORS = 3

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


def _planner_decision_from_result(result) -> tuple[list[str], str]:
    """Read planner decision off an AgentResult.

    Strands' ``structured_output_model`` parameter forces the model to
    invoke a generated tool whose input matches :class:`PlanOutput`; the
    validated instance is exposed at ``result.structured_output``.
    """
    so = getattr(result, "structured_output", None)
    if isinstance(so, PlanOutput):
        steps = [s.strip() for s in so.steps if isinstance(s, str) and s.strip()]
        return steps, (so.result or "").strip()
    return [], ""


@dataclass
class ReflectDecision:
    """Structured view of a reflect-phase JSON response."""

    next_steps: list[str]
    result: str
    leading_candidate: str = ""
    candidate_reason: str = ""
    outlier_candidate: str = ""
    outlier_reason: str = ""
    outstanding_probes: list[str] = field(default_factory=list)
    proposed_mechanism: str = ""
    mechanism_alternatives: list[str] = field(default_factory=list)
    mechanism_evidence: list[str] = field(default_factory=list)
    dimensions_invalidated: list[str] = field(default_factory=list)


def _reflect_decision_from_result(result) -> ReflectDecision:
    """Read reflector decision off an AgentResult.

    Strands' ``structured_output_model`` mechanism delivers a validated
    :class:`ReflectOutput` instance at ``result.structured_output``. If
    the structured-output tool somehow didn't land we surface an empty
    decision; ``ReflectOutput``'s field validators already coerce most
    sloppy model inputs (None / scalar where list expected) so this
    fallback path is rare.
    """
    so = getattr(result, "structured_output", None)
    if not isinstance(so, ReflectOutput):
        return ReflectDecision(next_steps=[], result="")

    def _clean_list(values: list) -> list[str]:
        return [s.strip() for s in values if isinstance(s, str) and s.strip()]

    return ReflectDecision(
        next_steps=_clean_list(so.next_steps),
        result=(so.result or "").strip(),
        leading_candidate=(so.leading_candidate or "").strip(),
        candidate_reason=(so.candidate_reason or "").strip(),
        outlier_candidate=(so.outlier_candidate or "").strip(),
        outlier_reason=(so.outlier_reason or "").strip(),
        outstanding_probes=_clean_list(so.outstanding_probes),
        proposed_mechanism=(so.proposed_mechanism or "").strip(),
        mechanism_alternatives=_clean_list(so.mechanism_alternatives),
        mechanism_evidence=_clean_list(so.mechanism_evidence),
        dimensions_invalidated=_clean_list(so.dimensions_invalidated),
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
    # outstanding_probes carries everything that still blocks finalize:
    # unqueried direct indicators, unresolved [symptom] facts, unprobed
    # dimensions. Items mentioning the leading candidate are blocking;
    # items about unrelated entities are allowed to remain (the report
    # can call them out as unresolved without forcing more probing —
    # this prevents the recursive-expansion failure mode where each new
    # probe surfaces new entities, each of which produces new symptoms).
    candidate_lc = candidate.lower() if candidate else ""
    blocking_probes = [
        p for p in decision.outstanding_probes
        if not candidate_lc or candidate_lc in p.lower()
    ]
    if blocking_probes:
        probes = "; ".join(blocking_probes)
        violations.append(
            "outstanding_probes contains entries that touch the leading "
            f"candidate: {probes}. Per critical rules 9/12/16, each must be "
            "either probed (queue a step) or invalidated (move to "
            "dimensions_invalidated with a citing KNOWN_FACT) before "
            "finalizing."
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

    # Mechanism gate (critical rule 14): proposed_mechanism must be
    # populated whenever a candidate is named. Empty mechanism with a
    # populated candidate means the reflector identified WHO but not the
    # CAUSAL PROCESS — finalization in this state collapses "service is
    # at fault" with "service is at fault BECAUSE …", which is exactly
    # the failure mode reflection logs surfaced repeatedly.
    if candidate and not decision.proposed_mechanism:
        violations.append(
            "proposed_mechanism is empty but leading_candidate is named. "
            "Per critical rule 14, finalization requires naming the causal "
            "MECHANISM — not just the entity. State the underlying process "
            "by which the candidate produces the observed symptoms."
        )

    # Mechanism alternatives gate (critical rule 15): at least 2
    # competing mechanisms must be enumerated whenever proposed_mechanism
    # is set. Forces hypothesis-space enumeration before finalize so the
    # reflector cannot collapse onto the first self-consistent story.
    if decision.proposed_mechanism and len(decision.mechanism_alternatives) < 2:
        violations.append(
            "mechanism_alternatives has fewer than 2 entries. Per critical "
            "rule 15, finalization requires explicit enumeration of at "
            "least two competing mechanisms — list them with falsification "
            "rationale or remaining-plausibility notes."
        )

    # Mechanism evidence gate (critical rule 14 + 9): at least one
    # [direct] or [deviation] fact must back the mechanism claim.
    # Mechanism claims supported only by [symptom] facts are exactly the
    # "exception class = root cause" anti-pattern.
    if decision.proposed_mechanism:
        evidence_blob = "\n".join(decision.mechanism_evidence).lower()
        if not ("[direct]" in evidence_blob or "[deviation]" in evidence_blob):
            violations.append(
                "mechanism_evidence does not cite any [direct] or "
                "[deviation] KNOWN_FACT. Per critical rule 14, mechanism "
                "claims must be backed by direct evidence — symptom-only "
                "support is insufficient for naming a causal process."
            )

    return violations


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

    Returns input/output plus cache read/write tokens — the latter two
    are produced by Bedrock when prompt caching is in effect (see
    ``cache_config`` / ``cache_tools`` in ``sub_agents.py``). Without
    these the cache-hit rate is invisible to downstream callers and
    benchmark token accounting under-counts cached prefixes.
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


_SYMPTOM_FACTS_RECENT_CAP = 20


def _build_known_facts_block(
    artifacts: ArtifactStore, leading_candidate: str
) -> str:
    """Render every recorded fact in tag-priority order as one block.

    Order is fixed so the reflector's eye always lands on
    ranking-eligible evidence first: [deviation] → [direct] → [symptom]
    (LRU-capped, leading-candidate whitelisted) → [schema]/unclassified.
    Each section contributes its lines verbatim with the artifact-id
    prefix already attached by the store; sections that are empty are
    silently skipped so the result is dense.
    """
    parts: list[str] = []
    deviation = artifacts.all_deviation_facts()
    if deviation:
        parts.append(deviation)
    direct = artifacts.all_direct_facts()
    if direct:
        parts.append(direct)
    symptom = artifacts.all_symptom_facts(
        leading_candidate=leading_candidate,
        max_recent=_SYMPTOM_FACTS_RECENT_CAP,
    )
    if symptom:
        parts.append(symptom)
    all_facts = artifacts.all_facts()
    schema_lines = [
        line for line in all_facts.splitlines()
        if "[direct]" not in line.lower()
        and "[symptom]" not in line.lower()
        and "[deviation]" not in line.lower()
    ]
    schema_block = "\n".join(schema_lines).strip()
    if schema_block:
        parts.append(schema_block)
    return "\n".join(parts)


_NORMAL_REFLECT_INSTRUCTIONS = (
    "Output the next step(s) to execute in `next_steps` (a list — "
    "prefer parallel dispatch when steps target independent data "
    "sources; otherwise return a single-element list), or finalize "
    "with a comprehensive report in `result`. Always populate "
    "`leading_candidate`, `candidate_reason`, `outlier_candidate`, "
    "`outlier_reason`, and `outstanding_probes` so the orchestrator "
    "can verify finalize gates. Never repeat a completed step. "
    "Never propose work that contradicts KNOWN_FACTS. Finalization "
    "will be REJECTED by the orchestrator if ANY of: (a) the "
    "leading_candidate has no [direct] evidence, (b) "
    "outstanding_probes contains entries that touch the leading "
    "candidate, (c) candidate_reason does not record a causal-"
    "direction walk outcome, or (d) candidate_reason does not cite "
    "a RELATIVE deviation against a comparable reference (absolute "
    "magnitude is forbidden). Use the `outlier_candidate` slot "
    "every turn to keep the second-most-anomalous entity visible "
    "— confirmation-bias collapse onto the leading candidate is a "
    "recurring miss path."
)

_FORCE_FINALIZE_INSTRUCTIONS = (
    "FORCE FINALIZE: the investigation has run long enough that "
    "further probing is unlikely to change the conclusion more than "
    "it lengthens the run. You MUST now produce your best `result` "
    "from the evidence above. Critical rules 5–13 and the normal "
    "finalize gates are suspended for this turn — your `next_steps` "
    "will be IGNORED. Do not propose more probes.\n\n"
    "EXCEPTION — critical rule 15 (ENUMERATE COMPETING MECHANISMS) "
    "is NOT suspended. You MUST still populate `mechanism_alternatives` "
    "with at least 2 entries when `proposed_mechanism` is set. "
    "Treat this as the most important guard against confirmation-bias "
    "collapse: finding one self-consistent story (especially a "
    "demand-side / usage-pattern story like 'service X does N+1 "
    "calls' or 'service X drives more load') does not rule out the "
    "supply-side / dependency-degradation alternative ('the "
    "downstream socket / connection / disk / dependency that X "
    "consumes is itself slower'). Both stories produce identical "
    "fingerprints when the discriminating signal isn't probed; if "
    "you cannot rule out the symmetric supply-side mechanism with "
    "a citing fact, you MUST list it in `mechanism_alternatives` "
    "with status 'plausible alternative — discriminating evidence "
    "not collected' rather than silently committing to your top "
    "pick.\n\n"
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
    "leave `next_steps` and `outstanding_probes` empty. The result "
    "you produce now is what will be returned to the user."
)


def _build_reflect_input(
    objective: str,
    plan_steps: list[str],
    artifacts: ArtifactStore,
    leading_candidate: str = "",
    *,
    force_finalize: bool = False,
) -> str:
    """Assemble the reflect-phase prompt.

    Two modes share rendering:
      - Normal (``force_finalize=False``): full context — completed-step
        compact table, QUERIES_EXECUTED, all four fact tags, latest
        full findings, normal-reflect instructions.
      - Force-finalize (``force_finalize=True``): trimmed context —
        only deviation + direct facts (the only kinds eligible to
        ground a final attribution); compact table, queries, symptom,
        schema, and latest-findings sections are dropped because at
        wrap-up time the reflector should be writing up, not auditing.
        Closes with FORCE FINALIZE instructions that suspend most
        finalize gates while keeping critical rule 15 (mechanism
        enumeration) live.
    """
    sections = [f"Objective:\n{objective}"]

    if plan_steps:
        plan_block = "\n".join(f"- {step}" for step in plan_steps)
        plan_label = (
            "Original plan (for context only):"
            if force_finalize
            else "Original plan (for context only — do not echo or restate):"
        )
        sections.append(f"{plan_label}\n{plan_block}")

    if not force_finalize:
        compact = artifacts.compact_table()
        if compact:
            sections.append(f"Completed steps (summary):\n{compact}")

        queries = artifacts.all_queries()
        if queries:
            sections.append(
                "QUERIES_EXECUTED (what was ACTUALLY probed — use this to detect "
                "scope-narrowing, not free-text findings):\n" + queries
            )

        facts_block = _build_known_facts_block(artifacts, leading_candidate)
        if facts_block:
            sections.append(
                "KNOWN_FACTS (tag semantics: [deviation] = relative-change "
                "evidence, only kind eligible for candidate ranking; [direct] "
                "= can support a final attribution on its own; [symptom] = "
                "parked hypothesis, context only — must be promoted or "
                "invalidated before finalize; [schema] = operational, do not "
                "rediscover):\n" + facts_block
            )

        latest = artifacts.full_findings(last_n=1)
        if latest:
            sections.append("Most recent step (full findings):\n" + latest)
    else:
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

    sections.append(
        _FORCE_FINALIZE_INSTRUCTIONS if force_finalize else _NORMAL_REFLECT_INSTRUCTIONS
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

    Returns ``(result, elapsed_ms, tokens_in, tokens_out)`` — the same
    4-tuple call sites have always consumed. Cache tokens are surfaced
    only on the result object's ``per_phase_usage`` attribute and via
    the log event below, so existing callers keep working unchanged
    while the orchestration loop can opt into the richer view.
    """
    started = time.perf_counter()
    result = await agent.invoke_async(prompt)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    usage = _full_usage(result)
    tokens_in = usage["input"]
    tokens_out = usage["output"]
    cache_read = usage["cache_read"]
    cache_write = usage["cache_write"]
    # Stash full usage on the result so the orchestrator can accumulate
    # cache stats without us having to widen the return tuple (and break
    # every existing call site).
    try:
        result.per_phase_usage = usage  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover — defensive against frozen models
        pass
    log_info_event(
        logger,
        f"[per] {phase} phase complete (step {step_index})",
        f"per.phase.{phase}",
        phase=phase,
        step_index=step_index,
        elapsed_ms=elapsed_ms,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        cache_read_tokens=cache_read,
        cache_write_tokens=cache_write,
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
        os.environ["BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN"] = _DEFAULT_BEDROCK_MODEL_ID
        log_info_event(
            logger,
            f"BEDROCK_EXECUTOR_INFERENCE_PROFILE_ARN not set, defaulting to "
            f"{_DEFAULT_BEDROCK_MODEL_ID}",
            "per_agent.default_executor_model",
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
        # Accumulator for Bedrock-reported token usage across every
        # sub-agent call this pipeline makes (plan + each reflect + each
        # executor + each executor's nested tool-use turns). Logged on
        # pipeline finish so external benchmarks / billing stop having
        # to estimate from SSE event deltas, which under-counts the
        # PER pipeline's internal Bedrock traffic by ~40× (none of the
        # internal sub-agent traffic ever surfaces as an outer-tool
        # SSE event).
        usage_totals = {
            "input": 0,
            "output": 0,
            "cache_read": 0,
            "cache_write": 0,
            "phase_calls": 0,
        }

        def _accumulate(result_obj) -> None:
            phase_usage = getattr(result_obj, "per_phase_usage", None)
            if not phase_usage:
                return
            usage_totals["input"] += int(phase_usage.get("input", 0))
            usage_totals["output"] += int(phase_usage.get("output", 0))
            usage_totals["cache_read"] += int(phase_usage.get("cache_read", 0))
            usage_totals["cache_write"] += int(phase_usage.get("cache_write", 0))
            usage_totals["phase_calls"] += 1

        def _attach_usage_trailer(report_text: str) -> str:
            """Append a machine-readable token-usage trailer to the report.

            Wrapped in an HTML comment so any markdown renderer hides it
            from the user, while clients (e.g., benchmark.py) can parse
            authoritative Bedrock-reported totals via a simple regex
            search of the streamed text. This is the only escape hatch
            that survives the SSE pipeline — internal sub-agent calls
            never surface as outer SSE events, so without this trailer
            external clients cannot see the pipeline's true token cost.
            """
            trailer = (
                "\n\n<!-- per_token_usage: "
                + json.dumps(
                    {
                        "input": usage_totals["input"],
                        "output": usage_totals["output"],
                        "cache_read": usage_totals["cache_read"],
                        "cache_write": usage_totals["cache_write"],
                        "phase_calls": usage_totals["phase_calls"],
                    },
                    separators=(",", ":"),
                )
                + " -->"
            )
            return (report_text or "") + trailer

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
        _accumulate(plan_result)
        plan_steps, plan_final = _planner_decision_from_result(plan_result)

        if plan_final:
            # Planner answered directly without needing execution.
            log_info_event(
                logger,
                "[per] planner returned final result without execution",
                "per.pipeline.early_finish",
                phase="plan",
            )
            return _attach_usage_trailer(plan_final)

        if not plan_steps:
            log_warning_event(
                logger,
                "[per] planner emitted no steps and no result; aborting",
                "per.pipeline.empty_plan",
            )
            return _attach_usage_trailer(
                f"Planner produced no actionable plan.\n\nRaw output:\n{plan_result}"
            )

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
        # Track the most recent reflect-phase leading_candidate so the
        # next reflect prompt's symptom-LRU keeps every symptom about the
        # current candidate even when older symptoms are elided. Empty
        # string for the first iteration (no candidate has been declared
        # yet), which falls back to the pure recency cap.
        last_leading_candidate = ""
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

        while pending_steps and steps_executed < _MAX_STEPS:
            # Slice off this iteration's batch. Two ceilings apply: the
            # remaining step budget and the parallel-executor cap. Any
            # steps the reflector declared but we cut off here are NOT
            # carried over — the reflect phase that runs after the batch
            # will see the new evidence and re-decide what to dispatch
            # next, including whether the cut-off steps are still worth
            # running. This is the framework's intended control flow.
            batch_ceiling = min(
                max(1, _MAX_STEPS - steps_executed),
                _MAX_PARALLEL_EXECUTORS,
            )
            batch = pending_steps[:batch_ceiling]
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
                _accumulate(exec_result)
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

            log_info_event(
                logger,
                f"[per] convergence: phase={phase} new={new_evidence} "
                f"stagnant_rounds={stagnant_rounds} "
                f"high_grade_total={current_high_grade_count}",
                "per.pipeline.convergence",
                steps_executed=steps_executed,
                phase=phase,
                new_high_grade=new_evidence,
                stagnant_rounds=stagnant_rounds,
                high_grade_total=current_high_grade_count,
            )

            reflect_agent = build_reflect_agent()
            stagnant_force = stagnant_rounds >= _STAGNATION_ROUNDS_FORCE
            step_force = steps_executed >= _FORCE_FINALIZE_STEPS
            elapsed_seconds = time.perf_counter() - pipeline_started
            wallclock_force = elapsed_seconds >= _PIPELINE_WALL_CLOCK_BUDGET_SECONDS
            force_finalize = stagnant_force or step_force or wallclock_force
            if force_finalize:
                if wallclock_force:
                    triggered_by = "wallclock_budget"
                elif stagnant_force:
                    triggered_by = "stagnation"
                else:
                    triggered_by = "step_cap"
                log_warning_event(
                    logger,
                    f"[per] force-finalize triggered at step {steps_executed} "
                    f"(stagnant={stagnant_force}, step_cap={step_force}, "
                    f"wallclock={wallclock_force} elapsed={int(elapsed_seconds)}s)",
                    "per.pipeline.force_finalize",
                    steps_executed=steps_executed,
                    stagnant_rounds=stagnant_rounds,
                    stagnation_threshold=_STAGNATION_ROUNDS_FORCE,
                    step_threshold=_FORCE_FINALIZE_STEPS,
                    elapsed_seconds=int(elapsed_seconds),
                    wallclock_budget_seconds=_PIPELINE_WALL_CLOCK_BUDGET_SECONDS,
                    triggered_by=triggered_by,
                )
                reflect_prompt = _build_reflect_input(
                    objective=problem_statement,
                    plan_steps=plan_steps,
                    artifacts=artifacts,
                    force_finalize=True,
                )
            else:
                # Reflect input always shows full findings for the LAST
                # artifact only. Sibling artifacts from the same parallel
                # batch contribute through (a) their compact_table row
                # and (b) their KNOWN_FACTS entries — both already
                # carry the artifact id, which is enough for reflect to
                # reason about them as a unit. Including every batch
                # sibling's full findings every turn made reflect input
                # grow with both step count AND batch size, which is
                # what blew past Bedrock's context window after ~10
                # steps in earlier benchmark runs.
                reflect_prompt = _build_reflect_input(
                    objective=problem_statement,
                    plan_steps=plan_steps,
                    artifacts=artifacts,
                    leading_candidate=last_leading_candidate,
                )
            reflect_result, _, _, _ = await _run_phase(
                reflect_agent, reflect_prompt, "reflect", steps_executed
            )
            _accumulate(reflect_result)
            last_reflect_text = str(reflect_result)
            decision = _reflect_decision_from_result(reflect_result)
            if decision.leading_candidate:
                last_leading_candidate = decision.leading_candidate

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
                    total_input_tokens=usage_totals["input"],
                    total_output_tokens=usage_totals["output"],
                    total_cache_read_tokens=usage_totals["cache_read"],
                    total_cache_write_tokens=usage_totals["cache_write"],
                    total_phase_calls=usage_totals["phase_calls"],
                )
                return _attach_usage_trailer(final_text)

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
                    for probe in decision.outstanding_probes:
                        forced.append(
                            "Resolve this outstanding probe for the leading "
                            f"candidate '{decision.leading_candidate}': "
                            f"{probe}. Either produce [direct] / [deviation] "
                            "facts that confirm or rule out a mechanism on "
                            "this dimension, or record a KNOWN_FACT that "
                            "explicitly invalidates it (move it to "
                            "`dimensions_invalidated` next turn)."
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
                    total_input_tokens=usage_totals["input"],
                    total_output_tokens=usage_totals["output"],
                    total_cache_read_tokens=usage_totals["cache_read"],
                    total_cache_write_tokens=usage_totals["cache_write"],
                    total_phase_calls=usage_totals["phase_calls"],
                )
                return _attach_usage_trailer(decision.result)

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
            total_input_tokens=usage_totals["input"],
            total_output_tokens=usage_totals["output"],
            total_cache_read_tokens=usage_totals["cache_read"],
            total_cache_write_tokens=usage_totals["cache_write"],
            total_phase_calls=usage_totals["phase_calls"],
        )
        return _attach_usage_trailer(
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

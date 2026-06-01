"""PER Agent — generic Plan / Execute / Reflect investigation pipeline.

Domain methodology (root-cause analysis, security forensics, performance
regression bisect, etc.) is supplied at runtime by skills loaded from
``skills/`` (see ``_load_skills`` in ``sub_agents``). The framework
itself is domain-neutral.

Sub-agent prompts and orchestrator-injected instructions encode only
structural contracts — evidence-tag syntax, structured-output schema,
finalize-gate semantics, parallel-dispatch rules. Anything domain-
specific (what counts as a "comparable reference", what relation graph
to walk, what mechanisms compete with what, what ranking criterion to
order candidates by) is delegated to whichever skill is active. When
this comment block ever drifts out of sync with the prompt text, treat
the prompt as the source of truth and adjust here.

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
from typing import Final

import boto3
import httpx
from mcp.client.streamable_http import streamable_http_client
from strands import Agent
from strands.models.bedrock import BedrockModel
from strands.models.model import CacheConfig
from strands.tools.mcp import MCPClient

from agents.per.artifact_store import ArtifactStore
from agents.per.mechanism_discriminators import (
    MechanismClass,
    discriminator_violations,
)
from agents.per.sub_agents import (
    BaselineComparison,
    PlanOutput,
    RelatedEntityCheck,
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

@dataclass(frozen=True)
class PipelineLimits:
    """Pipeline thresholds expressed in REFLECT-ROUND units, not step units.

    A reflect ROUND is one plan→execute→reflect cycle. A step is one
    executor invocation; a single round can dispatch up to
    ``max_parallel_executors`` steps in parallel, so step count grows
    faster than round count when the reflector finds independent work.

    Investigation depth — how many distinct decisions the reflector has
    been allowed to make — tracks rounds, not steps. Earlier versions
    bounded the loop on step count, which let a 3-wide parallel batch
    consume 3× the depth budget per decision. This dataclass moves all
    depth bounds to rounds; step count is retained only as a telemetry
    metric and as a hard ceiling.

    Required ordering (verified in ``__post_init__``):

      ``finalize_gate_soft_cap_rounds``
      ``< force_finalize_after_rounds``
      ``< max_reflect_rounds``
    """

    # After this many reflect rounds, finalize gates STOP rejecting the
    # reflector's result. The gates catch premature finalization (one or
    # two rounds in); past this threshold the bigger risk is unbounded
    # looping where each gate-driven follow-up produces new outstanding
    # indicators that produce more follow-ups. Gate rejection becomes a
    # caveat in the log instead of a blocker.
    finalize_gate_soft_cap_rounds: int = 3

    # Hard fallback round count. If we hit this without converging, force
    # finalize regardless of state. Sized so a typical OTel-style triple-
    # source investigation (schema discovery + per-source comparison +
    # baseline + candidate walk) gets at least one full cycle of headroom
    # past the canonical 4-round floor before being force-finalized.
    force_finalize_after_rounds: int = 5

    # Absolute hard ceiling on reflect rounds. Investigations should
    # almost always force-finalize (above) before reaching this; this is
    # a safety net for cases where force-finalize itself fails to
    # produce a usable result and the loop somehow keeps going.
    max_reflect_rounds: int = 8

    # Number of consecutive reflect rounds with no new [direct] /
    # [deviation] fact at which we treat the investigation as stagnant
    # and switch to force-finalize. Investigations that produce evidence
    # are allowed to run; investigations that don't, aren't. This handles
    # the "reflector voluntarily keeps adding probes" failure where the
    # reflector never tries to finalize on its own — once new high-grade
    # evidence stops arriving, further probing is unlikely to change
    # the conclusion.
    stagnation_rounds_force: int = 2

    # Wall-clock budget for an entire PER pipeline run, in seconds. Once
    # the elapsed time crosses this, the NEXT reflect phase switches to
    # force-finalize regardless of round count or convergence state.
    # The hard guarantee that pipeline runs cannot drag past ~12 minutes
    # — independent of how cheaply or expensively each round happens to
    # behave on a given run. Force-finalize itself adds another 30-60s
    # for the final reflect plus output streaming, so the end-to-end
    # ceiling is roughly budget + ~120s.
    pipeline_wall_clock_budget_seconds: int = 720

    # Maximum number of executor sub-agents we will run concurrently in
    # a single fan-out batch. The reflector is allowed to declare more
    # independent steps than this — we just slice them across multiple
    # rounds. Three is the sweet spot for OTel-style triple-source
    # investigations (logs + traces + metrics) where all three are
    # commonly independent at the same round. Going higher (4+)
    # reproduces the context-overflow retry storm: each executor
    # inherits the full ArtifactStore-derived KNOWN_FACTS block plus
    # its own multi-tool-call conversation history, and 4+ of those in
    # flight push Bedrock's accumulated payload past the model's
    # context window.
    max_parallel_executors: int = 3

    # Hard ceiling on TOTAL executor invocations across all rounds. Sized
    # well above ``max_reflect_rounds * max_parallel_executors`` so it
    # never trips before the round-based gates do; exists only to bound
    # cost telemetry in pathological cases.
    max_total_steps: int = 30

    def __post_init__(self) -> None:
        if not (
            self.finalize_gate_soft_cap_rounds
            < self.force_finalize_after_rounds
            < self.max_reflect_rounds
        ):
            raise ValueError(
                "PipelineLimits: round thresholds must satisfy "
                "finalize_gate_soft_cap_rounds < force_finalize_after_rounds "
                "< max_reflect_rounds, got "
                f"{self.finalize_gate_soft_cap_rounds} < "
                f"{self.force_finalize_after_rounds} < "
                f"{self.max_reflect_rounds}"
            )


_LIMITS: Final = PipelineLimits()

ORCHESTRATOR_SYSTEM_PROMPT = """You are an investigation orchestrator. Your role
is to dispatch the user's investigation question to the plan→execute→
reflect pipeline and deliver the result in the format the user asked for.

When the user reports a problem that calls for a structured investigation
(an incident, anomaly, regression, outage, or any "why is this
happening" question that requires evidence-based reasoning across
available data), call the `run_per_pipeline` tool ONCE with a concise
problem statement that captures:
  - what is wrong (the symptom or observation)
  - what entity / scope is affected (if known)
  - the relevant time window (if known)

The pipeline returns a comprehensive markdown investigation report.

# Delivering the answer

Default behavior: return the pipeline's report verbatim — do NOT
rewrite, summarize, paraphrase, condense, or reformat it. Do NOT
prepend or append commentary, mitigation suggestions, or follow-up
questions. The pipeline's report is already the deliverable; reprocessing
it doubles token consumption and risks truncating the output.

Override: if the user's message explicitly specifies an output format,
schema, or shape (for example "respond with a JSON object", "use this
schema: {...}", "answer as a numbered list", "output only the service
name"), honor that request — read the pipeline's report and produce a
response in the requested format, populated from the report's findings.
The user's explicit format request takes precedence over the verbatim
default.

If `run_per_pipeline` returns an error message (e.g. "Max Steps Limit
Reached" or "Planner produced no actionable plan"), surface that
message — verbatim by default, or coerced into the user's requested
format if they specified one (using whatever partial signal the error
carries; explicitly mark unknowns).
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

    next_steps: list[str] = field(default_factory=list)
    result: str = ""
    leading_candidate: str = ""
    candidate_reason: str = ""
    outlier_candidate: str = ""
    outlier_reason: str = ""
    candidate_baseline: BaselineComparison = field(default_factory=BaselineComparison)
    related_entities_check: RelatedEntityCheck = field(default_factory=RelatedEntityCheck)
    outstanding_probes: list[str] = field(default_factory=list)
    proposed_mechanism: str = ""
    mechanism_class: MechanismClass = MechanismClass.OTHER
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
        candidate_baseline=so.candidate_baseline or BaselineComparison(),
        related_entities_check=so.related_entities_check or RelatedEntityCheck(),
        outstanding_probes=_clean_list(so.outstanding_probes),
        proposed_mechanism=(so.proposed_mechanism or "").strip(),
        mechanism_class=so.mechanism_class or MechanismClass.OTHER,
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


# Finalize-gate violation severity. ``critical`` violations are
# evidence-quality checks whose failure means the finalize is unsound
# regardless of how long the run has gone (e.g., naming a candidate with
# no [direct] fact, claiming a mechanism class without its discriminator
# token). ``weak`` violations are scope-coverage / audit-completeness
# checks (outstanding probes still listed, relation-graph walk not
# recorded) — useful early but, past the soft cap, more likely to drive
# unbounded looping than to catch real errors. The soft-cap branch in
# the pipeline drops ``weak`` violations and keeps ``critical`` ones live.
_GATE_CRITICAL = "critical"
_GATE_WEAK = "weak"


def _finalize_gate_violations(
    decision: ReflectDecision, artifacts: ArtifactStore
) -> list[tuple[str, str]]:
    """Deterministic checks that must pass before ``result`` is accepted.

    The reflector's prompt asks it to self-police several conditions
    (direct evidence required, causal-direction walk recorded, no outstanding
    indicators). These checks are duplicated in code so a reflector that
    finalizes prematurely is overruled rather than trusted. Returns a
    list of ``(severity, message)`` tuples — empty list means the
    finalization is allowed to proceed. Severity is ``_GATE_CRITICAL`` for
    evidence-soundness checks and ``_GATE_WEAK`` for scope-coverage /
    audit-completeness checks; the pipeline's soft-cap branch drops weak
    violations only.
    """
    if not decision.result:
        return []
    violations: list[tuple[str, str]] = []
    candidate = decision.leading_candidate
    if not candidate:
        violations.append((
            _GATE_CRITICAL,
            "leading_candidate is empty but result is populated — finalization "
            "requires naming the entity the conclusion attributes the cause to.",
        ))
    elif not artifacts.has_direct_fact_for(candidate):
        violations.append((
            _GATE_CRITICAL,
            f"No KNOWN_FACTS bullet tagged [direct] mentions '{candidate}'. "
            "Per rule 5 (RANK BY RELATIVE DEVIATION, BACK BY DIRECT EVIDENCE), "
            "finalization requires at least one direct fact for the named "
            "candidate; symptom-only attribution is forbidden.",
        ))
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
        violations.append((
            _GATE_WEAK,
            "outstanding_probes contains entries that touch the leading "
            f"candidate: {probes}. Per rule 3 (SCOPE COVERAGE BEFORE "
            "FINALIZE), each must be either probed (queue a step) or "
            "invalidated (move to dimensions_invalidated with a citing "
            "KNOWN_FACT) before finalizing.",
        ))
    if candidate and not decision.related_entities_check.is_populated():
        violations.append((
            _GATE_WEAK,
            "related_entities_check.entities_examined is empty. Per rule 4 "
            "(WALK BEFORE BLAMING), the walk is mandatory before "
            "finalizing — name the related entities (per the active "
            "skill's relation graph) you inspected and set "
            "promotion_made / promoted_to accordingly. If the active "
            "skill explicitly declares no relation graph applies to "
            "this task, list the candidate itself as the only entity "
            "examined to record that fact for the audit log.",
        ))
    if candidate and not decision.candidate_baseline.is_populated():
        missing = []
        if not decision.candidate_baseline.candidate_value.strip():
            missing.append("candidate_value")
        if not decision.candidate_baseline.reference_value.strip():
            missing.append("reference_value")
        if not decision.candidate_baseline.deviation_summary.strip():
            missing.append("deviation_summary")
        violations.append((
            _GATE_CRITICAL,
            "candidate_baseline is incomplete (missing: "
            f"{', '.join(missing)}). Per rule 5 (RANK BY RELATIVE "
            "DEVIATION), ranking by absolute magnitude is forbidden — "
            "populate all three sub-fields with the candidate's value, "
            "the comparable reference value (per the active skill's "
            "ranking criterion), and a summary of the relationship "
            "(ratio, percentage delta, fold change).",
        ))

    # Mechanism gate (critical rule 14): proposed_mechanism must be
    # populated whenever a candidate is named. Empty mechanism with a
    # populated candidate means the reflector identified WHO but not the
    # CAUSAL PROCESS — finalization in this state collapses "service is
    # at fault" with "service is at fault BECAUSE …", which is exactly
    # the failure mode reflection logs surfaced repeatedly.
    if candidate and not decision.proposed_mechanism:
        violations.append((
            _GATE_CRITICAL,
            "proposed_mechanism is empty but leading_candidate is named. "
            "Per rule 6 (MECHANISM), finalization requires naming the "
            "causal MECHANISM — not just the entity. State the underlying "
            "process by which the candidate produces the observed symptoms.",
        ))

    # Mechanism alternatives gate (critical rule 15): at least 2
    # competing mechanisms must be enumerated whenever proposed_mechanism
    # is set. Forces hypothesis-space enumeration before finalize so the
    # reflector cannot collapse onto the first self-consistent story.
    if decision.proposed_mechanism and len(decision.mechanism_alternatives) < 2:
        violations.append((
            _GATE_CRITICAL,
            "mechanism_alternatives has fewer than 2 entries. Per rule 6 "
            "(MECHANISM), finalization requires explicit enumeration of "
            "at least two competing mechanisms — list them with "
            "falsification rationale or remaining-plausibility notes.",
        ))

    # Mechanism evidence gate (critical rule 14 + 9): at least one
    # [direct] or [deviation] fact must back the mechanism claim.
    # Mechanism claims supported only by [symptom] facts are exactly the
    # "exception class = root cause" anti-pattern.
    if decision.proposed_mechanism:
        evidence_blob = "\n".join(decision.mechanism_evidence).lower()
        if not ("[direct]" in evidence_blob or "[deviation]" in evidence_blob):
            violations.append((
                _GATE_CRITICAL,
                "mechanism_evidence does not cite any [direct] or "
                "[deviation] KNOWN_FACT. Per rule 6 (MECHANISM), mechanism "
                "claims must be backed by direct evidence — symptom-only "
                "support is insufficient for naming a causal process.",
            ))

    # Discriminator gate (mode coverage): if the reflector committed to
    # a specific mechanism class, the artifact store must contain at
    # least one [direct] / [deviation] fact carrying a discriminator
    # token for that class. Scanning the store (not just
    # mechanism_evidence) is intentional — the executor frequently
    # produces a discriminator fact that the reflector forgets to cite
    # in mechanism_evidence; we don't want the gate to reject a
    # finalize whose underlying evidence actually exists. Conversely,
    # a mechanism_class commitment with NO matching fact anywhere is a
    # hard reject: it is the exact failure pattern the benchmark
    # reflections kept surfacing (mechanism named, discriminator
    # never queried).
    if decision.proposed_mechanism and decision.mechanism_class != MechanismClass.OTHER:
        high_grade_facts = "\n".join(
            [artifacts.all_direct_facts(), artifacts.all_deviation_facts()]
        )
        # Combine artifact-store evidence with anything the reflector
        # cited inline; either qualifies as "the fact exists in this
        # run" for gate purposes.
        combined = (
            high_grade_facts + "\n" + "\n".join(decision.mechanism_evidence)
        )
        for msg in discriminator_violations(decision.mechanism_class, combined):
            violations.append((_GATE_CRITICAL, msg))

    return violations


def _discriminator_unmet(
    decision: ReflectDecision, artifacts: ArtifactStore
) -> bool:
    """True iff the reflector's mechanism_class lacks any discriminator token
    in the high-grade evidence + cited mechanism_evidence.

    Same semantics as the discriminator branch of ``_finalize_gate_violations``,
    extracted so the force-finalize branch can act on it (downgrade to OTHER)
    without re-running the rest of the gate, which is intentionally suspended
    at force-finalize.
    """
    if not decision.proposed_mechanism:
        return False
    if decision.mechanism_class == MechanismClass.OTHER:
        return False
    high_grade = "\n".join(
        [artifacts.all_direct_facts(), artifacts.all_deviation_facts()]
    )
    combined = high_grade + "\n" + "\n".join(decision.mechanism_evidence)
    return bool(discriminator_violations(decision.mechanism_class, combined))


def _extract_assistant_text(result) -> str:
    """Best-effort extraction of an assistant-authored final string from an
    ``AgentResult``.

    ``str(AgentResult)`` is unsuitable as a user-facing fallback because
    its repr can include internal tool-call debugging fields. Walk the
    standard Strands result shape (``result.message["content"][i]["text"]``
    blocks) and concatenate any text blocks. Returns an empty string when
    no text is available so callers can decide on their own fallback
    string rather than getting an internal repr by accident.
    """
    message = getattr(result, "message", None)
    if not message:
        return ""
    content = message.get("content") if isinstance(message, dict) else None
    if not content:
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, dict):
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "\n".join(parts).strip()


def _split_violations(
    violations: list[tuple[str, str]],
) -> tuple[list[str], list[str]]:
    """Partition gate violations into ``(critical, weak)`` message lists."""
    critical: list[str] = []
    weak: list[str] = []
    for severity, message in violations:
        if severity == _GATE_CRITICAL:
            critical.append(message)
        else:
            weak.append(message)
    return critical, weak


def _compute_convergence(
    artifacts: ArtifactStore, last_high_grade_count: int
) -> tuple[int, int, str]:
    """Re-count [direct]/[deviation] facts and classify the round.

    Returns ``(current_high_grade_count, new_evidence, phase)`` where
    ``phase`` is one of ``"discovery"``, ``"advancing"``, ``"stagnant"``.
    Pure function — no global state, easy to unit-test.
    """
    current = sum(
        1
        for art in artifacts
        for fact in art.facts
        if fact.lstrip().lower().startswith(("[direct]", "[deviation]"))
    )
    new_evidence = current - last_high_grade_count
    if current == 0:
        return current, new_evidence, "discovery"
    if new_evidence > 0:
        return current, new_evidence, "advancing"
    return current, new_evidence, "stagnant"


def _decide_force_finalize(
    *,
    next_round: int,
    stagnant_rounds: int,
    pipeline_started: float,
    limits: PipelineLimits,
) -> tuple[bool, str | None, int]:
    """Decide whether the upcoming reflect round must force-finalize.

    Returns ``(force, triggered_by, elapsed_seconds)``. ``triggered_by``
    is ``None`` when ``force`` is False; otherwise one of
    ``"wallclock_budget"`` / ``"stagnation"`` / ``"round_cap"``.
    """
    elapsed = int(time.perf_counter() - pipeline_started)
    stagnant_force = stagnant_rounds >= limits.stagnation_rounds_force
    round_force = next_round >= limits.force_finalize_after_rounds
    wallclock_force = elapsed >= limits.pipeline_wall_clock_budget_seconds
    if not (stagnant_force or round_force or wallclock_force):
        return False, None, elapsed
    if wallclock_force:
        return True, "wallclock_budget", elapsed
    if stagnant_force:
        return True, "stagnation", elapsed
    return True, "round_cap", elapsed


def _synthesize_followup_steps(
    decision: ReflectDecision, all_violations: list[str]
) -> list[str]:
    """Build the next-iteration pending_steps after a finalize-gate reject.

    Prefer probing each entry in ``outstanding_probes`` (these are the
    structured items the reflector itself flagged as still-unresolved).
    Fall back to a coaching step that surfaces the violations verbatim
    when no outstanding probe is available — typically when the
    violation is in candidate_baseline / related_entities_check shape.
    """
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
        return forced
    return [
        "The previous reflect attempt to finalize was "
        "rejected by the finalize gate. Violations:\n- "
        + "\n- ".join(all_violations)
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


def _force_finalize_fallback_report(
    *,
    decision: ReflectDecision,
    artifacts: ArtifactStore,
    reflect_rounds: int,
    steps_executed: int,
) -> str:
    """Synthesize a minimal report when force-finalize returned no text.

    Reached only when both (a) the reflector ignored the FORCE FINALIZE
    instruction and emitted an empty ``result``, and (b) no assistant
    prose could be recovered from the message blocks. Surfacing
    ``str(AgentResult)`` here would dump internal repr to the user; we
    instead emit a clearly-labelled "incomplete" report that lists the
    leading candidate (if any) and the best high-grade evidence we
    have, so the run is still useful as audit material.
    """
    deviation = artifacts.all_deviation_facts()
    direct = artifacts.all_direct_facts()
    sections = [
        "# Investigation incomplete",
        (
            "The PER pipeline reached the force-finalize point but the "
            "reflector did not produce a final report. Surfacing "
            "captured high-grade evidence below for audit."
        ),
        f"- reflect_rounds: {reflect_rounds}",
        f"- steps_executed: {steps_executed}",
    ]
    if decision.leading_candidate:
        sections.append(
            f"- last leading_candidate: `{decision.leading_candidate}`"
        )
    if decision.proposed_mechanism:
        sections.append(
            f"- last proposed_mechanism: `{decision.proposed_mechanism}`"
        )
    if deviation:
        sections.append("\n## [deviation] facts\n" + deviation)
    if direct:
        sections.append("\n## [direct] facts\n" + direct)
    if not deviation and not direct:
        sections.append(
            "\nNo [direct] or [deviation] facts were recorded — the "
            "investigation never reached evidence-quality findings."
        )
    return "\n".join(sections)


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

# Cap on the number of schema facts the reflector sees verbatim each
# round. Schema facts (field names, units, query constraints) are WORM:
# once recorded, the truth doesn't change. Recent schema discoveries
# matter more than ones from the first round of an 8-round investigation.
_SCHEMA_FACTS_RECENT_CAP = 25

# Soft character budget on the entire reflect prompt. When exceeded,
# older artifact full-findings are dropped and the LRU-collapsible fact
# sections (symptom / schema) get tighter caps. The number is tuned for
# ~50-60% headroom under Bedrock Sonnet's 200K-context limit, leaving
# room for executor sub-agent payloads, system prompt, and skill content.
_REFLECT_PROMPT_SOFT_CHAR_BUDGET = 120_000


def _build_known_facts_block(
    artifacts: ArtifactStore,
    leading_candidate: str,
    *,
    symptom_cap: int = _SYMPTOM_FACTS_RECENT_CAP,
    schema_cap: int = _SCHEMA_FACTS_RECENT_CAP,
) -> str:
    """Render every recorded fact in tag-priority order as one block.

    Order is fixed so the reflector's eye always lands on
    ranking-eligible evidence first: [deviation] → [direct] → [symptom]
    (LRU-capped, leading-candidate whitelisted) → [schema]/unclassified
    (LRU-capped). Each section contributes its lines verbatim with the
    artifact-id prefix already attached by the store; sections that are
    empty are silently skipped so the result is dense.

    The deviation and direct sections are NEVER capped — they are the
    only evidence eligible to ground a final attribution, and an elided
    [direct] fact for the leading candidate would defeat the whole
    finalize-gate machinery.
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
        max_recent=symptom_cap,
    )
    if symptom:
        parts.append(symptom)
    schema = artifacts.all_schema_facts(max_recent=schema_cap)
    if schema:
        parts.append(schema)
    return "\n".join(parts)


_NORMAL_REFLECT_INSTRUCTIONS = (
    "Output the next step(s) to execute in `next_steps` (a list — "
    "prefer parallel dispatch when steps target independent data "
    "sources; otherwise return a single-element list), or finalize "
    "with a comprehensive report in `result`. Populate every audit "
    "field — `leading_candidate`, `candidate_reason`, "
    "`outlier_candidate`, `outlier_reason`, `candidate_baseline`, "
    "`related_entities_check`, and `outstanding_probes` — every turn "
    "so the orchestrator can verify finalize gates. Never repeat a "
    "completed step. Never propose work that contradicts "
    "KNOWN_FACTS.\n\n"
    "Finalization will be REJECTED by the orchestrator if ANY of:\n"
    "  (a) `leading_candidate` has no [direct] evidence;\n"
    "  (b) `outstanding_probes` contains entries that touch the "
    "leading candidate;\n"
    "  (c) `related_entities_check.entities_examined` is empty (the "
    "active domain skill's relation graph — dependencies, peers, "
    "callees, parents, etc. — must have been walked, even when no "
    "promotion happens);\n"
    "  (d) `candidate_baseline` is incomplete (`candidate_value`, "
    "`reference_value`, and `deviation_summary` must all be present "
    "— a relative comparison against a comparable reference, never an "
    "absolute magnitude).\n\n"
    "Use the `outlier_candidate` slot every turn to keep the "
    "second-most-anomalous entity visible — confirmation-bias "
    "collapse onto the leading candidate is a recurring miss path.\n\n"
    "RANKING DISCIPLINE — when an active domain skill defines a "
    "ranking criterion (a formula, score, or comparable axis along "
    "which candidates are ordered), `leading_candidate` MUST come "
    "from the top of that ranking. Before naming a candidate, ensure "
    "a [deviation] fact exists that lets the criterion be computed "
    "for every entity with non-trivial activity in the anomaly "
    "window; if no such ranking fact exists yet, dispatch a step "
    "that produces it before naming a leading_candidate. Do NOT pick "
    "candidates from intuition or from the loudest absolute change. "
    "To deviate from the top-ranked entry, cite in "
    "`candidate_reason` the specific [direct] fact that overrides "
    "the skill's ranking criterion. (If no skill defines a ranking "
    "criterion, derive one from the data: a relative-deviation × "
    "throughput-weight composite is the framework default.)"
)

_FORCE_FINALIZE_INSTRUCTIONS = (
    "FORCE FINALIZE: the investigation has run long enough that "
    "further probing is unlikely to change the conclusion more than "
    "it lengthens the run. You MUST now produce your best `result` "
    "from the evidence above. Most finalize gates and the normal "
    "scope-coverage discipline are suspended for this turn — your "
    "`next_steps` will be IGNORED. Do not propose more probes.\n\n"
    "EXCEPTION — `mechanism_alternatives` is NOT suspended. You "
    "MUST still populate it with at least 2 entries when "
    "`proposed_mechanism` is set. Treat this as the most important "
    "guard against confirmation-bias collapse. When the active "
    "domain skill names symmetric / fingerprint-equivalent "
    "alternative mechanisms (different processes producing identical "
    "observations), include them here even if you cannot fully "
    "discriminate among them — list each as 'plausible alternative — "
    "discriminating evidence not collected' rather than silently "
    "committing to your top pick. If no skill applies, derive at "
    "least one symmetric alternative from first principles for any "
    "mechanism you propose.\n\n"
    "EXCEPTION — the ranking-discipline rule is NOT suspended at "
    "force-finalize either: `leading_candidate` MUST come from the "
    "top of the active skill's ranking criterion (or the "
    "framework-default deviation × throughput-weight composite) when "
    "[deviation] facts allow that score to be computed. If you "
    "cannot produce or cite such a ranking from the gathered "
    "evidence, mark the conclusion explicitly low-confidence in "
    "`result` and list the un-ranked-out candidates in "
    "`mechanism_alternatives`.\n\n"
    "In `result`, write a comprehensive report that:\n"
    "1. Names your best top candidate(s) with the relative-deviation "
    "evidence that places them.\n"
    "2. Cites the specific [deviation] / [direct] facts that ground "
    "each named candidate.\n"
    "3. Acknowledges what could not be determined from available "
    "evidence (unprobed dimensions, missing baselines, etc.) — these "
    "are caveats inside the report, NOT reasons to keep "
    "investigating.\n"
    "4. Distinguishes the entity that is the actual cause from "
    "entities where the symptom merely surfaces, per the evidence "
    "at hand; if the distinction is genuinely undetermined, say so.\n\n"
    "Populate `leading_candidate`, `candidate_reason`, "
    "`outlier_candidate`, `outlier_reason`, `candidate_baseline`, "
    "and `related_entities_check` for the audit log, and leave "
    "`next_steps` and `outstanding_probes` empty. The result you "
    "produce now is what will be returned to the user."
)


def _build_reflect_input(
    objective: str,
    plan_steps: list[str],
    artifacts: ArtifactStore,
    leading_candidate: str = "",
    *,
    force_finalize: bool = False,
    char_budget: int = _REFLECT_PROMPT_SOFT_CHAR_BUDGET,
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
    prompt = "\n\n".join(sections)

    # Soft budget guard. If the assembled prompt is over budget, retry
    # once with halved LRU caps and the latest-findings block dropped.
    # We don't recurse beyond one retry — going below the half-cap risks
    # eliding evidence the reflector actually needs to make its decision.
    # Force-finalize input is already minimal (deviation + direct only)
    # so it skips the retry.
    if force_finalize or len(prompt) <= char_budget:
        return prompt
    log_warning_event(
        logger,
        f"[per] reflect prompt exceeds soft char budget "
        f"({len(prompt)} > {char_budget}); tightening LRU caps",
        "per.pipeline.reflect_prompt_over_budget",
        prompt_chars=len(prompt),
        char_budget=char_budget,
    )
    return _build_reflect_input_tightened(
        objective=objective,
        plan_steps=plan_steps,
        artifacts=artifacts,
        leading_candidate=leading_candidate,
    )


def _build_reflect_input_tightened(
    objective: str,
    plan_steps: list[str],
    artifacts: ArtifactStore,
    leading_candidate: str,
) -> str:
    """Slim version of ``_build_reflect_input`` for over-budget cases.

    Halves the symptom + schema LRU caps and drops the most-recent-step
    full findings (its facts are already in KNOWN_FACTS). Keeps everything
    finalize-gate-relevant (deviation + direct in full) intact.
    """
    sections = [f"Objective:\n{objective}"]
    if plan_steps:
        plan_block = "\n".join(f"- {step}" for step in plan_steps)
        sections.append(
            "Original plan (for context only — do not echo or restate):\n"
            + plan_block
        )
    compact = artifacts.compact_table()
    if compact:
        sections.append(f"Completed steps (summary):\n{compact}")
    queries = artifacts.all_queries()
    if queries:
        sections.append(
            "QUERIES_EXECUTED (what was ACTUALLY probed):\n" + queries
        )
    facts_block = _build_known_facts_block(
        artifacts,
        leading_candidate,
        symptom_cap=max(5, _SYMPTOM_FACTS_RECENT_CAP // 2),
        schema_cap=max(5, _SCHEMA_FACTS_RECENT_CAP // 2),
    )
    if facts_block:
        sections.append(
            "KNOWN_FACTS (older symptom / schema entries elided to fit budget):\n"
            + facts_block
        )
    sections.append(_NORMAL_REFLECT_INSTRUCTIONS)
    return "\n\n".join(sections)


# Soft character budget on the entire execute prompt. The executor's
# multi-turn loop accumulates tool_results ON TOP of this prefix, so the
# prefix needs more headroom than the reflect prompt: by step 8 a single
# executor turn can carry 30-50K chars of tool_result alongside the
# pre-injected KNOWN_FACTS/QUERIES_EXECUTED. Set lower than reflect's
# 120K accordingly.
_EXECUTE_PROMPT_SOFT_CHAR_BUDGET = 80_000


def _build_execute_input(step: str, artifacts: ArtifactStore) -> str:
    """Assemble the execute-phase prompt.

    The executor is rebuilt fresh each iteration (no cross-step conversation
    memory), so KNOWN_FACTS and QUERIES_EXECUTED established by prior steps
    must be reintroduced explicitly. Without this, the executor reflexively
    re-runs index/field discovery it has no way of knowing was already done.

    When the assembled prompt exceeds the soft budget, drop low-value
    sections in priority order: queries → schema/symptom older entries →
    keep [deviation] and [direct] full. Mirror the reflect-side LRU
    discipline so a single long-running pipeline doesn't blow past the
    executor's context window through repeated KNOWN_FACTS injection.
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
    prompt = "\n\n".join(sections)
    if len(prompt) <= _EXECUTE_PROMPT_SOFT_CHAR_BUDGET:
        return prompt

    log_warning_event(
        logger,
        f"[per] execute prompt exceeds soft char budget "
        f"({len(prompt)} > {_EXECUTE_PROMPT_SOFT_CHAR_BUDGET}); "
        "tightening to high-grade facts only",
        "per.pipeline.execute_prompt_over_budget",
        prompt_chars=len(prompt),
        char_budget=_EXECUTE_PROMPT_SOFT_CHAR_BUDGET,
    )
    return _build_execute_input_tightened(step, artifacts)


def _build_execute_input_tightened(step: str, artifacts: ArtifactStore) -> str:
    """Slim version of ``_build_execute_input`` for over-budget cases.

    Keeps everything an executor needs to avoid rediscovery — schema
    facts (most recent N) and any high-grade ([deviation]/[direct])
    facts in full — but elides older [symptom] entries and drops the
    QUERIES_EXECUTED list entirely (executors can re-derive query text
    from KNOWN_FACTS faster than the reflector can audit a giant list).
    """
    sections = [f"Step to execute:\n{step}"]
    deviation = artifacts.all_deviation_facts()
    direct = artifacts.all_direct_facts()
    schema = artifacts.all_schema_facts(max_recent=15)
    parts: list[str] = []
    if deviation:
        parts.append(deviation)
    if direct:
        parts.append(direct)
    if schema:
        parts.append(schema)
    if parts:
        sections.append(
            "KNOWN_FACTS (older [symptom] entries elided, QUERIES_EXECUTED "
            "elided to fit budget — use these high-grade facts directly):\n"
            + "\n".join(parts)
        )
    sections.append(
        "Execute this single step and return your findings as plain text, "
        "ending with the required `QUERIES_EXECUTED:` and `KNOWN_FACTS:` sections."
    )
    return "\n\n".join(sections)


# Per-call cache hit-rate threshold below which we emit a warning. The
# definition is the standard Anthropic / Bedrock formula:
#
#     hit_ratio = cache_read / (cache_read + cache_write + input)
#
# Anthropic's published guidance for steady-state multi-turn workflows
# is ≥ 0.7 from the second turn onward. Using a softer 0.3 floor here
# because:
#   - the threshold has to fire on individual sub-agent calls, not
#     aggregated runs, and a single call can dip without indicating a
#     regression (e.g., the executor's first turn before the cached
#     prefix is reused);
#   - 0.3 is well below the "everything is uncached" 0.0 baseline but
#     still high enough that a real misconfiguration (cache_control in
#     the wrong place / tools list re-ordered every call) trips it.
#
# We additionally suppress the warning for the first time a given phase
# is exercised in a pipeline — a brand-new prefix has nothing to read.
_CACHE_HIT_WARN_THRESHOLD = 0.3


def _cache_hit_ratio(usage: dict[str, int]) -> float:
    """Standard Bedrock cache hit ratio for a single sub-agent call."""
    cr = int(usage.get("cache_read", 0))
    cw = int(usage.get("cache_write", 0))
    inp = int(usage.get("input", 0))
    denom = cr + cw + inp
    if denom <= 0:
        return 0.0
    return cr / denom


async def _run_phase(agent: Agent, prompt: str, phase: str, step_index: int):
    """Invoke a sub-agent and log timing + token usage for the phase.

    Returns ``(result, elapsed_ms, tokens_in, tokens_out)`` — the same
    4-tuple call sites have always consumed. Cache tokens (and the
    derived hit ratio) are surfaced on the result object's
    ``per_phase_usage`` attribute and via the log event below, so
    existing callers keep working unchanged while the orchestration
    loop can opt into the richer view.
    """
    started = time.perf_counter()
    result = await agent.invoke_async(prompt)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    usage = _full_usage(result)
    tokens_in = usage["input"]
    tokens_out = usage["output"]
    cache_read = usage["cache_read"]
    cache_write = usage["cache_write"]
    hit_ratio = _cache_hit_ratio(usage)
    usage["hit_ratio"] = round(hit_ratio, 4)
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
        cache_hit_ratio=round(hit_ratio, 4),
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
        # executor + each executor's nested tool-use turns). Three views
        # are maintained simultaneously:
        #   - top-level totals (input / output / cache_read / cache_write)
        #     mirror the v1 trailer for backwards compatibility;
        #   - ``by_phase`` lets benchmark.py answer "which phase is
        #     expensive?" — plan/reflect tend to dominate output cost
        #     while executor dominates input cost;
        #   - ``per_round`` lets benchmark.py answer "did context
        #     accumulation degrade later rounds?" by giving one row
        #     per reflect round with its parallel executor sub-totals.
        # Trailer is logged on pipeline finish so external benchmarks /
        # billing stop having to estimate from SSE event deltas, which
        # under-counts the PER pipeline's internal Bedrock traffic by
        # ~40× (none of the internal sub-agent traffic ever surfaces as
        # an outer-tool SSE event).
        def _empty_phase_bucket() -> dict[str, int]:
            return {
                "input": 0,
                "output": 0,
                "cache_read": 0,
                "cache_write": 0,
                "calls": 0,
            }

        usage_totals: dict = {
            "input": 0,
            "output": 0,
            "cache_read": 0,
            "cache_write": 0,
            "phase_calls": 0,
            "by_phase": {
                "plan": _empty_phase_bucket(),
                "reflect": _empty_phase_bucket(),
                "execute": _empty_phase_bucket(),
            },
            "per_round": [],  # populated as the loop runs
        }

        def _phase_usage(result_obj) -> dict[str, int] | None:
            """Pull the per-phase usage dict ``_run_phase`` stashed on result."""
            phase_usage = getattr(result_obj, "per_phase_usage", None)
            if not phase_usage:
                return None
            return {
                "input": int(phase_usage.get("input", 0)),
                "output": int(phase_usage.get("output", 0)),
                "cache_read": int(phase_usage.get("cache_read", 0)),
                "cache_write": int(phase_usage.get("cache_write", 0)),
            }

        def _accumulate(result_obj, phase: str) -> dict[str, int] | None:
            """Add a sub-agent call's usage to top-level + per-phase totals.

            Returns the per-call usage dict so callers (the executor batch
            loop) can stash it on per-round records. ``phase`` must be
            one of ``"plan"`` / ``"reflect"`` / ``"execute"``.

            Also fires a soft cache-regression warning when this phase's
            second-or-later call dips below ``_CACHE_HIT_WARN_THRESHOLD``.
            The first call in a phase has nothing to hit (the cached
            prefix is being created), so we never warn on call #1.
            """
            usage = _phase_usage(result_obj)
            if usage is None:
                return None
            usage_totals["input"] += usage["input"]
            usage_totals["output"] += usage["output"]
            usage_totals["cache_read"] += usage["cache_read"]
            usage_totals["cache_write"] += usage["cache_write"]
            usage_totals["phase_calls"] += 1
            bucket = usage_totals["by_phase"].setdefault(
                phase, _empty_phase_bucket()
            )
            bucket["input"] += usage["input"]
            bucket["output"] += usage["output"]
            bucket["cache_read"] += usage["cache_read"]
            bucket["cache_write"] += usage["cache_write"]
            bucket["calls"] += 1

            ratio = _cache_hit_ratio(usage)
            if (
                bucket["calls"] >= 2
                and ratio < _CACHE_HIT_WARN_THRESHOLD
            ):
                log_warning_event(
                    logger,
                    f"[per] low cache hit ratio on {phase} call "
                    f"#{bucket['calls']}: {ratio:.2%} "
                    f"(threshold {_CACHE_HIT_WARN_THRESHOLD:.0%}); "
                    "cache_control may be misconfigured or the cached "
                    "prefix may have been invalidated (tool list "
                    "reorder / system prompt drift).",
                    "per.pipeline.cache_hit_low",
                    phase=phase,
                    phase_call_index=bucket["calls"],
                    cache_hit_ratio=round(ratio, 4),
                    threshold=_CACHE_HIT_WARN_THRESHOLD,
                    input_tokens=usage["input"],
                    cache_read_tokens=usage["cache_read"],
                    cache_write_tokens=usage["cache_write"],
                )
            return usage

        def _attach_usage_trailer(report_text: str) -> str:
            """Append a machine-readable token-usage trailer to the report.

            Format (v2): a sentinel-pair HTML comment block carrying a
            JSON object with the schema_version + breakdown. Markdown
            renderers hide HTML comments, so end users never see this;
            clients (e.g., benchmark.py) parse the block via the matching
            sentinel pair to recover authoritative Bedrock-reported
            totals. This is the only escape hatch that survives the SSE
            pipeline — internal sub-agent calls never surface as
            outer SSE events, so without this trailer external clients
            cannot see the pipeline's true token cost.

            The sentinel-pair (begin / end) form replaces the v1 single-
            comment format because v1's ``-->`` terminator could
            collide with HTML comments inside a model-authored report;
            the explicit pair anchors parsing unambiguously and the
            ``v=N`` field lets older clients fall back gracefully when
            new fields appear.
            """
            # Bake per-phase aggregate hit ratio into the trailer so
            # benchmark.py doesn't have to recompute it. Same formula as
            # the per-call ratio, applied to the running totals.
            by_phase_with_ratio = {}
            for phase_name, bucket in usage_totals["by_phase"].items():
                by_phase_with_ratio[phase_name] = {
                    **bucket,
                    "cache_hit_ratio": round(_cache_hit_ratio(bucket), 4),
                }
            top_level_ratio = _cache_hit_ratio(
                {
                    "input": usage_totals["input"],
                    "cache_read": usage_totals["cache_read"],
                    "cache_write": usage_totals["cache_write"],
                }
            )
            payload = {
                "schema_version": 2,
                "input": usage_totals["input"],
                "output": usage_totals["output"],
                "cache_read": usage_totals["cache_read"],
                "cache_write": usage_totals["cache_write"],
                "cache_hit_ratio": round(top_level_ratio, 4),
                "phase_calls": usage_totals["phase_calls"],
                "by_phase": by_phase_with_ratio,
                "per_round": usage_totals["per_round"],
            }
            trailer = (
                "\n\n<!-- per_token_usage_begin v=2 -->\n"
                + json.dumps(payload, separators=(",", ":"))
                + "\n<!-- per_token_usage_end -->"
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
        _accumulate(plan_result, phase="plan")
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
        reflect_rounds = 0
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
        # Bind early so the post-loop ceiling-reached branch can reach
        # the most recent reflect decision (if the loop body ran) or
        # fall back to a fresh empty one (if it didn't).
        decision = ReflectDecision()

        while (
            pending_steps
            and reflect_rounds < _LIMITS.max_reflect_rounds
            and steps_executed < _LIMITS.max_total_steps
        ):
            # Slice off this iteration's batch. Two ceilings apply: the
            # remaining step budget and the parallel-executor cap. Any
            # steps the reflector declared but we cut off here are NOT
            # carried over — the reflect phase that runs after the batch
            # will see the new evidence and re-decide what to dispatch
            # next, including whether the cut-off steps are still worth
            # running. This is the framework's intended control flow.
            batch_ceiling = min(
                max(1, _LIMITS.max_total_steps - steps_executed),
                _LIMITS.max_parallel_executors,
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

            executor_usage_records: list[dict] = []
            for i, (step_text, outcome) in enumerate(zip(batch, exec_outcomes)):
                exec_result, exec_ms, exec_in, exec_out = outcome
                exec_usage = _accumulate(exec_result, phase="execute")
                step_index = steps_executed + 1 + i
                if exec_usage is not None:
                    executor_usage_records.append(
                        {
                            "step": step_index,
                            "elapsed_ms": exec_ms,
                            **exec_usage,
                        }
                    )
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
            # or [deviation] fact? Stagnation is only meaningful AFTER
            # the investigation has produced at least one — the
            # discovery phase produces only [schema] facts by design,
            # and a strict "no high-grade for N rounds" rule would
            # force-finalize before any per-entity comparison runs.
            current_high_grade_count, new_evidence, phase = _compute_convergence(
                artifacts, last_high_grade_count
            )
            if phase == "stagnant":
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
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
            # The reflect we are about to dispatch is round (reflect_rounds + 1).
            # Use the post-increment value when comparing against thresholds
            # so a limit of N means "force finalize when entering round N".
            next_round = reflect_rounds + 1
            force_finalize, triggered_by, elapsed_seconds = _decide_force_finalize(
                next_round=next_round,
                stagnant_rounds=stagnant_rounds,
                pipeline_started=pipeline_started,
                limits=_LIMITS,
            )
            if force_finalize:
                log_warning_event(
                    logger,
                    f"[per] force-finalize triggered at round {next_round} "
                    f"(triggered_by={triggered_by}, "
                    f"stagnant_rounds={stagnant_rounds}, "
                    f"elapsed={elapsed_seconds}s)",
                    "per.pipeline.force_finalize",
                    reflect_round=next_round,
                    steps_executed=steps_executed,
                    stagnant_rounds=stagnant_rounds,
                    stagnation_threshold=_LIMITS.stagnation_rounds_force,
                    round_threshold=_LIMITS.force_finalize_after_rounds,
                    elapsed_seconds=elapsed_seconds,
                    wallclock_budget_seconds=_LIMITS.pipeline_wall_clock_budget_seconds,
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
            reflect_result, reflect_ms, _, _ = await _run_phase(
                reflect_agent, reflect_prompt, "reflect", steps_executed
            )
            reflect_usage = _accumulate(reflect_result, phase="reflect")
            reflect_rounds += 1
            # Record this round's full usage breakdown so benchmark.py
            # can reconstruct cost over time. The list grows once per
            # reflect call; force-finalize / soft-cap branches read out
            # the accumulated state below before returning.
            usage_totals["per_round"].append(
                {
                    "round": reflect_rounds,
                    "force_finalize": force_finalize,
                    "reflect": {
                        "elapsed_ms": reflect_ms,
                        **(
                            reflect_usage
                            or {
                                "input": 0,
                                "output": 0,
                                "cache_read": 0,
                                "cache_write": 0,
                            }
                        ),
                    },
                    "executors": executor_usage_records,
                }
            )
            last_reflect_text = _extract_assistant_text(reflect_result)
            decision = _reflect_decision_from_result(reflect_result)
            if decision.leading_candidate:
                last_leading_candidate = decision.leading_candidate

            # Under force-finalize, whatever the model returns is
            # accepted as the final report — gates are suspended and
            # next_steps are ignored. If the model returned an empty
            # `result` despite the explicit instruction, fall back to
            # surfacing the raw text so the user gets something.
            if force_finalize:
                # Discriminator gate: the only critical gate we keep live
                # at force-finalize. Rejecting outright would mean
                # returning nothing, so instead we DOWNGRADE the
                # commitment — rewrite mechanism_class to OTHER and
                # prepend a caveat to the result so the user sees that
                # the named mechanism was not backed by its
                # discriminator. This catches the failure pattern where
                # the reflector keeps proposing a class whose
                # discriminator token was never observed and the
                # round/wallclock cap finally lets it through.
                downgraded_class: MechanismClass | None = None
                if _discriminator_unmet(decision, artifacts):
                    downgraded_class = decision.mechanism_class
                    log_warning_event(
                        logger,
                        "[per] force-finalize: discriminator token never "
                        f"observed for mechanism_class={downgraded_class.value}; "
                        "downgrading to OTHER and surfacing caveat",
                        "per.pipeline.force_finalize_downgrade",
                        reflect_rounds=reflect_rounds,
                        steps_executed=steps_executed,
                        original_mechanism_class=downgraded_class.value,
                        leading_candidate=decision.leading_candidate,
                    )
                    decision.mechanism_class = MechanismClass.OTHER

                base_text = (
                    decision.result
                    or last_reflect_text
                    or _force_finalize_fallback_report(
                        decision=decision,
                        artifacts=artifacts,
                        reflect_rounds=reflect_rounds,
                        steps_executed=steps_executed,
                    )
                )
                if downgraded_class is not None:
                    caveat = (
                        f"> **Caveat (force-finalize downgrade):** the "
                        f"reflector named `{downgraded_class.value}` as the "
                        "mechanism, but no [direct] / [deviation] fact in "
                        "this run carries a discriminator token for that "
                        "class (e.g. `cfs_throttled` for "
                        "cpu-compute-saturation, `packets_dropped` / `RTO` "
                        "for network-loss, `working_set` / `OOMKilled` for "
                        "memory-pressure). The mechanism is therefore "
                        "**unverified**; treat the named cause as the "
                        "reflector's best guess from indirect evidence "
                        "rather than a discriminator-backed conclusion."
                    )
                    final_text = caveat + "\n\n" + base_text
                else:
                    final_text = base_text
                total_ms = int((time.perf_counter() - pipeline_started) * 1000)
                log_info_event(
                    logger,
                    "[per] pipeline complete (force-finalized)",
                    "per.pipeline.finish",
                    reflect_rounds=reflect_rounds,
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
                raw_violations = _finalize_gate_violations(decision, artifacts)
                critical, weak = _split_violations(raw_violations)
                # Soft cap behaviour: past `finalize_gate_soft_cap_rounds`,
                # drop WEAK violations only (scope coverage, audit
                # completeness). CRITICAL violations — no [direct] fact
                # for the candidate, missing mechanism, missing
                # alternatives, missing discriminator token — stay
                # binding regardless of round count, because accepting
                # those past the soft cap would silently allow exactly
                # the symptom-only / un-falsifiable mechanism finalization
                # the gates exist to catch. Unbounded looping is bounded
                # separately by `pipeline_wall_clock_budget_seconds` and
                # `force_finalize_after_rounds` (which suspends gates
                # except `mechanism_alternatives`).
                if weak and reflect_rounds >= _LIMITS.finalize_gate_soft_cap_rounds:
                    log_warning_event(
                        logger,
                        "[per] weak finalize gates not satisfied but soft "
                        "cap reached; dropping weak gates, keeping critical",
                        "per.pipeline.finalize_soft_cap",
                        reflect_rounds=reflect_rounds,
                        steps_executed=steps_executed,
                        weak_violation_count=len(weak),
                        critical_violation_count=len(critical),
                        weak_violations=weak,
                        leading_candidate=decision.leading_candidate,
                    )
                    weak = []
                violations = critical + weak
                if violations:
                    log_warning_event(
                        logger,
                        "[per] finalize gates rejected reflector's result; "
                        "forcing continued investigation",
                        "per.pipeline.finalize_rejected",
                        reflect_rounds=reflect_rounds,
                        steps_executed=steps_executed,
                        violation_count=len(violations),
                        critical_violation_count=len(critical),
                        weak_violation_count=len(weak),
                        leading_candidate=decision.leading_candidate,
                    )
                    pending_steps = _synthesize_followup_steps(decision, violations)
                    continue

                total_ms = int((time.perf_counter() - pipeline_started) * 1000)
                log_info_event(
                    logger,
                    "[per] pipeline complete",
                    "per.pipeline.finish",
                    reflect_rounds=reflect_rounds,
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

        # Loop exhausted without a final result. This is normally
        # unreachable — force-finalize triggers earlier — but we surface
        # the last reflector output so the user gets something usable.
        log_warning_event(
            logger,
            "[per] reflect-round / step ceiling reached without final result",
            "per.pipeline.ceiling_reached",
            reflect_rounds=reflect_rounds,
            steps_executed=steps_executed,
            max_reflect_rounds=_LIMITS.max_reflect_rounds,
            max_total_steps=_LIMITS.max_total_steps,
            total_input_tokens=usage_totals["input"],
            total_output_tokens=usage_totals["output"],
            total_cache_read_tokens=usage_totals["cache_read"],
            total_cache_write_tokens=usage_totals["cache_write"],
            total_phase_calls=usage_totals["phase_calls"],
        )
        tail = last_reflect_text or _force_finalize_fallback_report(
            decision=decision,
            artifacts=artifacts,
            reflect_rounds=reflect_rounds,
            steps_executed=steps_executed,
        )
        return _attach_usage_trailer(
            f"Investigation ceiling reached "
            f"(rounds={reflect_rounds}/{_LIMITS.max_reflect_rounds}, "
            f"steps={steps_executed}/{_LIMITS.max_total_steps}) "
            "without a final result. Use the same conversation to "
            f"continue.\n\nLast reflection:\n{tail}"
        )

    # ``temperature`` deliberately omitted: newer Claude inference
    # profiles on Bedrock reject the parameter ("ValidationException:
    # `temperature` is deprecated for this model"). The orchestrator's
    # job is just relaying the user's question into the
    # ``run_per_pipeline`` tool and returning the result verbatim, so
    # the loss of explicit sampling control is harmless here.
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

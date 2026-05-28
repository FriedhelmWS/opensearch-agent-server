"""Artifact store for the PER pipeline.

Each executor step's output is captured here as an ``Artifact``. The reflector
prompt receives compact ``[id, intent, summary]`` rows instead of the full
findings text, keeping the context window bounded as the loop iterates.

The store is intentionally simple — in-memory, scoped to a single pipeline
run. Two retrieval modes:
  - ``compact_table()``: one-line-per-artifact for the reflect prompt;
  - ``full(id)``: untruncated findings, surfaced only on demand (e.g. by
    the final-result formatter or replay tooling).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator


@dataclass
class Artifact:
    """A single executor step's findings, stored once and referenced by id."""

    id: str
    step_index: int
    step_intent: str
    findings: str
    facts: list[str] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)
    tokens_in: int = 0
    tokens_out: int = 0
    elapsed_ms: int = 0


@dataclass
class ArtifactStore:
    """In-memory artifact registry for one PER pipeline run."""

    _artifacts: list[Artifact] = field(default_factory=list)

    def add(
        self,
        step_index: int,
        step_intent: str,
        findings: str,
        facts: list[str] | None = None,
        queries: list[str] | None = None,
        tokens_in: int = 0,
        tokens_out: int = 0,
        elapsed_ms: int = 0,
    ) -> Artifact:
        """Append a new artifact. ``id`` is auto-assigned as ``A{step_index}``."""
        artifact = Artifact(
            id=f"A{step_index}",
            step_index=step_index,
            step_intent=step_intent,
            findings=findings,
            facts=list(facts) if facts else [],
            queries=list(queries) if queries else [],
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            elapsed_ms=elapsed_ms,
        )
        self._artifacts.append(artifact)
        return artifact

    def __iter__(self) -> Iterator[Artifact]:
        return iter(self._artifacts)

    def __len__(self) -> int:
        return len(self._artifacts)

    def get(self, artifact_id: str) -> Artifact | None:
        for artifact in self._artifacts:
            if artifact.id == artifact_id:
                return artifact
        return None

    def latest(self) -> Artifact | None:
        return self._artifacts[-1] if self._artifacts else None

    def compact_table(self) -> str:
        """Render artifacts as a compact table for prompt injection.

        Format::

            [A1] (step 1) <intent> [3 facts, 2 queries]
            [A2] (step 2) <intent> [5 facts, 1 query]
            ...

        Only the artifact id, step index, intent, and counts are
        included — the durable content (KNOWN_FACTS bullets, full
        findings of the latest step) is rendered separately by the
        reflect prompt builder. Free-text summaries of older findings
        were dropped because they competed for prompt budget without
        carrying structured information the reflector could verify.

        Empty store returns an empty string so callers can detect
        "nothing to inject yet".
        """
        if not self._artifacts:
            return ""
        lines = []
        for artifact in self._artifacts:
            n_facts = len(artifact.facts)
            n_queries = len(artifact.queries)
            facts_word = "fact" if n_facts == 1 else "facts"
            queries_word = "query" if n_queries == 1 else "queries"
            lines.append(
                f"[{artifact.id}] (step {artifact.step_index}) "
                f"{artifact.step_intent} "
                f"[{n_facts} {facts_word}, {n_queries} {queries_word}]"
            )
        return "\n".join(lines)

    def all_queries(self) -> str:
        """Render every recorded executed-query line as a flat list.

        ``QUERIES_EXECUTED`` rows are the executor's structured record of
        what it ACTUALLY queried (index :: fields/aggregations), as
        opposed to what it merely discovered or summarized in findings.
        Reflectors and plan-coverage detectors should prefer these over
        free-text findings when they need to know whether a specific
        field/metric/dimension was actively probed.
        """
        if not self._artifacts:
            return ""
        lines: list[str] = []
        for artifact in self._artifacts:
            for query in artifact.queries:
                lines.append(f"- [{artifact.id}] {query}")
        return "\n".join(lines)

    def _facts_with_prefix(self, prefix: str) -> str:
        prefix_lc = prefix.lower()
        if not self._artifacts:
            return ""
        lines: list[str] = []
        for artifact in self._artifacts:
            for fact in artifact.facts:
                if fact.lower().lstrip().startswith(prefix_lc):
                    lines.append(f"- [{artifact.id}] {fact}")
        return "\n".join(lines)

    def all_deviation_facts(self) -> str:
        """Render only ``[deviation]``-tagged facts.

        Deviation facts cite both incident and baseline values for a
        component, making relative-deviation ranking possible. They are
        the only evidence eligible to rank candidates without succumbing
        to magnitude bias (per the investigation skill's rank-by-relative
        rule).
        """
        return self._facts_with_prefix("[deviation]")

    def all_direct_facts(self) -> str:
        """Render only ``[direct]``-tagged facts.

        Direct facts are evidence whose presence implies a specific failure
        mode (per the investigation skill C.1: a resource gauge near its
        limit, a log message that names a class of failure unambiguously,
        a structural trace signature). They are the only kind of evidence
        that can name a root cause; symptom evidence cannot.
        """
        return self._facts_with_prefix("[direct]")

    def all_symptom_facts(
        self,
        leading_candidate: str = "",
        max_recent: int | None = None,
    ) -> str:
        """Render ``[symptom]``-tagged facts with an optional LRU cap.

        Symptom facts are consistent with the leading hypothesis but also
        consistent with several other modes (generic error / exception
        log lines, "downstream timed out", "connection refused"). They
        are CONTEXT only — they cannot name a mode by themselves.

        Symptom volume grows quickly during a long investigation: each
        executor step typically records several symptoms while ranking
        them as `[symptom]` rather than promoting to `[direct]`. When the
        symptom list exceeds the reflector's useful working set, older
        symptoms unrelated to the current leading candidate become dead
        weight in every reflect prompt for the rest of the run.

        ``max_recent`` (when set) keeps:
          - every symptom mentioning ``leading_candidate`` (case-insensitive),
            so the finalize gate's parked-symptoms-resolved check still
            sees the relevant ones in full; PLUS
          - the most recent ``max_recent`` symptoms regardless of subject.

        Older symptoms outside both buckets collapse to a single
        ``- (… N earlier symptom facts elided)`` line so the reflector
        knows they exist without paying their token cost.
        """
        all_lines = self._facts_with_prefix("[symptom]")
        if max_recent is None or not all_lines:
            return all_lines
        lines = all_lines.split("\n")
        if len(lines) <= max_recent:
            return all_lines
        candidate_lc = leading_candidate.lower().strip()
        kept_indices: set[int] = set()
        if candidate_lc:
            for idx, line in enumerate(lines):
                if candidate_lc in line.lower():
                    kept_indices.add(idx)
        # Most recent N (by artifact iteration order = chronological).
        for idx in range(len(lines) - max_recent, len(lines)):
            kept_indices.add(idx)
        elided = len(lines) - len(kept_indices)
        if elided <= 0:
            return all_lines
        kept_lines = [lines[i] for i in sorted(kept_indices)]
        kept_lines.insert(
            0,
            f"- (… {elided} earlier symptom fact(s) elided to bound prompt "
            "size; kept: most recent + any mentioning current leading "
            "candidate)",
        )
        return "\n".join(kept_lines)

    def all_schema_facts(self, max_recent: int | None = None) -> str:
        """Render ``[schema]``-tagged AND unclassified facts with optional LRU cap.

        Schema facts (field presence / units / naming conventions / query
        constraints) are operationally critical — they prevent the next
        executor from rediscovering the same gotcha — but they are
        WORM-shaped: once recorded, the truth doesn't change. Older
        schema discoveries past the working set become dead weight in
        every reflect prompt for the rest of the run.

        ``max_recent`` (when set) keeps only the most recent N schema
        facts and collapses older ones into a single ``- (… N earlier
        schema facts elided)`` line so the reflector knows they exist
        without paying their token cost.

        We treat any fact that is NOT tagged ``[direct]`` / ``[symptom]``
        / ``[deviation]`` as schema-class for elision purposes. The
        executor system prompt requires every fact to carry a tag; an
        un-tagged or oddly-tagged fact is closer in semantics to schema
        (an artifact of execution, not a hypothesis) than to anything
        else.
        """
        if not self._artifacts:
            return ""
        keep_prefixes = ("[direct]", "[symptom]", "[deviation]")
        lines: list[str] = []
        for artifact in self._artifacts:
            for fact in artifact.facts:
                lc = fact.lower().lstrip()
                if lc.startswith(keep_prefixes):
                    continue
                lines.append(f"- [{artifact.id}] {fact}")
        if max_recent is None or len(lines) <= max_recent:
            return "\n".join(lines)
        elided = len(lines) - max_recent
        kept = lines[-max_recent:]
        return "\n".join(
            [
                f"- (… {elided} earlier schema fact(s) elided to bound prompt "
                "size; kept: most recent)"
            ]
            + kept
        )

    def has_direct_fact_for(self, candidate: str) -> bool:
        """Whether any ``[direct]`` fact mentions the candidate component.

        Used by the finalize gate to refuse declaring a root cause whose
        only support is symptom evidence. ``candidate`` is matched
        case-insensitively against the bullet text.
        """
        if not candidate:
            return False
        needle = candidate.lower()
        for artifact in self._artifacts:
            for fact in artifact.facts:
                lc = fact.lower().lstrip()
                if lc.startswith("[direct]") and needle in lc:
                    return True
        return False

    def all_facts(self) -> str:
        """Render every recorded structured fact as a flat bullet list.

        Facts are preserved in full (never truncated like ``compact_table``)
        because they are the persistent memory the reflector and next
        executor rely on to avoid re-running schema/discovery work.
        Each line is prefixed with the artifact id so the reflector can
        attribute facts back to the step that established them.
        """
        if not self._artifacts:
            return ""
        lines: list[str] = []
        for artifact in self._artifacts:
            for fact in artifact.facts:
                lines.append(f"- [{artifact.id}] {fact}")
        return "\n".join(lines)

    def full_findings(self, last_n: int = 1) -> str:
        """Return raw findings for the last ``n`` artifacts.

        Used to give the reflector full fidelity for the most recent step
        while older steps are summarized. Defaults to 1 (last step only).
        """
        if last_n <= 0 or not self._artifacts:
            return ""
        slice_ = self._artifacts[-last_n:]
        blocks = []
        for artifact in slice_:
            blocks.append(
                f"<artifact id=\"{artifact.id}\" step=\"{artifact.step_index}\">\n"
                f"intent: {artifact.step_intent}\n"
                f"findings:\n{artifact.findings}\n"
                f"</artifact>"
            )
        return "\n\n".join(blocks)

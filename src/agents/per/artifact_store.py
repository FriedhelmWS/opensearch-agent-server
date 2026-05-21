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


_DEFAULT_SUMMARY_CHARS = 280


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

    def summary(self, max_chars: int = _DEFAULT_SUMMARY_CHARS) -> str:
        """Return a truncated single-line preview of ``findings``."""
        text = " ".join(self.findings.split())
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 1].rstrip() + "…"


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

    def compact_table(self, summary_chars: int = _DEFAULT_SUMMARY_CHARS) -> str:
        """Render artifacts as a compact table for prompt injection.

        Format::

            [A1] (step 1) <intent> :: <summary>
            [A2] (step 2) <intent> :: <summary>
            ...

        Empty store returns an empty string so callers can detect "nothing
        to inject yet".
        """
        if not self._artifacts:
            return ""
        lines = []
        for artifact in self._artifacts:
            lines.append(
                f"[{artifact.id}] (step {artifact.step_index}) "
                f"{artifact.step_intent} :: {artifact.summary(summary_chars)}"
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

    def all_symptom_facts(self) -> str:
        """Render only ``[symptom]``-tagged facts.

        Symptom facts are consistent with the leading hypothesis but also
        consistent with several other modes (generic error / exception
        log lines, "downstream timed out", "connection refused"). They
        are CONTEXT only — they cannot name a mode by themselves.
        """
        return self._facts_with_prefix("[symptom]")

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

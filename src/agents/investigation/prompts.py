"""Investigation-specific prompt overlays.

These overlays are appended to the underlying PER sub-agent system
prompts when an investigation runs. They tell the reflect sub-agent
that, when it produces the final ``result`` field, the value MUST be
a JSON-stringified :class:`PERAgentInvestigationResponse` — not free
text. Without this, the reflect agent emits a markdown report that
fails the OSD frontend's ``isValidPERAgentInvestigationResponse``
validator (the very same validator the ml-commons-backed flow has
always pointed at).

The overlay text is a Python port of the ``commonResponseFormat`` /
``getTimeScopePrompt`` blocks the OSD plugin's
``server/routes/notebooks/agent_router.ts`` injects when calling the
ml-commons PER agent — so the resulting model behavior matches what
users have been seeing in the managed ml-commons deployment, and old
ml-commons memory documents importing into agent-server's SQLite
remain in the same shape.

This module contains zero PER-framework details. The PER pipeline
remains a generic plan-execute-reflect loop; the investigation
specialization is purely additive prompt content threaded through via
``run_per_pipeline_core(extra_reflect_system_prompt=...)``.
"""

from __future__ import annotations

from agents.investigation.backend import InvestigationContext


_INVESTIGATION_RESPONSE_FORMAT_OVERLAY = """\
# Investigation Final Result Format

When you decide to finalize the investigation (i.e. you set ``result`` to
a non-empty value), the ``result`` field of your JSON response MUST be a
**stringified JSON object** that strictly follows this schema:

```json
{
    "findings": array[object],
    "hypotheses": array[object],
    "topologies": array[object],
    "investigationName": "string, max 30 characters, auto-generated investigation title"
}
```

Each finding object:
```json
{
    "id": string,                    // unique id, e.g. "F1", "F2"
    "description": string,           // clear statement of the finding
    "importance": number (0-100),   // significance rating
    "evidence": string               // specific data / quotes / observations
}
```

Each hypothesis object:
```json
{
    "id": string,                    // unique id, e.g. "H1"
    "title": string,                 // concise title
    "description": string,           // clear statement
    "likelihood": number (0-100),   // probability of being correct
    "supporting_findings": array[string]  // finding ids referenced from "findings"
}
```

Each topology object (only when trace data with traceId is available):
```json
{
    "id": string,                    // e.g. "T1"
    "description": string,
    "traceId": string,
    "hypothesisIds": array[string],
    "nodes": array[{
        "id": string,
        "name": string,
        "startTime": string,         // ISO format
        "duration": string,
        "status": string,            // "success" | "failed" | "error" | "latency" | "timeout" | ...
        "parentId": string | null    // null for the root node
    }]
}
```

Likelihood guidelines for hypotheses:
- Strong (70-100): high confidence, substantial supporting evidence
- Moderate (40-70): medium confidence, some supporting evidence
- Weak (0-40): low confidence, limited supporting evidence

Critical rules for the final result:
1. The ``result`` field MUST contain a properly escaped JSON string
   (i.e. it is a string-typed field whose value, when JSON-parsed,
   yields the object described above).
2. Every hypothesis's ``supporting_findings`` MUST reference ids that
   appear in your ``findings`` array.
3. When trace data is available, generate exactly one topology object
   describing the most critical service call hierarchy. Limit nodes
   to the critical path, max 10 nodes.
4. Do not add any prose, commentary, or markdown around the JSON in
   ``result`` — the consumer parses ``result`` directly as JSON.

Final response example (PER outer envelope shown):
```json
{
  "next_steps": [],
  "result": "{\\"investigationName\\":\\"Payment latency spike\\",\\"findings\\":[{\\"id\\":\\"F1\\",\\"description\\":\\"p99 latency on /pay rose 4x at 09:48 UTC\\",\\"importance\\":85,\\"evidence\\":\\"istio-latency-99 0.12s -> 0.48s in train-ticket-metrics-1\\"}],\\"hypotheses\\":[{\\"id\\":\\"H1\\",\\"title\\":\\"DB connection pool exhaustion\\",\\"description\\":\\"Sustained MongoDB connection-pool saturation at ts-payment-mongo correlates with the latency spike\\",\\"likelihood\\":80,\\"supporting_findings\\":[\\"F1\\"]}],\\"topologies\\":[]}"
}
```
"""


def _build_time_scope_overlay(time_range: dict | None) -> str:
    """Render the user-selected time window as a system-prompt fragment.

    ``time_range`` follows the OSD frontend convention of
    ``{"selectionFrom": <epoch_ms>, "selectionTo": <epoch_ms>}``. When
    either bound is missing, returns an empty string so the rest of the
    overlay still concatenates cleanly.
    """
    if not isinstance(time_range, dict):
        return ""
    start = time_range.get("selectionFrom")
    end = time_range.get("selectionTo")
    if not (isinstance(start, (int, float)) and isinstance(end, (int, float))):
        return ""
    # Local import — datetime is only needed when we have a window.
    from datetime import UTC, datetime

    def _iso(ms: float) -> str:
        return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat().replace(
            "+00:00", "Z"
        )

    return f"""\
# Time Scope

CRITICAL: Use this exact time range for your investigation:
- Start time: {_iso(start)}
- End time:   {_iso(end)}

Use these ISO 8601 UTC timestamps for all time-based queries and analysis.
"""


def build_reflect_overlay(ctx: InvestigationContext) -> str:
    """Return the reflect-agent system prompt overlay for this run.

    The PER ``run_per_pipeline_core`` appends this string to the
    ``REFLECT_SYSTEM_PROMPT`` so the reflect sub-agent — which decides
    when the investigation finalizes and what the ``result`` field
    contains — knows the strict structure the OSD frontend expects.
    """
    parts: list[str] = []
    time_scope = _build_time_scope_overlay(ctx.time_range)
    if time_scope:
        parts.append(time_scope)
    parts.append(_INVESTIGATION_RESPONSE_FORMAT_OVERLAY)
    return "\n\n".join(parts)

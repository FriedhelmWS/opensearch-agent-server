"""``PERAgentInvestigationResponse`` — terminal-message JSON shape.

Field names mirror the TypeScript interface in
``dashboards-investigation/common/types/notebooks.ts`` so the OSD frontend
can deserialize the terminal memory message without changes.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class PERAgentHypothesisFinding(BaseModel):
    id: str
    description: str
    # OSD validator requires ``importance: number`` and
    # ``evidence: string``. Accept None for permissive ingest, then
    # sanitize via ``model_dump`` / the backend's _sanitize step.
    importance: int | None = None
    evidence: str | None = None
    type: str | None = None


class PERAgentHypothesisItem(BaseModel):
    id: str
    title: str
    description: str
    # OSD's ``isValidPERAgentHypothesisItem`` requires
    # ``typeof likelihood === 'number'``. We accept None on parse
    # (some models emit null) then coerce to 0 at output time.
    likelihood: int | None = None
    supporting_findings: list[str] = Field(default_factory=list)


class PERAgentTopologyNode(BaseModel):
    """Topology node shape required by OSD's
    ``/api/investigation/note/updateHypotheses`` route schema.

    All five fields are strict ``string`` on the OSD side; ``parentId``
    is nullable. We accept ``None`` on parse so a slightly malformed
    model output doesn't crash the whole response, then the backend's
    ``_sanitize_for_osd`` step fills missing strings with ``""`` so
    the row still passes the OSD route validator.
    """

    id: str
    name: str | None = None
    startTime: str | None = None
    duration: str | None = None
    status: str | None = None
    parentId: str | None = None


class PERAgentTopology(BaseModel):
    """Topology shape required by OSD's updateHypotheses route schema.

    ``description`` and ``traceId`` are strict ``string`` on the OSD
    side. Same pattern as the node: permissive on parse, sanitized
    to ``""`` before persisting.
    """

    id: str
    description: str | None = None
    traceId: str | None = None
    hypothesisIds: list[str] = Field(default_factory=list)
    nodes: list[PERAgentTopologyNode] = Field(default_factory=list)


class PERAgentInvestigationResponse(BaseModel):
    """Terminal investigation result consumed by the OSD frontend.

    The frontend parses ``structured_data_blob.response`` of the terminal
    memory message into this shape (see ``use_investigation.ts:356-408``).
    Empty lists are valid — they degrade rendering but keep the shape valid.
    """

    findings: list[PERAgentHypothesisFinding] = Field(default_factory=list)
    hypotheses: list[PERAgentHypothesisItem] = Field(default_factory=list)
    topologies: list[PERAgentTopology] = Field(default_factory=list)
    investigationName: str | None = None

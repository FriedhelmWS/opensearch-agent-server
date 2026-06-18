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
    importance: str | None = None
    evidence: str | None = None
    type: str | None = None


class PERAgentHypothesisItem(BaseModel):
    id: str
    title: str
    description: str
    likelihood: str | None = None
    supporting_findings: list[str] = Field(default_factory=list)


class PERAgentTopologyNode(BaseModel):
    id: str
    label: str | None = None
    type: str | None = None


class PERAgentTopology(BaseModel):
    id: str
    description: str | None = None
    traceId: str | None = None
    nodes: list[PERAgentTopologyNode] = Field(default_factory=list)
    hypothesisIds: list[str] = Field(default_factory=list)


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

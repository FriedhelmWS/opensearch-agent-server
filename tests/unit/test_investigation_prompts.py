"""Unit tests for ``agents.investigation.prompts``.

Asserts the overlay text the investigation backend appends to the PER
reflect sub-agent's system prompt — focused on the contract the OSD
frontend depends on:
  * the structured-output schema names (findings / hypotheses /
    topologies / investigationName) appear so the reflect agent
    produces a parseable response
  * the time-window block is rendered when the trigger payload
    carries ``selectionFrom`` / ``selectionTo``, and skipped otherwise
"""

from __future__ import annotations

import pytest

from agents.investigation.backend import InvestigationContext
from agents.investigation.prompts import build_reflect_overlay

pytestmark = pytest.mark.unit


def _ctx(**overrides) -> InvestigationContext:
    base = {"question": "why latency"}
    base.update(overrides)
    return InvestigationContext(**base)


class TestBuildReflectOverlay:
    def test_includes_structured_schema_keys(self) -> None:
        overlay = build_reflect_overlay(_ctx())
        # The OSD validator requires these keys in the final JSON.
        for key in ("findings", "hypotheses", "topologies", "investigationName"):
            assert key in overlay
        # Stringified-JSON requirement is the bit the reflect agent
        # most often gets wrong without the overlay — keep this asserted.
        assert "stringified JSON object" in overlay

    def test_time_scope_rendered_when_present(self) -> None:
        # 2026-06-18T07:00:00Z → 1781766000000 ms (epoch)
        overlay = build_reflect_overlay(
            _ctx(time_range={"selectionFrom": 1781766000000, "selectionTo": 1781769600000})
        )
        assert "Time Scope" in overlay
        assert "2026-06-18T07:00:00Z" in overlay
        assert "2026-06-18T08:00:00Z" in overlay

    def test_time_scope_skipped_when_missing_or_invalid(self) -> None:
        no_range = build_reflect_overlay(_ctx())
        bad_range = build_reflect_overlay(_ctx(time_range={"selectionFrom": "x"}))
        partial = build_reflect_overlay(_ctx(time_range={"selectionFrom": 1}))
        for o in (no_range, bad_range, partial):
            assert "Time Scope" not in o
            # Schema block must still be present.
            assert "investigationName" in o

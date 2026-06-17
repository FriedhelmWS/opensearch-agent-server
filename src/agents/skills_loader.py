"""Shared skills loading for Strands-based agents.

Auto-discovers ``skills/`` at the project root and exposes a logging
``AgentSkills`` plugin variant. Used by both the default agent and the
PER sub-agents so a single ``skills/`` tree drives both pipelines.

All structured log events emitted from this module use the ``skills.*``
namespace (event name follows module semantics, not the caller). The
``caller`` field on every event is set by the calling agent so log
consumers can slice by which agent triggered a load or activation
without inflating the event-name space.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from strands import Agent, AgentSkills, Skill

from utils.logging_helpers import get_logger, log_info_event

logger = get_logger(__name__)


def _skills_dir() -> Path:
    """Resolve ``skills/`` at the project root each call.

    Keeping this lazy (rather than caching at import time) lets tests
    monkeypatch ``__file__`` to point ``load_all_skills`` at a synthetic
    project root.
    """
    return Path(__file__).resolve().parent.parent.parent / "skills"


class LoggingAgentSkills(AgentSkills):
    """AgentSkills plugin that logs skill activations at INFO level.

    The vended strands plugin logs activations at DEBUG only. This subclass
    emits a structured INFO event whenever the LLM invokes a skill, so
    auto-selection is visible in standard logs without enabling DEBUG
    globally. ``caller`` is attached to every activation event so log
    consumers can distinguish, e.g., default-agent activations from PER
    sub-agent activations.
    """

    def __init__(self, *, caller: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._caller = caller

    def _track_activated_skill(self, agent: Agent, skill_name: str) -> None:
        log_info_event(
            logger,
            f"Skill activated by agent: {skill_name}",
            "skills.activated",
            caller=self._caller,
            skill_name=skill_name,
        )
        super()._track_activated_skill(agent, skill_name)


def load_all_skills(*, caller: str) -> list[Skill]:
    """Auto-discover and load all skills from the project's ``skills/`` directory.

    Each subdirectory containing a ``SKILL.md`` is loaded via
    ``Skill.from_file()``. Invalid or missing skills are skipped with a
    warning log so a malformed skill never breaks agent startup.

    Args:
        caller: Identifier for the agent triggering the load (e.g.
            ``"default_agent"``, ``"per"``). Attached to every emitted
            log event so downstream consumers can slice by caller without
            relying on per-caller event names.
    """
    skills_dir = _skills_dir()
    if not skills_dir.exists():
        log_info_event(
            logger,
            f"Skills directory not found at {skills_dir}, skipping skill loading",
            "skills.dir_not_found",
            caller=caller,
            skills_dir=str(skills_dir),
        )
        return []

    skills: list[Skill] = []
    for skill_path in sorted(skills_dir.iterdir()):
        if not skill_path.is_dir() or not (skill_path / "SKILL.md").exists():
            continue
        try:
            skill = Skill.from_file(skill_path)
            skills.append(skill)
            log_info_event(
                logger,
                f"Loaded skill: {skill.name}",
                "skills.loaded",
                caller=caller,
                skill_name=skill.name,
                skill_path=str(skill_path),
            )
        except Exception as e:
            log_info_event(
                logger,
                f"Failed to load skill at {skill_path}: {e}",
                "skills.load_failed",
                caller=caller,
                skill_path=str(skill_path),
                error=str(e),
            )

    return skills

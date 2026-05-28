"""Shared AgentSkills plugin and skill-discovery helper.

The vended Strands ``AgentSkills`` plugin logs activations at DEBUG only,
so by default skill auto-selection is invisible in production logs.
This module wraps it with an INFO-level log event and provides a single
``load_skills_from_dir`` helper so the default agent and the PER
sub-agents share the exact same loader semantics.
"""

from __future__ import annotations

from pathlib import Path

from strands import Agent, AgentSkills, Skill

from utils.logging_helpers import get_logger, log_info_event

logger = get_logger(__name__)


class LoggingAgentSkills(AgentSkills):
    """``AgentSkills`` subclass that logs activations at INFO.

    The base plugin tracks activation via the protected
    ``_track_activated_skill(agent, skill_name)`` hook. We override it
    to emit a structured event before delegating, so observability is
    available without enabling DEBUG globally.
    """

    def _track_activated_skill(self, agent: Agent, skill_name: str) -> None:
        log_info_event(
            logger,
            f"Skill activated by agent: {skill_name}",
            "agents.skill_activated",
            skill_name=skill_name,
            agent_name=getattr(agent, "name", None),
        )
        super()._track_activated_skill(agent, skill_name)


def load_skills_from_dir(skills_dir: Path) -> list[Skill]:
    """Load every subdirectory of ``skills_dir`` that contains ``SKILL.md``.

    Bad / unparseable skills are logged and skipped so a single broken
    skill cannot prevent the rest from loading. Order is alphabetical
    so the activation log is reproducible across runs.
    """
    if not skills_dir.exists():
        log_info_event(
            logger,
            f"Skills directory not found at {skills_dir}",
            "agents.skills_dir_missing",
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
                "agents.skill_loaded",
                skill_name=skill.name,
                skill_path=str(skill_path),
            )
        except Exception as exc:  # pragma: no cover — surface load errors but keep going
            log_info_event(
                logger,
                f"Failed to load skill at {skill_path}: {exc}",
                "agents.skill_load_failed",
                skill_path=str(skill_path),
                error=str(exc),
            )
    return skills

"""Smoke-checklist doc guard (Phase 13c).

The real-adapter smoke is a *human-run* checklist by design (Vision §9.2 —
"recorded as a checklist, not automated"). This guard does NOT run any real
harness, spawn a PTY, or touch the network. It only asserts the durable T6
artifact exists and stays coherent, and it locks the G2 fix (Phase 13c): each
persona dir must ship a ``CLAUDE.md``, because a live Claude Code reads
``CLAUDE.md`` (not ``AGENTS.md``) — without it the sample silently doesn't load
its personas.

Repo-root discovery uses ``Path(__file__).resolve().parents[1]`` so the test is
location-independent (works from any CWD, in CI or locally). Runs under the
default ``-m 'not budget'`` suite.
"""
from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CHECKLIST = _REPO_ROOT / "docs" / "SMOKE_CHECKLIST.md"
_PERSONA_DIRS = (
    _REPO_ROOT / "examples" / "two-claudes-debating" / "claude-a",
    _REPO_ROOT / "examples" / "two-claudes-debating" / "claude-b",
)

# The CLI subcommand tokens the checklist must reference for each real adapter
# (the Antigravity adapter's CLI name is "antigravity", not its binary "agy").
_ADAPTER_TOKENS = ("claude", "gemini", "antigravity")


def test_checklist_exists() -> None:
    """The T6 deliverable is present in docs/."""
    assert _CHECKLIST.is_file(), f"missing T6 artifact: {_CHECKLIST}"


@pytest.mark.parametrize("token", _ADAPTER_TOKENS)
def test_checklist_names_each_adapter(token: str) -> None:
    """Every shipped adapter has a section in the checklist."""
    text = _CHECKLIST.read_text(encoding="utf-8")
    assert f"## Adapter: `{token}`" in text, (
        f"checklist is missing the '{token}' adapter section"
    )


def test_checklist_covers_spawn_inject_teardown() -> None:
    """The checklist documents all three smoke steps."""
    text = _CHECKLIST.read_text(encoding="utf-8").lower()
    for step in ("spawn", "injection", "teardown"):
        assert step in text, f"checklist does not mention '{step}'"


@pytest.mark.parametrize("persona_dir", _PERSONA_DIRS, ids=lambda p: p.name)
def test_persona_dir_ships_claude_md(persona_dir: Path) -> None:
    """Each persona dir ships a CLAUDE.md (the G2 fix).

    Live Claude Code reads ``CLAUDE.md`` from its cwd, not ``AGENTS.md``
    (resolved in Phase 13c). The persona content is authored in ``AGENTS.md``
    and duplicated to ``CLAUDE.md`` so the sample loads; this guard fails if a
    future edit drops the ``CLAUDE.md`` and silently re-breaks the sample.
    """
    claude_md = persona_dir / "CLAUDE.md"
    assert claude_md.is_file(), (
        f"{persona_dir.name}/CLAUDE.md missing — sample personas won't load "
        f"for a real `letterbox claude` (Claude Code reads CLAUDE.md, not AGENTS.md)"
    )
    assert claude_md.read_text(encoding="utf-8").strip(), (
        f"{persona_dir.name}/CLAUDE.md is empty"
    )

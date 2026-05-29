"""Tests for Phase 11a — the public sample project under ``examples/``.

A guard against silent drift of a *public* artifact: the sample
``letterbox.toml`` must keep round-tripping through the real config parser,
and the channel name it declares must stay byte-identical to what the two
``AGENTS.md`` persona files reference.

No mocks. Points ``LETTERBOX_CONFIG`` at the on-disk sample TOML and reads it
through the real ``load_config()`` (the 9b/9c env-lever idiom; mirrors
``tests/test_config.py::TestSampleFile``).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from letterbox.config import load_config

# The example lives at a fixed repo path: <repo>/examples/two-claudes-debating/.
# This file is <repo>/tests/test_sample_project.py, so the repo root is parent.parent.
SAMPLE_DIR = Path(__file__).parent.parent / "examples" / "two-claudes-debating"
SAMPLE_TOML = SAMPLE_DIR / "letterbox.toml"
CHANNEL_NAME = "debate-01"


def test_sample_files_exist() -> None:
    """The four artifact files exist at the documented paths."""
    assert SAMPLE_TOML.is_file()
    assert (SAMPLE_DIR / "README.md").is_file()
    assert (SAMPLE_DIR / "claude-a" / "AGENTS.md").is_file()
    assert (SAMPLE_DIR / "claude-b" / "AGENTS.md").is_file()


def test_sample_toml_loads_and_registers_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sample TOML parses without error and registers ``debate-01``."""
    monkeypatch.setenv("LETTERBOX_CONFIG", str(SAMPLE_TOML))
    config = load_config()  # raises ConfigError on a malformed sample
    assert any(c.name == CHANNEL_NAME for c in config.channels)


def test_channel_name_consistent_across_personas() -> None:
    """The channel both personas reference matches the registered name."""
    for persona in ("claude-a", "claude-b"):
        text = (SAMPLE_DIR / persona / "AGENTS.md").read_text(encoding="utf-8")
        assert CHANNEL_NAME in text

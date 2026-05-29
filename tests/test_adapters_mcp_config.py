"""Tests for ``letterbox.adapters.mcp_config`` — Phase 5c.

Covers the temp MCP config generator + cleanup hook: the ``mcpServers``
envelope structure, the join-key argv (``mcp --channel … --as … --instance-id
…``), the consumer-shape oracle round-trip (``tests.fake_harness._parse_mcp_config``),
mode-0600 under permissive AND owner-stripping umasks (proving the explicit
``os.chmod`` defeats the umask, not just mkstemp's default), ``ensure_ascii=False``
UTF-8 round-trip, ``sort_keys=True`` determinism, idempotent cleanup, input
guards, and the public surface + tier-header lock.

Real filesystem (system temp), no mocks on the production path; every
generated file is deleted in fixture teardown so the suite leaves no ``/tmp``
litter under ``filterwarnings = ["error"]``.
"""
from __future__ import annotations

import inspect
import json
import logging
import os
from pathlib import Path

import pytest

from letterbox.adapters import mcp_config
from letterbox.adapters.mcp_config import cleanup_mcp_config, generate_mcp_config
from tests.fake_harness import _parse_mcp_config

_IID = "lb-20260527T143000Z-7f3a9c"

# The expanded tier-header (line 4's import list widened to the actual set per
# scout Gotcha §7; lines 3 and 5 preserved verbatim). Locks the header so a
# future edit can't silently drop the §13.5 bulkhead phrasing.
_EXPECTED_TIER_HEADER = [
    '"""Generates temp MCP config files per harness conventions; cleanup hook.',
    "",
    "Tier: 2",
    "May import from: stdlib (``json``, ``logging``, ``os``, ``shutil``, ``tempfile``, ``pathlib``).",
    "Must NOT import from: concrete adapters or any Tier 4 module — bulkhead §13.5.",
    "",
    "Filled in: Phase 5c per PHASE_INDEX.",
    '"""',
]


# ──────────────────────────────────────────────────────────────
# Local fixtures + helpers
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def permissive_umask():
    """Save/restore process umask around the body, setting ``os.umask(0)``.

    Cloned from ``tests/test_channel.py`` — lets the mode assertion verify the
    explicit ``os.chmod`` rather than colliding with a default-umask strip.
    Yields nothing — used as a barrier, not a value.
    """
    old = os.umask(0)
    try:
        yield
    finally:
        os.umask(old)


@pytest.fixture
def gen():
    """Return a generate_mcp_config wrapper that tracks + cleans up its files."""
    paths: list[Path] = []

    def _gen(
        harness: str = "claude",
        channel: str = "debate-01",
        sender_label: str = "claude-a",
        instance_id: str = _IID,
    ) -> Path:
        path = generate_mcp_config(harness, channel, sender_label, instance_id)
        paths.append(path)
        return path

    yield _gen
    for path in paths:
        path.unlink(missing_ok=True)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ──────────────────────────────────────────────────────────────
# Structure + join-key argv + command resolution
# ──────────────────────────────────────────────────────────────


class TestStructure:
    def test_mcpservers_envelope_shape(self, gen):
        entry = _load(gen())["mcpServers"]["letterbox"]
        assert Path(entry["command"]).name == "letterbox"
        assert entry["args"] == [
            "mcp",
            "--channel",
            "debate-01",
            "--as",
            "claude-a",
            "--instance-id",
            _IID,
        ]

    def test_harness_not_in_args(self, gen):
        # harness selects the adapter / temp filename, never the child identity.
        args = _load(gen(harness="gemini"))["mcpServers"]["letterbox"]["args"]
        assert "gemini" not in args
        assert "--harness" not in args

    def test_returns_absolute_path(self, gen):
        assert gen().is_absolute()

    def test_args_is_a_list(self, gen):
        assert isinstance(_load(gen())["mcpServers"]["letterbox"]["args"], list)

    def test_command_uses_resolved_absolute_path(self, monkeypatch, gen):
        monkeypatch.setattr(
            mcp_config.shutil, "which", lambda _name: "/usr/local/bin/letterbox"
        )
        assert _load(gen())["mcpServers"]["letterbox"]["command"] == (
            "/usr/local/bin/letterbox"
        )

    def test_command_falls_back_to_bare_name_and_warns(self, monkeypatch, caplog, gen):
        monkeypatch.setattr(mcp_config.shutil, "which", lambda _name: None)
        with caplog.at_level(
            logging.WARNING, logger="letterbox.adapters.mcp_config"
        ):
            path = gen()
        assert _load(path)["mcpServers"]["letterbox"]["command"] == "letterbox"
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1


class TestConsumerOracle:
    def test_fake_harness_parses_generated_file(self, gen):
        command, args = _parse_mcp_config(gen())
        assert Path(command).name == "letterbox"
        assert args == [
            "mcp",
            "--channel",
            "debate-01",
            "--as",
            "claude-a",
            "--instance-id",
            _IID,
        ]


# ──────────────────────────────────────────────────────────────
# File mode (umask defense)
# ──────────────────────────────────────────────────────────────


class TestFileMode:
    def test_mode_is_0600_under_permissive_umask(self, permissive_umask, gen):
        assert gen().stat().st_mode & 0o777 == 0o600

    def test_chmod_defeats_owner_stripping_umask(self, gen):
        # umask 0o600 would mask mkstemp's 0o600 default down to 0o000; the
        # explicit os.chmod is the only thing that restores 0o600.
        old = os.umask(0o600)
        try:
            path = gen()
        finally:
            os.umask(old)
        assert path.stat().st_mode & 0o777 == 0o600


# ──────────────────────────────────────────────────────────────
# Encoding + determinism
# ──────────────────────────────────────────────────────────────


class TestEncoding:
    def test_non_ascii_sender_label_is_raw_utf8(self, gen):
        label = "ąčęėįšųūž"
        raw = gen(sender_label=label).read_bytes()
        assert label.encode("utf-8") in raw
        assert b"\\u" not in raw


class TestDeterminism:
    def test_identical_inputs_produce_identical_content(self, gen):
        first = gen()
        second = gen()
        assert first != second  # distinct temp paths
        assert first.read_bytes() == second.read_bytes()


# ──────────────────────────────────────────────────────────────
# Cleanup (idempotent unlink)
# ──────────────────────────────────────────────────────────────


class TestCleanup:
    def test_deletes_the_file(self, gen):
        path = gen()
        assert path.exists()
        cleanup_mcp_config(path)
        assert not path.exists()

    def test_idempotent_on_already_deleted(self, gen):
        path = gen()
        cleanup_mcp_config(path)
        cleanup_mcp_config(path)  # second call must not raise

    def test_no_raise_on_never_existed_path(self, tmp_path):
        cleanup_mcp_config(tmp_path / "never-existed.json")


# ──────────────────────────────────────────────────────────────
# Input guards (vector errors)
# ──────────────────────────────────────────────────────────────


class TestInputGuards:
    _VALID = {
        "harness": "claude",
        "channel": "debate-01",
        "sender_label": "claude-a",
        "instance_id": _IID,
    }

    @pytest.mark.parametrize(
        "field", ["harness", "channel", "sender_label", "instance_id"]
    )
    def test_empty_string_raises_value_error(self, field):
        kwargs = {**self._VALID, field: ""}
        with pytest.raises(ValueError, match=field):
            generate_mcp_config(**kwargs)

    @pytest.mark.parametrize(
        "field", ["harness", "channel", "sender_label", "instance_id"]
    )
    @pytest.mark.parametrize("bad", [None, 123, ["x"]])
    def test_non_str_raises_type_error(self, field, bad):
        kwargs = {**self._VALID, field: bad}
        with pytest.raises(TypeError, match=field):
            generate_mcp_config(**kwargs)


# ──────────────────────────────────────────────────────────────
# Public surface
# ──────────────────────────────────────────────────────────────


class TestPublicSurface:
    def test_public_exports(self):
        assert mcp_config.__all__ == ["cleanup_mcp_config", "generate_mcp_config"]

    def test_private_helpers_not_exported(self):
        for name in (
            "_build_payload",
            "_resolve_letterbox_command",
            "_require_nonempty_str",
            "_MCP_SERVER_NAME",
            "_LETTERBOX_COMMAND",
            "_CONFIG_FILE_MODE",
            "_TEMP_PREFIX",
            "_LOGGER",
        ):
            assert hasattr(mcp_config, name)
            assert name not in mcp_config.__all__

    def test_tier_header_preserved_verbatim(self):
        source_lines = inspect.getsource(mcp_config).splitlines()
        assert source_lines[:8] == _EXPECTED_TIER_HEADER

    def test_imports_no_letterbox_module(self):
        # Tier-2 stdlib-only leaf: no letterbox.* import (esp. not base /
        # pty_common). A leaf importing only stdlib makes a tier breach
        # structurally impossible.
        source = inspect.getsource(mcp_config)
        assert "import letterbox" not in source
        assert "from letterbox" not in source

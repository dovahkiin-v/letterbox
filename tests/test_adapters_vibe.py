"""Tests for ``letterbox.adapters.vibe`` — Vibe adapter (ADR-067).

Three test classes:

* ``TestVibeAdapterConfig`` (sync) — the shipped class attrs ARE the contract;
  assert each verbatim, plus self-registration, the declarative-only
  (zero-override) shape, launch-safety of the template, the public surface,
  and the verbatim tier-header lock.
* ``TestVibeAdapterSpawnComposition`` (async) — monkeypatch ``base.spawn_pty``
  to a recording stub and prove ``["vibe", "--yolo", *extra_args]`` is the
  assembled argv. Asserted against the real attrs, without executing the
  (uninstalled) ``vibe`` binary (K5a / G2).
* ``TestVibeAdapterEndToEnd`` (async) — a real PTY against the bundled
  ``fake_harness`` via a fake-pointed ``VibeAdapter`` subclass: one
  spawn → inject(CR) → teardown smoke (K5b). test_adapters_parametrized owns
  the exhaustive cross-adapter matrix; this is one end-to-end proof.

Idioms cloned (not imported — the clone-per-file convention 5a-6b follow)
from ``tests/test_adapters_base.py``: ``_FAST_TEARDOWN``, ``_minimal_env``,
``tty.setraw`` before the CR assertion (input line discipline maps ``\\r``→``\\n``
otherwise), and asserting on the echo file (what the child received) — not
master-fd readback (OPOST-mangled).

Importing ``letterbox.adapters.vibe`` at module load fires ``@register_adapter``
once per process (import caching → no double registration), so
``get_adapter("vibe")`` works without a fixture. The registry-reset fixture from
``test_adapters_base.py`` is module-local and deliberately NOT reused
(re-registering "vibe" would raise ``AdapterAlreadyRegistered``).
"""
from __future__ import annotations

import inspect
import os
import sys
import tty
from pathlib import Path

import pytest

from letterbox import notifications
from letterbox.adapters import base, vibe
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.pty_common import PTYHandle
from letterbox.adapters.vibe import VibeAdapter
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# The exact template that ships — the K4 drift guard asserts on it.
# Byte-identical to Gemini/Antigravity ("Use check_messages." not Claude's
# "Call check_messages to read.").
_VISION_TEMPLATE = "📬 Peer message on channel {channel}. Use check_messages."

# Verbatim copy of vibe.py's tier-header (lines 1-10). The
# test_tier_header_preserved_verbatim lock fails if the body fill-in ever
# disturbs the §13.5 import-discipline record (10 lines, same as Gemini/Antigravity).
_EXPECTED_TIER_HEADER = [
    '"""Vibe CLI adapter — ``vibe`` CLI.',
    "",
    "Tier: 3",
    "May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,",
    "    ``letterbox.adapters.pty_common``.",
    "Must NOT import from: ``letterbox.adapters.claude``, ``letterbox.adapters.gemini``,",
    "    ``letterbox.adapters.antigravity``",
    "    (sibling-Tier-3 isolation), or any Tier 4 module — bulkhead §13.5.",
    "",
    "Filled in: vibe adapter per ADR-067.",
]


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _dummy_handle() -> PTYHandle:
    """A handle for the argv-composition stub, whose code path never uses fds."""
    return PTYHandle(pid=-1, master_fd=-1, slave_fd=-1, process=None)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# TestVibeAdapterConfig — the shipped class attrs ARE the contract
# ──────────────────────────────────────────────────────────────────────


class TestVibeAdapterConfig:
    def test_class_attrs_match_adr_067(self) -> None:
        assert VibeAdapter.name == "vibe"
        # Registry key ("vibe") and the binary on PATH ("vibe") are the same
        # string — unlike Antigravity's "antigravity" / "agy" split.
        assert VibeAdapter.command == "vibe"
        assert VibeAdapter.default_args == ["--yolo"]
        assert VibeAdapter.notification_template == _VISION_TEMPLATE
        # line_terminator is the inherited base default — NOT overridden.
        assert VibeAdapter.line_terminator == b"\r"
        # Like Gemini: no --mcp-config flag (ADR-054) and delayed terminator
        # for Textual's asyncio event-loop timing gap (ADR-057). ADR-067.
        assert VibeAdapter.mcp_config_via_flag is False
        assert VibeAdapter.terminator_delay == 0.1

    def test_registered_under_vibe(self) -> None:
        # Module-level @register_adapter fired at import; the live registry
        # resolves "vibe" to a fresh VibeAdapter instance.
        adapter = get_adapter("vibe")
        assert isinstance(adapter, VibeAdapter)

    def test_no_method_overrides(self) -> None:
        # The declarative-only contract (K1): every method resolves to the
        # SAME function object on Adapter via the MRO.
        assert VibeAdapter.spawn is Adapter.spawn
        assert VibeAdapter.inject is Adapter.inject
        assert VibeAdapter.teardown is Adapter.teardown
        assert VibeAdapter.pre_spawn is Adapter.pre_spawn
        assert VibeAdapter.post_spawn is Adapter.post_spawn
        assert VibeAdapter.pre_inject is Adapter.pre_inject
        assert VibeAdapter.pre_teardown is Adapter.pre_teardown

    def test_notification_template_is_launch_safe(self) -> None:
        # register_adapter does NOT validate the template (tier discipline).
        # This is the earliest place the shipped template is checked against
        # the real whitelist, so a typo fails here rather than at launcher
        # startup. Must not raise.
        notifications.validate_template(VibeAdapter.notification_template)

    def test_public_surface(self) -> None:
        assert vibe.__all__ == ["VibeAdapter"]

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(vibe).splitlines()
        assert source_lines[:10] == _EXPECTED_TIER_HEADER


# ──────────────────────────────────────────────────────────────────────
# TestVibeAdapterSpawnComposition — argv shape via a recording stub
# ──────────────────────────────────────────────────────────────────────


class TestVibeAdapterSpawnComposition:
    @pytest.mark.asyncio
    async def test_mcp_config_flows_through_extra_args(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Monkeypatch the name imported into base's OWN namespace;
        # spawn() resolves the bare name `spawn_pty` from there, so patching
        # pty_common.spawn_pty would silently NOT intercept (G2b). The stub is
        # sync (matching spawn_pty's real signature) and returns a dummy handle
        # — post_spawn runs after, but it's the inherited no-op, so any handle
        # works. Proves the real ADR-067 attrs compose with the launcher's
        # --mcp-config WITHOUT executing the (uninstalled) `vibe` binary.
        recorded: list[list[str]] = []

        def _recording_spawn_pty(
            cmd: list[str],
            cwd: Path,
            env: dict[str, str],
            *,
            start_new_session: bool = True,
        ) -> PTYHandle:
            recorded.append(cmd)
            return _dummy_handle()

        monkeypatch.setattr(base, "spawn_pty", _recording_spawn_pty)

        adapter = get_adapter("vibe")
        await adapter.spawn(
            ["--mcp-config", "/tmp/x.json"], tmp_path, _minimal_env()
        )

        # default_args == ["--yolo"] → "vibe" then "--yolo" then extra_args.
        assert recorded == [
            ["vibe", "--yolo", "--mcp-config", "/tmp/x.json"]
        ]


# ──────────────────────────────────────────────────────────────────────
# TestVibeAdapterEndToEnd — real PTY against fake_harness
# ──────────────────────────────────────────────────────────────────────


class TestVibeAdapterEndToEnd:
    @pytest.mark.asyncio
    async def test_spawn_inject_teardown_against_fake_harness(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # A fake-pointed subclass: command/default_args swapped to launch the
        # bundled fake_harness (the real `vibe` is not on PATH in CI and must
        # never run — G2). NOT decorated, so no registry conflict; it inherits
        # name="vibe", the ADR-067 template, and the b"\r" terminator.
        class _FakeVibe(VibeAdapter):
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]

        adapter = _FakeVibe()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            # Raw slave end so input line discipline (ICRNL) doesn't map the
            # injected \r → \n; assert on what the child received (echo file),
            # NOT master-fd readback (OPOST mangles \r on the echo-back path).
            tty.setraw(handle.slave_fd)
            await adapter.inject(handle, "test notification")
            await wait_for(
                lambda: fake_harness.read_echo().endswith(b"test notification\r"),
                timeout=5.0,
            )
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert fake_harness.read_echo().endswith(b"test notification\r")
        # No --mcp-config passed → fake_harness spawns no MCP child, so the
        # "tree" is a single process; poll() is not None proves it was reaped.
        assert handle.process.poll() is not None

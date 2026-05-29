"""Tests for ``letterbox.adapters.claude`` — Phase 6a (first concrete adapter).

Three test classes:

* ``TestClaudeAdapterConfig`` (sync) — the shipped class attrs ARE the
  contract (Vision §5.3); assert each verbatim, plus self-registration,
  the declarative-only (zero-override) shape, launch-safety of the
  template, the public surface, and the verbatim tier-header lock.
* ``TestClaudeAdapterSpawnComposition`` (async) — monkeypatch
  ``base.spawn_pty`` to a recording stub and prove the launcher's
  ``--mcp-config <path>`` flows through ``spawn``'s ``extra_args`` into
  the assembled argv *after* ``default_args`` — against the real attrs,
  without executing the (uninstalled) ``claude`` binary (K5a / G2).
* ``TestClaudeAdapterEndToEnd`` (async) — a real PTY against the bundled
  ``fake_harness`` via a fake-pointed ``ClaudeAdapter`` subclass: one
  spawn → inject(CR) → teardown smoke (K5b). 6d owns the exhaustive
  cross-adapter matrix; this is one end-to-end proof, not the matrix.

Idioms cloned (not imported — the clone-per-file convention 5a-5d follow)
from ``tests/test_adapters_base.py``: ``_FAST_TEARDOWN`` (fake_harness pays
the full SIGTERM timeout, PEP 475), ``_minimal_env``, ``tty.setraw`` before
the CR assertion (input line discipline maps ``\\r``→``\\n`` otherwise), and
asserting on the echo file (what the child received) — not master-fd
readback (OPOST-mangled).

Importing ``letterbox.adapters.claude`` at module load fires
``@register_adapter`` once per process (import caching → no double
registration), so ``get_adapter("claude")`` works without a fixture. The
registry-reset fixture from ``test_adapters_base.py`` is module-local and
deliberately NOT reused (re-registering "claude" would raise
``AdapterAlreadyRegistered``).
"""
from __future__ import annotations

import inspect
import os
import sys
import tty
from pathlib import Path

import pytest

from letterbox import notifications
from letterbox.adapters import base, claude
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.claude import ClaudeAdapter
from letterbox.adapters.pty_common import PTYHandle
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# The exact §5.3 template that ships — the K4 drift guard asserts on it.
_VISION_TEMPLATE = (
    "📬 Peer message on channel {channel}. Call check_messages to read."
)

# Verbatim copy of claude.py's tier-header (lines 1-11). The
# test_tier_header_preserved_verbatim lock fails if the body fill-in ever
# disturbs the §13.5 import-discipline record.
_EXPECTED_TIER_HEADER = [
    '"""Claude Code adapter — ``claude`` CLI, ``--dangerously-skip-permissions``.',
    "",
    "Tier: 3",
    "May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,",
    "    ``letterbox.adapters.pty_common``.",
    "Must NOT import from: ``letterbox.adapters.gemini``, ``letterbox.adapters.antigravity``",
    "    (sibling-Tier-3 isolation — each concrete adapter stays self-contained), or any",
    "    Tier 4 module — bulkhead §13.5.",
    "",
    "Filled in: Phase 6a per PHASE_INDEX.",
    '"""',
]


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _dummy_handle() -> PTYHandle:
    """A handle for the argv-composition stub, whose code path never uses fds."""
    return PTYHandle(pid=-1, master_fd=-1, slave_fd=-1, process=None)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# TestClaudeAdapterConfig — the shipped class attrs ARE the contract
# ──────────────────────────────────────────────────────────────────────


class TestClaudeAdapterConfig:
    def test_class_attrs_match_vision_5_3(self) -> None:
        assert ClaudeAdapter.name == "claude"
        assert ClaudeAdapter.command == "claude"
        assert ClaudeAdapter.default_args == ["--dangerously-skip-permissions"]
        assert ClaudeAdapter.notification_template == _VISION_TEMPLATE
        # line_terminator is the inherited base default — NOT overridden.
        assert ClaudeAdapter.line_terminator == b"\r"

    def test_registered_under_claude(self) -> None:
        # Module-level @register_adapter fired at import; the live registry
        # resolves "claude" to a fresh ClaudeAdapter instance.
        adapter = get_adapter("claude")
        assert isinstance(adapter, ClaudeAdapter)

    def test_no_method_overrides(self) -> None:
        # The declarative-only contract (K1) and the K2 "pre_spawn stays the
        # inherited no-op" decision, asserted structurally: every method
        # resolves to the SAME function object on Adapter via the MRO.
        assert ClaudeAdapter.spawn is Adapter.spawn
        assert ClaudeAdapter.inject is Adapter.inject
        assert ClaudeAdapter.teardown is Adapter.teardown
        assert ClaudeAdapter.pre_spawn is Adapter.pre_spawn
        assert ClaudeAdapter.post_spawn is Adapter.post_spawn
        assert ClaudeAdapter.pre_inject is Adapter.pre_inject
        assert ClaudeAdapter.pre_teardown is Adapter.pre_teardown

    def test_notification_template_is_launch_safe(self) -> None:
        # register_adapter does NOT validate the template (5b K7 — tier
        # discipline). This is the earliest place the shipped template is
        # checked against the real whitelist, so a typo fails at 6a rather
        # than silently at launcher startup (8a). Must not raise.
        notifications.validate_template(ClaudeAdapter.notification_template)

    def test_public_surface(self) -> None:
        assert claude.__all__ == ["ClaudeAdapter"]

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(claude).splitlines()
        assert source_lines[:11] == _EXPECTED_TIER_HEADER


# ──────────────────────────────────────────────────────────────────────
# TestClaudeAdapterSpawnComposition — argv shape via a recording stub
# ──────────────────────────────────────────────────────────────────────


class TestClaudeAdapterSpawnComposition:
    @pytest.mark.asyncio
    async def test_mcp_config_flows_through_extra_args(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Monkeypatch the name imported into base's OWN namespace (base.py L19);
        # spawn() resolves the bare name `spawn_pty` from there, so patching
        # pty_common.spawn_pty would silently NOT intercept (G2). The stub is
        # sync (matching spawn_pty's real signature) and returns a dummy handle
        # — post_spawn runs after, but it's the inherited no-op, so any handle
        # works. This proves the real §5.3 attrs compose with the launcher's
        # --mcp-config WITHOUT executing the (uninstalled) `claude` binary.
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

        adapter = get_adapter("claude")
        await adapter.spawn(
            ["--mcp-config", "/tmp/x.json"], tmp_path, _minimal_env()
        )

        assert recorded == [
            ["claude", "--dangerously-skip-permissions", "--mcp-config", "/tmp/x.json"]
        ]


# ──────────────────────────────────────────────────────────────────────
# TestClaudeAdapterEndToEnd — real PTY against fake_harness
# ──────────────────────────────────────────────────────────────────────


class TestClaudeAdapterEndToEnd:
    @pytest.mark.asyncio
    async def test_spawn_inject_teardown_against_fake_harness(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # A fake-pointed subclass: command/default_args swapped to launch the
        # bundled fake_harness (the real `claude` is not on PATH in CI and must
        # never run — G2). NOT decorated, so no registry conflict; it inherits
        # name="claude", the §5.3 template, and the b"\r" terminator. This is
        # the canonical close path for Wiring Ledger entry-001: fake_harness
        # driven end-to-end through a concrete, registered adapter shape.
        class _FakeClaude(ClaudeAdapter):
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]

        adapter = _FakeClaude()
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

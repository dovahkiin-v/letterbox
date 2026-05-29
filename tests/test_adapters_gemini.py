"""Tests for ``letterbox.adapters.gemini`` — Phase 6b (second concrete adapter).

Three test classes:

* ``TestGeminiAdapterConfig`` (sync) — the shipped class attrs ARE the
  contract (Vision §5.3); assert each verbatim, plus self-registration,
  the declarative-only (zero-override) shape, launch-safety of the
  template, the public surface, and the verbatim tier-header lock.
* ``TestGeminiAdapterSpawnComposition`` (async) — monkeypatch
  ``base.spawn_pty`` to a recording stub and prove the launcher's
  ``--mcp-config <path>`` flows through ``spawn``'s ``extra_args`` into
  the assembled argv *after* ``default_args`` — against the real attrs,
  without executing the (uninstalled) ``gemini`` binary (K5a / G2).
* ``TestGeminiAdapterEndToEnd`` (async) — a real PTY against the bundled
  ``fake_harness`` via a fake-pointed ``GeminiAdapter`` subclass: one
  spawn → inject(CR) → teardown smoke (K5b). 6d owns the exhaustive
  cross-adapter matrix; this is one end-to-end proof, not the matrix.

Idioms cloned (not imported — the clone-per-file convention 5a-6a follow)
from ``tests/test_adapters_base.py``: ``_FAST_TEARDOWN`` (fake_harness pays
the full SIGTERM timeout, PEP 475), ``_minimal_env``, ``tty.setraw`` before
the CR assertion (input line discipline maps ``\\r``→``\\n`` otherwise), and
asserting on the echo file (what the child received) — not master-fd
readback (OPOST-mangled).

Importing ``letterbox.adapters.gemini`` at module load fires
``@register_adapter`` once per process (import caching → no double
registration), so ``get_adapter("gemini")`` works without a fixture. The
registry-reset fixture from ``test_adapters_base.py`` is module-local and
deliberately NOT reused (re-registering "gemini" would raise
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
from letterbox.adapters import base, gemini
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.gemini import GeminiAdapter
from letterbox.adapters.pty_common import PTYHandle
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# The exact §5.3 template that ships — the K4 drift guard asserts on it.
# NOTE: Gemini ends "Use check_messages." — NOT Claude's
# "Call check_messages to read." (Vision §5.3 line 462 vs 457).
_VISION_TEMPLATE = "📬 Peer message on channel {channel}. Use check_messages."

# Verbatim copy of gemini.py's tier-header (lines 1-10). The
# test_tier_header_preserved_verbatim lock fails if the body fill-in ever
# disturbs the §13.5 import-discipline record. gemini.py's header is 10 lines
# (claude.py's was 11 — gemini's "Must NOT import" sentence wraps to 2 lines,
# not 3): the assertion is source_lines[:10], not [:11] (G1b).
_EXPECTED_TIER_HEADER = [
    '"""Gemini CLI adapter — ``gemini`` CLI, ``--yolo``.',
    "",
    "Tier: 3",
    "May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,",
    "    ``letterbox.adapters.pty_common``.",
    "Must NOT import from: ``letterbox.adapters.claude``, ``letterbox.adapters.antigravity``",
    "    (sibling-Tier-3 isolation), or any Tier 4 module — bulkhead §13.5.",
    "",
    "Filled in: Phase 6b per PHASE_INDEX.",
    '"""',
]


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _dummy_handle() -> PTYHandle:
    """A handle for the argv-composition stub, whose code path never uses fds."""
    return PTYHandle(pid=-1, master_fd=-1, slave_fd=-1, process=None)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# TestGeminiAdapterConfig — the shipped class attrs ARE the contract
# ──────────────────────────────────────────────────────────────────────


class TestGeminiAdapterConfig:
    def test_class_attrs_match_vision_5_3(self) -> None:
        assert GeminiAdapter.name == "gemini"
        assert GeminiAdapter.command == "gemini"
        assert GeminiAdapter.default_args == ["--yolo"]
        assert GeminiAdapter.notification_template == _VISION_TEMPLATE
        # line_terminator is the inherited base default — NOT overridden.
        assert GeminiAdapter.line_terminator == b"\r"

    def test_registered_under_gemini(self) -> None:
        # Module-level @register_adapter fired at import; the live registry
        # resolves "gemini" to a fresh GeminiAdapter instance.
        adapter = get_adapter("gemini")
        assert isinstance(adapter, GeminiAdapter)

    def test_no_method_overrides(self) -> None:
        # The declarative-only contract (K1) and the K2 "pre_spawn stays the
        # inherited no-op" decision, asserted structurally: every method
        # resolves to the SAME function object on Adapter via the MRO.
        assert GeminiAdapter.spawn is Adapter.spawn
        assert GeminiAdapter.inject is Adapter.inject
        assert GeminiAdapter.teardown is Adapter.teardown
        assert GeminiAdapter.pre_spawn is Adapter.pre_spawn
        assert GeminiAdapter.post_spawn is Adapter.post_spawn
        assert GeminiAdapter.pre_inject is Adapter.pre_inject
        assert GeminiAdapter.pre_teardown is Adapter.pre_teardown

    def test_notification_template_is_launch_safe(self) -> None:
        # register_adapter does NOT validate the template (5b K7 — tier
        # discipline). This is the earliest place the shipped template is
        # checked against the real whitelist, so a typo fails at 6b rather
        # than silently at launcher startup (8a). Must not raise.
        notifications.validate_template(GeminiAdapter.notification_template)

    def test_public_surface(self) -> None:
        assert gemini.__all__ == ["GeminiAdapter"]

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(gemini).splitlines()
        assert source_lines[:10] == _EXPECTED_TIER_HEADER


# ──────────────────────────────────────────────────────────────────────
# TestGeminiAdapterSpawnComposition — argv shape via a recording stub
# ──────────────────────────────────────────────────────────────────────


class TestGeminiAdapterSpawnComposition:
    @pytest.mark.asyncio
    async def test_mcp_config_flows_through_extra_args(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Monkeypatch the name imported into base's OWN namespace (base.py L19);
        # spawn() resolves the bare name `spawn_pty` from there, so patching
        # pty_common.spawn_pty would silently NOT intercept (G2b). The stub is
        # sync (matching spawn_pty's real signature) and returns a dummy handle
        # — post_spawn runs after, but it's the inherited no-op, so any handle
        # works. This proves the real §5.3 attrs compose with the launcher's
        # --mcp-config WITHOUT executing the (uninstalled) `gemini` binary.
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

        adapter = get_adapter("gemini")
        await adapter.spawn(
            ["--mcp-config", "/tmp/x.json"], tmp_path, _minimal_env()
        )

        assert recorded == [
            ["gemini", "--yolo", "--mcp-config", "/tmp/x.json"]
        ]


# ──────────────────────────────────────────────────────────────────────
# TestGeminiAdapterEndToEnd — real PTY against fake_harness
# ──────────────────────────────────────────────────────────────────────


class TestGeminiAdapterEndToEnd:
    @pytest.mark.asyncio
    async def test_spawn_inject_teardown_against_fake_harness(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # A fake-pointed subclass: command/default_args swapped to launch the
        # bundled fake_harness (the real `gemini` is not on PATH in CI and must
        # never run — G2). NOT decorated, so no registry conflict; it inherits
        # name="gemini", the §5.3 template, and the b"\r" terminator.
        class _FakeGemini(GeminiAdapter):
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]

        adapter = _FakeGemini()
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

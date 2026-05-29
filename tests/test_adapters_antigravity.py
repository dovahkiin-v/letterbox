"""Tests for ``letterbox.adapters.antigravity`` — Phase 6c (third concrete adapter).

Three test classes:

* ``TestAntigravityAdapterConfig`` (sync) — the shipped class attrs ARE the
  contract (Vision §5.3); assert each verbatim, plus self-registration,
  the declarative-only (zero-override) shape, launch-safety of the
  template, the public surface, and the verbatim tier-header lock.
* ``TestAntigravityAdapterSpawnComposition`` (async) — monkeypatch
  ``base.spawn_pty`` to a recording stub and prove the launcher's
  ``--mcp-config <path>`` flows through ``spawn``'s ``extra_args`` into
  the assembled argv. Antigravity's ``default_args`` is empty, so the argv
  collapses to ``["agy", *extra_args]`` — there is NO token between
  ``command`` and ``extra_args`` (G2c). Asserted against the real attrs,
  without executing the (uninstalled) ``agy`` binary (K5a / G2).
* ``TestAntigravityAdapterEndToEnd`` (async) — a real PTY against the bundled
  ``fake_harness`` via a fake-pointed ``AntigravityAdapter`` subclass: one
  spawn → inject(CR) → teardown smoke (K5b). 6d owns the exhaustive
  cross-adapter matrix; this is one end-to-end proof, not the matrix.

Idioms cloned (not imported — the clone-per-file convention 5a-6b follow)
from ``tests/test_adapters_base.py``: ``_FAST_TEARDOWN`` (fake_harness pays
the full SIGTERM timeout, PEP 475), ``_minimal_env``, ``tty.setraw`` before
the CR assertion (input line discipline maps ``\\r``→``\\n`` otherwise), and
asserting on the echo file (what the child received) — not master-fd
readback (OPOST-mangled).

Importing ``letterbox.adapters.antigravity`` at module load fires
``@register_adapter`` once per process (import caching → no double
registration), so ``get_adapter("antigravity")`` works without a fixture. The
registry-reset fixture from ``test_adapters_base.py`` is module-local and
deliberately NOT reused (re-registering "antigravity" would raise
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
from letterbox.adapters import antigravity, base
from letterbox.adapters.antigravity import AntigravityAdapter
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.pty_common import PTYHandle
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# The exact §5.3 template that ships — the K4 drift guard asserts on it.
# NOTE: Antigravity is byte-identical to Gemini ("Use check_messages.") —
# NOT Claude's "Call check_messages to read." (Vision §5.3 line 467 == 462).
_VISION_TEMPLATE = "📬 Peer message on channel {channel}. Use check_messages."

# Verbatim copy of antigravity.py's tier-header (lines 1-10). The
# test_tier_header_preserved_verbatim lock fails if the body fill-in ever
# disturbs the §13.5 import-discipline record. antigravity.py's header is 10
# lines (matches gemini.py; claude.py's was 11): the assertion is
# source_lines[:10], not [:11] (G1b).
_EXPECTED_TIER_HEADER = [
    '"""Antigravity CLI adapter — ``agy`` CLI.',
    "",
    "Tier: 3",
    "May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,",
    "    ``letterbox.adapters.pty_common``.",
    "Must NOT import from: ``letterbox.adapters.claude``, ``letterbox.adapters.gemini``",
    "    (sibling-Tier-3 isolation), or any Tier 4 module — bulkhead §13.5.",
    "",
    "Filled in: Phase 6c per PHASE_INDEX.",
    '"""',
]


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _dummy_handle() -> PTYHandle:
    """A handle for the argv-composition stub, whose code path never uses fds."""
    return PTYHandle(pid=-1, master_fd=-1, slave_fd=-1, process=None)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# TestAntigravityAdapterConfig — the shipped class attrs ARE the contract
# ──────────────────────────────────────────────────────────────────────


class TestAntigravityAdapterConfig:
    def test_class_attrs_match_vision_5_3(self) -> None:
        assert AntigravityAdapter.name == "antigravity"
        # The registry key ("antigravity") and the binary on PATH ("agy") are
        # DIFFERENT strings — the §3 trap-1 the K4 test guards.
        assert AntigravityAdapter.command == "agy"
        # The only v1 adapter with empty default_args (Claude/Gemini have one).
        assert AntigravityAdapter.default_args == []
        assert AntigravityAdapter.notification_template == _VISION_TEMPLATE
        # line_terminator is the inherited base default — NOT overridden.
        assert AntigravityAdapter.line_terminator == b"\r"

    def test_registered_under_antigravity(self) -> None:
        # Module-level @register_adapter fired at import; the live registry
        # resolves "antigravity" to a fresh AntigravityAdapter instance.
        adapter = get_adapter("antigravity")
        assert isinstance(adapter, AntigravityAdapter)

    def test_no_method_overrides(self) -> None:
        # The declarative-only contract (K1) and the K2 "pre_spawn stays the
        # inherited no-op" decision, asserted structurally: every method
        # resolves to the SAME function object on Adapter via the MRO.
        assert AntigravityAdapter.spawn is Adapter.spawn
        assert AntigravityAdapter.inject is Adapter.inject
        assert AntigravityAdapter.teardown is Adapter.teardown
        assert AntigravityAdapter.pre_spawn is Adapter.pre_spawn
        assert AntigravityAdapter.post_spawn is Adapter.post_spawn
        assert AntigravityAdapter.pre_inject is Adapter.pre_inject
        assert AntigravityAdapter.pre_teardown is Adapter.pre_teardown

    def test_notification_template_is_launch_safe(self) -> None:
        # register_adapter does NOT validate the template (5b K7 — tier
        # discipline). This is the earliest place the shipped template is
        # checked against the real whitelist, so a typo fails at 6c rather
        # than silently at launcher startup (8a). Must not raise.
        notifications.validate_template(AntigravityAdapter.notification_template)

    def test_public_surface(self) -> None:
        assert antigravity.__all__ == ["AntigravityAdapter"]

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(antigravity).splitlines()
        assert source_lines[:10] == _EXPECTED_TIER_HEADER


# ──────────────────────────────────────────────────────────────────────
# TestAntigravityAdapterSpawnComposition — argv shape via a recording stub
# ──────────────────────────────────────────────────────────────────────


class TestAntigravityAdapterSpawnComposition:
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
        # --mcp-config WITHOUT executing the (uninstalled) `agy` binary.
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

        adapter = get_adapter("antigravity")
        await adapter.spawn(
            ["--mcp-config", "/tmp/x.json"], tmp_path, _minimal_env()
        )

        # default_args == [] → NO token between "agy" and "--mcp-config"
        # (contrast Gemini's ["gemini", "--yolo", "--mcp-config", …]). G2c.
        assert recorded == [
            ["agy", "--mcp-config", "/tmp/x.json"]
        ]


# ──────────────────────────────────────────────────────────────────────
# TestAntigravityAdapterEndToEnd — real PTY against fake_harness
# ──────────────────────────────────────────────────────────────────────


class TestAntigravityAdapterEndToEnd:
    @pytest.mark.asyncio
    async def test_spawn_inject_teardown_against_fake_harness(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # A fake-pointed subclass: command/default_args swapped to launch the
        # bundled fake_harness (the real `agy` is not on PATH in CI and must
        # never run — G2). NOT decorated, so no registry conflict; it inherits
        # name="antigravity", the §5.3 template, and the b"\r" terminator.
        # Note: this subclass OVERRIDES default_args to point at the fake
        # harness, so the production class's empty default_args is irrelevant
        # here — the empty-list shape is exercised by test 7, not this one.
        class _FakeAntigravity(AntigravityAdapter):
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]

        adapter = _FakeAntigravity()
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

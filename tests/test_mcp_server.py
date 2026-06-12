"""Tests for ``letterbox.mcp_server`` — Phase 7a.

Six test classes covering the W13 join-key argparse lock (the consumer-side
mirror of 6d Family B), the six-tool registry + generated input schemas, the
fail-loud stub bodies, channel-open under the resolved state dir (W18), the
in-process ``run`` orchestration, and the headline "clean shutdown on SIGTERM"
integration via a real subprocess.

The async surface (``FastMCP.list_tools``) is exercised with
``asyncio.run(...)`` from plain sync tests (the 5a idiom) — no
``pytest.mark.asyncio``, no AsyncMock. Subprocess tests mirror
``tests/test_pty_common.py``'s death-poll + returncode shapes but spawn a
plain ``subprocess.Popen`` (not a PTY) running ``run(sys.argv[1:])`` directly,
the way 9a's dispatch will (G5).
"""
from __future__ import annotations

import asyncio
import inspect
import json
import os
import pty
import shutil
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from letterbox import mcp_server
from letterbox.channel import Channel, read_state
from letterbox.mcp_server import (
    _align_read_marker,
    _build_server,
    _missing_join_keys,
    _open_channel,
    _parse_args,
    run,
)
from letterbox.protocol import (
    MAX_BODY_BYTES,
    Message,
    MessageTooLarge,
    is_valid_message_filename,
    make_message_filename,
    new_message,
    read_message,
    write_message,
)
from mcp.server.fastmcp import FastMCP
from mcp.shared.memory import create_connected_server_and_client_session
from tests.helpers import wait_for

# The six Vision §6.1 tools the registry must expose, verbatim.
_EXPECTED_TOOLS = {
    "send_message",
    "check_latest_message",
    "check_messages",
    "acknowledge",
    "list_channels",
    "channel_info",
}

# Minimal valid call kwargs per tool, for the inverted no-stub assertion (K5).
# acknowledge's stem must be a VALID message-id (a bad stem would raise
# ValueError, not NotImplementedError — the inverted test tolerates that, but a
# valid stem keeps the assertion clean). After 7d every body is live, so this
# map exists to prove no body raises NotImplementedError, not to find stubs.
_MINIMAL_CALL_ARGS: dict[str, dict[str, object]] = {
    "send_message": {"body": "x"},
    "check_latest_message": {},
    "check_messages": {},
    "acknowledge": {"message_id": make_message_filename().removesuffix(".json")},
    "list_channels": {},
    "channel_info": {},
}

# The §3.2 wire-dict key set `_message_to_dict` (and thus `check_latest_message`)
# must emit. Locks the serializer shape 7c/7d reuse.
_MESSAGE_WIRE_KEYS = {
    "schema_version",
    "id",
    "channel",
    "address",
    "instance_id",
    "sender",
    "recipient",
    "timestamp",
    "body",
    "in_reply_to",
    "metadata",
}

# Verbatim copy of mcp_server.py's tier-header (lines 1-10). The
# test_tier_header_preserved_verbatim lock fails if a future body fill-in
# disturbs the §13.5 import-discipline record. Line 4 reads "Tier 1+2"
# (corrected from the stub's loose "Tier 1" per G9 option (b) — channel is
# Tier 2; see IMPLEMENTATION_NOTES 7a).
_EXPECTED_TIER_HEADER = [
    '"""stdio MCP server body for ``letterbox mcp`` subcommand. Spawned BY the agent.',
    "",
    "Tier: 4",
    "May import from: stdlib; Tier 1+2 (``protocol``, ``channel``, ``config``, and ``notifications``",
    "    if needed for tool error messaging); ``mcp`` SDK.",
    "Must NOT import from: ``letterbox.launcher`` or ``letterbox.cli`` (Tier 4 sibling isolation —",
    "    bulkhead §13.5).",
    "",
    "Filled in: Phase 7a/7b/7c/7d per PHASE_INDEX.",
    '"""',
]

# Program run by the subprocess tests: the exact shape 9a forwards to (G5/G8).
_RUN_PROG = "import sys; from letterbox.mcp_server import run; run(sys.argv[1:])"


# ──────────────────────────────────────────────────────────────────────
# Local helpers
# ──────────────────────────────────────────────────────────────────────


def _dummy_channel() -> Channel:
    """A no-disk ``Channel`` for registry tests (``_build_server`` never touches it)."""
    return Channel(
        name="c",
        path=Path("/tmp/letterbox-test-c"),
        sender_label="claude",
        recipient_label="",
    )


def _spawn_server(
    home: Path,
    *,
    channel: str = "test",
    label: str = "claude",
    instance_id: str = "lb-x",
    extra_args: list[str] | None = None,
) -> subprocess.Popen[bytes]:
    """Spawn ``run(sys.argv[1:])`` as a real subprocess with a held-open stdin.

    ``stdin`` is a pipe we deliberately leave open: an EOF would let the stdio
    reader exit on its own and muddy the SIGTERM test (G4). The subprocess
    resolves the same state dir via ``LETTERBOX_HOME`` in its env.
    """
    args = (
        ["--channel", channel, "--as", label, "--instance-id", instance_id]
        if extra_args is None
        else extra_args
    )
    env = {**os.environ, "LETTERBOX_HOME": str(home)}
    return subprocess.Popen(
        [sys.executable, "-c", _RUN_PROG, *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )


def _ensure_dead(proc: subprocess.Popen[bytes]) -> None:
    """Force-kill if still alive and close all pipe fds.

    Idempotent w.r.t. a prior ``communicate()`` — closing an already-closed
    file is a no-op. Required because ``filterwarnings=["error"]`` (1b) would
    promote a leaked pipe's ``ResourceWarning`` to a test failure.
    """
    if proc.poll() is None:
        proc.kill()
        proc.wait(timeout=5)
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None:
            stream.close()


# ──────────────────────────────────────────────────────────────────────
# TestParseArgs — the W13 join-key spelling lock (consumer side, mirrors 6d)
# ──────────────────────────────────────────────────────────────────────


class TestParseArgs:
    def test_parses_all_three_flags(self) -> None:
        ns = _parse_args(
            ["--channel", "c", "--as", "claude", "--instance-id", "lb-x"]
        )
        assert ns.channel == "c"
        # G1: --as maps to dest "sender_label" (``as`` is a Python keyword).
        assert ns.sender_label == "claude"
        assert ns.instance_id == "lb-x"

    def test_no_attribute_named_as(self) -> None:
        # Defensive: the namespace must NOT carry an ``as`` attribute (G1).
        ns = _parse_args(
            ["--channel", "c", "--as", "claude", "--instance-id", "lb-x"]
        )
        assert not hasattr(ns, "as")

    @pytest.mark.parametrize(
        "argv",
        [
            # --sender instead of --as
            ["--channel", "c", "--sender", "claude", "--instance-id", "lb-x"],
            # underscore instead of hyphen
            ["--channel", "c", "--as", "claude", "--instance_id", "lb-x"],
            # squashed instance id flag
            ["--channel", "c", "--as", "claude", "--instanceid", "lb-x"],
            # renamed channel flag
            ["--channel-name", "c", "--as", "claude", "--instance-id", "lb-x"],
        ],
    )
    def test_drifted_flag_spellings_rejected(self, argv: list[str]) -> None:
        # If anyone renames a join-key flag, the agent's MCP child parse-errors
        # at spawn and the channel goes silent. This screams instead (K1).
        with pytest.raises(SystemExit) as excinfo:
            _parse_args(argv)
        assert excinfo.value.code == 2

    @pytest.mark.parametrize(
        ("argv", "missing_attr", "missing_spec"),
        [
            (["--as", "claude", "--instance-id", "lb-x"], "channel",
             "--channel / $LETTERBOX_CHANNEL"),
            (["--channel", "c", "--instance-id", "lb-x"], "sender_label",
             "--as / $LETTERBOX_SENDER"),
            (["--channel", "c", "--as", "claude"], "instance_id",
             "--instance-id / $LETTERBOX_INSTANCE_ID"),
        ],
    )
    def test_missing_value_resolves_to_none(
        self,
        argv: list[str],
        missing_attr: str,
        missing_spec: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # ADR-056: _parse_args is LENIENT — a join key absent from both flags
        # and env resolves to None (run() decides dormant-vs-error), it does NOT
        # exit. _missing_join_keys names it as a "flag / $ENV" vector (P3).
        for var in (
            "LETTERBOX_CHANNEL", "LETTERBOX_SENDER", "LETTERBOX_INSTANCE_ID"
        ):
            monkeypatch.delenv(var, raising=False)
        ns = _parse_args(argv)
        assert getattr(ns, missing_attr) is None
        assert _missing_join_keys(ns) == [missing_spec]

    @pytest.mark.parametrize(
        ("envvar", "attr"),
        [
            ("LETTERBOX_CHANNEL", "channel"),
            ("LETTERBOX_SENDER", "sender_label"),
            ("LETTERBOX_INSTANCE_ID", "instance_id"),
        ],
    )
    def test_env_fallback_supplies_missing_value(
        self, envvar: str, attr: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ADR-055: a value absent as a flag is taken from its env var, so a
        # settings-wired harness (channel-agnostic ["mcp"]) gets the per-launch
        # join keys the launcher exported.
        monkeypatch.setenv(envvar, "from-env")
        ns = _parse_args([])  # no flags at all
        assert getattr(ns, attr) == "from-env"

    def test_flag_wins_over_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # An explicit flag is the stronger signal and overrides the env (ADR-055).
        monkeypatch.setenv("LETTERBOX_CHANNEL", "env-chan")
        ns = _parse_args(["--channel", "flag-chan"])
        assert ns.channel == "flag-chan"


# ──────────────────────────────────────────────────────────────────────
# TestBuildServer — registry completeness, schemas, fail-loud stubs (K3)
# ──────────────────────────────────────────────────────────────────────


class TestBuildServer:
    def test_registers_all_six_tools(self) -> None:
        server = _build_server(_dummy_channel(), "lb-x")
        tools = asyncio.run(server.list_tools())
        assert {t.name for t in tools} == _EXPECTED_TOOLS

    def test_tool_input_schemas(self) -> None:
        server = _build_server(_dummy_channel(), "lb-x")
        schemas = {t.name: t.inputSchema for t in asyncio.run(server.list_tools())}

        send = schemas["send_message"]
        assert send["required"] == ["body"]
        assert "in_reply_to" in send["properties"]
        assert "in_reply_to" not in send.get("required", [])

        assert schemas["acknowledge"]["required"] == ["message_id"]

        check = schemas["check_messages"]
        assert set(check["properties"]) == {"limit", "since_id"}
        # Both have defaults → neither is required.
        assert check.get("required", []) == []

        # The no-param tools advertise no inputs.
        for name in ("check_latest_message", "list_channels", "channel_info"):
            assert schemas[name].get("properties", {}) == {}
            assert schemas[name].get("required", []) == []

    def test_tools_have_agent_facing_descriptions(self) -> None:
        # The docstrings become the MCP tool descriptions the agent reads — a
        # blank one is a real defect, so assert each tool carries prose.
        tools = asyncio.run(_build_server(_dummy_channel(), "lb-x").list_tools())
        for tool in tools:
            assert tool.description and tool.description.strip()

    def test_no_tool_body_is_stubbed(self, tmp_letterbox_home: Path) -> None:
        # K5 — after 7d every body is live; the stub-raise test is retired and
        # inverted. Calling each registered body with minimal valid args on a
        # real on-disk channel must NOT raise NotImplementedError. This locks
        # the closure-complete invariant: a future regression that re-stubs a
        # tool would be caught. (Other exceptions are tolerable here — the
        # acknowledge stem is valid, so no ValueError fires either.)
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        registered = server._tool_manager._tools  # noqa: SLF001
        assert set(registered) == _EXPECTED_TOOLS
        for name, kwargs in _MINIMAL_CALL_ARGS.items():
            try:
                registered[name].fn(**kwargs)
            except NotImplementedError as exc:
                pytest.fail(f"tool {name!r} is still a stub: {exc}")


# ──────────────────────────────────────────────────────────────────────
# TestOpenChannel — W18 state-dir consumption + channel creation (K5, G6)
# ──────────────────────────────────────────────────────────────────────


class TestOpenChannel:
    def test_creates_dir_under_state_dir(self, tmp_letterbox_home: Path) -> None:
        args = _parse_args(
            ["--channel", "review", "--as", "claude", "--instance-id", "lb-x"]
        )
        channel = _open_channel(args)
        assert channel.path == tmp_letterbox_home / "channels" / "review"
        assert channel.path.is_dir()
        assert (channel.path.stat().st_mode & 0o777) == 0o700
        # --as flows to sender_label; recipient is empty at launch (G6).
        assert channel.sender_label == "claude"
        assert channel.recipient_label == ""

    def test_honors_letterbox_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Prove the K5 resolver follows LETTERBOX_HOME precedence: point it at a
        # not-yet-existing dir and assert the channel lands under it.
        custom_home = tmp_path / "custom-home"
        monkeypatch.setenv("LETTERBOX_HOME", str(custom_home))
        args = _parse_args(
            ["--channel", "c", "--as", "gemini", "--instance-id", "lb-y"]
        )
        channel = _open_channel(args)
        assert channel.path == custom_home / "channels" / "c"
        assert channel.path.is_dir()


# ──────────────────────────────────────────────────────────────────────
# TestAlignReadMarker — launch watermark = read marker (ADR-058 fix a)
# ──────────────────────────────────────────────────────────────────────


class TestAlignReadMarker:
    def test_advances_marker_to_latest_message_at_launch(
        self, tmp_letterbox_home: Path
    ) -> None:
        # ADR-058 — at launch the read marker jumps to the newest message on
        # disk, so the inbox starts as "messages since I launched" (matching
        # the watcher's start watermark, ADR-024) rather than the whole backlog.
        ch = Channel.get_or_create("review", "claude", "", state_dir=tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        stems = [
            _write_peer_message(ch, body=f"m{i}", timestamp=base + timedelta(microseconds=i))
            for i in range(3)
        ]
        _align_read_marker(ch, "lb-launch")
        assert read_state(ch, ch.sender_label).high_water_mark == stems[-1]

    def test_kills_cross_session_backlog(
        self, tmp_letterbox_home: Path
    ) -> None:
        # The headline fix: a stale message from a prior session is treated as
        # read after launch alignment, so check_messages does not replay it.
        ch = Channel.get_or_create("review", "claude", "", state_dir=tmp_letterbox_home)
        _write_peer_message(ch, body="stale from last session")
        _align_read_marker(ch, "lb-launch")
        _, server = ch, _build_server(ch, "lb-launch")
        assert _fn(server, "check_messages")()["messages"] == []

    def test_uses_latest_including_own_writes(
        self, tmp_letterbox_home: Path
    ) -> None:
        # The watermark is the newest filename regardless of sender — own
        # writes are filtered downstream, so it simply means "up to launch is
        # not new". A newest-own-write must still advance the marker past the
        # earlier peer message.
        ch = Channel.get_or_create("review", "claude", "", state_dir=tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        _write_peer_message(ch, sender="gemini", body="peer", timestamp=base)
        own_stem = _write_peer_message(
            ch, sender="claude", instance_id="lb-mine", body="mine",
            timestamp=base + timedelta(microseconds=1),
        )
        _align_read_marker(ch, "lb-launch")
        assert read_state(ch, ch.sender_label).high_water_mark == own_stem

    def test_empty_channel_is_noop(self, tmp_letterbox_home: Path) -> None:
        # No messages → nothing to acknowledge → no .read/ file written.
        ch = Channel.get_or_create("review", "claude", "", state_dir=tmp_letterbox_home)
        _align_read_marker(ch, "lb-launch")
        assert read_state(ch, ch.sender_label).high_water_mark == ""
        assert not (ch.path / ".read").exists()

    def test_monotonic_never_rewinds(self, tmp_letterbox_home: Path) -> None:
        # A prior session may have advanced the marker past the current latest
        # (e.g. via a since_id catch-up that acknowledged ahead); relaunch must
        # not rewind it. The clamp is max(current, latest).
        ch = Channel.get_or_create("review", "claude", "", state_dir=tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        stem = _write_peer_message(ch, body="m0", timestamp=base)
        ahead = make_message_filename(
            base + timedelta(microseconds=5)
        ).removesuffix(".json")
        ch.acknowledge(ahead, self_instance_id="lb-prev")
        _align_read_marker(ch, "lb-launch")
        # latest-on-disk (stem) < already-acknowledged (ahead) → no rewind.
        assert read_state(ch, ch.sender_label).high_water_mark == ahead
        assert stem < ahead


# ──────────────────────────────────────────────────────────────────────
# TestRun — in-process orchestration coverage (no blocking stdio loop)
# ──────────────────────────────────────────────────────────────────────


class TestRun:
    def test_opens_channel_then_starts_server(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        transports: list[str] = []

        def fake_run(
            self: FastMCP, transport: str = "stdio", mount_path: str | None = None
        ) -> None:
            transports.append(transport)

        # No-op the blocking call so run()'s orchestration is covered in-process.
        monkeypatch.setattr(FastMCP, "run", fake_run)
        run(["--channel", "test", "--as", "claude", "--instance-id", "lb-x"])
        assert transports == ["stdio"]
        assert (tmp_letterbox_home / "channels" / "test").is_dir()

    def test_aligns_read_marker_at_launch(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ADR-058 — run() aligns the marker between opening the channel and
        # starting the server. Pre-seed a message on the channel run() will
        # open, then prove the marker landed on it after run() returns.
        ch = Channel.get_or_create("test", "claude", "", state_dir=tmp_letterbox_home)
        stem = _write_peer_message(ch, body="pre-launch")
        monkeypatch.setattr(FastMCP, "run", lambda self, transport="stdio": None)
        run(["--channel", "test", "--as", "claude", "--instance-id", "lb-x"])
        assert read_state(ch, ch.sender_label).high_water_mark == stem

    def test_argv_none_reads_sys_argv(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # argv=None falls back to sys.argv[1:] (the 9a-forward shape).
        monkeypatch.setattr(FastMCP, "run", lambda self, transport="stdio": None)
        monkeypatch.setattr(
            sys,
            "argv",
            ["letterbox-mcp", "--channel", "viasys", "--as", "claude", "--instance-id", "lb-z"],
        )
        run()
        assert (tmp_letterbox_home / "channels" / "viasys").is_dir()


# ──────────────────────────────────────────────────────────────────────
# TestRunShutdown — integration: clean SIGTERM shutdown (Vision §6.3 step 5)
# ──────────────────────────────────────────────────────────────────────


class TestRunShutdown:
    def test_sigterm_terminates_cleanly(self, tmp_letterbox_home: Path) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "test"
        proc = _spawn_server(tmp_letterbox_home, channel="test")
        try:
            # Readiness: _open_channel creates the dir before server.run blocks.
            asyncio.run(wait_for(lambda: channel_dir.exists(), timeout=10.0))
            proc.send_signal(signal.SIGTERM)
            out, err = proc.communicate(timeout=10.0)
            # K4: default SIGTERM disposition is the clean exit — no handler,
            # no traceback, returncode -15.
            assert proc.returncode == -signal.SIGTERM
            assert b"Traceback" not in err
            # G2: stdout carries only the MCP JSON-RPC stream. With no client
            # request, nothing is written there — a stray print would break it.
            assert out == b""
        finally:
            _ensure_dead(proc)

    def test_starts_cleanly_with_valid_args(self, tmp_letterbox_home: Path) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "live"
        proc = _spawn_server(tmp_letterbox_home, channel="live")
        try:
            asyncio.run(wait_for(lambda: channel_dir.exists(), timeout=10.0))
            # Server reached readiness AND is still blocking in the serve loop.
            assert proc.poll() is None
            assert (channel_dir.stat().st_mode & 0o777) == 0o700
        finally:
            proc.send_signal(signal.SIGTERM)
            _ensure_dead(proc)

    @staticmethod
    def _env_without_join_keys(home: Path) -> dict[str, str]:
        """A subprocess env with LETTERBOX_HOME but NO channel/sender/instance
        join keys — so a missing flag is genuinely missing from both sources."""
        env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in (
                "LETTERBOX_CHANNEL",
                "LETTERBOX_SENDER",
                "LETTERBOX_INSTANCE_ID",
            )
        }
        env["LETTERBOX_HOME"] = str(home)
        return env

    def test_missing_arg_tty_exits_loud(self, tmp_letterbox_home: Path) -> None:
        # Human-misuse path (ADR-056): a person runs `letterbox mcp` by hand
        # with an incomplete invocation. stdin is a TTY → fail loud, exit 2,
        # name the missing key — not a dormant hang on a handshake that never
        # arrives.
        master, slave = pty.openpty()
        proc = subprocess.Popen(
            [sys.executable, "-c", _RUN_PROG, "--channel", "c", "--as", "claude"],
            stdin=slave,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self._env_without_join_keys(tmp_letterbox_home),
        )
        os.close(slave)
        try:
            _out, err = proc.communicate(timeout=10.0)
            assert proc.returncode == 2
            assert b"--instance-id" in err
        finally:
            os.close(master)
            _ensure_dead(proc)

    def test_missing_arg_pipe_starts_dormant(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Host-spawn path (ADR-056): an MCP host spawns `letterbox mcp` over a
        # pipe (non-TTY) with no channel (e.g. the server left in user-level
        # settings, a plain session). It must NOT exit — it starts DORMANT and
        # blocks in the serve loop so the harness shows a calm "connected".
        proc = subprocess.Popen(
            [sys.executable, "-c", _RUN_PROG, "--channel", "c", "--as", "claude"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self._env_without_join_keys(tmp_letterbox_home),
        )
        try:
            # wait() (NOT communicate(), which would CLOSE stdin and give the
            # server a clean EOF exit) blocks until the process exits. A dormant
            # server stays in its serve loop while stdin is open, so wait times
            # out → it did not exit 2, it went dormant.
            with pytest.raises(subprocess.TimeoutExpired):
                proc.wait(timeout=1.5)
            assert proc.poll() is None
        finally:
            _ensure_dead(proc)


# ──────────────────────────────────────────────────────────────────────
# TestPublicSurface — module exports + tier-header lock
# ──────────────────────────────────────────────────────────────────────


class TestPublicSurface:
    def test_public_exports(self) -> None:
        assert mcp_server.__all__ == ["run", "UnknownRecipientError"]

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(mcp_server).splitlines()
        assert source_lines[:10] == _EXPECTED_TIER_HEADER


# ──────────────────────────────────────────────────────────────────────
# 7b helpers — real on-disk channel + server (the tool bodies do real I/O)
# ──────────────────────────────────────────────────────────────────────


def _real_channel_and_server(
    home: Path,
    *,
    channel: str = "review",
    label: str = "claude",
    instance_id: str = "lb-x",
) -> tuple[Channel, FastMCP]:
    """Open a real on-disk channel under ``home`` and build its server.

    Unlike ``_dummy_channel`` (no-disk, registry-only), the tool-body tests
    read from and write to a real channel directory. Mirrors ``_open_channel``:
    the channel lands at ``home/channels/<channel>``.
    """
    ch = Channel.get_or_create(channel, label, "", state_dir=home)
    return ch, _build_server(ch, instance_id)


def _fn(server: FastMCP, name: str):
    """Return the raw Python closure behind a registered tool (7a `.fn` idiom).

    Calls the body directly, bypassing FastMCP/pydantic input validation — the
    only way to assert body behavior (G5). Deliberate internals access; ``mcp``
    is pinned ``>=1.27,<1.28`` (ADR-029) so the layout is stable.
    """
    return server._tool_manager._tools[name].fn  # noqa: SLF001


def _plant_live_lock(home: Path, *, channel: str, label: str) -> None:
    """Plant a pid-lock owned by this (alive) test process so ``label`` reads as
    a live participant on ``channel`` — the precondition for directing a message
    at it (ADR-064 send-time validation; mirrors ``list_live_participants``).
    """
    lock_dir = home / "locks" / channel
    lock_dir.mkdir(parents=True, exist_ok=True)
    (lock_dir / f"{label}.pid").write_text(f"{os.getpid()}\n")


def _write_peer_message(
    channel: Channel,
    *,
    sender: str = "gemini",
    instance_id: str = "lb-peer",
    body: str = "hi",
    timestamp: datetime | None = None,
) -> str:
    """Write one peer message to ``channel`` and return its id stem."""
    stem = make_message_filename(timestamp).removesuffix(".json")
    write_message(
        channel.path,
        new_message(
            id=stem,
            channel=channel.name,
            instance_id=instance_id,
            sender=sender,
            body=body,
        ),
    )
    return stem


# ──────────────────────────────────────────────────────────────────────
# TestSendMessage — the write path (K3/K4 server-side identity, §13.2/§13.3)
# ──────────────────────────────────────────────────────────────────────


class TestSendMessage:
    def test_returns_valid_id_and_writes_file(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = _fn(server, "send_message")(body="hello")
        msg_id = result["id"]
        # G1 — the returned id is a valid message-id stem (no `.json`).
        assert is_valid_message_filename(f"{msg_id}.json")
        assert (ch.path / f"{msg_id}.json").is_file()

    def test_unicode_body_round_trips_unescaped(
        self, tmp_letterbox_home: Path
    ) -> None:
        """§13.2 — Lithuanian + CJK + emoji round-trip, stored as raw UTF-8
        (not ``\\uXXXX`` escapes) via ``write_message``→``to_json_bytes``.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        body = "ąčęėįšųūž 日本語 😀"
        result = _fn(server, "send_message")(body=body)
        path = ch.path / f"{result['id']}.json"
        loaded = read_message(path)
        assert isinstance(loaded, Message)
        assert loaded.body == body
        raw = path.read_bytes()
        assert "ąčęėįšųūž".encode("utf-8") in raw
        assert b"\\u" not in raw  # no JSON unicode escapes

    def test_identity_is_server_side(self, tmp_letterbox_home: Path) -> None:
        """§13.3 — sender from the channel handle, instance_id from the launch
        context. The agent supplies neither (no parameter exists for them).
        """
        ch, server = _real_channel_and_server(
            tmp_letterbox_home, label="claude", instance_id="lb-launch-1"
        )
        result = _fn(server, "send_message")(body="x")
        loaded = read_message(ch.path / f"{result['id']}.json")
        assert isinstance(loaded, Message)
        assert loaded.sender == "claude"
        assert loaded.instance_id == "lb-launch-1"

    def test_recipient_is_null_when_broadcast(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Default (no ``to``) is a broadcast: recipient stays None.

        channel.recipient_label "" is NOT threaded into the message — the wire
        recipient is driven solely by the ``to`` parameter.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = _fn(server, "send_message")(body="x")
        loaded = read_message(ch.path / f"{result['id']}.json")
        assert isinstance(loaded, Message)
        assert loaded.recipient is None

    def test_to_sets_directed_recipient(self, tmp_letterbox_home: Path) -> None:
        """``to`` directs the message: the wire recipient carries the label.

        The target must be a live participant (ADR-064), so plant its pid-lock
        first.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        _plant_live_lock(tmp_letterbox_home, channel="review", label="claude-commit")
        result = _fn(server, "send_message")(body="x", to="claude-commit")
        loaded = read_message(ch.path / f"{result['id']}.json")
        assert isinstance(loaded, Message)
        assert loaded.recipient == "claude-commit"

    def test_to_unknown_recipient_raises_before_write(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-064 — directing at a label that is not live fails loud, names who
        is live, and writes nothing to the channel.
        """
        ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        _plant_live_lock(tmp_letterbox_home, channel="review", label="claude")
        _plant_live_lock(tmp_letterbox_home, channel="review", label="gemini")
        before = set(ch.path.glob("msg-*.json"))
        with pytest.raises(mcp_server.UnknownRecipientError) as exc:
            _fn(server, "send_message")(body="x", to="nobody")
        # Roster names the live peer; the sender's own label is excluded.
        assert "gemini" in str(exc.value)
        assert "nobody" in str(exc.value)
        # Nothing was written — the guard runs before any disk I/O.
        assert set(ch.path.glob("msg-*.json")) == before

    def test_to_wrong_case_raises_with_hint(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-064 — a case-only mismatch (the original Antigravity bug) is
        caught and the error suggests the correct casing.
        """
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="gemini"
        )
        _plant_live_lock(tmp_letterbox_home, channel="review", label="claude")
        _plant_live_lock(tmp_letterbox_home, channel="review", label="gemini")
        with pytest.raises(mcp_server.UnknownRecipientError) as exc:
            _fn(server, "send_message")(body="x", to="Claude")
        msg = str(exc.value)
        assert "case-sensitive" in msg
        assert "'claude'" in msg  # the suggested correct casing

    def test_to_own_label_raises(self, tmp_letterbox_home: Path) -> None:
        """ADR-064 — directing at your own label notifies no one (own writes are
        never echoed back), so it is refused with a broadcast nudge.
        """
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        _plant_live_lock(tmp_letterbox_home, channel="review", label="claude")
        with pytest.raises(mcp_server.UnknownRecipientError) as exc:
            _fn(server, "send_message")(body="x", to="claude")
        assert "your own label" in str(exc.value)

    def test_broadcast_skips_liveness_validation(
        self, tmp_letterbox_home: Path
    ) -> None:
        """A broadcast (no ``to``) is for everyone, present and future, so it
        needs no live participant — it sends even on an empty channel.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        result = _fn(server, "send_message")(body="x")  # no peers live at all
        assert (ch.path / f"{result['id']}.json").is_file()

    def test_confirmation_envelope_broadcast_with_live_peer(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-065 — a broadcast with a live peer returns delivered + notified +
        a stand-down notice naming who will get the 📬.
        """
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        _plant_live_lock(tmp_letterbox_home, channel="review", label="gemini")
        result = _fn(server, "send_message")(body="x")
        assert result["delivered"] is True
        assert result["notified"] == ["gemini"]
        assert "gemini" in result["notice"]
        assert "poll" in result["notice"].lower()

    def test_confirmation_envelope_directed_notifies_only_recipient(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-065 — a directed send reports exactly the recipient as notified
        (Filter 7 suppresses the 📬 for everyone else, ADR-062).
        """
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        _plant_live_lock(tmp_letterbox_home, channel="review", label="gemini")
        _plant_live_lock(tmp_letterbox_home, channel="review", label="claude-commit")
        result = _fn(server, "send_message")(body="x", to="gemini")
        assert result["notified"] == ["gemini"]  # NOT claude-commit
        assert "gemini" in result["notice"]

    def test_confirmation_envelope_excludes_self_from_notified(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-065 — the sender never appears in ``notified``: own writes are
        never echoed back (ADR-022), so a 📬 for yourself is impossible.
        """
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        _plant_live_lock(tmp_letterbox_home, channel="review", label="claude")
        _plant_live_lock(tmp_letterbox_home, channel="review", label="gemini")
        result = _fn(server, "send_message")(body="x")
        assert result["notified"] == ["gemini"]

    def test_confirmation_envelope_empty_room_warns_no_ping(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-065 — a broadcast into an empty room still delivers to disk, but
        ``notified`` is empty and the notice warns instead of promising a reply
        (a later joiner won't be auto-notified, ADR-024).
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        result = _fn(server, "send_message")(body="x")  # no peers live at all
        assert (ch.path / f"{result['id']}.json").is_file()
        assert result["delivered"] is True
        assert result["notified"] == []
        notice = result["notice"]
        assert "no other participant is live" in notice
        assert "Don't wait" in notice

    def test_empty_to_normalizes_to_broadcast(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``to=""`` is treated as "not supplied" → broadcast (recipient None),
        never a directed message addressed to the empty-string label.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = _fn(server, "send_message")(body="x", to="")
        loaded = read_message(ch.path / f"{result['id']}.json")
        assert isinstance(loaded, Message)
        assert loaded.recipient is None

    def test_in_reply_to_passed_through(self, tmp_letterbox_home: Path) -> None:
        """ADR-020 — in_reply_to is trusted verbatim; default omitted → None."""
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        send = _fn(server, "send_message")

        r1 = send(body="reply", in_reply_to="msg-parent-stem")
        loaded1 = read_message(ch.path / f"{r1['id']}.json")
        assert isinstance(loaded1, Message)
        assert loaded1.in_reply_to == "msg-parent-stem"

        r2 = send(body="fresh")
        loaded2 = read_message(ch.path / f"{r2['id']}.json")
        assert isinstance(loaded2, Message)
        assert loaded2.in_reply_to is None

    def test_empty_body_is_allowed(self, tmp_letterbox_home: Path) -> None:
        """G8 — an empty body is valid (new_message rejects only empty sender,
        which is always server-side non-empty).
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = _fn(server, "send_message")(body="")
        loaded = read_message(ch.path / f"{result['id']}.json")
        assert isinstance(loaded, Message)
        assert loaded.body == ""

    def test_oversized_body_raises_and_writes_no_file(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K3 — a >5 MB body raises MessageTooLarge before any disk I/O; no
        `.json` and no `.json.tmp` is left behind (encode-first contract).
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        big = "a" * (MAX_BODY_BYTES + 1)
        with pytest.raises(MessageTooLarge):
            _fn(server, "send_message")(body=big)
        assert list(ch.path.glob("msg-*.json")) == []
        assert list(ch.path.glob("msg-*.json.tmp")) == []

    def test_no_execution_body_stored_verbatim(
        self, tmp_letterbox_home: Path
    ) -> None:
        """§6.4 / ADR-032 — shell metacharacters and template-like text are
        stored as inert bytes; there is structurally no exec path.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        body = "$(rm -rf /) `whoami` {sender} ${HOME} && echo pwned; |pipe|"
        result = _fn(server, "send_message")(body=body)
        loaded = read_message(ch.path / f"{result['id']}.json")
        assert isinstance(loaded, Message)
        assert loaded.body == body

    def test_call_tool_roundtrip_returns_id(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Vision §6.2 — the agent's-eye wire view. Per the scout brief,
        send_message's plain `dict` annotation makes `call_tool` return a bare
        list of TextContent (no structured-content wrap); the id is in the JSON
        text. We tolerate a tuple shape defensively.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = asyncio.run(server.call_tool("send_message", {"body": "wire"}))
        content = result[0] if isinstance(result, tuple) else result
        payload = json.loads(content[0].text)
        assert (ch.path / f"{payload['id']}.json").is_file()


# ──────────────────────────────────────────────────────────────────────
# TestCheckLatestMessage — the common-case read path (K1/K2, thin wrapper)
# ──────────────────────────────────────────────────────────────────────


class TestCheckLatestMessage:
    def test_returns_none_when_no_unread(
        self, tmp_letterbox_home: Path
    ) -> None:
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        assert _fn(server, "check_latest_message")() is None

    def test_returns_dict_for_newest_unread_peer(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stem = _write_peer_message(ch, sender="gemini", body="hi there")
        result = _fn(server, "check_latest_message")()
        assert result is not None
        assert result["id"] == stem
        assert result["body"] == "hi there"
        assert result["sender"] == "gemini"
        # Full §3.2 wire shape; metadata flattened to a nested dict (§3).
        assert set(result) == _MESSAGE_WIRE_KEYS
        assert set(result["metadata"]) == {"encryption", "ext"}
        assert result["metadata"]["encryption"] is None
        assert result["metadata"]["ext"] == {}

    def test_returns_newest_when_multiple_unread(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        stems = [
            _write_peer_message(
                ch, body=f"m{i}", timestamp=base + timedelta(microseconds=i)
            )
            for i in range(3)
        ]
        result = _fn(server, "check_latest_message")()
        assert result is not None
        assert result["id"] == stems[-1]
        assert result["body"] == "m2"

    def test_does_not_advance_read_marker(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        _write_peer_message(ch, body="hi")
        before = read_state(ch, ch.sender_label).high_water_mark
        _fn(server, "check_latest_message")()
        after = read_state(ch, ch.sender_label).high_water_mark
        assert after == before

    def test_skips_own_writes(self, tmp_letterbox_home: Path) -> None:
        """Integration — a message just written via send_message is own-write
        from this endpoint's view, so check_latest_message does not return it.
        """
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        _fn(server, "send_message")(body="my own message")
        assert _fn(server, "check_latest_message")() is None

    def test_call_tool_roundtrip_none(self, tmp_letterbox_home: Path) -> None:
        """Vision §6.2 — None wire shape. Per the scout brief, the `dict | None`
        annotation makes `call_tool` return a tuple `(unstructured, structured)`
        with `structured == {"result": None}` when there are no unread.
        """
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        _unstructured, structured = asyncio.run(
            server.call_tool("check_latest_message", {})
        )
        assert structured == {"result": None}

    def test_call_tool_roundtrip_message(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Vision §6.2 — message wire shape: structured content is wrapped
        under `"result"` (union annotation → wrap_output True, per scout).
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stem = _write_peer_message(ch, body="wire hi")
        _unstructured, structured = asyncio.run(
            server.call_tool("check_latest_message", {})
        )
        assert structured["result"]["id"] == stem
        assert structured["result"]["body"] == "wire hi"


# ──────────────────────────────────────────────────────────────────────
# TestCheckMessages — the catch-up read path (K1 merge, K3/K5 delegation)
# ──────────────────────────────────────────────────────────────────────


class TestCheckMessages:
    def _seed(self, ch: Channel, n: int, *, body_prefix: str = "m") -> list[str]:
        """Seed ``n`` ordered peer messages; return their id stems oldest-first."""
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        return [
            _write_peer_message(
                ch,
                body=f"{body_prefix}{i}",
                timestamp=base + timedelta(microseconds=i),
            )
            for i in range(n)
        ]

    def test_empty_channel_returns_empty(self, tmp_letterbox_home: Path) -> None:
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = _fn(server, "check_messages")()
        assert result == {"messages": [], "has_more": False}
        # No clamp fired → the "warning" key is omitted entirely (not null).
        assert "warning" not in result

    def test_default_limit_caps_at_20_oldest_first(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stems = self._seed(ch, 25)
        result = _fn(server, "check_messages")()
        ids = [item["id"] for item in result["messages"]]
        assert len(ids) == 20
        assert result["has_more"] is True
        # Oldest-first: the 20 oldest stems, in ascending id order.
        assert ids == stems[:20]
        assert ids == sorted(ids)
        assert "warning" not in result

    def test_exact_fit_has_more_false(self, tmp_letterbox_home: Path) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        self._seed(ch, 20)
        result = _fn(server, "check_messages")()
        assert len(result["messages"]) == 20
        assert result["has_more"] is False

    def test_limit_above_max_clamped_to_100_with_warning(
        self, tmp_letterbox_home: Path
    ) -> None:
        # The clamp fires on the requested limit regardless of message count.
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        result = _fn(server, "check_messages")(limit=200)
        assert "warning" in result
        assert "100" in result["warning"]  # names the rule (the max)
        assert "200" in result["warning"]  # names the rejected value

    def test_limit_below_min_clamped_to_1_with_warning(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        self._seed(ch, 3)
        result = _fn(server, "check_messages")(limit=0)
        assert len(result["messages"]) == 1
        assert result["has_more"] is True
        assert "warning" in result
        assert "1" in result["warning"]  # names the rule (the min)
        assert "0" in result["warning"]  # names the rejected value

    def test_in_range_limit_omits_warning(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        self._seed(ch, 1)
        result = _fn(server, "check_messages")(limit=10)
        assert "warning" not in result

    def test_since_id_filters_without_advancing_marker(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stems = self._seed(ch, 4)
        result = _fn(server, "check_messages")(since_id=stems[1])
        ids = [item["id"] for item in result["messages"]]
        # Strictly after stems[1] (ADR-012 — since_id is path.stem > bound).
        assert ids == stems[2:]
        # K3 — since_id never advances the persisted marker, and the read
        # is non-mutating: no .read/ file is created.
        assert read_state(ch, ch.sender_label).high_water_mark == ""
        assert not (ch.path / ".read").exists()

    def test_own_writes_excluded(self, tmp_letterbox_home: Path) -> None:
        # A message just written via send_message is own-write from this
        # endpoint's view (closure instance_id half of the combined filter).
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        _fn(server, "send_message")(body="mine")
        result = _fn(server, "check_messages")()
        assert result["messages"] == []
        assert result["has_more"] is False

    def test_parse_error_envelope_inlined_in_id_order(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        stem_a = _write_peer_message(ch, body="first", timestamp=base)
        # Malformed-file trap: VALID msg-*.json name, garbage content (a
        # bad NAME would be dropped by list_messages' regex and never
        # surface as a ParseError).
        bad_name = make_message_filename(base + timedelta(microseconds=1))
        bad_stem = bad_name.removesuffix(".json")
        (ch.path / bad_name).write_bytes(b"{not valid json")
        stem_c = _write_peer_message(
            ch, body="third", timestamp=base + timedelta(microseconds=2)
        )
        items = _fn(server, "check_messages")()["messages"]
        ids = [item["id"] for item in items]
        # The K1 merge re-sorts the two ascending lists into one id order,
        # so the poison message lands between the two clean ones.
        assert ids == [stem_a, bad_stem, stem_c]
        bad_item = items[1]
        assert bad_item["body"] is None
        assert "malformed_json" in bad_item["parse_error"]
        # Discriminator is the presence of "parse_error", not body is None
        # (a clean message may legitimately have body == "").
        assert "parse_error" not in items[0]
        assert "parse_error" not in items[2]

    def test_default_read_advances_marker_to_newest_returned(
        self, tmp_letterbox_home: Path
    ) -> None:
        # ADR-058 — a default (no since_id) catch-up read IS an acknowledge:
        # the persisted marker advances to the newest item returned, and the
        # .read/ file is now created (the inverse of the old read-only contract).
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stems = self._seed(ch, 3)
        _fn(server, "check_messages")()
        assert read_state(ch, ch.sender_label).high_water_mark == stems[-1]
        assert (ch.path / ".read").exists()

    def test_default_read_drains_inbox_on_repeat(
        self, tmp_letterbox_home: Path
    ) -> None:
        # The headline ADR-058 behavior: reading clears the inbox, so a second
        # call with no new arrivals returns nothing and leaves the marker put.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stems = self._seed(ch, 3)
        first = _fn(server, "check_messages")()
        assert [item["id"] for item in first["messages"]] == stems
        second = _fn(server, "check_messages")()
        assert second == {"messages": [], "has_more": False}
        # Marker unchanged by the empty second read (nothing to advance to).
        assert read_state(ch, ch.sender_label).high_water_mark == stems[-1]

    def test_marker_drives_pagination_without_since_id(
        self, tmp_letterbox_home: Path
    ) -> None:
        # ADR-058 — because each default read advances the marker, successive
        # calls page through the backlog with no since_id threading: drain by
        # calling until has_more is false.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stems = self._seed(ch, 25)
        first = _fn(server, "check_messages")()
        assert [item["id"] for item in first["messages"]] == stems[:20]
        assert first["has_more"] is True
        assert read_state(ch, ch.sender_label).high_water_mark == stems[19]
        second = _fn(server, "check_messages")()
        assert [item["id"] for item in second["messages"]] == stems[20:]
        assert second["has_more"] is False
        assert read_state(ch, ch.sender_label).high_water_mark == stems[-1]

    def test_advance_includes_parse_error_items(
        self, tmp_letterbox_home: Path
    ) -> None:
        # A parse-error envelope consumes an inbox slot and is surfaced in the
        # response, so the marker must advance past it too — otherwise a
        # malformed file would re-appear on every read forever.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        _write_peer_message(ch, body="clean", timestamp=base)
        bad_name = make_message_filename(base + timedelta(microseconds=1))
        bad_stem = bad_name.removesuffix(".json")
        (ch.path / bad_name).write_bytes(b"{not valid json")
        _fn(server, "check_messages")()
        # Marker advanced to the newest returned item (the parse error).
        assert read_state(ch, ch.sender_label).high_water_mark == bad_stem
        # And the malformed file does not re-surface on the next read.
        assert _fn(server, "check_messages")()["messages"] == []

    def test_empty_default_read_creates_no_read_state_file(
        self, tmp_letterbox_home: Path
    ) -> None:
        # No unread items → nothing to acknowledge → no marker write, so an
        # empty channel still touches no .read/ file (the advance is guarded).
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        _fn(server, "check_messages")()
        assert not (ch.path / ".read").exists()

    def test_call_tool_roundtrip_returns_messages(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Vision §6.2 — the agent's-eye wire view. A plain `dict` return
        (per the 7b finding) yields a bare list of TextContent; tolerate a
        tuple shape defensively, exactly as send_message's roundtrip test.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stem = _write_peer_message(ch, body="wire")
        result = asyncio.run(server.call_tool("check_messages", {}))
        content = result[0] if isinstance(result, tuple) else result
        payload = json.loads(content[0].text)
        assert payload["messages"][0]["id"] == stem
        assert payload["has_more"] is False


# ──────────────────────────────────────────────────────────────────────
# TestAcknowledge — the read-marker advance (K2 validate-the-write boundary)
# ──────────────────────────────────────────────────────────────────────


class TestAcknowledge:
    def test_advances_marker_and_returns_ok(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stem = _write_peer_message(ch, body="hi")
        result = _fn(server, "acknowledge")(message_id=stem)
        assert result == {"ok": True}
        assert read_state(ch, ch.sender_label).high_water_mark == stem

    def test_idempotent_same_id_twice(self, tmp_letterbox_home: Path) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stem = _write_peer_message(ch, body="hi")
        ack = _fn(server, "acknowledge")
        ack(message_id=stem)
        ack(message_id=stem)
        assert read_state(ch, ch.sender_label).high_water_mark == stem

    def test_monotonic_older_after_newer_is_noop(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        older = _write_peer_message(ch, body="old", timestamp=base)
        newer = _write_peer_message(
            ch, body="new", timestamp=base + timedelta(microseconds=1)
        )
        ack = _fn(server, "acknowledge")
        ack(message_id=newer)
        ack(message_id=older)
        # Monotonic clamp (max) — the older id does not move the marker back.
        assert read_state(ch, ch.sender_label).high_water_mark == newer

    @pytest.mark.parametrize(
        "bad_id",
        [
            "not-an-id",
            "msg-bad.json",  # ".json" makes f"{id}.json" double-suffixed
            "../escape",
            "$(whoami)",
            "",
        ],
    )
    def test_invalid_message_id_raises_and_leaves_state_untouched(
        self, tmp_letterbox_home: Path, bad_id: str
    ) -> None:
        # K2 — a malformed message_id must raise BEFORE touching state, so a
        # hallucinated id can never blank the inbox via the lexical max.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        with pytest.raises(ValueError) as excinfo:
            _fn(server, "acknowledge")(message_id=bad_id)
        # Vector error: names the rejected value (Framework P3).
        assert repr(bad_id) in str(excinfo.value)
        # No marker advance, no .read/ file created.
        assert read_state(ch, ch.sender_label).high_water_mark == ""
        assert not (ch.path / ".read").exists()

    def test_valid_format_nonexistent_id_accepted(
        self, tmp_letterbox_home: Path
    ) -> None:
        # K2 only validates wire-FORMAT; a well-formed stem with no file is
        # trusted and advances the marker (ADR-020 blind-trust on shape).
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        fake = make_message_filename(
            datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        ).removesuffix(".json")
        result = _fn(server, "acknowledge")(message_id=fake)
        assert result == {"ok": True}
        assert read_state(ch, ch.sender_label).high_water_mark == fake

    def test_does_not_delete_message_or_touch_peer_state(
        self, tmp_letterbox_home: Path
    ) -> None:
        # L6 — acknowledge is a marker advance, never a deletion; ADR-021 —
        # per-agent isolation, only this endpoint's .read/ file is written.
        ch, server = _real_channel_and_server(tmp_letterbox_home, label="claude")
        stem = _write_peer_message(ch, sender="gemini", body="hi")
        _fn(server, "acknowledge")(message_id=stem)
        assert (ch.path / f"{stem}.json").is_file()
        read_files = sorted(p.name for p in (ch.path / ".read").iterdir())
        assert read_files == ["claude.json"]

    def test_acknowledge_clears_message_from_check_messages(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Integration: advancing the marker past a message removes it from
        # this endpoint's subsequent catch-up read.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        s0 = _write_peer_message(ch, body="m0", timestamp=base)
        s1 = _write_peer_message(
            ch, body="m1", timestamp=base + timedelta(microseconds=1)
        )
        _fn(server, "acknowledge")(message_id=s0)
        ids = [item["id"] for item in _fn(server, "check_messages")()["messages"]]
        assert s0 not in ids
        assert ids == [s1]

    def test_call_tool_roundtrip_ok(self, tmp_letterbox_home: Path) -> None:
        """Vision §6.2 — {"ok": True} round-trips as a bare list of
        TextContent (plain `dict` return); tolerate a tuple defensively.
        """
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        stem = _write_peer_message(ch, body="hi")
        result = asyncio.run(
            server.call_tool("acknowledge", {"message_id": stem})
        )
        content = result[0] if isinstance(result, tuple) else result
        payload = json.loads(content[0].text)
        assert payload == {"ok": True}


# ──────────────────────────────────────────────────────────────────────
# TestListChannels — the channel-enumeration tool body (3d delegate, K1/K2)
# ──────────────────────────────────────────────────────────────────────


class TestListChannels:
    def test_empty_state_single_launch_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Only the launch channel, no messages → one entry, last_activity None
        # (3d K3 honest empty-channel semantics, surfaced verbatim).
        _ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        assert _fn(server, "list_channels")() == [
            {"name": "review", "last_activity": None}
        ]

    def test_multiple_channels_sorted_no_path_leak(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Channels surface sorted by name; each entry is EXACTLY
        # {name, last_activity} — ChannelSummary.path must never reach the agent.
        _ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        for name in ("zebra", "alpha"):
            Channel.get_or_create(name, "claude", "", state_dir=tmp_letterbox_home)
        result = _fn(server, "list_channels")()
        assert [entry["name"] for entry in result] == ["alpha", "review", "zebra"]
        for entry in result:
            assert set(entry) == {"name", "last_activity"}

    def test_last_activity_is_newest_message_iso(
        self, tmp_letterbox_home: Path
    ) -> None:
        # last_activity is the ISO-8601 UTC timestamp embedded in the newest
        # msg-*.json filename (3d K5 — from the filename, NOT mtime), derived
        # from what we seeded rather than hardcoded.
        ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        base = datetime(2026, 5, 27, 14, 30, 15, 123456, tzinfo=timezone.utc)
        _write_peer_message(ch, body="old", timestamp=base)
        newest = base + timedelta(microseconds=5)
        _write_peer_message(ch, body="new", timestamp=newest)
        entry = next(
            e for e in _fn(server, "list_channels")() if e["name"] == "review"
        )
        assert entry["last_activity"] == newest.isoformat()

    def test_auto_created_and_extra_channels_both_appear(
        self, tmp_letterbox_home: Path
    ) -> None:
        # The launch channel (auto-created by get_or_create) AND a separately
        # created, populated channel both surface.
        _ch, server = _real_channel_and_server(tmp_letterbox_home, channel="review")
        other = Channel.get_or_create(
            "other", "claude", "", state_dir=tmp_letterbox_home
        )
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        _write_peer_message(other, body="hi", timestamp=base)
        by_name = {e["name"]: e for e in _fn(server, "list_channels")()}
        assert set(by_name) == {"review", "other"}
        assert by_name["review"]["last_activity"] is None
        assert by_name["other"]["last_activity"] == base.isoformat()


# ──────────────────────────────────────────────────────────────────────
# TestChannelInfo — the situational-awareness tool body (3d delegate, K2/K4)
# ──────────────────────────────────────────────────────────────────────


class TestChannelInfo:
    def test_shape_and_identity_bridged(
        self, tmp_letterbox_home: Path
    ) -> None:
        # The bridged state-oracle shape (ADR-056): bridged True, identity from
        # the channel handle, and — with no peer message yet — peer None /
        # peer_has_spoken False / last_peer_activity None (the honest "peer has
        # never spoken" answer, more useful than v1's always-"" recipient_label).
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        assert _fn(server, "channel_info")() == {
            "bridged": True,
            "channel": "review",
            "sender": "claude",
            "unread": 0,
            "peer": None,
            "peer_has_spoken": False,
            "last_peer_activity": None,
            # No launcher ran in this test, so no pid-locks exist for the channel.
            "participants": [],
        }

    def test_participants_reflects_live_locks(
        self, tmp_letterbox_home: Path
    ) -> None:
        # channel_info surfaces who is RUNNING on the channel, read from the
        # per-channel pid-locks (state_dir/locks/<channel>/<label>.pid). Plant
        # two locks owned by this (alive) test process; both must appear, sorted.
        _ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude-review"
        )
        lock_dir = tmp_letterbox_home / "locks" / "review"
        lock_dir.mkdir(parents=True)
        (lock_dir / "claude-review.pid").write_text(f"{os.getpid()}\n")
        (lock_dir / "claude-commit.pid").write_text(f"{os.getpid()}\n")
        info = _fn(server, "channel_info")()
        assert info["participants"] == ["claude-commit", "claude-review"]

    def test_peer_observed_from_traffic(self, tmp_letterbox_home: Path) -> None:
        # ADR-056: once the peer writes, channel_info reports WHO it is (sender
        # of the latest peer message) and that it has spoken, with a timestamp.
        ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        _write_peer_message(ch, sender="gemini", body="hello")
        info = _fn(server, "channel_info")()
        assert info["peer"] == "gemini"
        assert info["peer_has_spoken"] is True
        assert info["last_peer_activity"] is not None

    def test_unread_count_reflects_peer_messages(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        stems = [
            _write_peer_message(
                ch, body=f"m{i}", timestamp=base + timedelta(microseconds=i)
            )
            for i in range(3)
        ]
        assert _fn(server, "channel_info")()["unread"] == 3
        # Acknowledging the newest advances the marker past all three.
        _fn(server, "acknowledge")(message_id=stems[-1])
        assert _fn(server, "channel_info")()["unread"] == 0

    def test_own_writes_excluded(self, tmp_letterbox_home: Path) -> None:
        # A send_message write is own-write from this endpoint's view
        # (instance_id half of the ADR-022 combined filter via the captured
        # closure instance_id) → not counted.
        _ch, server = _real_channel_and_server(tmp_letterbox_home)
        _fn(server, "send_message")(body="mine")
        assert _fn(server, "channel_info")()["unread"] == 0

    def test_parse_error_counted(self, tmp_letterbox_home: Path) -> None:
        # A malformed file with a VALID msg-*.json name (garbage content)
        # consumes an inbox slot → counts toward unread_count (3d K4). A bad
        # NAME would be dropped upstream and never count.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        (ch.path / make_message_filename(base)).write_bytes(b"{not valid json")
        assert _fn(server, "channel_info")()["unread"] == 1

    def test_true_count_not_capped_at_100(self, tmp_letterbox_home: Path) -> None:
        # 3d K4 honest-count contract — the count is the true figure, NOT
        # capped at 100 ("101 unread" is decision-relevant for the agent).
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        for i in range(101):
            _write_peer_message(
                ch, body=f"m{i}", timestamp=base + timedelta(microseconds=i)
            )
        assert _fn(server, "channel_info")()["unread"] == 101


class TestDormantServer:
    """ADR-056: a server built with ``channel=None`` connects but is bridge-less.

    The calm-surface contract: same six tools, same schemas (so a plain session
    shows "connected", not "disconnected"), but ``channel_info`` is honest about
    the absent bridge and the four messaging tools fail loud the moment they are
    actually used — so a misconfigured bridge surfaces, a deliberate plain
    session stays quiet.
    """

    def test_registers_all_six_tools_even_dormant(self) -> None:
        server = _build_server(None, None)
        tools = asyncio.run(server.list_tools())
        assert {t.name for t in tools} == _EXPECTED_TOOLS

    def test_channel_info_reports_not_bridged(self) -> None:
        info = _fn(_build_server(None, None), "channel_info")()
        assert info["bridged"] is False
        assert "detail" in info
        # Actionable, agent-facing: it names what a human must do.
        assert "letterbox" in info["detail"]

    @pytest.mark.parametrize(
        ("tool", "kwargs"),
        [
            ("send_message", {"body": "x"}),
            ("check_latest_message", {}),
            ("check_messages", {}),
            ("acknowledge", {"message_id": "msg-x"}),
        ],
    )
    def test_messaging_tools_fail_loud(
        self, tool: str, kwargs: dict[str, object]
    ) -> None:
        # Never silently dark (Vision §7.1): the four messaging tools raise a
        # clear "no active bridge" error rather than no-op or hang. acknowledge's
        # bridge guard precedes its message_id format check.
        server = _build_server(None, None)
        with pytest.raises(RuntimeError, match="no active bridge"):
            _fn(server, tool)(**kwargs)

    def test_list_channels_works_dormant(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # list_channels is filesystem enumeration, not a per-channel op — a
        # dormant agent can still discover what channels exist (no bridge gate).
        monkeypatch.setenv("LETTERBOX_HOME", str(tmp_letterbox_home))
        result = _fn(_build_server(None, None), "list_channels")()
        assert isinstance(result, list)


# ──────────────────────────────────────────────────────────────────────
# TestMcpIntegration — Vision §6.2, via the REAL in-memory mcp SDK client
# ──────────────────────────────────────────────────────────────────────


class TestMcpIntegration:
    """The six-tool server exercised through the real ``mcp`` SDK in-memory
    client (the agent's-eye JSON-RPC view), stronger than ``server.call_tool``.

    Clean SIGTERM lifecycle (Vision §6.2 "server starts/stops cleanly when the
    parent exits") is already covered by ``TestRunShutdown`` via a real
    subprocess. The tool bodies run only on tool calls, not in the idle serve
    loop, so all six bodies being live does not change shutdown behavior — that
    §6.2 category stays satisfied without duplicating the subprocess machinery.
    """

    def test_all_six_tools_roundtrip(self, tmp_letterbox_home: Path) -> None:
        # The index requires "across all six tools" — core, not stretch. Each
        # tool is driven through the SDK client; the point is the MCP protocol
        # path works for every tool, with the per-return-annotation wire shape.
        ch, server = _real_channel_and_server(
            tmp_letterbox_home, channel="review", label="claude"
        )
        peer_stem = _write_peer_message(ch, sender="gemini", body="peer hi")

        async def _go() -> None:
            async with create_connected_server_and_client_session(server) as session:
                names = {t.name for t in (await session.list_tools()).tools}
                assert names == _EXPECTED_TOOLS

                # list_channels: list[dict] → structuredContent {"result": [...]}.
                lc = await session.call_tool("list_channels", {})
                assert lc.isError is False
                assert any(
                    e["name"] == "review" for e in lc.structuredContent["result"]
                )

                # channel_info: plain dict → JSON in content[0].text. The
                # bridged state-oracle shape (ADR-056); the peer message above
                # makes peer observable as "gemini".
                ci = await session.call_tool("channel_info", {})
                assert ci.isError is False
                ci_payload = json.loads(ci.content[0].text)
                assert ci_payload["bridged"] is True
                assert ci_payload["channel"] == "review"
                assert ci_payload["sender"] == "claude"
                assert ci_payload["unread"] == 1
                assert ci_payload["peer"] == "gemini"
                assert ci_payload["peer_has_spoken"] is True
                assert ci_payload["last_peer_activity"] is not None

                # check_latest_message: dict|None → structuredContent {"result": ...}.
                clm = await session.call_tool("check_latest_message", {})
                assert clm.isError is False
                assert clm.structuredContent["result"]["id"] == peer_stem

                # check_messages: plain dict → JSON in content[0].text.
                cm = await session.call_tool("check_messages", {})
                assert cm.isError is False
                cm_payload = json.loads(cm.content[0].text)
                assert [m["id"] for m in cm_payload["messages"]] == [peer_stem]
                assert cm_payload["has_more"] is False

                # send_message: plain dict → {"id": ...}; the file lands.
                sm = await session.call_tool("send_message", {"body": "from me"})
                assert sm.isError is False
                sm_id = json.loads(sm.content[0].text)["id"]
                assert (ch.path / f"{sm_id}.json").is_file()

                # acknowledge (valid stem): plain dict → {"ok": True}.
                ack = await session.call_tool(
                    "acknowledge", {"message_id": peer_stem}
                )
                assert ack.isError is False
                assert json.loads(ack.content[0].text) == {"ok": True}

        asyncio.run(_go())

    def test_invalid_message_id_surfaces_as_error(
        self, tmp_letterbox_home: Path
    ) -> None:
        # 7c K2 — a malformed message_id raises ValueError in the body, which
        # the SDK surfaces as isError=True (NOT a raised exception), the
        # rejected value named in the error text (Framework P3 vector error).
        _ch, server = _real_channel_and_server(tmp_letterbox_home)

        async def _go() -> None:
            async with create_connected_server_and_client_session(server) as session:
                cr = await session.call_tool(
                    "acknowledge", {"message_id": "not-an-id"}
                )
                assert cr.isError is True
                text = " ".join(
                    block.text for block in cr.content if hasattr(block, "text")
                )
                assert "not-an-id" in text

        asyncio.run(_go())

    def test_missing_channel_surfaces_as_error(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Mid-run channel-dir deletion → channel_info's list_messages raises
        # FileNotFoundError, surfaced as a clean tool error (not a silent wrong
        # answer). Recovery is 10b/T4, NOT 7d — 7d only proves honest erroring.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        shutil.rmtree(ch.path)

        async def _go() -> None:
            async with create_connected_server_and_client_session(server) as session:
                cr = await session.call_tool("channel_info", {})
                assert cr.isError is True

        asyncio.run(_go())

    def test_malformed_body_seen_without_crashing(
        self, tmp_letterbox_home: Path
    ) -> None:
        # A malformed (valid-name, garbage-content) message file: check_messages
        # surfaces a parse_error envelope and channel_info counts it — both via
        # the SDK client, without the client crashing. list_channels is immune
        # (filename-only enumeration).
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        bad_stem = make_message_filename(base).removesuffix(".json")
        (ch.path / f"{bad_stem}.json").write_bytes(b"{not valid json")

        async def _go() -> None:
            async with create_connected_server_and_client_session(server) as session:
                # channel_info is a peek (non-advancing), so count the unread
                # parse-error slot BEFORE the catch-up read consumes it:
                # check_messages now advances the marker past it (ADR-058).
                ci = await session.call_tool("channel_info", {})
                assert ci.isError is False
                assert json.loads(ci.content[0].text)["unread"] == 1

                cm = await session.call_tool("check_messages", {})
                assert cm.isError is False
                items = json.loads(cm.content[0].text)["messages"]
                assert len(items) == 1
                assert items[0]["id"] == bad_stem
                assert "parse_error" in items[0]
                assert items[0]["body"] is None

        asyncio.run(_go())

    def test_concurrent_send_message_no_corruption(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Vision §6.2 — concurrent tool calls don't corrupt state. Use
        # server.call_tool (FastMCP-direct) with gather, NOT the ClientSession
        # (one session serializes over one JSON-RPC stream). Rides on 2c's
        # atomic-rename + 2b's UUID4 collision-safety; 7d's contribution is
        # proving the tool layer adds no corruption path.
        ch, server = _real_channel_and_server(tmp_letterbox_home)
        n = 15

        async def _go() -> None:
            await asyncio.gather(
                *[
                    server.call_tool("send_message", {"body": f"m{i}"})
                    for i in range(n)
                ]
            )

        asyncio.run(_go())
        files = list(ch.path.glob("msg-*.json"))
        assert len(files) == n
        assert len({p.name for p in files}) == n  # all distinct
        for path in files:
            assert is_valid_message_filename(path.name)
        # Zero partial-write residue — atomic-rename leaves no .tmp behind.
        assert list(ch.path.glob("*.tmp")) == []

"""End-to-end launcher tests — Phase 8d (the first real two-process exchange).

Every phase so far proved a *piece* in isolation: 8b/8c drove B's receive path
with a **test-written** message (``_write_peer_message`` → disk), 7d exercised the
six MCP tools over an **in-memory** SDK client, and 6d proved the ``letterbox mcp``
process-tree position against a *sleeper* stand-in. Nothing yet has driven the
whole chain end-to-end across two real endpoints over the real stdio transport.
This module is that test — it closes the launcher half of **T1** and formally
closes **W14** ("the in-memory client is NOT the real stdio transport").

Two ``@pytest.mark.asyncio`` tests, both against the bundled ``fake_harness`` and
a real ``Watcher``:

* ``test_mcp_send_over_stdio_reaches_peer_pty`` (T1 headline / closes W14) — the
  test acts as **claude-a**'s agent, driving a genuine ``letterbox mcp`` server
  over the real ``mcp.client.stdio`` transport to call ``send_message``; the
  message lands in the shared channel directory and **claude-b**'s live
  ``run_launcher`` watcher detects it, renders the notification, and injects it
  into B's PTY (B's echo file shows it exactly once).
* ``test_real_mcp_child_is_spawned_by_harness_not_pty_parent`` (Vision §9.2 line
  65) — under the full ``run_launcher`` composition with the *real*
  ``generate_mcp_config``, the spawned ``letterbox mcp`` child's parent is the
  fake_harness (the PTY child), NOT the PTY-Parent (this pytest process).

K1 (ADR-044): T1's "two letterbox processes" is realized as ONE receiver
``run_launcher`` (B) + a real MCP stdio-client send (A), not "two run_launchers".
``fake_harness`` only echoes stdin and spawns+reaps its MCP child — it never
*calls* a tool, so the triggerable send must come from the test wielding a real
stdio client. The two coordinating letterbox processes are then B's PTY-Parent
runtime and the A-side ``letterbox mcp`` server — coordinating ONLY via the
filesystem (the shared channel dir), exactly the §2.1 / CLAUDE.md architecture.

Idioms are cloned (not imported — the clone-per-file convention 5a-6d follow):
the ``reset_registry`` / ``fake_adapter`` fixtures + ``_write_harness_config`` /
``_patch_sleeper_mcp_config`` / ``_spy_setup_launcher`` / ``_session_torn_down``
helpers from ``test_launcher.py``, and the ``_read_all_available`` / ``_read_ppid``
/ ``_process_dead`` / ``_MCP_PID_RE`` topology idioms from
``test_adapters_parametrized.py`` (6d/T10).

Module pinned to the ``watcher`` xdist group (real ``watchdog.Observer`` +
inotify — same pin as ``test_launcher.py`` / ``test_watcher.py``); without it,
``-n auto`` can exhaust ``fs.inotify.max_user_instances``.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import tty
from contextlib import suppress
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

import letterbox.adapters.base as base
from letterbox import launcher
from letterbox.adapters.base import Adapter, register_adapter
from letterbox.launcher import LauncherSession, generate_instance_id, run_launcher
from tests.conftest import FakeHarness
from tests.helpers import wait_for

pytestmark = pytest.mark.xdist_group("watcher")

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# fake_harness prints this to stderr → readable on the PTY master fd (the slave
# is wired to the child's stdio by spawn_pty), so Test 2 reads the MCP child pid.
_MCP_PID_RE = re.compile(rb"spawned MCP child pid=(\d+)")

# Distinct identities: A (sender) and B (receiver) MUST differ on BOTH
# sender_label AND instance_id so B's watcher classes A's message as a peer
# (passes the combined own-write OR-filter) and fires (K4).
_A_SENDER = "claude-a"
_B_SENDER = "claude-b"


# ──────────────────────────────────────────────────────────────────────
# Cloned fixtures / helpers (clone-per-file convention, 5a-6d)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def reset_registry(monkeypatch: pytest.MonkeyPatch) -> dict[str, type[Adapter]]:
    """Replace the module-level ``_REGISTRY`` with a fresh empty dict (cloned 8a)."""
    fresh: dict[str, type[Adapter]] = {}
    monkeypatch.setattr(base, "_REGISTRY", fresh)
    return fresh


@pytest.fixture
def fake_adapter(reset_registry: dict[str, type[Adapter]]) -> type[Adapter]:
    """Register a placeholder ``fakeharness`` adapter into the reset registry.

    Its class attrs are deliberately placeholders — the launcher (ADR-041)
    overrides ``command`` / ``default_args`` / ``notification_template`` from
    config at launch, so the spawn never uses these. They exist only to satisfy
    ``register_adapter``'s non-empty validation.
    """

    @register_adapter
    class _FakeHarnessAdapter(Adapter):
        name = "fakeharness"
        command = "fakeharness-placeholder"
        default_args = ["placeholder"]
        notification_template = "placeholder {channel}"

    return _FakeHarnessAdapter


def _write_harness_config(
    config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    command: str,
    default_args: list[str],
    template: str,
    name: str = "fakeharness",
) -> None:
    """Write a minimal ``[harness.<name>]`` TOML and point LETTERBOX_CONFIG at it.

    LETTERBOX_CONFIG is the project-local config override (config.py K2), the only
    config-file hook that doesn't read the real ``~/.letterbox``. POSIX tmp paths
    carry no quotes/backslashes, so naive double-quoting is safe. (Cloned 8a.)
    """
    args_items = ", ".join(f'"{a}"' for a in default_args)
    config_path.write_text(
        f"[harness.{name}]\n"
        f'command = "{command}"\n'
        f"default_args = [{args_items}]\n"
        f'notification_template = "{template}"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("LETTERBOX_CONFIG", str(config_path))


def _patch_sleeper_mcp_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Path:
    """Make ``setup_launcher`` emit a benign sleeper MCP config (cloned 8b).

    ``setup_launcher`` always wires ``--mcp-config`` at a config whose command is
    ``letterbox`` (the console script). Test 1 cares about B's *receive* path, not
    B's own MCP-child topology (that is Test 2), so we point B's fake_harness
    child at a harmless sleeper — fake_harness spawns and reaps it without needing
    the console script resolvable. (Test 2 deliberately does NOT patch this — it
    wants the real ``letterbox mcp`` child.)
    """
    cfg = tmp_path / "mcp-sleeper.json"

    def _fake_gen(*_args: object, **_kwargs: object) -> Path:
        cfg.write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "letterbox": {
                            "command": sys.executable,
                            "args": ["-c", "import time; time.sleep(30)"],
                        }
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return cfg

    monkeypatch.setattr(launcher, "generate_mcp_config", _fake_gen)
    return cfg


def _spy_setup_launcher(monkeypatch: pytest.MonkeyPatch) -> list[LauncherSession]:
    """Capture the ``LauncherSession`` ``run_launcher`` builds internally (cloned 8c).

    ``run_launcher`` owns its session privately, but the no-orphan / config-removed
    assertions need its ``handle`` / ``watcher`` / ``mcp_config_path`` / ``state_dir``,
    and the test must ``wait_for`` the watcher to start before A's send (avoiding
    the backlog-watermark race, ADR-024).
    """
    captured: list[LauncherSession] = []
    real_setup = launcher.setup_launcher

    async def _spy(*args: object, **kwargs: object) -> LauncherSession:
        session = await real_setup(*args, **kwargs)
        captured.append(session)
        return session

    monkeypatch.setattr(launcher, "setup_launcher", _spy)
    return captured


def _session_torn_down(session: LauncherSession) -> None:
    """Assert the §2.1 clean-exit contract: no orphan process, no temp file (cloned 8c)."""
    assert session.handle.process.poll() is not None  # harness reaped
    assert not session.mcp_config_path.exists()  # temp MCP config deleted
    assert session.watcher._started is False  # watcher stopped


def _read_all_available(master_fd: int, buf: bytearray) -> bool:
    """Drain whatever bytes are currently in ``master_fd`` into ``buf`` (cloned 6d).

    Returns True if any bytes were read this call. Used inside ``wait_for``
    predicates so a poll cycle accumulates whatever data has arrived without
    blocking past the cycle.
    """
    try:
        chunk = os.read(master_fd, 4096)
    except BlockingIOError:
        return False
    except OSError:
        # Slave closed → EIO on some POSIX kernels. Treat as "no progress".
        return False
    if chunk:
        buf.extend(chunk)
        return True
    return False


def _read_ppid(pid: int) -> int:
    """Return the parent pid of ``pid`` by parsing ``/proc/{pid}/stat`` (cloned 6d).

    The ``comm`` field (field 1, parenthesized) may contain spaces or parens, so
    split on the LAST ``)``: after it come ``state ppid ...`` — ``ppid`` is index
    1. ``/proc`` is Linux-only, consistent with the project's POSIX-only stance
    (ADR-031) and Linux CI.
    """
    stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    after_comm = stat_text.rpartition(")")[2].split()
    return int(after_comm[1])


def _process_dead(pid: int) -> bool:
    """True once ``pid`` no longer exists (the death-poll idiom, cloned 6d)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


# Drives A's real `letterbox mcp` SERVER over real stdio. The `letterbox` console
# script's `mcp` route lives in `cli.py`, a stub until Phase 9a (it prints "CLI
# not yet implemented" and exits) — 9a comes AFTER 8d, so the console route is
# not wired yet. We invoke the Phase-7 `mcp_server.run` body DIRECTLY via the
# exact `run(sys.argv[1:])` shape its own docstring names as supported. This is
# still a genuine stdio MCP server subprocess over the REAL transport (real
# JSON-RPC, real send_message body, real on-disk write) — what W14 requires; only
# the process *launcher* differs from the not-yet-wired console route (9a's job).
# See IMPLEMENTATION_NOTES Phase 8d + ADR-044 sub-note. (`sys.executable` is the
# venv python where `letterbox` is editable-installed, so the import resolves.)
_MCP_SERVER_BOOT = "import sys; from letterbox.mcp_server import run; run(sys.argv[1:])"


# ══════════════════════════════════════════════════════════════════════
# Test 1 — real-stdio MCP send reaches the peer's PTY (T1 / closes W14)
# ══════════════════════════════════════════════════════════════════════


class TestMcpSendOverStdioReachesPeerPty:
    @pytest.mark.asyncio
    async def test_mcp_send_over_stdio_reaches_peer_pty(
        self,
        fake_harness: FakeHarness,
        fake_adapter: type[Adapter],
        tmp_letterbox_home: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # B is a real run_launcher (the receiver). Its fake_harness spawns a
        # benign sleeper MCP child — Test 1 is about message flow, not B's child
        # topology (that's Test 2), and the sleeper keeps it PATH-independent.
        _write_harness_config(
            tmp_path / "letterbox.toml",
            monkeypatch,
            command=sys.executable,
            default_args=[
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ],
            template="📬 {channel}",
        )
        _patch_sleeper_mcp_config(monkeypatch, tmp_path)
        captured = _spy_setup_launcher(monkeypatch)
        rendered = "📬 test-e2e".encode("utf-8")

        task = asyncio.create_task(
            run_launcher(
                "fakeharness",
                "test-e2e",
                as_label=_B_SENDER,
                cwd=tmp_path,
                teardown_timeout=_FAST_TEARDOWN,
            )
        )
        try:
            # Post-start write only (ADR-024): a message written before B's
            # watcher start watermark is backlog and is deliberately never
            # injected. Wait for the watcher to actually start before A sends.
            await wait_for(
                lambda: bool(captured) and captured[0].watcher._started,
                timeout=10.0,
            )
            session = captured[0]
            # Raw slave end so ICRNL doesn't map the injected \r → \n on the echo
            # path; assert on the rendered text the child received (Family-C).
            tty.setraw(session.handle.slave_fd)

            # ── A's send over the REAL stdio transport (K2 — the line that
            # closes W14). Spawn a genuine `letterbox mcp` server subprocess,
            # speak JSON-RPC over its stdio, invoke the real send_message body.
            # A non-ASCII body (Lithuanian + emoji) is a cheap bonus assertion
            # that UTF-8 survives the full path (Vision §13.2).
            body = "Labas, ar girdi mane? 📨 ąčęėįš"
            a_id = generate_instance_id()
            a_home = tmp_path / "a-home"  # isolate A's server's global-config read (K5)
            a_cwd = tmp_path / "a-cwd"  # clean cwd → no stray project letterbox.toml
            a_home.mkdir()
            a_cwd.mkdir()
            params = StdioServerParameters(
                command=sys.executable,
                args=[
                    "-c",
                    _MCP_SERVER_BOOT,
                    "--channel",
                    "test-e2e",
                    "--as",
                    _A_SENDER,
                    "--instance-id",
                    a_id,
                ],
                # LETTERBOX_HOME is NOT in the SDK's inherited allowlist — pass it
                # explicitly (it wins state_dir resolution unconditionally,
                # config.py:499). HOME isolates the global-config read; PATH lets
                # the spawned server find its own interpreter (K5).
                env={
                    "LETTERBOX_HOME": str(session.state_dir),
                    "HOME": str(a_home),
                    "PATH": os.environ["PATH"],
                },
                cwd=str(a_cwd),
            )
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as client:
                    await client.initialize()
                    await client.call_tool("send_message", {"body": body})

            # K5 directory-agreement proof (and UTF-8 survival): A's stdio server
            # and B's run_launcher resolved the SAME channels/test-e2e dir, so
            # A's file is visible to B. Read it back from the shared dir before
            # waiting on B — a failure here points at K5, not at the watcher.
            channel_dir = session.state_dir / "channels" / "test-e2e"
            written = list(channel_dir.glob("*.json"))
            assert len(written) == 1, f"expected one message, found {written}"
            payload = json.loads(written[0].read_text(encoding="utf-8"))
            assert payload["body"] == body  # UTF-8 round-trips end-to-end
            assert payload["sender"] == _A_SENDER

            # B's watcher sees A's file (peer write — A's sender/instance differ
            # from B's), enqueues it, the injection loop renders + injects it into
            # B's PTY. Exactly once: proves A's real MCP send drove the inject.
            await wait_for(
                lambda: fake_harness.read_echo().count(rendered) == 1,
                timeout=10.0,
            )
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        # Cancellation ran the identical teardown ladder (no orphan, no temp file,
        # watcher stopped). A's stdio server was reaped by the `async with` exit.
        _session_torn_down(captured[0])
        assert fake_harness.read_echo().count(rendered) == 1


# ══════════════════════════════════════════════════════════════════════
# Test 2 — the real MCP child is spawned by the harness, not the PTY-Parent
# (Vision §9.2 line 65)
# ══════════════════════════════════════════════════════════════════════


class TestRealMcpChildSpawnedByHarness:
    @pytest.mark.asyncio
    async def test_real_mcp_child_is_spawned_by_harness_not_pty_parent(
        self,
        fake_harness: FakeHarness,
        fake_adapter: type[Adapter],
        tmp_letterbox_home: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Real generate_mcp_config (NO sleeper patch) → fake_harness spawns the
        # genuine `letterbox mcp` console-script child. generate_mcp_config
        # resolves the command via shutil.which("letterbox") against PATH, and
        # the launcher's spawn env is {**os.environ, LETTERBOX_HOME: …}, so
        # fake_harness inherits this PATH — prepend venv/bin so it resolves (K3).
        monkeypatch.setenv(
            "PATH",
            f"{Path(sys.executable).parent}{os.pathsep}{os.environ['PATH']}",
        )
        _write_harness_config(
            tmp_path / "letterbox.toml",
            monkeypatch,
            command=sys.executable,
            default_args=[
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ],
            template="📬 {channel}",
        )
        captured = _spy_setup_launcher(monkeypatch)

        task = asyncio.create_task(
            run_launcher(
                "fakeharness",
                "topo-ch",
                as_label=_A_SENDER,
                cwd=tmp_path,
                teardown_timeout=_FAST_TEARDOWN,
            )
        )
        mcp_pid: int | None = None
        try:
            await wait_for(
                lambda: bool(captured) and captured[0].watcher._started,
                timeout=10.0,
            )
            handle = captured[0].handle
            # The pid line is on fake_harness's stderr → master fd. `topo-ch` has
            # no peer write, so the injection loop never writes the master fd —
            # the test owns it for reading (6d idiom). Force a non-blocking drain
            # each cycle, return truthy only once the full line is in buf.
            os.set_blocking(handle.master_fd, False)
            buf = bytearray()
            await wait_for(
                lambda: (
                    _read_all_available(handle.master_fd, buf)
                    or _MCP_PID_RE.search(buf)
                )
                and _MCP_PID_RE.search(buf),
                timeout=10.0,
            )
            match = _MCP_PID_RE.search(buf)
            assert match is not None
            mcp_pid = int(match.group(1))

            # The agent (fake_harness, the PTY child) spawned the MCP child — NOT
            # the PTY-Parent (this pytest process). Vision §9.2 line 65 / §5.5:
            # PTY-Parent → fake_harness → letterbox mcp. A refactor that made the
            # MCP child a direct child of the PTY-Parent fails loudly right here.
            ppid = _read_ppid(mcp_pid)
            assert ppid == handle.pid
            assert ppid != os.getpid()
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        # The whole process TREE dies: the MCP child inherits fake_harness's
        # process group, so teardown's killpg reaps it transitively. The grandchild
        # reap is asynchronous, so death-poll rather than assert synchronously.
        assert mcp_pid is not None
        await wait_for(lambda: _process_dead(mcp_pid), timeout=10.0)
        _session_torn_down(captured[0])

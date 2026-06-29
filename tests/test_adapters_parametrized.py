"""Cross-adapter matrix + T10 — Phase 6d (last sub-phase of the adapter tier).

The per-adapter files (6a/6b/6c) each prove *one* adapter end-to-end. This
file proves them **uniform**: the behaviours where a §5.3 class attr *could*
change the outcome are parameterized across all three adapters, so a drift in
any one fails with the adapter's name in the test id. It also lands the one
genuinely new test shape the trio deferred — **T10**: the MCP child is spawned
*by the agent*, sitting below it in the process tree (Vision §6.3 / §5.5), not
as a direct child of the PTY-Parent.

Five parameterized families + the standalone T10:

* Family A — argv composition law (×3): the launcher's ``--mcp-config <path>``
  flows through ``spawn``'s ``extra_args`` *after* ``default_args``, per the law
  ``[command, *default_args, *extra_args]``. Recording stub, no subprocess.
* Family B — W13 join-key / harness-invariance (×3): ``generate_mcp_config``
  emits the identical four-token argv (``mcp``/``--channel``/``--as``/
  ``--instance-id``) for every harness. The early lock for 7a's argparse — a
  drift here (``--sender``, ``--instance_id``) is a silent channel-death bug.
* Family C — CR enforcement end-to-end (×3): exactly one ``\\r`` per injection
  against the real ``fake_harness`` subprocess.
* Family D — error path (×3): a missing ``command`` raises a clean, catchable
  ``OSError``; reaching the assertion under ``filterwarnings=["error"]`` proves
  no fd leak (spawn_pty closes both fds on Popen failure).
* Family E — T10 (×1, representative adapter): the MCP child's PPID is the
  agent's pid (not pytest's), and the whole process *tree* tears down.
* Family F — registration sanity (×3): import-time ``@register_adapter`` makes
  ``get_adapter`` resolve each name to its class.

Idioms cloned (not imported — the clone-per-file convention 5a-6c follow):
``_FAST_TEARDOWN``, ``_minimal_env``, ``_dummy_handle``, ``_read_all_available``.
Only ``wait_for`` (helpers) and ``FakeHarness`` (conftest) are imported.

This is the first file that imports all three concrete adapter modules together,
so ``get_adapter`` resolves all three names here purely because the test imports
them. It therefore does NOT — and cannot — exercise the 8a launcher-import
concern (the launcher must import all three concrete adapters at startup or
``get_adapter`` raises ``KeyError``); see IMPLEMENTATION_NOTES.
"""
from __future__ import annotations

import json
import os
import re
import sys
import tty
from pathlib import Path

import pytest

from letterbox.adapters import base
from letterbox.adapters.antigravity import AntigravityAdapter
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.claude import ClaudeAdapter
from letterbox.adapters.gemini import GeminiAdapter
from letterbox.adapters.vibe import VibeAdapter
from letterbox.adapters.mcp_config import cleanup_mcp_config, generate_mcp_config
from letterbox.adapters.pty_common import PTYHandle
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# The complete adapter population — 4 adapters (Claude, Gemini, Antigravity, Vibe).
_ADAPTER_CLASSES = [ClaudeAdapter, GeminiAdapter, AntigravityAdapter, VibeAdapter]
_ADAPTER_NAMES = ["claude", "gemini", "antigravity", "vibe"]

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# fake_harness prints this to stderr → readable on the PTY master fd (slave is
# wired to stdin/stdout/stderr by spawn_pty), so T10 reads the MCP child's pid.
_MCP_PID_RE = re.compile(rb"spawned MCP child pid=(\d+)")


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _dummy_handle() -> PTYHandle:
    """A handle for the argv-composition stub, whose code path never uses fds."""
    return PTYHandle(pid=-1, master_fd=-1, slave_fd=-1, process=None)  # type: ignore[arg-type]


def _read_all_available(master_fd: int, buf: bytearray) -> bool:
    """Drain whatever bytes are currently in master_fd into ``buf``.

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


def _fake_pointed(adapter_cls: type[Adapter], fake_harness: FakeHarness) -> Adapter:
    """Return an instance of ``adapter_cls`` re-pointed at the bundled harness.

    Swaps ``command``/``default_args`` so ``spawn`` launches ``fake_harness.py``
    instead of the real (uninstalled, must-never-run) harness binary. The
    subclass is UNDECORATED, so it never hits the registry
    (no ``AdapterAlreadyRegistered``); it inherits the real ``name``, the §5.3
    template, and the ``b"\\r"`` terminator. This generalizes 6a/6b/6c's
    ``_FakeGemini`` pattern to any adapter class.
    """

    class _FakePointed(adapter_cls):  # type: ignore[valid-type,misc]
        command = sys.executable
        default_args = [
            str(fake_harness.script_path),
            "--echo-to",
            str(fake_harness.echo_file),
        ]

    return _FakePointed()


def _read_ppid(pid: int) -> int:
    """Return the parent pid of ``pid`` by parsing ``/proc/{pid}/stat``.

    The ``comm`` field (field 1, parenthesized) may contain spaces or parens,
    so split on the LAST ``)`` (G7): after it come ``state ppid ...`` —
    ``ppid`` is index 1. ``/proc`` is Linux-only, consistent with the project's
    POSIX-only stance (ADR-031) and Linux CI.
    """
    stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    after_comm = stat_text.rpartition(")")[2].split()
    return int(after_comm[1])


def _process_dead(pid: int) -> bool:
    """True once ``pid`` no longer exists (the death-poll idiom, G8)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


# ──────────────────────────────────────────────────────────────────────
# Family A — argv composition law (×3, real attrs, no subprocess)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_cls", _ADAPTER_CLASSES, ids=lambda c: c.name)
async def test_mcp_config_flows_through_extra_args(
    adapter_cls: type[Adapter],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Monkeypatch the name imported into base's OWN namespace (base.py L19):
    # spawn() resolves the bare `spawn_pty` from there, so patching
    # pty_common.spawn_pty would silently NOT intercept (G1). The stub is sync
    # (matching spawn_pty's real signature) and returns a dummy handle; the
    # inherited no-op post_spawn runs after, so any handle works. Proves the
    # real §5.3 attrs compose with the launcher's --mcp-config WITHOUT executing
    # the (uninstalled) harness binary — including Antigravity's empty
    # default_args shape (no intervening token).
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

    cfg = generate_mcp_config(adapter_cls.name, "ch", "snd", "iid")
    try:
        adapter = get_adapter(adapter_cls.name)
        await adapter.spawn(["--mcp-config", str(cfg)], tmp_path, _minimal_env())
        # The composition LAW, not three hardcoded literals (§9 testing strategy).
        assert recorded == [
            [adapter_cls.command, *adapter_cls.default_args, "--mcp-config", str(cfg)]
        ]
    finally:
        cleanup_mcp_config(cfg)


# ──────────────────────────────────────────────────────────────────────
# Family B — W13 join-key / harness-invariance (×3, by name)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", _ADAPTER_NAMES)
def test_mcp_config_join_key_is_harness_invariant(name: str) -> None:
    # The exact four-token argv 7a's `letterbox mcp` argparse must parse. A
    # drift here (`--sender`, `--instance_id`) silently breaks 7a at first
    # launch (the channel goes deaf). This is the early lock (K3); 6d's added
    # value over 5c is proving the envelope is IDENTICAL across all three
    # harness names (ADR-033 unified envelope).
    cfg = generate_mcp_config(name, "ch", "snd", "iid")
    try:
        data = json.loads(cfg.read_text(encoding="utf-8"))
        server = data["mcpServers"]["letterbox"]
        assert server["args"] == [
            "mcp",
            "--channel",
            "ch",
            "--as",
            "snd",
            "--instance-id",
            "iid",
        ]
        # Bare "letterbox" OR an abs path, depending on PATH under the venv —
        # assert the basename, never `== "letterbox"` (5c IMPLEMENTATION_NOTES).
        assert Path(server["command"]).name == "letterbox"
        assert oct(cfg.stat().st_mode & 0o777) == "0o600"
    finally:
        cleanup_mcp_config(cfg)
    # cleanup_mcp_config is idempotent and actually removes the file (the
    # removal MECHANISM the launcher (8c) owns — NOT adapter.teardown, G10).
    assert not cfg.exists()


# ──────────────────────────────────────────────────────────────────────
# Family C — CR enforcement end-to-end (×3, real PTY vs fake_harness)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_cls", _ADAPTER_CLASSES, ids=lambda c: c.name)
async def test_cr_enforced_end_to_end(
    adapter_cls: type[Adapter],
    fake_harness: FakeHarness,
    tmp_path: Path,
) -> None:
    adapter = _fake_pointed(adapter_cls, fake_harness)
    handle = await adapter.spawn([], tmp_path, _minimal_env())
    try:
        # Raw slave end so input line discipline (ICRNL) doesn't map the
        # injected \r → \n; assert on what the child received (echo file), NOT
        # master-fd readback (OPOST mangles \r on the echo-back path) — G2.
        tty.setraw(handle.slave_fd)

        await adapter.inject(handle, "test notification")
        await wait_for(
            lambda: fake_harness.read_echo().endswith(b"test notification\r")
        )
        echo = fake_harness.read_echo()
        assert echo.count(b"\r") == 1  # exactly one "submit" per injection
        assert b"\n" not in echo

        # Strengthening: a second injection adds exactly one more \r.
        await adapter.inject(handle, "second notification")
        await wait_for(lambda: fake_harness.read_echo().count(b"\r") == 2)
        echo = fake_harness.read_echo()
        assert echo.endswith(b"second notification\r")
        assert echo.count(b"\r") == 2
        assert b"\n" not in echo
    finally:
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
    # No --mcp-config → no MCP child → single-process tree; reaped on teardown.
    assert handle.process.poll() is not None


# ──────────────────────────────────────────────────────────────────────
# Family D — error path (×3): missing command → clean, catchable OSError
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_cls", _ADAPTER_CLASSES, ids=lambda c: c.name)
async def test_missing_command_raises_clean_error(
    adapter_cls: type[Adapter],
    tmp_path: Path,
) -> None:
    missing = str(tmp_path / f"does-not-exist-{adapter_cls.name}")

    class _Missing(adapter_cls):  # type: ignore[valid-type,misc]
        command = missing

    adapter = _Missing()
    # Adapter-layer errors propagate uncaught — the launcher (8a) formats them,
    # not the adapter (K5). Reaching this assertion under filterwarnings=error
    # proves spawn_pty's BaseException handler closed both fds (no fd leak).
    with pytest.raises((FileNotFoundError, OSError)) as exc_info:
        await adapter.spawn([], tmp_path, _minimal_env())
    assert "does-not-exist" in str(exc_info.value)


# ──────────────────────────────────────────────────────────────────────
# Family E — T10: MCP-child process-tree position + multi-process teardown
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_mcp_child_is_below_the_agent_and_tree_tears_down(
    fake_harness: FakeHarness,
    tmp_path: Path,
) -> None:
    # The real `letterbox mcp` is OPEN until 7a, so the child is a benign
    # long-lived stand-in spawned through the same mcpServers envelope a real
    # config uses (K2). T10's claim is process-tree POSITION, not MCP behaviour.
    sleeper_cfg = tmp_path / "mcp.json"
    sleeper_cfg.write_text(
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

    # T10 is run ONCE against a representative adapter (K1): the topology is a
    # property of fake_harness + spawn_pty's start_new_session, not of any §5.3
    # attr, so ×3 would add ~3× spawn/teardown cost for zero new coverage.
    adapter = _fake_pointed(ClaudeAdapter, fake_harness)
    handle = await adapter.spawn(
        ["--mcp-config", str(sleeper_cfg)], tmp_path, _minimal_env()
    )
    mcp_pid: int | None = None
    try:
        # The pid line is on fake_harness's stderr → master fd (G4). Force a
        # non-blocking drain each cycle but only return truthy once the full
        # line is present in buf (mirrors test_pty_common's read idiom).
        os.set_blocking(handle.master_fd, False)
        buf = bytearray()
        await wait_for(
            lambda: (_read_all_available(handle.master_fd, buf) or _MCP_PID_RE.search(buf))
            and _MCP_PID_RE.search(buf)
        )
        match = _MCP_PID_RE.search(buf)
        assert match is not None
        mcp_pid = int(match.group(1))

        ppid = _read_ppid(mcp_pid)
        # The agent (fake_harness) spawned the MCP child — NOT the PTY-Parent
        # (pytest). This is the load-bearing topology of Vision §6.3 / §5.5:
        # PTY-Parent → agent → letterbox-mcp. A future refactor that makes the
        # MCP child a direct child of the PTY-Parent fails loudly right here.
        assert ppid == handle.pid
        assert ppid != os.getpid()
    finally:
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        cleanup_mcp_config(sleeper_cfg)

    # The whole process TREE dies: the un-setsid'd sleeper inherits
    # fake_harness's process group (start_new_session=True made fake_harness the
    # group leader), so close_pty_handle's killpg reaps both. The zombie is
    # reaped asynchronously by init, so death-poll rather than assume (G8).
    assert handle.process.poll() is not None
    assert mcp_pid is not None
    await wait_for(lambda: _process_dead(mcp_pid))


# ──────────────────────────────────────────────────────────────────────
# Family F — registration sanity (×3): import-time @register_adapter resolves
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name, adapter_cls",
    list(zip(_ADAPTER_NAMES, _ADAPTER_CLASSES)),
    ids=_ADAPTER_NAMES,
)
def test_get_adapter_resolves_each_name(
    name: str, adapter_cls: type[Adapter]
) -> None:
    # Proves the matrix's precondition: importing the three adapter modules
    # fired @register_adapter once per process (import cache → no double
    # registration), so get_adapter resolves all three without a fixture (K4 —
    # NO reset_registry; reusing it would empty the registry and raise KeyError).
    adapter = get_adapter(name)
    assert isinstance(adapter, adapter_cls)
    assert adapter.name == name

"""Tests for ``letterbox.adapters.pty_common`` — Phase 5a.

Six test classes covering the PTYHandle shape, spawn_pty, inject_to_pty,
close_pty_handle, the no-shell-true lint script, and the public surface.
All sync (no asyncio fixtures); ``wait_for`` is invoked via
``asyncio.run`` where async-arrival assertions are needed.

Real PTYs, real subprocesses, real fds. The only mock-equivalent is
``_spawn_ignores_sigterm`` which uses a real subprocess that
deliberately ignores SIGTERM via ``signal.signal(SIGTERM, SIG_IGN)`` —
that's a real subprocess, not a mock.

Every ``spawn_pty`` call is wrapped in ``try/finally close_pty_handle``
per G6: ``filterwarnings=["error"]`` would promote any leaked fd's
``ResourceWarning`` to a test failure.
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import os
import signal
import subprocess
import sys
import textwrap
import time
import tty
from pathlib import Path

import pytest

from letterbox.adapters import pty_common
from letterbox.adapters.pty_common import (
    PTYHandle,
    close_pty_handle,
    inject_to_pty,
    spawn_pty,
)
from tests.conftest import FakeHarness
from tests.helpers import wait_for

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LINT_SCRIPT_NO_SHELL = _REPO_ROOT / "scripts" / "lint_no_shell_true.sh"


# ──────────────────────────────────────────────────────────────────────
# Local helpers
# ──────────────────────────────────────────────────────────────────────


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _spawn_echo(
    fake_harness: FakeHarness,
    cwd: Path,
    env: dict[str, str] | None = None,
) -> PTYHandle:
    """Spawn ``fake_harness`` over a PTY with ``--echo-to <echo_file>``."""
    return spawn_pty(
        [
            sys.executable,
            str(fake_harness.script_path),
            "--echo-to",
            str(fake_harness.echo_file),
        ],
        cwd,
        env or _minimal_env(),
    )


def _spawn_ignores_sigterm(cwd: Path) -> PTYHandle:
    """Spawn a python that installs SIG_IGN for SIGTERM, signals readiness, then sleeps.

    The "READY\\n" line on stdout closes the startup race: without it the
    test's ``close_pty_handle`` could send SIGTERM before the child has
    installed ``SIG_IGN``, and the child would die on SIGTERM instead of
    surviving until SIGKILL.
    """
    code = (
        "import signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "sys.stdout.write('READY\\n'); sys.stdout.flush(); "
        "time.sleep(60)"
    )
    return spawn_pty([sys.executable, "-c", code], cwd, _minimal_env())


def _read_all_available(master_fd: int, buf: bytearray) -> bool:
    """Drain whatever bytes are currently in master_fd into ``buf``.

    Returns True if any bytes were read this call. Used inside ``wait_for``
    predicates so a poll cycle accumulates whatever data has arrived
    without blocking past the cycle.
    """
    try:
        chunk = os.read(master_fd, 4096)
    except BlockingIOError:
        return False
    except OSError:
        # Slave closed → EIO on some POSIX kernels. Predicate treats
        # that as "no progress this poll" rather than crashing.
        return False
    if chunk:
        buf.extend(chunk)
        return True
    return False


@pytest.fixture
def reset_closed_handles(monkeypatch: pytest.MonkeyPatch) -> set[int]:
    """Replace the module-level ``_closed_handles`` set with a fresh empty one.

    Prevents accumulation across tests and gives ``test_idempotent_double_close``
    a clean starting state for its membership assertion.
    """
    fresh: set[int] = set()
    monkeypatch.setattr(pty_common, "_closed_handles", fresh)
    return fresh


# ──────────────────────────────────────────────────────────────────────
# TestPTYHandle
# ──────────────────────────────────────────────────────────────────────


class TestPTYHandle:
    def test_is_frozen_dataclass(self) -> None:
        assert dataclasses.is_dataclass(PTYHandle)
        assert PTYHandle.__dataclass_params__.frozen is True
        handle = PTYHandle(pid=1, master_fd=2, slave_fd=3, process=None)  # type: ignore[arg-type]
        with pytest.raises(dataclasses.FrozenInstanceError):
            handle.pid = 999  # type: ignore[misc]

    def test_field_names_match_vision_5_2(self) -> None:
        assert set(PTYHandle.__dataclass_fields__) == {
            "pid",
            "master_fd",
            "slave_fd",
            "process",
        }

    def test_no_default_values(self) -> None:
        # Every field is required — constructing without one raises TypeError.
        with pytest.raises(TypeError):
            PTYHandle(pid=1, master_fd=2, slave_fd=3)  # type: ignore[call-arg]


# ──────────────────────────────────────────────────────────────────────
# TestSpawnPty
# ──────────────────────────────────────────────────────────────────────


class TestSpawnPty:
    def test_spawns_argv_subprocess_and_returns_handle(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            assert handle.pid > 0
            assert handle.master_fd > 0
            assert handle.slave_fd > 0
            assert isinstance(handle.process, subprocess.Popen)
            assert handle.process.poll() is None
        finally:
            close_pty_handle(handle)

    def test_rejects_str_cmd(self, tmp_path: Path) -> None:
        with pytest.raises(TypeError, match="list"):
            spawn_pty("ls", tmp_path, _minimal_env())  # type: ignore[arg-type]

    def test_rejects_empty_cmd_list(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            spawn_pty([], tmp_path, _minimal_env())

    def test_rejects_non_str_cmd_elements(self, tmp_path: Path) -> None:
        with pytest.raises(TypeError, match="cmd\\[1\\]"):
            spawn_pty([sys.executable, 123], tmp_path, _minimal_env())  # type: ignore[list-item]

    def test_cwd_is_honored(self, tmp_path: Path) -> None:
        code = (
            "import os, sys; "
            "sys.stdout.write(os.getcwd()); "
            "sys.stdout.flush()"
        )
        handle = spawn_pty([sys.executable, "-c", code], tmp_path, _minimal_env())
        buf = bytearray()
        try:
            os.set_blocking(handle.master_fd, False)
            asyncio.run(
                wait_for(
                    lambda: (
                        _read_all_available(handle.master_fd, buf)
                        or str(tmp_path) in buf.decode("utf-8", errors="ignore")
                    )
                    and str(tmp_path) in buf.decode("utf-8", errors="ignore"),
                    timeout=5.0,
                )
            )
        finally:
            close_pty_handle(handle)
        assert str(tmp_path) in buf.decode("utf-8", errors="ignore")

    def test_env_is_honored(self, tmp_path: Path) -> None:
        code = (
            "import os, sys; "
            "sys.stdout.write(os.environ.get('LETTERBOX_TEST_KEY', 'MISSING')); "
            "sys.stdout.flush()"
        )
        env = {"PATH": os.environ["PATH"], "LETTERBOX_TEST_KEY": "value123"}
        handle = spawn_pty([sys.executable, "-c", code], tmp_path, env)
        buf = bytearray()
        try:
            os.set_blocking(handle.master_fd, False)
            asyncio.run(
                wait_for(
                    lambda: (
                        _read_all_available(handle.master_fd, buf)
                        or b"value123" in buf
                    )
                    and b"value123" in buf,
                    timeout=5.0,
                )
            )
        finally:
            close_pty_handle(handle)
        assert b"value123" in buf

    def test_env_does_not_inherit_parent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Set a key on the parent that the child must NOT see, because we
        # pass an explicit env that omits it (K6: spawn_pty does not merge
        # with os.environ).
        monkeypatch.setenv("LETTERBOX_PARENT_LEAK", "secret-do-not-leak")
        code = (
            "import os, sys; "
            "sys.stdout.write(os.environ.get('LETTERBOX_PARENT_LEAK', 'ABSENT')); "
            "sys.stdout.flush()"
        )
        handle = spawn_pty(
            [sys.executable, "-c", code], tmp_path, {"PATH": os.environ["PATH"]}
        )
        buf = bytearray()
        try:
            os.set_blocking(handle.master_fd, False)
            asyncio.run(
                wait_for(
                    lambda: (
                        _read_all_available(handle.master_fd, buf)
                        or b"ABSENT" in buf
                    )
                    and b"ABSENT" in buf,
                    timeout=5.0,
                )
            )
        finally:
            close_pty_handle(handle)
        assert b"ABSENT" in buf
        assert b"secret-do-not-leak" not in buf

    def test_spawn_failure_closes_fds_no_leak(self, tmp_path: Path) -> None:
        """Popen failure must not leak the openpty fds.

        Verified implicitly via ``filterwarnings=["error"]`` — a leaked fd
        emits ``ResourceWarning`` at process exit, which would crash the
        test suite. Reaching this point with no warning proves the
        BaseException handler closed both fds.
        """
        nonexistent = tmp_path / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            spawn_pty([str(nonexistent)], tmp_path, _minimal_env())

    def test_start_new_session_makes_child_pgid_equal_to_pid(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            assert os.getpgid(handle.pid) == handle.pid
        finally:
            close_pty_handle(handle)


# ──────────────────────────────────────────────────────────────────────
# TestInjectToPty
# ──────────────────────────────────────────────────────────────────────


class TestInjectToPty:
    def test_writes_payload_to_master_fd(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            inject_to_pty(handle.master_fd, b"hello\r")
            asyncio.run(
                wait_for(
                    lambda: b"hello" in fake_harness.read_echo(),
                    timeout=5.0,
                )
            )
        finally:
            close_pty_handle(handle)

    def test_short_write_loop_completes_large_payload(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # Cooked-mode line discipline buffers up to MAX_CANON (~4 KB) per
        # line; without ``\n`` terminators 100 KB of pure 'x' would never
        # drain to the child, and ``os.write`` would block waiting for
        # the kernel input buffer. tty.setraw drops line discipline so the
        # bytes flow unbuffered — the K4 short-write loop is what's under
        # test, not the line discipline.
        payload = b"x" * 100_000
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            tty.setraw(handle.slave_fd)
            inject_to_pty(handle.master_fd, payload)
            asyncio.run(
                wait_for(
                    lambda: len(fake_harness.read_echo()) >= len(payload),
                    timeout=10.0,
                )
            )
        finally:
            close_pty_handle(handle)
        echo = fake_harness.read_echo()
        assert len(echo) == len(payload)
        assert echo == payload

    def test_raises_oserror_on_closed_slave(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        close_pty_handle(handle)
        with pytest.raises(OSError):
            inject_to_pty(handle.master_fd, b"x")

    def test_empty_payload_is_noop(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            inject_to_pty(handle.master_fd, b"")
            # Don't wait — just confirm no exception, then close.
        finally:
            close_pty_handle(handle)
        assert fake_harness.read_echo() == b""

    def test_rejects_non_bytes_payload(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            with pytest.raises(TypeError, match="bytes"):
                inject_to_pty(handle.master_fd, "hello")  # type: ignore[arg-type]
        finally:
            close_pty_handle(handle)


# ──────────────────────────────────────────────────────────────────────
# TestClosePtyHandle
# ──────────────────────────────────────────────────────────────────────


class TestClosePtyHandle:
    def test_terminates_child_via_sigterm_happy_path(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        close_pty_handle(handle)
        assert handle.process.returncode is not None
        # Child is fully reaped — no zombie. waitpid(WNOHANG) on a reaped
        # pid raises ChildProcessError.
        with pytest.raises(ChildProcessError):
            os.waitpid(handle.pid, os.WNOHANG)

    def test_closes_master_and_slave_fds(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        close_pty_handle(handle)
        with pytest.raises(OSError):
            os.fstat(handle.master_fd)
        with pytest.raises(OSError):
            os.fstat(handle.slave_fd)

    def test_sigkill_fallback_on_hung_child(self, tmp_path: Path) -> None:
        handle = _spawn_ignores_sigterm(tmp_path)
        try:
            # Wait for the child to print READY — proves SIG_IGN is installed
            # before we send the SIGTERM (otherwise close_pty_handle would
            # win the race and kill the child via the default SIGTERM action).
            os.set_blocking(handle.master_fd, False)
            buf = bytearray()
            asyncio.run(
                wait_for(
                    lambda: (
                        _read_all_available(handle.master_fd, buf) or b"READY" in buf
                    )
                    and b"READY" in buf,
                    timeout=5.0,
                )
            )
            start = time.monotonic()
            close_pty_handle(handle, timeout=1.0)
            elapsed = time.monotonic() - start
            assert handle.process.returncode == -signal.SIGKILL
            # 1.0s SIGTERM wait + ≤1.0s SIGKILL reap + scheduling jitter.
            assert 1.0 <= elapsed <= 3.0, f"elapsed={elapsed!r}"
        finally:
            # Idempotent — second call is a no-op if close already ran above.
            with contextlib.suppress(Exception):
                close_pty_handle(handle)

    def test_idempotent_double_close(
        self,
        fake_harness: FakeHarness,
        tmp_path: Path,
        reset_closed_handles: set[int],
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        assert handle.pid not in reset_closed_handles
        close_pty_handle(handle)
        assert handle.pid in reset_closed_handles
        # Second call: short-circuits via the idempotence set, no signals
        # sent, no exception raised.
        close_pty_handle(handle)

    def test_handles_already_exited_child(self, tmp_path: Path) -> None:
        handle = spawn_pty(
            [sys.executable, "-c", "import sys; sys.exit(0)"],
            tmp_path,
            _minimal_env(),
        )
        # Wait for natural exit before tearing down.
        asyncio.run(
            wait_for(
                lambda: handle.process.poll() is not None,
                timeout=5.0,
            )
        )
        # Must NOT raise on killpg of a non-existent process (G4).
        close_pty_handle(handle)

    def test_signals_process_group_not_just_pid(self, tmp_path: Path) -> None:
        code = textwrap.dedent(
            """\
            import sys, time, subprocess
            child = subprocess.Popen(
                [sys.executable, '-c', 'import time; time.sleep(60)']
            )
            sys.stdout.write(str(child.pid) + '\\n')
            sys.stdout.flush()
            time.sleep(60)
            """
        )
        handle = spawn_pty([sys.executable, "-c", code], tmp_path, _minimal_env())
        buf = bytearray()
        try:
            os.set_blocking(handle.master_fd, False)
            asyncio.run(
                wait_for(
                    lambda: (
                        _read_all_available(handle.master_fd, buf) or b"\n" in buf
                    )
                    and b"\n" in buf,
                    timeout=5.0,
                )
            )
            grandchild_pid = int(buf.decode("utf-8").splitlines()[0].strip())
        finally:
            close_pty_handle(handle)

        # The grandchild inherits the parent's process group (Popen doesn't
        # setsid by default), so killpg(handle.pid, SIGTERM) reaches it too.
        # After teardown it should be gone; init reaps the zombie promptly,
        # so wait_for instead of an immediate assertion.
        def grandchild_dead() -> bool:
            try:
                os.kill(grandchild_pid, 0)
            except ProcessLookupError:
                return True
            return False

        asyncio.run(wait_for(grandchild_dead, timeout=5.0))

    def test_inflight_writes_before_close_are_safe(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        handle = _spawn_echo(fake_harness, tmp_path)
        try:
            inject_to_pty(handle.master_fd, b"x")
            # IMMEDIATELY close — no wait_for. Must NOT crash.
        finally:
            close_pty_handle(handle)

    def test_default_timeout_constant_is_five_seconds(self) -> None:
        assert pty_common._DEFAULT_TEARDOWN_TIMEOUT_SECONDS == 5.0


# ──────────────────────────────────────────────────────────────────────
# TestLintNoShellTrue
# ──────────────────────────────────────────────────────────────────────


class TestLintNoShellTrue:
    def test_lint_passes_on_clean_letterbox_tree(self) -> None:
        result = subprocess.run(
            [str(_LINT_SCRIPT_NO_SHELL)],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=15.0,
        )
        assert result.returncode == 0, (
            f"lint failed unexpectedly: stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )

    def test_lint_fails_on_planted_violation(self, tmp_path: Path) -> None:
        bad_dir = tmp_path / "planted"
        bad_dir.mkdir()
        bad_file = bad_dir / "bad.py"
        bad_file.write_text(
            textwrap.dedent(
                """\
                import subprocess
                subprocess.Popen(["x"], shell=True)
                """
            ),
            encoding="utf-8",
        )
        result = subprocess.run(
            [str(_LINT_SCRIPT_NO_SHELL), str(bad_dir)],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=15.0,
        )
        assert result.returncode == 1, (
            f"expected exit 1, got {result.returncode}; "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        assert str(bad_file) in result.stderr
        assert ":2:" in result.stderr

    def test_lint_ignores_tests_directory(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "uses_shell.py").write_text(
            textwrap.dedent(
                """\
                import subprocess
                subprocess.Popen("echo x", shell=True)
                """
            ),
            encoding="utf-8",
        )
        result = subprocess.run(
            [str(_LINT_SCRIPT_NO_SHELL), str(tmp_path)],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=15.0,
        )
        assert result.returncode == 0, (
            f"tests/ exclusion failed: stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )

    def test_lint_catches_whitespace_variants(self, tmp_path: Path) -> None:
        for index, snippet in enumerate(
            ["shell=True", "shell = True", "shell= True", "shell =True"]
        ):
            variant_dir = tmp_path / f"variant_{index}"
            variant_dir.mkdir()
            (variant_dir / "bad.py").write_text(
                textwrap.dedent(
                    f"""\
                    import subprocess
                    subprocess.Popen(["x"], {snippet})
                    """
                ),
                encoding="utf-8",
            )
            result = subprocess.run(
                [str(_LINT_SCRIPT_NO_SHELL), str(variant_dir)],
                cwd=_REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=15.0,
            )
            assert result.returncode == 1, (
                f"variant {snippet!r} not caught: "
                f"stdout={result.stdout!r} stderr={result.stderr!r}"
            )

    def test_lint_missing_target_dir_exits_2(self, tmp_path: Path) -> None:
        missing = tmp_path / "does-not-exist"
        result = subprocess.run(
            [str(_LINT_SCRIPT_NO_SHELL), str(missing)],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=15.0,
        )
        assert result.returncode == 2, (
            f"expected exit 2, got {result.returncode}; "
            f"stderr={result.stderr!r}"
        )


# ──────────────────────────────────────────────────────────────────────
# TestPublicSurface
# ──────────────────────────────────────────────────────────────────────


class TestPublicSurface:
    def test_public_exports(self) -> None:
        assert set(pty_common.__all__) == {
            "PTYHandle",
            "close_pty_handle",
            "inject_to_pty",
            "spawn_pty",
        }

    def test_private_state_not_exported(self) -> None:
        # The private constants/state exist at module level but are NOT in __all__.
        assert hasattr(pty_common, "_DEFAULT_TEARDOWN_TIMEOUT_SECONDS")
        assert hasattr(pty_common, "_SIGKILL_REAP_TIMEOUT_SECONDS")
        assert hasattr(pty_common, "_closed_handles")
        assert hasattr(pty_common, "_LOGGER")
        for name in (
            "_DEFAULT_TEARDOWN_TIMEOUT_SECONDS",
            "_SIGKILL_REAP_TIMEOUT_SECONDS",
            "_closed_handles",
            "_LOGGER",
        ):
            assert name not in pty_common.__all__

"""Shared PTY spawn/inject helpers (forkpty, master/slave fd, teardown).

Tier: 2
May import from: stdlib only (``pty``, ``os``, ``subprocess``, ``signal``, ``fcntl``,
    ``struct``, ``termios``, ``tty``, ``contextlib``).
Must NOT import from: any other ``letterbox.*`` module — pty_common is a pure-stdlib
    leaf within the adapters package.

Filled in: Phase 5a per PHASE_INDEX; terminal primitives (``get_winsize`` /
    ``set_winsize`` / ``raw_mode``) added in remediation r1 (interactive bridge).
"""
from __future__ import annotations

import fcntl
import logging
import os
import signal
import struct
import subprocess
import termios
import tty
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "PTYHandle",
    "close_pty_handle",
    "get_winsize",
    "inject_to_pty",
    "raw_mode",
    "set_winsize",
    "spawn_pty",
]

# ── Module-private constants ──────────────────────────────────────────
_DEFAULT_TEARDOWN_TIMEOUT_SECONDS: float = 5.0
_SIGKILL_REAP_TIMEOUT_SECONDS: float = 1.0

# ── Module-private state ──────────────────────────────────────────────
# Pids whose ``close_pty_handle`` has run. Mirrors 2c's ``_WARNED_BAD_NAMES``
# precedent (module-level set, GIL-atomic add/contains, no lock, cleared
# only on process restart). Pid recycling is harmless: a recycled pid that
# has not gone through ``spawn_pty`` won't be in the set.
_closed_handles: set[int] = set()

_LOGGER = logging.getLogger("letterbox.adapters.pty_common")


@dataclass(frozen=True)
class PTYHandle:
    """Record of a spawned PTY-attached subprocess (Vision §5.2).

    Attributes:
        pid: OS process id of the spawned child.
        master_fd: File descriptor held by this process; writes to it are
            received by the child as stdin and reads from it return the
            child's stdout/stderr bytes.
        slave_fd: File descriptor connected to the child's stdin/stdout/
            stderr (all three share the same kernel pty slave). The parent
            keeps a handle so ``close_pty_handle`` can close it at teardown.
        process: ``subprocess.Popen`` wrapping the child; exposes ``poll``,
            ``wait``, ``returncode``, etc.
    """

    pid: int
    master_fd: int
    slave_fd: int
    process: subprocess.Popen


def spawn_pty(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    *,
    start_new_session: bool = True,
) -> PTYHandle:
    """Spawn ``cmd`` (argv list, no shell) attached to a fresh PTY pair.

    Returns a :class:`PTYHandle` whose ``master_fd`` is held by this
    process for :func:`inject_to_pty`. The ``slave_fd`` is the child's
    stdin/stdout/stderr (all three share the same kernel pty slave).

    The caller passes a fully-specified ``env`` — this function does not
    merge with ``os.environ``. See K6: the launcher (Phase 8a) decides
    which keys to forward to the harness.

    Args:
        cmd: Argv list. NEVER a shell string — a peer message body cannot
            become an argv list without an explicit deliberate call site
            that splits it; this signature is the structural enforcement
            of Vision §6.4 (no execution path). Empty list is rejected.
        cwd: Working directory for the child process.
        env: Fully-specified environment dict for the child. NOT merged
            with ``os.environ``.
        start_new_session: When True (default and production-path value),
            the child is placed in its own session/process group so
            :func:`close_pty_handle` can ``os.killpg`` the whole tree
            without signaling letterbox itself. Tests may pass ``False``
            for inspection scenarios.

    Returns:
        :class:`PTYHandle` with non-zero ``pid``/``master_fd``/``slave_fd``
        and a live ``Popen``.

    Raises:
        TypeError: If ``cmd`` is not a list of strings.
        ValueError: If ``cmd`` is empty.
        OSError: If ``os.openpty`` or the underlying ``Popen`` fails
            (e.g. command not found, fd exhaustion).
    """
    if not isinstance(cmd, list):
        raise TypeError(
            f"spawn_pty: cmd must be list[str], got {type(cmd).__name__}"
        )
    if not cmd:
        raise ValueError("spawn_pty: cmd must be a non-empty list")
    for index, element in enumerate(cmd):
        if not isinstance(element, str):
            raise TypeError(
                f"spawn_pty: cmd[{index}] must be str, "
                f"got {type(element).__name__}"
            )

    master_fd, slave_fd = os.openpty()
    try:
        process = subprocess.Popen(
            cmd,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            cwd=cwd,
            env=env,
            start_new_session=start_new_session,
            close_fds=True,
        )
    except BaseException:
        # If Popen fails (command not found, etc.) the kernel fds we opened
        # would leak; close them before re-raising.
        with suppress(OSError):
            os.close(master_fd)
        with suppress(OSError):
            os.close(slave_fd)
        raise

    return PTYHandle(
        pid=process.pid,
        master_fd=master_fd,
        slave_fd=slave_fd,
        process=process,
    )


def inject_to_pty(master_fd: int, payload: bytes) -> None:
    """Write ``payload`` to the PTY master fd; loops on short writes.

    Fire-and-forget per Vision §5.5: no ``os.tcgetpgrp`` check, no
    coordination with the harness's foreground process. The three v1
    target harnesses (Claude Code, Gemini CLI, agy) all queue PTY input
    during tool calls (empirically verified, ADR-023).

    Args:
        master_fd: The PTY master file descriptor returned by
            :func:`spawn_pty` (``handle.master_fd``).
        payload: Raw bytes to write. Empty payload is a no-op. NOT a str
            — the policy layer (5b's adapter base) is responsible for
            appending the trailing ``\\r`` (ADR-018) before calling here.

    Raises:
        TypeError: If ``payload`` is not bytes.
        OSError: When the slave end has closed (typically ``EIO`` on
            POSIX) or the master fd is invalid (``EBADF``). Per Vision
            §12 ("silent injection failures must surface, not swallow"),
            this layer does not catch — the caller (launcher injection
            loop, 8b) wraps in its own error-surfacing path.
    """
    if not isinstance(payload, bytes):
        raise TypeError(
            f"inject_to_pty: payload must be bytes, got {type(payload).__name__}"
        )
    if not payload:
        return
    written = 0
    while written < len(payload):
        written += os.write(master_fd, payload[written:])


def close_pty_handle(
    handle: PTYHandle,
    timeout: float = _DEFAULT_TEARDOWN_TIMEOUT_SECONDS,
) -> None:
    """Graceful-then-forceful teardown of the spawned process tree.

    Sends ``SIGTERM`` to the process group (``start_new_session=True``
    at spawn means ``handle.pid`` is its own pgid, so ``killpg`` reaches
    every descendant). Waits up to ``timeout`` seconds; escalates to
    ``SIGKILL`` on hang. Closes both ``master_fd`` and ``slave_fd``.

    Idempotent: a second call on the same handle is a no-op. Module-level
    ``_closed_handles`` set defends against double-teardown from a
    launcher's signal handler racing with its ``finally`` block.

    Args:
        handle: The handle returned by :func:`spawn_pty`.
        timeout: Seconds to wait for the SIGTERM to take effect before
            escalating to SIGKILL. Default 5.0.
    """
    if handle.pid in _closed_handles:
        return
    _closed_handles.add(handle.pid)

    with suppress(ProcessLookupError):
        os.killpg(handle.pid, signal.SIGTERM)

    try:
        handle.process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _LOGGER.info(
            "close_pty_handle: SIGKILL escalation for pid=%d after %.1fs",
            handle.pid,
            timeout,
        )
        with suppress(ProcessLookupError):
            os.killpg(handle.pid, signal.SIGKILL)
        with suppress(subprocess.TimeoutExpired):
            handle.process.wait(timeout=_SIGKILL_REAP_TIMEOUT_SECONDS)

    with suppress(OSError):
        os.close(handle.master_fd)
    with suppress(OSError):
        os.close(handle.slave_fd)


# ── Terminal primitives (interactive bridge — remediation r1) ─────────
# Small, pure, unit-testable helpers used by the launcher's terminal bridge to
# size the harness PTY and to put the controlling tty in raw mode. They live here
# (the PTY home) rather than in ``base.py`` (the per-adapter ABC) because the relay
# is harness-agnostic. pty_common stays a stdlib-only leaf.


def get_winsize(fd: int) -> tuple[int, int]:
    """Return the ``(rows, cols)`` window size of the terminal at ``fd``.

    Reads the size via the ``TIOCGWINSZ`` ioctl. Used by the bridge to learn the
    controlling terminal's dimensions so it can mirror them onto the harness PTY.

    Args:
        fd: An open file descriptor referring to a tty.

    Returns:
        A ``(rows, cols)`` tuple. The ioctl also reports pixel dimensions; they
        are read but discarded (terminals report them as zero in practice).

    Raises:
        OSError: If the ioctl fails (typically because ``fd`` is not a tty).
    """
    packed = fcntl.ioctl(fd, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0))
    rows, cols, _xpixels, _ypixels = struct.unpack("HHHH", packed)
    return rows, cols


def set_winsize(master_fd: int, rows: int, cols: int) -> None:
    """Set the window size of the PTY behind ``master_fd`` to ``rows`` x ``cols``.

    Writes the size via the ``TIOCSWINSZ`` ioctl on the master fd; the kernel
    delivers ``SIGWINCH`` to the foreground process group of the slave so the
    spawned harness re-renders its TUI at the new dimensions.

    Args:
        master_fd: The PTY master file descriptor whose size to set.
        rows: Number of character rows.
        cols: Number of character columns.

    Returns:
        None.

    Raises:
        OSError: If the ``TIOCSWINSZ`` ioctl fails.
    """
    fcntl.ioctl(
        master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0)
    )


@contextmanager
def raw_mode(fd: int) -> "Iterator[None]":
    """Put ``fd`` into raw mode for the duration of the context, then restore.

    Saves the terminal attributes with ``termios.tcgetattr`` on entry, switches
    the fd to raw mode with ``tty.setraw`` (so keystrokes, control characters,
    and escape sequences pass through byte-faithfully — ``ISIG``/``ICANON``/
    ``ECHO`` are disabled), and **unconditionally** restores the saved attributes
    with ``termios.tcsetattr(fd, TCSADRAIN, saved)`` in a ``finally``. An
    exception raised inside the ``with`` block still cooks the terminal back.
    ``TCSADRAIN`` drains queued output before applying the change so in-flight
    bytes are not clipped.

    Args:
        fd: An open file descriptor referring to a tty.

    Yields:
        None. The fd is in raw mode for the body of the ``with`` block.

    Raises:
        termios.error: If ``fd`` is not a tty (propagated from ``tcgetattr``).
    """
    saved = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, saved)

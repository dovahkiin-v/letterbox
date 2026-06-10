"""Per-channel pid-lock primitives — duplicate-instance guard + live-participant discovery.

Tier: 1
May import from: stdlib only.
Must NOT import from: any other ``letterbox`` module (leaf — both ``launcher`` (Tier 4)
    and ``mcp_server`` (Tier 4) depend on this, and Tier-4 siblings may not import each
    other (bulkhead §13.5), so the shared lock convention must live below them).

A pid lock is a file at ``state_dir/locks/<channel>/<sender_label>.pid`` containing the
owning launcher process's pid. It serves two jobs:

* **Duplicate-instance guard** (``claim_pid_lock``): refuse a second launch of the same
  sender label *on the same channel*, so every endpoint on a channel is distinctly
  attributable. The guard is per-channel by design — ``claude`` on ``demo`` and ``claude``
  on ``review`` are different conversations and must not false-collide.
* **Live-participant discovery** (``list_live_participants``): enumerate who is currently
  running on a channel, by reading the lock dir and liveness-probing each pid. This is the
  "who is in the room" signal ``channel_info`` surfaces — including a participant who has
  launched but not yet spoken (invisible to message-derived peer detection).

Liveness is a best-effort ``os.kill(pid, 0)`` probe (trust model §13.3): a recycled pid
could in principle make a stale lock read as alive, the same property the guard has always
had. This is identity-by-convention, not an enforcement boundary.
"""
from __future__ import annotations

import contextlib
import os
from pathlib import Path

__all__ = [
    "AlreadyRunningError",
    "claim_pid_lock",
    "list_live_participants",
    "pid_lock_dir",
    "pid_lock_path",
    "release_pid_lock",
]


class AlreadyRunningError(Exception):
    """Raised when an instance with the same sender label is already running on a channel."""


def pid_lock_dir(state_dir: Path, channel: str) -> Path:
    """Return the per-channel lock directory ``state_dir/locks/<channel>/``."""
    return state_dir / "locks" / channel


def pid_lock_path(state_dir: Path, channel: str, sender_label: str) -> Path:
    """Return the canonical lock path for a (channel, sender_label) pair."""
    return pid_lock_dir(state_dir, channel) / f"{sender_label}.pid"


def _pid_is_alive(pid: int) -> bool:
    """Return ``True`` iff signalling *pid* with signal 0 succeeds (or is EPERM).

    ``os.kill(pid, 0)`` sends no signal but performs the existence + permission
    check. ``ProcessLookupError`` means the process is gone (dead/stale lock).
    ``PermissionError`` (EPERM) means the process exists under a different owner —
    still alive for our purposes.

    Args:
        pid: The process id to probe.

    Returns:
        ``True`` if the process exists, ``False`` if it is gone.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def claim_pid_lock(
    state_dir: Path,
    channel: str,
    sender_label: str,
    harness_name: str,
) -> Path:
    """Claim a per-channel pid lock for *sender_label*, or raise if one is alive.

    Creates ``state_dir/locks/<channel>/`` on first use. A stale lock (dead pid,
    or unreadable contents) is overwritten silently. The returned path must be
    released via :func:`release_pid_lock` when the session ends.

    Args:
        state_dir: The resolved letterbox state directory.
        channel: The channel being launched on (scopes the lock).
        sender_label: The resolved identity label being claimed.
        harness_name: Used only to build the suggested-alias error hint.

    Returns:
        The path of the lock file (now containing this process's pid).

    Raises:
        AlreadyRunningError: If a process holding the lock is still alive.
    """
    lock_path = pid_lock_path(state_dir, channel, sender_label)
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    if lock_path.exists():
        raw = lock_path.read_text().strip()
        try:
            existing_pid = int(raw)
        except ValueError:
            pass  # unreadable contents — treat as stale, overwrite below
        else:
            if _pid_is_alive(existing_pid):
                raise AlreadyRunningError(
                    f"{sender_label!r} is already running on channel {channel!r} "
                    f"(pid {existing_pid}).\n"
                    f"Use a different name to run a second instance, e.g.:\n"
                    f"  letterbox {harness_name} --channel {channel} --as {sender_label}-2"
                )

    lock_path.write_text(f"{os.getpid()}\n")
    return lock_path


def release_pid_lock(lock_path: Path) -> None:
    """Remove *lock_path* silently; ignores a missing file (idempotent teardown).

    Args:
        lock_path: The path returned by :func:`claim_pid_lock`.
    """
    with contextlib.suppress(FileNotFoundError):
        lock_path.unlink()


def list_live_participants(state_dir: Path, channel: str) -> list[str]:
    """Return the sorted sender labels currently running on *channel*.

    Scans ``state_dir/locks/<channel>/*.pid``, liveness-probes each owning pid,
    and returns the labels whose process is still alive. Stale locks (dead pids)
    are skipped, not pruned — a relaunch overwrites them, and skipping avoids a
    read-path mutation/race. Missing lock dir → empty list (no one has launched).

    Args:
        state_dir: The resolved letterbox state directory.
        channel: The channel to enumerate participants for.

    Returns:
        Sorted list of live sender labels (may be empty).
    """
    lock_dir = pid_lock_dir(state_dir, channel)
    if not lock_dir.is_dir():
        return []

    live: list[str] = []
    for entry in lock_dir.glob("*.pid"):
        try:
            pid = int(entry.read_text().strip())
        except (ValueError, FileNotFoundError):
            continue  # unreadable / vanished mid-scan — skip
        if _pid_is_alive(pid):
            live.append(entry.stem)
    return sorted(live)

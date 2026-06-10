"""Tests for ``letterbox.locks`` — the per-channel pid-lock guard + participant discovery.

Two behaviours under test, both load-bearing for the multi-participant channel model:

* **Duplicate-instance guard** (``claim_pid_lock``): a second launch of the same
  sender label *on the same channel* is refused while the first is alive; the same
  label on a *different* channel is allowed (different conversation, no collision);
  a stale lock (dead pid / unreadable contents) is silently overwritten.
* **Live-participant discovery** (``list_live_participants``): reports the labels
  whose owning process is currently alive, skips the dead, and is channel-scoped.

Dead pids are produced deterministically by spawning a trivial subprocess and
reaping it — its pid is then gone (pid reuse inside a single test run is
negligible). The current test process (``os.getpid()``) stands in for a live
owner. No mocking of ``os.kill`` — the real liveness probe is exercised.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from letterbox.locks import (
    AlreadyRunningError,
    claim_pid_lock,
    list_live_participants,
    pid_lock_path,
    release_pid_lock,
)


def _dead_pid() -> int:
    """Return a pid that is guaranteed gone — a reaped trivial subprocess."""
    proc = subprocess.Popen([sys.executable, "-c", ""])
    proc.wait()
    return proc.pid


class TestClaimPidLock:
    def test_claim_writes_our_pid_at_per_channel_path(self, tmp_path: Path) -> None:
        lock = claim_pid_lock(tmp_path, "demo", "claude", "claude")
        assert lock == pid_lock_path(tmp_path, "demo", "claude")
        assert lock.parent == tmp_path / "locks" / "demo"
        assert lock.read_text().strip() == str(os.getpid())

    def test_second_claim_same_channel_and_label_is_refused(
        self, tmp_path: Path
    ) -> None:
        claim_pid_lock(tmp_path, "demo", "claude", "claude")
        with pytest.raises(AlreadyRunningError) as exc:
            claim_pid_lock(tmp_path, "demo", "claude", "claude")
        # The error must hand the human a ready-to-run distinct-name command.
        assert "already running on channel 'demo'" in str(exc.value)
        assert "--as claude-2" in str(exc.value)

    def test_same_label_different_channel_is_allowed(self, tmp_path: Path) -> None:
        # The guard is per-channel: claude on demo and claude on review are
        # different conversations and must not false-collide.
        claim_pid_lock(tmp_path, "demo", "claude", "claude")
        lock2 = claim_pid_lock(tmp_path, "review", "claude", "claude")
        assert lock2.exists()
        assert lock2.parent == tmp_path / "locks" / "review"

    def test_stale_lock_dead_pid_is_overwritten(self, tmp_path: Path) -> None:
        lock = pid_lock_path(tmp_path, "demo", "claude")
        lock.parent.mkdir(mode=0o700, parents=True)
        lock.write_text(f"{_dead_pid()}\n")
        # No raise — the dead owner means the lock is stale and reclaimable.
        reclaimed = claim_pid_lock(tmp_path, "demo", "claude", "claude")
        assert reclaimed.read_text().strip() == str(os.getpid())

    def test_unreadable_lock_contents_are_overwritten(self, tmp_path: Path) -> None:
        lock = pid_lock_path(tmp_path, "demo", "claude")
        lock.parent.mkdir(mode=0o700, parents=True)
        lock.write_text("not-a-pid\n")
        reclaimed = claim_pid_lock(tmp_path, "demo", "claude", "claude")
        assert reclaimed.read_text().strip() == str(os.getpid())

    def test_release_after_claim_allows_reclaim(self, tmp_path: Path) -> None:
        lock = claim_pid_lock(tmp_path, "demo", "claude", "claude")
        release_pid_lock(lock)
        assert not lock.exists()
        # A fresh claim succeeds now that the lock is released.
        claim_pid_lock(tmp_path, "demo", "claude", "claude")


class TestReleasePidLock:
    def test_release_missing_file_is_silent(self, tmp_path: Path) -> None:
        # Idempotent teardown — releasing a never-claimed / already-released lock
        # must not raise (run_launcher's finally calls it unconditionally).
        release_pid_lock(tmp_path / "locks" / "demo" / "ghost.pid")


class TestListLiveParticipants:
    def test_no_lock_dir_returns_empty(self, tmp_path: Path) -> None:
        assert list_live_participants(tmp_path, "demo") == []

    def test_lists_live_labels_sorted(self, tmp_path: Path) -> None:
        claim_pid_lock(tmp_path, "demo", "claude-review", "claude")
        claim_pid_lock(tmp_path, "demo", "claude-commit", "claude")
        # Both owned by this (alive) test process.
        assert list_live_participants(tmp_path, "demo") == [
            "claude-commit",
            "claude-review",
        ]

    def test_dead_owner_is_skipped(self, tmp_path: Path) -> None:
        claim_pid_lock(tmp_path, "demo", "claude-review", "claude")
        # Plant a lock owned by a reaped process — must not be reported.
        dead = pid_lock_path(tmp_path, "demo", "claude-zombie")
        dead.write_text(f"{_dead_pid()}\n")
        assert list_live_participants(tmp_path, "demo") == ["claude-review"]

    def test_is_channel_scoped(self, tmp_path: Path) -> None:
        claim_pid_lock(tmp_path, "demo", "claude", "claude")
        claim_pid_lock(tmp_path, "review", "gemini", "gemini")
        assert list_live_participants(tmp_path, "demo") == ["claude"]
        assert list_live_participants(tmp_path, "review") == ["gemini"]

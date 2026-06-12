"""Tests for the once-a-day 'new version available' check (letterbox/_update.py).

Covers the pure helpers (version parsing/ordering), the opt-out and fail-silent
contracts, the 24h cache behaviour, and the CLI wiring that surfaces the notice
on human commands while skipping the ``mcp`` stdio server. The suite is offline
by default (the autouse ``_hermetic_update_check`` fixture in ``conftest.py``);
these tests re-enable the check explicitly and stub the network seam.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from letterbox import _update, cli


@pytest.fixture
def enable_update_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """Undo the autouse opt-out so the check actually runs."""
    monkeypatch.delenv("LETTERBOX_NO_UPDATE_CHECK", raising=False)


# ──────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────


def test_version_tuple_ordering() -> None:
    vt = _update._version_tuple
    assert vt("1.0.1") > vt("1.0.0")
    assert vt("1.1.0") > vt("1.0.9")
    assert vt("2.0.0") > vt("1.9.9")
    assert vt("1.1.0") == vt("1.1.0")


def test_version_tuple_tolerates_non_int_segments() -> None:
    # Non-int pieces collapse to 0 rather than raising (fail-silent ordering).
    assert _update._version_tuple("1.0.0rc1") == (1, 0, 0)


def test_parse_version() -> None:
    assert _update._parse_version('__version__ = "1.1.0"') == "1.1.0"
    assert _update._parse_version("__version__ = '2.3.4'") == "2.3.4"
    assert _update._parse_version("no version anywhere") is None


# ──────────────────────────────────────────────────────────────
# update_notice — the public surface
# ──────────────────────────────────────────────────────────────


def test_notice_when_newer(enable_update_check, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: "9.9.9")
    notice = _update.update_notice("1.1.0")
    assert notice is not None
    assert "1.1.0" in notice and "9.9.9" in notice
    # The update command matches the chosen distribution story (source/git install).
    assert 'pip install --upgrade "git+https://github.com/dovahkiin-v/letterbox"' in notice


def test_no_notice_when_current(enable_update_check, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: "1.1.0")
    assert _update.update_notice("1.1.0") is None


def test_no_notice_when_local_is_ahead(
    enable_update_check, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: "1.0.0")
    assert _update.update_notice("1.1.0") is None


def test_opt_out_suppresses_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    # Autouse already sets the opt-out; assert it actually short-circuits even
    # when a newer version would otherwise be reported.
    monkeypatch.setenv("LETTERBOX_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: "9.9.9")
    assert _update.update_notice("1.1.0") is None


def test_network_failure_is_silent(
    enable_update_check, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: None)
    assert _update.update_notice("1.1.0") is None


# ──────────────────────────────────────────────────────────────
# Cache behaviour (24h TTL, network ≤ once/day)
# ──────────────────────────────────────────────────────────────


def test_cache_avoids_refetch_within_ttl(
    enable_update_check, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}

    def fake_fetch() -> str:
        calls["n"] += 1
        return "9.9.9"

    monkeypatch.setattr(_update, "_fetch_remote_version", fake_fetch)
    # First call at t=1000 fetches and caches.
    assert _update.update_notice("1.1.0", now=1000.0) is not None
    # Second call a minute later reuses the cache — no second fetch.
    assert _update.update_notice("1.1.0", now=1060.0) is not None
    assert calls["n"] == 1


def test_cache_refetched_when_stale(
    enable_update_check, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}

    def fake_fetch() -> str:
        calls["n"] += 1
        return "9.9.9"

    monkeypatch.setattr(_update, "_fetch_remote_version", fake_fetch)
    assert _update.update_notice("1.1.0", now=1000.0) is not None
    # More than 24h later → the cache is stale and a fresh fetch happens.
    assert _update.update_notice("1.1.0", now=1000.0 + 25 * 3600) is not None
    assert calls["n"] == 2


def test_stale_cache_value_reused_when_refetch_fails(
    enable_update_check, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Prime the cache with a successful fetch.
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: "9.9.9")
    assert _update.update_notice("1.1.0", now=1000.0) is not None
    # Network dies; a stale-TTL call falls back to the last known value rather
    # than thrashing or going silent.
    monkeypatch.setattr(_update, "_fetch_remote_version", lambda: None)
    notice = _update.update_notice("1.1.0", now=1000.0 + 25 * 3600)
    assert notice is not None and "9.9.9" in notice


def test_cache_path_honours_xdg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    assert _update._cache_path() == str(tmp_path / "cache" / "letterbox" / "update_check.json")


def test_write_then_read_cache_roundtrips(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    _update._write_cache("9.9.9", 1234.0)
    on_disk = json.loads(Path(_update._cache_path()).read_text(encoding="utf-8"))
    assert on_disk == {"checked_at": 1234.0, "latest": "9.9.9"}
    assert _update._read_cache() == {"checked_at": 1234.0, "latest": "9.9.9"}


def test_read_cache_missing_is_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "empty"))
    assert _update._read_cache() is None


# ──────────────────────────────────────────────────────────────
# CLI wiring — surfaced for humans, never for the mcp stdio server
# ──────────────────────────────────────────────────────────────


def test_cli_helper_prints_notice_for_human_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(_update, "update_notice", lambda _v: "📦 update available")
    cli._maybe_print_update_notice("claude")
    captured = capsys.readouterr()
    assert "📦 update available" in captured.err
    assert captured.out == ""  # never stdout — keeps jq/tail output clean


def test_cli_helper_skips_mcp_entirely(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Even if a notice WOULD be produced, the mcp path must print nothing — its
    # stdout is the agent's JSON-RPC stream.
    called = {"n": 0}

    def tripwire(_v: str) -> str:
        called["n"] += 1
        return "📦 update available"

    monkeypatch.setattr(_update, "update_notice", tripwire)
    cli._maybe_print_update_notice("mcp")
    captured = capsys.readouterr()
    assert captured.err == "" and captured.out == ""
    assert called["n"] == 0  # update_notice not even consulted for mcp


def test_cli_helper_is_fail_silent(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def boom(_v: str) -> str:
        raise RuntimeError("network meltdown")

    monkeypatch.setattr(_update, "update_notice", boom)
    # Must not raise — a broken check can never disrupt a real command.
    cli._maybe_print_update_notice("claude")
    captured = capsys.readouterr()
    assert captured.err == "" and captured.out == ""

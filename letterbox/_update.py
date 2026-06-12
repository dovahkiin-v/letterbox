"""Best-effort "a new version is available" check for the source install.

Tier: 1
May import from: stdlib only.
Must NOT import from: any other ``letterbox.*`` module (Tier 1 leaf — see
    PLANNING_FRAMEWORK P7). The running ``__version__`` is passed IN by the
    caller (``cli.main``) rather than imported here, so this module stays a leaf.

Letterbox ships from its git repo (no PyPI), so the only "is there something
newer?" signal is the ``__version__`` on ``main``. This module fetches that at
most once per day (cached under ``~/.cache/letterbox/``), compares it to the
running version, and returns a one-line notice the CLI prints to stderr on
human-facing commands. Everything here is best-effort and fail-silent: no
network, a timeout, or a parse error simply yields no notice — it must never
disrupt a launch.

This is the **only** network call letterbox ever makes; the messaging protocol
itself is purely file-based ("no network, no server"). It is therefore gated
behind a 24h cache, a tight timeout, and a hard opt-out, and is never run for
the ``letterbox mcp`` stdio server (which the agent spawns) so it can never
pollute the JSON-RPC stream.

Opt out entirely with ``LETTERBOX_NO_UPDATE_CHECK=1``.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from typing import Optional

_CACHE_TTL_SECONDS = 24 * 60 * 60
_REMOTE_VERSION_URL = (
    "https://raw.githubusercontent.com/dovahkiin-v/letterbox/main/letterbox/__init__.py"
)
_UPDATE_COMMAND = (
    'pip install --upgrade "git+https://github.com/dovahkiin-v/letterbox"'
)
_HTTP_TIMEOUT_SECONDS = 1.5


def _cache_path() -> str:
    """Return the update-check cache file path (honours ``XDG_CACHE_HOME``).

    Returns:
        Absolute path to ``<cache>/letterbox/update_check.json``.
    """
    cache_home = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"), ".cache"
    )
    return os.path.join(cache_home, "letterbox", "update_check.json")


def _parse_version(text: str) -> Optional[str]:
    """Extract a ``__version__ = "x.y.z"`` value from module source text.

    Args:
        text: The source of an ``__init__.py`` (local or fetched from ``main``).

    Returns:
        The dotted version string, or ``None`` if no assignment is found.
    """
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', text)
    return match.group(1) if match else None


def _version_tuple(version: str) -> tuple:
    """Parse a dotted version into an int tuple for ordering (non-ints → 0).

    Args:
        version: A dotted version string such as ``"1.2.3"``.

    Returns:
        A tuple of ints suitable for ``>`` comparison; unparseable segments
        become ``0`` rather than raising.
    """
    parts = []
    for piece in version.strip().split("."):
        try:
            parts.append(int(piece))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def _read_cache() -> Optional[dict]:
    """Return the cached check payload, or ``None`` if absent/unreadable.

    Returns:
        The decoded ``{"checked_at": float, "latest": str|None}`` dict, or
        ``None`` on any read/parse failure (fail-silent).
    """
    try:
        with open(_cache_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _write_cache(latest: Optional[str], now: float) -> None:
    """Persist the last-checked timestamp and latest seen version.

    Args:
        latest: The newest version observed, or ``None`` if the fetch failed.
        now: Current epoch seconds to stamp the check with.

    Returns:
        None. Any write failure is swallowed (fail-silent).
    """
    try:
        path = _cache_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            # ensure_ascii=False per the project-wide JSON convention (§13.2),
            # though this payload is plain ASCII (a timestamp + dotted version).
            json.dump({"checked_at": now, "latest": latest}, f, ensure_ascii=False)
    except OSError:
        pass


def _fetch_remote_version() -> Optional[str]:
    """Fetch ``__version__`` from ``main`` on GitHub (fail-silent).

    Uses the stdlib ``urllib`` so letterbox keeps its near-zero runtime
    dependency footprint — adding ``requests`` for a once-a-day check would
    not earn its keep.

    Returns:
        The remote version string, or ``None`` on any network/parse failure.
    """
    try:
        req = urllib.request.Request(
            _REMOTE_VERSION_URL, headers={"User-Agent": "letterbox-update-check"}
        )
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
            if resp.status == 200:
                return _parse_version(resp.read().decode("utf-8", "replace"))
    except Exception:
        pass
    return None


def _latest_version(now: float) -> Optional[str]:
    """Return the latest known remote version, hitting the network ≤ once/day.

    Within the cache TTL the cached value is returned with no network call.
    When stale, one fetch is attempted; on failure the previous cached value
    (if any) is reused rather than thrashing the network on every invocation.

    Args:
        now: Current epoch seconds (injected for testability).

    Returns:
        The newest version string known, or ``None`` if never resolved.
    """
    cache = _read_cache()
    if cache and (now - cache.get("checked_at", 0)) < _CACHE_TTL_SECONDS:
        return cache.get("latest")
    latest = _fetch_remote_version()
    if latest:
        _write_cache(latest, now)
        return latest
    return cache.get("latest") if cache else None


def update_notice(current_version: str, now: Optional[float] = None) -> Optional[str]:
    """Return a one-line "update available" notice, or ``None``.

    Args:
        current_version: The running ``__version__`` (passed in by the caller so
            this module stays a Tier-1 leaf).
        now: Override for the current epoch seconds (testing); defaults to
            ``time.time()``.

    Returns:
        A short stderr-ready notice when a newer version exists, else ``None``
        (also ``None`` when opted out, offline, or already current/ahead).
    """
    if os.environ.get("LETTERBOX_NO_UPDATE_CHECK"):
        return None
    now = time.time() if now is None else now
    latest = _latest_version(now)
    if not latest:
        return None
    try:
        if _version_tuple(latest) > _version_tuple(current_version):
            return (
                f"📦 A new version of letterbox is available "
                f"({current_version} → {latest}).\n"
                f"   Update: {_UPDATE_COMMAND}"
            )
    except Exception:
        pass
    return None

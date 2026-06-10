"""Filesystem watcher (watchdog + 5s polling fallback), start watermark, own-write filter, identity-collision warning.

Tier: 2
May import from: stdlib, ``watchdog``, Tier 1 modules (``letterbox.protocol``, ``letterbox.channel``,
    ``letterbox.notifications``).
Must NOT import from: ``letterbox.adapters.*`` (any tier), ``letterbox.launcher``,
    ``letterbox.mcp_server``, ``letterbox.cli`` — bulkhead §13.5.

Filled in: Phase 4b/4c/4d/4e per PHASE_INDEX.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from watchdog.events import (
    FileCreatedEvent,
    FileMovedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer

from letterbox.channel import Channel, _is_own_write
from letterbox.protocol import (
    ParseError,
    is_valid_message_filename,
    list_messages,
    read_message,
)


__all__ = ["Watcher", "WatcherEvent"]


_LOGGER = logging.getLogger("letterbox.watcher")

# Bounded wait for ``Observer.join()`` during ``stop()``. The launcher's
# teardown (8c) cannot hang on a pathological filesystem; if the watchdog
# thread refuses to join within this budget, we log WARN and return so
# the launcher's exit path stays unblocked (G8).
_STOP_JOIN_TIMEOUT_SECONDS: float = 5.0

# Default cadence for the polling fallback (Vision §2.3). Belt-and-
# suspenders against dropped inotify events: inotify is fast but
# best-effort under load / on overlayfs / on certain NFS configs. The
# polling loop catches any missed live message within ``poll_interval``
# seconds. Tests override via the ``poll_interval`` constructor kwarg.
_DEFAULT_POLL_INTERVAL_SECONDS: float = 5.0


# ──────────────────────────────────────────────────────────────
# Public dataclass — the queue payload (K2)
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WatcherEvent:
    """The four trusted-context values a notification needs.

    Pre-shaped to match ``notifications.render_notification``'s four
    keyword-only required parameters: ``channel``, ``sender``,
    ``message_id``, ``timestamp``. The launcher (8b) consumes via field
    access and feeds them into ``render_notification(template,
    channel=evt.channel_name, sender=evt.recipient_label,
    message_id=evt.message_id, timestamp=evt.timestamp)``.

    The field-name asymmetry (``channel_name`` → ``channel``,
    ``recipient_label`` → ``sender``) is the deliberate rename at 8b's
    call site — the watcher's names carry more context than the renderer
    needs, and the launcher does the projection.

    All four fields are trusted strings sourced from the watcher's own
    context (Vision §6.4 / §13.3): never from the peer-written JSON
    payload. The peer cannot smuggle text into the notification stream
    because no field reads ``msg.sender`` / ``msg.timestamp`` / etc.

    Runtime-only; does NOT cross a persistence boundary (no
    ``to_dict``/``from_dict``).
    """

    channel_name: str
    recipient_label: str
    message_id: str
    timestamp: str


# ──────────────────────────────────────────────────────────────
# Watermark helpers (K3)
# ──────────────────────────────────────────────────────────────


def _synthesize_empty_watermark(now: datetime) -> str:
    """Build a synthetic sentinel watermark for an empty channel.

    Shape: ``msg-YYYYMMDDTHHMMSSffffff-{f*32}.json`` — a filename that
    sorts strictly above any real UUID4 hex with the same timestamp
    prefix (the hex alphabet maxes at ``f``). Any subsequent real peer
    write inherits a fresh timestamp prefix, so the watermark is just
    "anything truly after my start time" (K3).

    The watermark IS the no-rescan contract (Vision §3.6.1 / ADR-024):
    events whose filenames are ``<=`` the watermark are dropped before
    the queue. Empty-channel callers get the synthetic sentinel;
    populated channels get the lexically-last existing filename.
    """
    ts_part = f"{now.year:04d}{now.strftime('%m%dT%H%M%S%f')}"
    return f"msg-{ts_part}-{'f' * 32}.json"


def _extract_message_id(filename: str) -> str:
    """Strip the ``.json`` suffix to get the message-id stem.

    Caller's contract: ``filename`` MUST already pass
    :func:`is_valid_message_filename` (the strict ADR-028 regex anchors
    on ``\\.json$``). Stripping ``.json`` is the canonical valid-id
    idiom (2c IMPLEMENTATION_NOTES).
    """
    return filename.removesuffix(".json")


def _extract_event_path(event: FileSystemEvent) -> str:
    """Return the relevant filesystem path for the event.

    ``FileMovedEvent``'s relevant path is ``dest_path`` (the destination
    of an atomic rename); ``FileCreatedEvent``'s is ``src_path`` (G11).
    Branching on ``event.event_type`` rather than ``hasattr("dest_path")``
    because every event has the attribute (scout brief discrepancy).
    """
    if event.event_type == "moved":
        return event.dest_path
    return event.src_path


def _format_identity_collision_warn(channel_name: str, colliding_sender: str) -> str:
    """Build the WARN body for filter 5b's identity-collision diagnostic (4d K2).

    Routed to **stderr** via ``_LOGGER.warning``, not to the agent's PTY.
    Stderr is the user-facing diagnostic surface; the notification
    template whitelist (4a) protects the PTY-injection path (Vision
    §13.3). Peer-controlled ``colliding_sender`` lands here only because
    the user — not the agent — reads stderr.

    Substring contract: callers (tests) match on the ``[identity-collision]``
    bracket prefix and on the literal tokens ``--as`` and
    ``LETTERBOX_SENDER`` for remediation discoverability.
    """
    return (
        f"[identity-collision] Both endpoints on channel {channel_name!r} "
        f"are using sender label {colliding_sender!r}. Use --as "
        f"<distinct-label> on at least one terminal (or set "
        f"LETTERBOX_SENDER to a distinct value before launching). "
        f"Mechanical message filtering still works via instance_id, but "
        f"read-state files will collide and `letterbox tail` output "
        f"will be unreadable until --as is fixed."
    )


def _format_missing_dir_warn(channel_name: str, channel_path: Path) -> str:
    """Build the WARN body for the channel-dir recovery diagnostic (4d K3).

    Fires once per outage (per-channel) from the polling tick. The body
    names both the human-readable channel name and the on-disk path so
    the user can grep either; the ``[channel-missing]`` bracket prefix
    is the substring tests filter on.
    """
    return (
        f"[channel-missing] Channel directory for {channel_name!r} "
        f"({channel_path}) disappeared mid-run; recreating with mode "
        f"0700 and continuing. The previous conversation has been "
        f"wiped from disk."
    )


# ──────────────────────────────────────────────────────────────
# Internal watchdog handler — dispatches to Watcher._on_fs_event (K4)
# ──────────────────────────────────────────────────────────────


class _DispatchHandler(FileSystemEventHandler):
    """Routes ``on_created`` / ``on_moved`` callbacks to the parent Watcher.

    Subscribing to both event types covers the inotify ``IN_MOVED_TO``
    (atomic-rename `.tmp` → `.json`) and ``IN_CREATE`` (direct write)
    paths (K4 / G2). Intra-watcher dedupe by message_id prevents
    double-emission if a single rename surfaces as both events on some
    filesystems.

    The handler runs on watchdog's background thread, NOT the asyncio
    loop (G1). All loop-touching code goes through
    ``Watcher._on_fs_event``, which uses ``call_soon_threadsafe`` to
    bridge.
    """

    def __init__(self, watcher: "Watcher", channel: Channel) -> None:
        super().__init__()
        self._watcher = watcher
        self._channel = channel

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._watcher._on_fs_event(self._channel, _extract_event_path(event))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._watcher._on_fs_event(self._channel, _extract_event_path(event))


# ──────────────────────────────────────────────────────────────
# Public Watcher class
# ──────────────────────────────────────────────────────────────


class Watcher:
    """Filesystem watcher for one or more channel directories.

    Wraps ``watchdog.Observer`` (inotify on Linux, FSEvents on macOS) and
    bridges its background-thread callbacks into the launcher's
    ``asyncio.Queue[WatcherEvent]`` via ``loop.call_soon_threadsafe``.

    The watcher is a pure producer: the caller owns the queue and the
    event loop (K6). The watcher is also L3 (Wake the Agent) made
    executable — every queued event will become a notification injected
    into the agent's PTY at the 8b stage.

    Behavioural guarantees:

    * **No backlog flood on startup** (Vision §3.6.1 / ADR-024). A
      per-channel start watermark gates every event; only filenames
      strictly exceeding the watermark reach the queue.
    * **Combined own-write filtering** (ADR-022). Events for messages
      this endpoint wrote are dropped before the queue.
    * **Multi-channel demultiplexing**. ``channels`` may be a single
      ``Channel`` or a list; events carry the originating channel's name.
    * **Trusted-source discipline** (Vision §6.4 / §13.3). The four
      ``WatcherEvent`` fields are sourced from the watcher's own context;
      no field reads the peer-written JSON payload.
    """

    def __init__(
        self,
        channels: Union[Channel, list[Channel]],
        *,
        self_sender: str,
        self_instance_id: str,
        queue: asyncio.Queue[WatcherEvent],
        poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
        _watchdog_enabled: bool = True,
    ) -> None:
        """Initialize the watcher with channels, identity, and a shared queue.

        Args:
            channels: A single ``Channel`` or a list of ``Channel``\\s to
                watch. Multi-channel demux is by parent-directory match.
            self_sender: This endpoint's durable identity. Used as the
                first half of the combined own-write filter (ADR-022).
            self_instance_id: This endpoint's ephemeral per-process
                identity. Second half of the combined own-write filter.
            queue: Caller-owned ``asyncio.Queue[WatcherEvent]`` (K6).
                The launcher (8a) owns the loop and the queue; the
                watcher writes via ``call_soon_threadsafe`` (K7).
            poll_interval: Cadence in seconds for the polling fallback
                (Vision §2.3). Defaults to 5.0; tests override to keep
                wall-clock bounded.
            _watchdog_enabled: Test-only seam (4c K3). When ``False``,
                the watchdog ``Observer`` is NOT constructed and only the
                polling loop runs — exercises the belt-and-suspenders
                fallback in isolation. Production callers MUST leave the
                default ``True``; both producers run together per Vision
                §2.3. The leading underscore signals "internal/test
                knob, not a production API."
        """
        if isinstance(channels, Channel):
            self._channels: list[Channel] = [channels]
        else:
            self._channels = list(channels)
        self._self_sender = self_sender
        self._self_instance_id = self_instance_id
        self._queue = queue
        self._poll_interval = poll_interval
        self._watchdog_enabled = _watchdog_enabled
        self._watermarks: dict[str, str] = {}
        self._observer: Observer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._seen_message_ids: set[str] = set()
        # 4d K1 — instance-level WARN dedupe sets, both cleared on every
        # start() so each session re-arms the diagnostics per Vision
        # §3.4.1 ("warning fires every session start until the user
        # fixes it") and Vision §3.6 ("WARN ... on next message").
        self._warned_collision_channels: set[str] = set()
        self._warned_missing_channels: set[str] = set()
        self._polling_task: asyncio.Task[None] | None = None
        self._started: bool = False

    async def start(self) -> None:
        """Compute per-channel watermarks, schedule the observer, launch it.

        Idempotent: calling ``start()`` on an already-running watcher is
        a no-op (a ``_started`` flag short-circuits). A previously
        stopped watcher CAN be restarted on the same instance — each
        ``start()`` allocates a fresh ``watchdog.Observer`` so the
        "start-after-stop raises ``RuntimeError``" constraint (G7) is
        structurally avoided. The documented contract from the plan is
        "re-instantiate to restart"; same-instance restart is a
        strictly-superset capability that costs one allocation.

        Order matters: capture the running loop BEFORE
        ``observer.start()`` so the watchdog thread cannot fire an event
        with ``self._loop is None`` (G1).
        """
        if self._started:
            return

        # Clear the intra-watcher dedupe set on every fresh start (per K4
        # "cleared only on stop()/start() cycle"). On a fresh instance the
        # set is already empty; this matters if a future caller does
        # start() → stop() → start() on the SAME instance — the recomputed
        # watermarks gate stale events anyway, but a fresh dedupe set
        # makes the contract explicit.
        #
        # 4d K1 extends the same lifecycle to the two WARN dedupe sets:
        # ``_warned_collision_channels`` and ``_warned_missing_channels``
        # are cleared on each ``start()`` so a stop()/start() cycle
        # re-arms both diagnostics — Vision §3.4.1's "every session
        # start until the user fixes it" maps cleanly onto Watcher
        # session boundaries.
        self._seen_message_ids.clear()
        self._warned_collision_channels.clear()
        self._warned_missing_channels.clear()

        now = datetime.now(timezone.utc)
        for ch in self._channels:
            existing = list_messages(ch.path)
            if existing:
                self._watermarks[ch.name] = existing[-1].name
            else:
                self._watermarks[ch.name] = _synthesize_empty_watermark(now)

        self._loop = asyncio.get_running_loop()

        if self._watchdog_enabled:
            observer = Observer()
            for ch in self._channels:
                observer.schedule(
                    _DispatchHandler(self, ch),
                    str(ch.path),
                    recursive=False,
                )
            observer.start()
            self._observer = observer
        # else: K3 test seam — `_observer` stays None. `stop()`'s
        # existing null-guard at line ~294 handles teardown.

        # Polling fallback (K1/K4). Belt-and-suspenders for L3 (Wake the
        # Agent) and P5 (Self-Healing). Launched AFTER watermarks (so the
        # first tick can't mistake real backlog for new arrivals) and
        # AFTER ``observer.start()`` (so the dedupe set lifecycle aligns
        # with the watchdog half). ``stop()`` cancels-and-awaits this task
        # BEFORE ``observer.stop()`` to keep the cancellation ordering
        # tight (G1).
        self._polling_task = asyncio.create_task(self._polling_loop())

        self._started = True

    async def stop(self) -> None:
        """Stop the observer and wait for the watchdog thread to join.

        Idempotent: calling ``stop()`` twice (or before ``start()``) is a
        no-op. Joins with a bounded timeout (``_STOP_JOIN_TIMEOUT_SECONDS``)
        to avoid hanging the launcher's teardown path (8c / G8). If the
        observer thread does not join within the timeout, logs WARN and
        returns anyway.
        """
        if not self._started:
            return

        # Cancel the polling loop FIRST (K4). Order matters: the polling
        # tick runs on the asyncio loop and can call ``_on_fs_event`` →
        # ``call_soon_threadsafe(queue.put_nowait, ...)``. Cancelling
        # before ``observer.stop()`` keeps the cross-producer dedupe set
        # frozen while both producers wind down. ``suppress(CancelledError)``
        # is the documented stdlib pattern for awaiting a cancelled task
        # without re-raising; the await itself is mandatory because
        # ``filterwarnings = ["error"]`` (1b) escalates a pending-task
        # ``RuntimeWarning`` to a test failure (G1).
        polling_task = self._polling_task
        if polling_task is not None:
            polling_task.cancel()
            with suppress(asyncio.CancelledError):
                await polling_task
            self._polling_task = None

        observer = self._observer
        if observer is not None:
            observer.stop()
            observer.join(timeout=_STOP_JOIN_TIMEOUT_SECONDS)
            if observer.is_alive():
                _LOGGER.warning(
                    "watcher observer thread did not join within %.2fs; "
                    "abandoning to keep teardown unblocked. This is a "
                    "defensive fallback for pathological filesystem hangs.",
                    _STOP_JOIN_TIMEOUT_SECONDS,
                )

        self._started = False

    def _on_fs_event(self, channel: Channel, path_str: str) -> None:
        """Process a filesystem event on the watchdog thread.

        Runs on watchdog's background thread (G1). Every queue write goes
        through ``self._loop.call_soon_threadsafe(queue.put_nowait, ...)``
        (K7). Direct ``queue.put_nowait`` would corrupt the queue's
        internal state because ``asyncio.Queue`` is not thread-safe.

        Filter pipeline (event reaches the queue only if every step
        succeeds):

        1. Regex gate on the filename (rejects ``.tmp``, ``.read/*``).
        2. Parent-directory match (defends against ``.read/*.json``,
           which would pass the basename regex; G6).
        3. Watermark gate (filename strictly > watermark; Vision §3.6.1).
        4. Intra-watcher dedupe by message_id (defends against
           double-emission on filesystems where one rename surfaces as
           both ``IN_CREATE`` and ``IN_MOVED_TO``; G2).
        5. ``read_message`` — ``FileNotFoundError`` is silently skipped
           (prune race, G4); ``ParseError`` logs one WARN and skips.
        6. ``_is_own_write`` — drops own writes (ADR-022).
        """
        path = Path(path_str)
        name = path.name

        # Filter 1: filename regex (excludes `.tmp`, `.read/*.json`'s
        # parent path doesn't match anyway, but basename gate is cheaper
        # first). Defensive: scout brief notes `is_valid_message_filename`
        # raises TypeError on non-str; we always pass `path.name` (a str).
        if not is_valid_message_filename(name):
            return

        # Filter 2: parent-directory match. `.read/{label}.json` writes
        # have a parent that is `channel.path / ".read"`, NOT `channel.path`.
        # Without this guard the `.read/foo.json` write would still pass
        # the basename regex (G6).
        if path.parent != channel.path:
            return

        # Filter 3: watermark. The watermark IS the no-rescan contract.
        watermark = self._watermarks.get(channel.name, "")
        if name <= watermark:
            return

        message_id = _extract_message_id(name)

        # Filter 4: intra-watcher dedupe. A single atomic rename may
        # surface as both `IN_CREATE` and `IN_MOVED_TO` on some
        # filesystems (G2). The set is cleared on `stop()`/`start()`
        # cycle by virtue of being a fresh Watcher instance.
        if message_id in self._seen_message_ids:
            return
        self._seen_message_ids.add(message_id)

        # Filter 5: read_message. The race-with-prune path
        # (`FileNotFoundError`) is silent; `ParseError` logs one WARN
        # per file (per-event, no dedupe — 3b's "corruption is per-event
        # semantics" precedent).
        try:
            result = read_message(path)
        except FileNotFoundError:
            return

        if isinstance(result, ParseError):
            _LOGGER.warning(
                "[watcher] skipping malformed peer message %s: %s",
                name,
                result.reason,
            )
            return

        # Filter 5b (4d K2): identity-collision diagnostic. Runs AFTER
        # ``read_message`` (we need ``result.sender``) and BEFORE filter
        # 6 (whose OR semantic at ``letterbox/channel.py:847-898`` would
        # drop the collision message alongside true own-writes — K7).
        # The collision gate is the precise pair (sender match AND
        # instance differ); a true own-write fails the second half and
        # passes through silently to filter 6 as it always has.
        #
        # Per K7, the collision message itself still drops at filter 6
        # under the existing OR semantic — the WARN is the user-facing
        # diagnostic that flags the broken ``--as``; the user MUST fix
        # the config to restore message flow. The Vision §3.4.1
        # ↔ ``_is_own_write`` OR divergence is surfaced in
        # IMPLEMENTATION_NOTES for vision-review reconciliation.
        if (
            result.sender == self._self_sender
            and result.instance_id != self._self_instance_id
        ):
            if channel.name not in self._warned_collision_channels:
                self._warned_collision_channels.add(channel.name)
                _LOGGER.warning(
                    _format_identity_collision_warn(
                        channel.name, result.sender
                    )
                )
            # Fall through to filter 6 — let the existing OR semantic
            # drop the message. Do NOT discard from the dedupe set:
            # collision recovery is user-fix-only (a new ``start()``
            # cycle re-arms the WARN, not next-tick traffic).

        # Filter 6: own-write (ADR-022 combined filter; friend-imported
        # from `letterbox.channel._is_own_write`).
        if _is_own_write(result, self._self_sender, self._self_instance_id):
            return

        # Filter 7: directed-elsewhere suppression (observable, not notifiable).
        # A message carrying a non-empty ``recipient`` that is NOT us is a
        # directed sub-dialogue between two other participants. We deliberately
        # do NOT wake the agent for it — but we also do NOT hide it: it stays on
        # disk and remains visible via ``check_messages`` (the read path applies
        # no recipient filter). A broadcast (``recipient`` None/empty) or a
        # message directed AT us falls through and notifies as normal.
        if result.recipient and result.recipient != self._self_sender:
            return

        # Shape the queue payload from trusted-context values only
        # (Vision §6.4 / §13.3). No field is sourced from `result`'s
        # peer-controlled payload.
        evt = WatcherEvent(
            channel_name=channel.name,
            recipient_label=channel.recipient_label,
            message_id=message_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # K7 bridge. `asyncio.Queue` is not thread-safe; we MUST go
        # through the loop. The watchdog thread cannot block on the
        # consumer, so `put_nowait` is the right primitive (the queue
        # is unbounded per K6).
        loop = self._loop
        if loop is None:
            # Defensive: should not happen given start() captures the
            # loop before observer.start(), but if a future refactor
            # introduces a race, silently drop rather than crash the
            # watchdog thread.
            return
        loop.call_soon_threadsafe(self._queue.put_nowait, evt)

    # ──────────────────────────────────────────────────────────────
    # Polling fallback — second producer (4c)
    # ──────────────────────────────────────────────────────────────

    async def _polling_loop(self) -> None:
        """Periodically scan every channel directory for new messages.

        Belt-and-suspenders for L3 (Wake the Agent) and P5 (Self-Healing).
        inotify is best-effort: events drop under load, on overlayfs, on
        certain NFS configs, and after observer-thread hiccups. A dropped
        event means the agent never wakes when the peer speaks. This
        coroutine catches the miss within ``poll_interval`` seconds by
        re-scanning every registered channel directory each tick and
        dispatching new files through the SAME ``_on_fs_event`` chokepoint
        the watchdog handler uses — so the queue can't tell which producer
        won, and the cross-producer dedupe set silently swallows
        double-emission.

        Loop shape: ``while True: sleep; scan all channels``. We sleep
        first so the first tick fires no earlier than ``poll_interval``
        after ``start()``; watchdog handles arrivals between t=0 and
        t=poll_interval, polling backstops everything after. The
        cancellation path runs through ``asyncio.sleep``'s natural
        ``CancelledError`` propagation — ``stop()`` cancels the task and
        awaits it under ``suppress(CancelledError)``.

        Exception discipline (G4): per-channel scans are wrapped — any
        non-``FileNotFoundError`` exception logs ONE WARN and the loop
        continues. ``FileNotFoundError`` is silent per G2 (4d adds the
        WARN + re-create on top). ``CancelledError`` MUST propagate so
        ``stop()``'s ``await`` returns.
        """
        while True:
            await asyncio.sleep(self._poll_interval)
            for channel in self._channels:
                try:
                    self._scan_channel(channel)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _LOGGER.warning(
                        "[polling] scan failed for channel %s: %s",
                        channel.name,
                        exc,
                    )

    def _scan_channel(self, channel: Channel) -> None:
        """Scan one channel directory and dispatch new messages.

        Calls ``list_messages`` (filename-sorted; cheap ``os.scandir``)
        and feeds every result through ``self._on_fs_event``. The
        chokepoint's six-step filter pipeline (regex, parent-dir,
        watermark, dedupe, ``read_message``, own-write) drops everything
        already-seen or out-of-watermark — so the polling tick is
        effectively a no-op on a quiet channel after backlog has been
        gated by the start watermark.

        ``FileNotFoundError`` (channel directory deleted mid-run) is
        routed to ``_handle_missing_channel_dir`` (4d K3): WARN once
        per outage, re-create the directory at mode ``0o700``, and (if
        watchdog is enabled) re-arm the inotify watch on the new inode.
        Other exceptions propagate to the polling loop's outer handler
        (G4 / G8) and produce the generic ``[polling]`` WARN.
        """
        try:
            paths = list_messages(channel.path)
        except FileNotFoundError:
            self._handle_missing_channel_dir(channel)
            return
        # 4d K4 — directory exists again AND has content: discard from
        # the missing-dir dedupe set so a subsequent deletion cycle
        # re-fires the WARN. Empty-but-present scans leave the entry in
        # the set (the user might still be mid-outage; recovery counts
        # only once messages actually arrive again).
        if paths and channel.name in self._warned_missing_channels:
            self._warned_missing_channels.discard(channel.name)
        for path in paths:
            self._on_fs_event(channel, str(path))

    def _handle_missing_channel_dir(self, channel: Channel) -> None:
        """Recovery handler for a deleted channel directory (4d K3/K4).

        Fires once per outage from the polling tick. Three responsibilities:

        1. **Diagnostic WARN** — gated by ``_warned_missing_channels`` so
           a long outage at the default 5-second cadence does not spam
           the log. Re-arming happens in :meth:`_scan_channel` when a
           successful scan returns at least one path (K4).
        2. **Lazy mkdir** — re-creates the directory at mode ``0o700``
           with the 3a precedent (``mkdir(mode=0o700) + os.chmod``) to
           defeat the process umask. ``parents=True, exist_ok=True``
           keeps the call race-safe against a concurrent ``send_message``
           on the peer side that re-creates the directory before our
           handler runs (G7).
        3. **Watchdog re-arm** — when watchdog is enabled, the kernel
           releases the inotify watch the moment the watched inode
           vanishes. Re-scheduling the observer on the new directory
           restores the event-path producer for subsequent writes (G2).
           Skipped in the polling-only test seam (``_watchdog_enabled``
           is False, ``_observer`` is None).
        """
        if channel.name not in self._warned_missing_channels:
            self._warned_missing_channels.add(channel.name)
            _LOGGER.warning(
                _format_missing_dir_warn(channel.name, channel.path)
            )

        # 3a precedent: mkdir + explicit chmod to defeat umask.
        # ``parents=True`` rebuilds the ``channels/`` intermediate if
        # the user wiped that too; ``exist_ok=True`` is race-safe.
        channel.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(channel.path, 0o700)

        # Re-arm watchdog on the new inode. ``self._observer`` is None
        # under the K3 test seam (``_watchdog_enabled=False``) and the
        # recovery is polling-only.
        observer = self._observer
        if observer is not None and self._watchdog_enabled:
            observer.schedule(
                _DispatchHandler(self, channel),
                str(channel.path),
                recursive=False,
            )

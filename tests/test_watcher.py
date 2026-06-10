"""Tests for Phase 4b — Watcher Core (Watchdog + Start Watermark).

Behavioral TDD per the plan's §9 grouping. Real ``watchdog.Observer`` over
real filesystems via ``tmp_letterbox_home`` (1b). Mocks are minimal —
isolated to the K7 bridge-correctness unit test (``call_soon_threadsafe``)
and the G8 hanging-observer timeout test.

Test classes:

* ``TestWatcherEventDataclass`` — frozen-dataclass shape + field order.
* ``TestStartWatermark`` — non-empty / empty / multi-channel / .tmp-excluded
  watermark computation at ``start()`` time.
* ``TestNoBacklogFlood`` — Vision §3.6.1 / ADR-024: backlog never auto-surfaces.
* ``TestEventFilter`` — ``.tmp`` / ParseError / own-write / .read/ filtering.
* ``TestMultiChannel`` — single / list constructor + demux correctness.
* ``TestWatcherLifecycle`` — start/stop idempotency + G7 restart contract
  + G8 hanging-observer timeout.
* ``TestWatchdogAsyncBridge`` — K7 ``call_soon_threadsafe`` usage + ordering.
* ``TestPublicSurface`` — ``__all__`` lock + friend-import contract re-lock.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import shutil
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable
from unittest.mock import MagicMock, patch

import pytest

# Pin every test in this module to one xdist worker so we don't exhaust
# the inotify-instance limit (default ``fs.inotify.max_user_instances`` is
# 128; with 16+ parallel workers each creating Observer instances the
# limit blows out and tests fail with ``OSError(EMFILE)``). Requires the
# project-wide ``--dist loadgroup`` setting in pyproject.toml addopts —
# documented as a 4b pyproject deviation in IMPLEMENTATION_NOTES.
pytestmark = pytest.mark.xdist_group("watcher")

from letterbox import channel as channel_mod
from letterbox import watcher as watcher_mod
from letterbox.channel import Channel
from letterbox.protocol import (
    Message,
    Metadata,
    make_message_filename,
    new_message,
    write_message,
)
from letterbox.watcher import Watcher, WatcherEvent
from tests.helpers import wait_for


# ──────────────────────────────────────────────────────────────
# Local helpers (clones of 3c/3d test patterns)
# ──────────────────────────────────────────────────────────────


_SELF_SENDER = "claude-a"
_SELF_INSTANCE = "lb-self"
_PEER_SENDER = "claude-b"
_PEER_INSTANCE = "lb-peer"


def make_channel(
    home: Path,
    name: str = "ch01",
    sender: str = _SELF_SENDER,
    recipient: str = _PEER_SENDER,
) -> Channel:
    """Shorthand for ``Channel.get_or_create`` against a tmp letterbox home."""
    return Channel.get_or_create(name, sender, recipient, state_dir=home)


def _make_msg(
    *,
    sender: str,
    instance_id: str,
    msg_id: str,
    channel: str = "ch01",
    body: str = "hi",
) -> Message:
    """Build a ``Message`` that bypasses ``new_message``'s empty-sender guard."""
    return Message(
        schema_version=1,
        id=msg_id,
        channel=channel,
        address="file://local",
        instance_id=instance_id,
        sender=sender,
        recipient=None,
        timestamp="2026-05-27T00:00:00+00:00",
        body=body,
        in_reply_to=None,
        metadata=Metadata(encryption=None, ext={}),
    )


def make_peer_message(
    channel: Channel,
    *,
    peer_sender: str = _PEER_SENDER,
    peer_instance: str = _PEER_INSTANCE,
    msg_id: str | None = None,
    body: str = "peer",
    timestamp: datetime | None = None,
) -> Path:
    """Write a real peer message and return the resulting file path."""
    if msg_id is None:
        stem = make_message_filename(timestamp=timestamp).removesuffix(".json")
    else:
        stem = msg_id
    msg = new_message(
        id=stem,
        channel=channel.name,
        instance_id=peer_instance,
        sender=peer_sender,
        body=body,
    )
    return write_message(channel.path, msg)


def make_self_message(
    channel: Channel,
    *,
    self_sender: str = _SELF_SENDER,
    self_instance: str = _SELF_INSTANCE,
    msg_id: str | None = None,
    body: str = "self",
    timestamp: datetime | None = None,
) -> Path:
    """Write a real own-write message (sender matches the watcher's self)."""
    if msg_id is None:
        stem = make_message_filename(timestamp=timestamp).removesuffix(".json")
    else:
        stem = msg_id
    msg = new_message(
        id=stem,
        channel=channel.name,
        instance_id=self_instance,
        sender=self_sender,
        body=body,
    )
    return write_message(channel.path, msg)


def populate_peer_msgs(
    channel: Channel,
    *,
    count: int,
    base: datetime | None = None,
) -> list[str]:
    """Write ``count`` peer messages with microsecond-offset filenames."""
    if base is None:
        base = datetime(2026, 5, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    stems: list[str] = []
    for i in range(count):
        stem = make_message_filename(
            base + timedelta(microseconds=i)
        ).removesuffix(".json")
        make_peer_message(channel, msg_id=stem, body=f"peer-{i}")
        stems.append(stem)
    return stems


# ──────────────────────────────────────────────────────────────
# TestWatcherEventDataclass
# ──────────────────────────────────────────────────────────────


class TestWatcherEventDataclass:
    """K2 — frozen four-field strings ready for ``render_notification``."""

    def test_fields_populated_and_typed(self) -> None:
        evt = WatcherEvent(
            channel_name="ch01",
            recipient_label="claude-b",
            message_id="msg-20260527T140000000000-" + "a" * 32,
            timestamp="2026-05-27T14:00:00+00:00",
        )
        assert evt.channel_name == "ch01"
        assert evt.recipient_label == "claude-b"
        assert evt.message_id.startswith("msg-")
        assert evt.timestamp.endswith("+00:00")

    def test_is_frozen(self) -> None:
        evt = WatcherEvent(
            channel_name="ch01",
            recipient_label="claude-b",
            message_id="msg-x",
            timestamp="t",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            evt.channel_name = "ch02"  # type: ignore[misc]

    def test_no_defaults_explicit_or_bust(self) -> None:
        """K2 — every field is required (mirrors 3d ChannelInfo's shape)."""
        with pytest.raises(TypeError):
            WatcherEvent()  # type: ignore[call-arg]


# ──────────────────────────────────────────────────────────────
# TestStartWatermark
# ──────────────────────────────────────────────────────────────


class TestStartWatermark:
    """K3 — per-channel watermarks. ``filename > watermark`` is the gate."""

    @pytest.mark.asyncio
    async def test_non_empty_channel_watermark_is_lexically_last(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        stems = populate_peer_msgs(ch, count=3)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            assert w._watermarks[ch.name] == stems[-1] + ".json"
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_empty_channel_watermark_is_synthetic_sentinel(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            wm = w._watermarks[ch.name]
            assert wm.startswith("msg-")
            assert wm.endswith("-" + "f" * 32 + ".json")
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_multi_channel_watermarks_are_independent(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch01 = make_channel(tmp_letterbox_home, name="ch01")
        ch02 = make_channel(tmp_letterbox_home, name="ch02")
        ch01_stems = populate_peer_msgs(ch01, count=2)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch01, ch02],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            assert w._watermarks[ch01.name] == ch01_stems[-1] + ".json"
            assert w._watermarks[ch02.name].endswith("-" + "f" * 32 + ".json")
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_tmp_files_do_not_count_toward_watermark(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        # Manually write a `.json.tmp` file that should be ignored.
        future_stem = make_message_filename(
            datetime(2099, 1, 1, tzinfo=timezone.utc)
        ).removesuffix(".json")
        (ch.path / (future_stem + ".json.tmp")).write_bytes(b"{}")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # No real messages -> synthetic sentinel, NOT the future-dated .tmp.
            assert w._watermarks[ch.name].endswith("-" + "f" * 32 + ".json")
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestNoBacklogFlood
# ──────────────────────────────────────────────────────────────


class TestNoBacklogFlood:
    """Vision §3.6.1 / ADR-024 — restart-as-fresh-start. No auto-surface."""

    @pytest.mark.asyncio
    async def test_pre_existing_messages_produce_zero_events(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=50)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # Negative-result wait: queue should remain empty for 1s.
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=1.0)
            assert queue.qsize() == 0
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_one_new_peer_message_produces_exactly_one_event(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=50)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_peer_message(ch, body="fresh")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            # Settle: give a moment for any extras to surface, then assert.
            await asyncio.sleep(0.2)
            assert queue.qsize() == 1
            evt = queue.get_nowait()
            assert evt.channel_name == "ch01"
            assert evt.recipient_label == _PEER_SENDER
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_empty_channel_first_message_produces_one_event(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_peer_message(ch, body="first")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            assert queue.qsize() == 1
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_backdated_filename_after_start_does_not_fire(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # Backdated filename: lexically less than the synthetic
            # ``msg-{NOW}-fff...`` watermark.
            backdated = datetime(2000, 1, 1, tzinfo=timezone.utc)
            make_peer_message(ch, body="ancient", timestamp=backdated)
            # Watcher will see the create event, but the filename is
            # lexically <= watermark, so no event reaches the queue.
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=1.0)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_restart_does_not_flood_existing_backlog(
        self, tmp_letterbox_home: Path
    ) -> None:
        """A fresh Watcher on a populated channel emits no events."""
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=10)
        # First instance — never started, just confirms state on disk.
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestEventFilter
# ──────────────────────────────────────────────────────────────


class TestEventFilter:
    """Filename regex, parse-error, own-write, .read/ filters."""

    @pytest.mark.asyncio
    async def test_tmp_file_produces_no_event(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            (ch.path / "garbage.json.tmp").write_bytes(b"{}")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_malformed_json_logs_warn_and_skips(
        self, tmp_letterbox_home: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # A regex-valid filename but the bytes are invalid JSON.
            bad_stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # Atomic rename so the watcher sees a finished file.
                tmp = ch.path / (bad_stem + ".json.tmp")
                final = ch.path / (bad_stem + ".json")
                tmp.write_bytes(b"not json{{{")
                tmp.rename(final)
                # Give the watcher time to process the event.
                await asyncio.sleep(0.5)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.3)
            warn_records = [
                r for r in caplog.records
                if r.name == "letterbox.watcher"
                and r.levelno == logging.WARNING
            ]
            assert len(warn_records) >= 1
            assert "malformed" in warn_records[0].getMessage().lower() or \
                "parse" in warn_records[0].getMessage().lower()
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_own_write_by_sender_match_filtered(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # Peer write with sender == self_sender (cross-restart half).
            stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            msg = new_message(
                id=stem,
                channel=ch.name,
                instance_id="lb-someone-else",
                sender=_SELF_SENDER,
                body="from-prior-life",
            )
            write_message(ch.path, msg)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_own_write_by_instance_id_match_filtered(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # Peer write with different sender but same instance_id.
            stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            msg = new_message(
                id=stem,
                channel=ch.name,
                instance_id=_SELF_INSTANCE,
                sender="other-label",
                body="from-same-process",
            )
            write_message(ch.path, msg)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_own_write_by_both_filtered(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_self_message(ch, body="exact-self")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_peer_write_both_differ_produces_event(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_peer_message(ch, body="genuine-peer")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            assert queue.qsize() == 1
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_race_with_prune_silently_skipped(
        self, tmp_letterbox_home: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """File deleted between event arrival and read_message: no event, no log."""
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # Patch read_message to raise FileNotFoundError to simulate
                # the prune race deterministically.
                original = watcher_mod.read_message

                def racy_read(path: Path):
                    raise FileNotFoundError(path)

                with patch.object(watcher_mod, "read_message", side_effect=racy_read):
                    make_peer_message(ch, body="will-race")
                    await asyncio.sleep(0.5)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.3)
            # No WARN log line for the race — silent skip.
            warn_records = [
                r for r in caplog.records
                if r.name == "letterbox.watcher"
                and r.levelno == logging.WARNING
            ]
            assert len(warn_records) == 0
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_read_state_writes_do_not_produce_events(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G6 — ``.read/{label}.json`` writes during acknowledge are filtered."""
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            ch.acknowledge("some-msg-id", self_instance_id=_SELF_INSTANCE)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestMultiChannel
# ──────────────────────────────────────────────────────────────


class TestMultiChannel:
    """K1 — single observer, per-channel schedule(), demux by parent dir."""

    @pytest.mark.asyncio
    async def test_single_channel_constructor_accepts_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_peer_message(ch)
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            evt = queue.get_nowait()
            assert evt.channel_name == "ch01"
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_list_constructor_watches_all_channels(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch01 = make_channel(tmp_letterbox_home, name="ch01", recipient="peer-01")
        ch02 = make_channel(tmp_letterbox_home, name="ch02", recipient="peer-02")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch01, ch02],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_peer_message(ch01, body="for-01")
            make_peer_message(ch02, body="for-02")
            await wait_for(lambda: queue.qsize() >= 2, timeout=2.0)
            evts = [queue.get_nowait() for _ in range(2)]
            names = {e.channel_name for e in evts}
            assert names == {"ch01", "ch02"}
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_demux_correctness_no_cross_channel_bleed(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch01 = make_channel(tmp_letterbox_home, name="ch01", recipient="peer-01")
        ch02 = make_channel(tmp_letterbox_home, name="ch02", recipient="peer-02")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch01, ch02],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            make_peer_message(ch01, body="for-01")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            await asyncio.sleep(0.2)
            assert queue.qsize() == 1
            evt = queue.get_nowait()
            assert evt.channel_name == "ch01"
            assert evt.recipient_label == "peer-01"
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_three_channels_concurrent_writes(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch01 = make_channel(tmp_letterbox_home, name="ch01", recipient="peer-01")
        ch02 = make_channel(tmp_letterbox_home, name="ch02", recipient="peer-02")
        ch03 = make_channel(tmp_letterbox_home, name="ch03", recipient="peer-03")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch01, ch02, ch03],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            barrier = threading.Barrier(3)

            def write_to(channel: Channel) -> None:
                barrier.wait()
                make_peer_message(channel, body=f"in-{channel.name}")

            threads = [
                threading.Thread(target=write_to, args=(ch,))
                for ch in (ch01, ch02, ch03)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            await wait_for(lambda: queue.qsize() >= 3, timeout=3.0)
            await asyncio.sleep(0.2)
            assert queue.qsize() == 3
            names = {queue.get_nowait().channel_name for _ in range(3)}
            assert names == {"ch01", "ch02", "ch03"}
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestWatcherLifecycle
# ──────────────────────────────────────────────────────────────


class TestWatcherLifecycle:
    """G7 / G8 — idempotent start/stop, bounded join, hanging-observer timeout."""

    @pytest.mark.asyncio
    async def test_start_then_stop_clean(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        await w.stop()
        assert w._observer is None or not w._observer.is_alive()

    @pytest.mark.asyncio
    async def test_double_start_is_noop(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            first_observer = w._observer
            await w.start()  # No-op
            assert w._observer is first_observer
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_double_stop_is_noop(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        await w.stop()
        # Second stop must not raise.
        await w.stop()

    @pytest.mark.asyncio
    async def test_stop_before_start_is_noop(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        # Must not raise.
        await w.stop()

    @pytest.mark.asyncio
    async def test_stop_joins_observer_thread(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        obs = w._observer
        assert obs is not None and obs.is_alive()
        await w.stop()
        assert not obs.is_alive()

    @pytest.mark.asyncio
    async def test_stop_times_out_on_hanging_observer(
        self, tmp_letterbox_home: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """G8 — observer.join times out, watcher logs WARN and returns."""
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()

        fake_observer = MagicMock()
        fake_observer.is_alive.return_value = True
        fake_observer.schedule.return_value = None
        fake_observer.start.return_value = None
        fake_observer.stop.return_value = None
        fake_observer.join.return_value = None

        with patch.object(watcher_mod, "Observer", return_value=fake_observer):
            # Patch the timeout to a small value so the test is fast.
            with patch.object(watcher_mod, "_STOP_JOIN_TIMEOUT_SECONDS", 0.1):
                w = Watcher(
                    ch,
                    self_sender=_SELF_SENDER,
                    self_instance_id=_SELF_INSTANCE,
                    queue=queue,
                )
                await w.start()
                with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                    await w.stop()
                warn_records = [
                    r for r in caplog.records
                    if r.name == "letterbox.watcher"
                    and r.levelno == logging.WARNING
                ]
                assert any(
                    "timeout" in r.getMessage().lower()
                    or "join" in r.getMessage().lower()
                    or "hang" in r.getMessage().lower()
                    for r in warn_records
                )


# ──────────────────────────────────────────────────────────────
# TestWatchdogAsyncBridge
# ──────────────────────────────────────────────────────────────


class TestWatchdogAsyncBridge:
    """K7 — ``call_soon_threadsafe`` is the bridge; never raw ``put_nowait``."""

    @pytest.mark.asyncio
    async def test_call_soon_threadsafe_is_used(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Unit-test the bridge: the watchdog callback must go through
        ``loop.call_soon_threadsafe``, not call ``queue.put_nowait`` directly.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        real_loop = asyncio.get_running_loop()
        spy_calls: list[tuple[Callable, tuple]] = []

        class LoopSpy:
            def call_soon_threadsafe(self, callback, *args):
                spy_calls.append((callback, args))
                return real_loop.call_soon_threadsafe(callback, *args)

        with patch.object(
            watcher_mod.asyncio,
            "get_running_loop",
            return_value=LoopSpy(),
        ):
            queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
            w = Watcher(
                ch,
                self_sender=_SELF_SENDER,
                self_instance_id=_SELF_INSTANCE,
                queue=queue,
            )
            await w.start()
            try:
                make_peer_message(ch, body="bridged")
                await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
                assert len(spy_calls) >= 1
                # The callback scheduled should be queue.put_nowait.
                cb, args = spy_calls[-1]
                assert cb == queue.put_nowait
                assert len(args) == 1
                assert isinstance(args[0], WatcherEvent)
            finally:
                await w.stop()

    @pytest.mark.asyncio
    async def test_events_from_non_loop_thread_reach_queue(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            t = threading.Thread(
                target=make_peer_message, args=(ch,), kwargs={"body": "off-loop"}
            )
            t.start()
            t.join()
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_event_ordering_preserved(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            base = datetime(2099, 6, 1, tzinfo=timezone.utc)
            stems = []
            for i in range(10):
                stem = make_message_filename(
                    base + timedelta(microseconds=i)
                ).removesuffix(".json")
                stems.append(stem)
                msg = new_message(
                    id=stem,
                    channel=ch.name,
                    instance_id=_PEER_INSTANCE,
                    sender=_PEER_SENDER,
                    body=f"o-{i}",
                )
                write_message(ch.path, msg)
            await wait_for(lambda: queue.qsize() >= 10, timeout=3.0)
            await asyncio.sleep(0.2)
            collected = [queue.get_nowait().message_id for _ in range(queue.qsize())]
            # The 10 written stems should appear in order.
            assert collected == stems
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestPublicSurface
# ──────────────────────────────────────────────────────────────


class TestPublicSurface:
    """``__all__`` lock + friend-import contract re-lock (3c K4)."""

    def test_public_exports(self) -> None:
        assert set(watcher_mod.__all__) == {"Watcher", "WatcherEvent"}

    def test_dispatch_handler_is_module_private(self) -> None:
        assert "_DispatchHandler" in dir(watcher_mod)
        assert "_DispatchHandler" not in watcher_mod.__all__

    def test_is_own_write_remains_module_private_in_channel(self) -> None:
        """3c K4 — the friend-import target must NOT be in channel.__all__.

        4b imports ``_is_own_write`` via friend-import; promoting it to
        ``__all__`` would overstate the API. Defends against a future
        refactor accidentally exporting it.
        """
        assert hasattr(channel_mod, "_is_own_write")
        assert "_is_own_write" not in channel_mod.__all__


# ──────────────────────────────────────────────────────────────
# TestDefensiveBranches — direct dispatch into _on_fs_event / handler
# ──────────────────────────────────────────────────────────────


class TestDefensiveBranches:
    """Direct-dispatch tests covering defense-in-depth branches.

    These paths are unreachable via the normal watchdog event flow on the
    current code paths (basename regex already excludes ``.read/*.json``;
    intra-watcher dedupe is structurally rare on inotify), but they
    encode load-bearing invariants. Direct ``_on_fs_event`` / handler
    invocation locks the contract.
    """

    @pytest.mark.asyncio
    async def test_parent_dir_mismatch_drops_event(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G6 defense-in-depth: a path with the right basename but the
        wrong parent dir is dropped.

        The basename regex rejects ``<label>.json`` (no ``msg-`` prefix),
        so the real-world ``.read/{label}.json`` event is caught at
        filter 1. Filter 2 catches the hypothetical case of a real
        ``msg-...json`` file landing under ``.read/`` (e.g., a future
        directory-layout change).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # Construct a path whose basename passes the regex but whose
            # parent is NOT the channel dir (simulate a `.read/`-nested
            # message-shaped filename).
            wrong_parent = ch.path / ".read"
            wrong_parent.mkdir(exist_ok=True)
            future_stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            fake_path = wrong_parent / (future_stem + ".json")
            w._on_fs_event(ch, str(fake_path))
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.3)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_intra_watcher_dedupe_drops_second_event(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G2 — if a single rename surfaces as both create + moved on
        some FS, the second dispatch on the same message_id is dropped.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            path = make_peer_message(ch, body="dup-twice")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            # Manually re-dispatch the same event — the dedupe set must
            # short-circuit before any read_message call.
            w._on_fs_event(ch, str(path))
            await asyncio.sleep(0.2)
            assert queue.qsize() == 1
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_directory_event_is_ignored(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``_DispatchHandler`` short-circuits on directory events.

        Watchdog can emit ``DirCreatedEvent`` / ``DirMovedEvent`` on
        ``mkdir`` operations under a watched directory. The handler must
        ignore them — only file events become messages.
        """
        from watchdog.events import DirCreatedEvent, DirMovedEvent

        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            handler = watcher_mod._DispatchHandler(w, ch)
            handler.on_created(DirCreatedEvent(str(ch.path / "subdir")))
            handler.on_moved(DirMovedEvent(
                str(ch.path / "src"), str(ch.path / "dst")
            ))
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.3)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_loop_unset_drops_event_silently(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Defensive: if a watchdog event fires before ``_loop`` is
        captured (a future-refactor race), the watcher silently drops
        rather than crashing the watchdog thread.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # Force the race: clear the captured loop.
            w._loop = None
            make_peer_message(ch, body="orphaned")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestPollingFallback — 4c belt-and-suspenders contract
# ──────────────────────────────────────────────────────────────


class TestPollingFallback:
    """4c K1/K3 — polling catches messages a dropped watchdog event missed."""

    @pytest.mark.asyncio
    async def test_polling_fires_when_watchdog_disabled(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Load-bearing belt-and-suspenders test.

        With the watchdog observer entirely disabled (K3 test seam), a
        peer message written post-``start()`` reaches the queue via the
        polling fallback within at most ~``poll_interval`` seconds.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            assert w._observer is None  # K3 seam: no observer constructed
            make_peer_message(ch, body="via-polling")
            await wait_for(lambda: queue.qsize() >= 1, timeout=1.0)
            evt = queue.get_nowait()
            assert evt.channel_name == "ch01"
            assert evt.recipient_label == _PEER_SENDER
            assert isinstance(evt.message_id, str)
            assert evt.message_id.startswith("msg-")
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_respects_start_watermark(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Polling NEVER auto-surfaces backlog — same watermark gate as
        watchdog. Vision §3.6.1 / ADR-024 applies to both producers.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=50)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_dedupes_against_own_writes(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-022 combined own-write filter applies on the polling path.

        Polling routes through ``_on_fs_event`` → ``_is_own_write``;
        a message whose ``sender == self_sender`` is dropped silently.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            make_self_message(ch, body="my-echo")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_rejects_tmp_files(
        self, tmp_letterbox_home: Path
    ) -> None:
        """The basename regex rejects ``.tmp`` files. Polling never
        surfaces a half-written message.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            tmp = ch.path / (stem + ".json.tmp")
            tmp.write_bytes(b'{"id": "x"}')  # bytes never read; regex rejects first
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_rejects_read_subdir_writes(
        self, tmp_letterbox_home: Path
    ) -> None:
        """The ``.read/{label}.json`` per-agent read-marker writes have a
        non-matching basename (no ``msg-`` prefix) AND a non-matching
        parent dir. Polling's ``list_messages`` only scans the channel
        directory itself, so ``.read/`` is structurally invisible — but
        even if a future refactor expanded the scan, the parent-dir gate
        in ``_on_fs_event`` would catch it.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            read_dir = ch.path / ".read"
            read_dir.mkdir(exist_ok=True)
            (read_dir / f"{_PEER_SENDER}.json").write_text(
                '{"high_water_mark": "msg-x"}', encoding="utf-8"
            )
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_swallows_parse_error_silently_with_one_warn(
        self, tmp_letterbox_home: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A regex-valid filename containing invalid bytes logs exactly
        one WARN and produces no queue event. The dedupe set is added to
        before ``read_message`` runs, so subsequent polling ticks
        short-circuit at the dedupe gate and produce no duplicate WARN.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            bad_stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # Atomic-rename so list_messages sees a finished file.
                tmp = ch.path / (bad_stem + ".json.tmp")
                final = ch.path / (bad_stem + ".json")
                tmp.write_bytes(b"not json{{{")
                tmp.rename(final)
                # Let polling fire several times; dedupe must collapse
                # the WARN count to 1.
                await asyncio.sleep(0.5)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.3)
            warn_records = [
                r for r in caplog.records
                if r.name == "letterbox.watcher"
                and r.levelno == logging.WARNING
                and "malformed" in r.getMessage().lower()
            ]
            assert len(warn_records) == 1
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_swallows_filenotfound_race_silently(
        self, tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A file that ``list_messages`` returns but vanishes before
        ``read_message`` (race-with-prune) is silently skipped — no
        queue event, no WARN. Matches 4b's existing race-with-prune
        semantics at the file level.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )

        def _raise_fnf(path: Path) -> object:
            raise FileNotFoundError(str(path))

        monkeypatch.setattr(watcher_mod, "read_message", _raise_fnf)
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # A real on-disk file. list_messages returns it; the
                # patched read_message simulates the prune race.
                make_peer_message(ch, body="ghost")
                await asyncio.sleep(0.4)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.3)
            warn_records = [
                r for r in caplog.records
                if r.name == "letterbox.watcher"
                and r.levelno == logging.WARNING
            ]
            assert warn_records == []
        finally:
            await w.stop()

    def test_polling_default_interval_is_five_seconds_per_vision_2_3(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Lock the Vision §2.3 default cadence at the constructor level.

        Production callers MUST get 5.0 s polling without specifying the
        kwarg explicitly. Light state assertion — no ``start()`` because
        the cadence is a constructor concern, not a runtime one.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        assert w._poll_interval == 5.0
        assert w._watchdog_enabled is True
        assert w._polling_task is None  # not yet started

    @pytest.mark.asyncio
    async def test_polling_race_message_written_at_tick_boundary(
        self, tmp_letterbox_home: Path
    ) -> None:
        """A message written near a polling tick boundary is never lost.

        ``list_messages`` re-scans the whole channel directory each
        tick; the watermark gate is a constant string post-``start()``.
        A message that arrives mid-scan lands in either the current
        ``list_messages`` result OR the next one — never invisible to
        both. The PHASE_INDEX "race test" line.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            # Land the write near the tick boundary; we don't need to
            # be sub-microsecond precise — the load-bearing claim is
            # "no scheduling sliver makes the write invisible to both
            # the current and next tick."
            await asyncio.sleep(0.05)
            make_peer_message(ch, body="boundary")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2 * 0.1 + 0.5)
            assert queue.qsize() == 1
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestPollingDedupe — cross-producer dedupe correctness (K2)
# ──────────────────────────────────────────────────────────────


class TestPollingDedupe:
    """K2 — both producers share ``_seen_message_ids`` and ``_watermarks``."""

    @pytest.mark.asyncio
    async def test_single_notification_when_watchdog_and_polling_both_see_message(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Both producers active; one peer message; exactly one queue event.

        Watchdog typically wins (sub-millisecond inotify) and the
        polling tick (at 0.1 s) sees the message_id already in the
        dedupe set on its next sweep, short-circuiting cleanly. The
        cross-producer dedupe contract is what keeps the launcher
        from rendering the same notification twice.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            make_peer_message(ch, body="seen-by-both")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            # Let polling run several more ticks AFTER the watchdog event.
            await asyncio.sleep(0.4)
            assert queue.qsize() == 1
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_dedupes_against_watchdog_seen_message_id(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Polling-only watcher: a pre-populated ``_seen_message_ids``
        entry blocks polling from emitting the message.

        Models the production scenario where watchdog fired first and
        recorded the message_id; the polling tick must short-circuit
        at the dedupe gate.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            # Predict the stem we'll write so we can mark it seen first.
            stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            w._seen_message_ids.add(stem)
            make_peer_message(ch, msg_id=stem, body="pre-marked")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.4)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_watchdog_dedupes_against_polling_seen_message_id(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Symmetric to the previous test, with watchdog ENABLED.

        Models the production scenario where polling fired first and
        recorded the message_id; the watchdog event must short-circuit
        at the same dedupe gate. The set IS the cross-producer
        dedupe contract — direction doesn't matter.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            w._seen_message_ids.add(stem)
            make_peer_message(ch, msg_id=stem, body="pre-marked-watchdog")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_seen_message_ids_set_grows_under_steady_traffic(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Documents the memory profile: the dedupe set grows once per
        unique message, never prunes. Vision §9.4 sets the practical
        ceiling at ~10K messages per channel; the set's per-id memory
        footprint (~100 bytes) puts that at ~1 MB per Watcher — well
        within the 4e budget envelope. No LRU eviction (would silently
        re-enable double-notification across producers).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            stems: list[str] = []
            base = datetime(2099, 1, 1, tzinfo=timezone.utc)
            for i in range(10):
                stem = make_message_filename(
                    base + timedelta(microseconds=i)
                ).removesuffix(".json")
                make_peer_message(ch, msg_id=stem, body=f"steady-{i}")
                stems.append(stem)
            await wait_for(lambda: queue.qsize() >= 10, timeout=3.0)
            assert len(w._seen_message_ids) == 10
            assert set(stems) <= w._seen_message_ids
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_and_watchdog_share_watermarks_dict(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Polling uses the SAME ``_watermarks`` dict as watchdog.

        Populated channel → watermark = newest existing filename. A
        post-``start()`` write with a BACKDATED filename is lexically
        less than the watermark and the polling tick drops it at the
        watermark gate. Symmetric to 4b's
        ``test_backdated_filename_after_start_does_not_fire`` but on
        the polling path.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=5, base=datetime(
            2050, 1, 1, tzinfo=timezone.utc
        ))
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            assert set(w._watermarks.keys()) == {"ch01"}
            backdated = datetime(2000, 1, 1, tzinfo=timezone.utc)
            make_peer_message(ch, body="ancient", timestamp=backdated)
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.5)
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestPollingLifecycle — task creation, cancellation, cleanup (G1/K4)
# ──────────────────────────────────────────────────────────────


class TestPollingLifecycle:
    """K4/G1 — polling task is created at start, cancelled-and-awaited at stop."""

    @pytest.mark.asyncio
    async def test_polling_task_created_after_start(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``_polling_task`` is None pre-``start()``, an ``asyncio.Task``
        post-``start()``, and None again post-``stop()``.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        assert w._polling_task is None
        await w.start()
        try:
            assert isinstance(w._polling_task, asyncio.Task)
            assert not w._polling_task.done()
        finally:
            await w.stop()
        assert w._polling_task is None

    @pytest.mark.asyncio
    async def test_polling_task_cancelled_on_stop(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``stop()`` cancels the polling task and awaits it; the task
        ends as cancelled (or completed by virtue of the cancellation).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        task = w._polling_task
        assert task is not None
        await w.stop()
        assert task.done()
        # The task should have ended through cancellation.
        assert task.cancelled() or task.exception() is None

    @pytest.mark.asyncio
    async def test_stop_before_start_is_noop_for_polling_too(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``stop()`` on a never-started watcher is a no-op; the
        polling task stays ``None`` and no exception is raised.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.stop()
        assert w._polling_task is None

    @pytest.mark.asyncio
    async def test_double_start_does_not_create_second_polling_task(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Idempotency: the ``_started`` flag short-circuits the second
        ``start()`` before the polling task is recreated.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            first_task = w._polling_task
            assert first_task is not None
            await w.start()
            assert w._polling_task is first_task
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_continues_after_filenotfound_in_one_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G2 — deleting one channel directory mid-run does NOT crash
        the polling loop; the surviving channel keeps producing events.

        Polling-only seam (``_watchdog_enabled=False``) so the test
        is isolated from watchdog's own behaviour on directory deletion.
        """
        ch1 = make_channel(tmp_letterbox_home, name="ch01")
        ch2 = make_channel(tmp_letterbox_home, name="ch02")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch1, ch2],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            # Delete ch1 outright (mimics Vision §3.6's "channel
            # directory deleted while letterbox is running" recovery
            # row; 4d will layer WARN + re-create on top).
            import shutil
            shutil.rmtree(ch1.path)
            # Write a peer message to the surviving channel.
            make_peer_message(ch2, body="survivor")
            await wait_for(lambda: queue.qsize() >= 1, timeout=1.0)
            evt = queue.get_nowait()
            assert evt.channel_name == "ch02"
            # The polling task itself must still be running (G2 silent skip).
            assert w._polling_task is not None
            assert not w._polling_task.done()
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_logs_warn_on_unexpected_scan_exception(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """G4 — an unexpected exception in ``_scan_channel`` (anything
        other than ``FileNotFoundError``) logs ONE WARN per tick and the
        loop continues. Cancellation still propagates because the
        handler explicitly re-raises ``CancelledError``.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        # Patch AFTER start() — start() itself calls list_messages to
        # compute the initial watermark; we only want the polling-tick
        # call site to explode.
        await w.start()
        try:
            def _explode(channel_dir: Path) -> list[Path]:
                raise PermissionError(
                    f"simulated permission error on {channel_dir}"
                )

            monkeypatch.setattr(watcher_mod, "list_messages", _explode)
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                await asyncio.sleep(0.3)  # ~2-3 polling ticks
            warn_records = [
                r for r in caplog.records
                if r.name == "letterbox.watcher"
                and r.levelno == logging.WARNING
                and "[polling]" in r.getMessage()
            ]
            assert warn_records, "expected at least one [polling] WARN"
            # Loop must still be running after the exception.
            assert w._polling_task is not None
            assert not w._polling_task.done()
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_cancelled_error_from_scan_propagates(
        self,
        tmp_letterbox_home: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """G4 defense-in-depth: a ``CancelledError`` surfacing from
        ``_scan_channel`` (e.g. from a future async refactor) MUST
        propagate past the polling loop's generic exception handler,
        not get swallowed as "scan failed". Locked via the explicit
        ``except asyncio.CancelledError: raise`` re-raise.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            def _cancel(channel_dir: Path) -> list[Path]:
                raise asyncio.CancelledError()

            monkeypatch.setattr(watcher_mod, "list_messages", _cancel)
            # Wait for the polling task to end (cancelled) via the
            # synthetic CancelledError.
            task = w._polling_task
            assert task is not None
            await wait_for(lambda: task.done(), timeout=1.0)
            assert task.cancelled() or isinstance(
                task.exception(), asyncio.CancelledError
            )
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_no_pending_task_warning_after_stop(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G1 — ``contextlib.suppress(CancelledError) + await`` keeps
        the cancelled task from emitting ``RuntimeWarning: coroutine
        ... was never awaited``. Under ``filterwarnings = ["error"]``,
        such a warning would fail the test; we override locally so we
        can record + assert explicitly.
        """
        import gc
        import warnings

        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            w = Watcher(
                ch,
                self_sender=_SELF_SENDER,
                self_instance_id=_SELF_INSTANCE,
                queue=queue,
                poll_interval=0.1,
            )
            await w.start()
            await w.stop()
            gc.collect()
        runtime_warnings = [
            x for x in caught if issubclass(x.category, RuntimeWarning)
        ]
        assert runtime_warnings == [], (
            "unexpected RuntimeWarnings after stop: "
            f"{[str(w.message) for w in runtime_warnings]}"
        )


# ──────────────────────────────────────────────────────────────
# Local fixture for the 4d directory-mode assertion
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def permissive_umask():
    """Save/restore the process umask for tests that assert created-dir modes.

    Cloned from ``tests/test_channel.py:91`` per 4d scout brief (single
    new consumer — kept local rather than promoted to ``conftest.py``).
    Setting ``os.umask(0)`` lets ``mkdir(mode=0o700)`` land its full mode
    without the default ``0o022`` mask altering the assertion outcome.
    """
    old = os.umask(0)
    try:
        yield
    finally:
        os.umask(old)


# Substrings the implementation uses in its WARN messages so tests can
# isolate via caplog substring filter without false positives from filter
# 5's ``[watcher] skipping malformed peer message ...`` WARN or 4c's
# ``[polling] scan failed ...`` outer-handler WARN.
_COLLISION_WARN_SUBSTRING = "[identity-collision]"
_MISSING_DIR_WARN_SUBSTRING = "[channel-missing]"


def _collision_warns(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Filter caplog records for 4d identity-collision WARN substring."""
    return [
        r for r in caplog.records
        if r.name == "letterbox.watcher"
        and r.levelno == logging.WARNING
        and _COLLISION_WARN_SUBSTRING in r.getMessage()
    ]


def _missing_dir_warns(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Filter caplog records for 4d channel-missing WARN substring."""
    return [
        r for r in caplog.records
        if r.name == "letterbox.watcher"
        and r.levelno == logging.WARNING
        and _MISSING_DIR_WARN_SUBSTRING in r.getMessage()
    ]


# ──────────────────────────────────────────────────────────────
# TestIdentityCollision — 4d filter 5b (sender match + instance differ)
# ──────────────────────────────────────────────────────────────


class TestIdentityCollision:
    """4d K1/K2/K7 — instance-level WARN dedupe at the _on_fs_event chokepoint.

    The collision gate is ``result.sender == self._self_sender AND
    result.instance_id != self._self_instance_id`` (filter 5b, inserted
    between filter 5 and filter 6). The WARN fires exactly once per
    channel per Watcher session; ``start()`` re-arms.

    Per K7, the collision message itself drops at filter 6 under the
    existing ``_is_own_write`` OR semantic. "Conversation continues" is
    re-interpreted as structural-survival (watcher stays alive, polling
    loop keeps running, ``stop()`` terminates cleanly) — NOT
    message-delivery. The architectural tension is surfaced in
    ``IMPLEMENTATION_NOTES`` for vision-review reconciliation.
    """

    @pytest.mark.asyncio
    async def test_warn_fires_once_on_first_collision(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """First collision peer message → exactly one WARN with channel
        name and colliding sender label.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # Collision: peer claims our sender label, but a distinct
                # instance_id (since instance_id is per-process).
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other",
                    body="collision",
                )
                await asyncio.sleep(0.3)
            warns = _collision_warns(caplog)
            assert len(warns) == 1
            msg = warns[0].getMessage()
            assert "ch01" in msg
            assert _SELF_SENDER in msg
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_warn_does_not_re_fire_on_second_collision(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Two collision peer messages in one session → exactly one WARN.

        Locks K1's instance-level dedupe via
        ``_warned_collision_channels`` (per channel, per session).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-first",
                    body="c1",
                )
                await asyncio.sleep(0.2)
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-second",
                    body="c2",
                )
                await asyncio.sleep(0.3)
            warns = _collision_warns(caplog)
            assert len(warns) == 1, (
                f"expected exactly one WARN, got {len(warns)}: "
                f"{[r.getMessage() for r in warns]}"
            )
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_warn_re_fires_after_stop_and_restart(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """``start()`` re-arms the WARN dedupe set.

        Locks K1 / Vision §3.4.1 "every session start until user fixes":
        a session is a ``start()`` cycle. Stop, restart, second collision
        → second WARN.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )

        with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
            await w.start()
            try:
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other-a",
                    body="c1",
                )
                await asyncio.sleep(0.3)
            finally:
                await w.stop()
            assert len(_collision_warns(caplog)) == 1

            # Re-arm: same Watcher instance, fresh start(). A new
            # collision message (later timestamp → passes the
            # recomputed watermark) must fire a second WARN.
            await w.start()
            try:
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other-b",
                    body="c2",
                )
                await asyncio.sleep(0.3)
            finally:
                await w.stop()
        assert len(_collision_warns(caplog)) == 2

    @pytest.mark.asyncio
    async def test_collision_message_dropped_at_filter_6_per_or_semantic(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """K7 — collision message drops at filter 6 (``_is_own_write`` OR).

        Vision §3.4.1 wording "conversation continues via instance_id"
        is INCONSISTENT with the existing OR semantic at
        ``letterbox/channel.py:847-898`` (``sender_match OR
        instance_match``). For a collision peer message
        (``sender == self_sender, instance_id != self_instance_id``)
        the filter evaluates ``True OR False = True`` and drops the
        message — the agent never sees it. The WARN is the user-facing
        diagnostic to fix ``--as``; "conversation continues" is
        re-interpreted as structural-survival (see
        ``test_watcher_stays_alive_through_repeated_collisions``).

        Reconciliation paths (out of 4d scope): (a) amend Vision §3.4.1
        wording, or (b) change ``_is_own_write`` to AND in a remediation
        phase superseding ADR-022. Logged in IMPLEMENTATION_NOTES.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other",
                    body="dropped-at-filter-6",
                )
                await asyncio.sleep(0.3)
            # WARN fired ...
            assert len(_collision_warns(caplog)) == 1
            # ... AND the queue stays empty (message dropped by filter 6).
            assert queue.qsize() == 0
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_watcher_stays_alive_through_repeated_collisions(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Structural-survival re-interpretation of K7 (per PHASE_INDEX 4d
        TDD line "conversation continues thereafter").

        Five collision messages → exactly 1 WARN; watcher remains alive,
        polling loop keeps running, a follow-up *legitimate* peer message
        (distinct sender) still reaches the queue, and ``stop()``
        terminates cleanly within ``_STOP_JOIN_TIMEOUT_SECONDS``.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                for i in range(5):
                    make_peer_message(
                        ch,
                        peer_sender=_SELF_SENDER,
                        peer_instance=f"lb-other-{i}",
                        body=f"c{i}",
                    )
                await asyncio.sleep(0.4)
            assert len(_collision_warns(caplog)) == 1
            # Polling task still alive after the collision storm.
            assert w._polling_task is not None
            assert not w._polling_task.done()
            # A legitimate peer message (distinct sender) still flows.
            make_peer_message(ch, body="legitimate-after-collisions")
            await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
        finally:
            await w.stop()
        # stop() terminated cleanly (no exception escaped the finally).

    @pytest.mark.asyncio
    async def test_true_own_write_drops_silently_no_warn(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """True own-write (sender AND instance_id match) → no WARN.

        The collision gate is ``sender == self_sender AND instance_id !=
        self_instance_id``; a true own-write fails the second half
        (``instance_id == self_instance_id``), so filter 5b does NOT
        fire and only filter 6 drops the message silently per ADR-022.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_self_message(ch, body="my-own-echo")
                await asyncio.sleep(0.3)
            assert _collision_warns(caplog) == []
            assert queue.qsize() == 0
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_two_channels_each_warn_once(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """G3 — dedupe key is ``channel.name``; multi-channel watcher
        fires one WARN per colliding channel.
        """
        ch_a = make_channel(tmp_letterbox_home, name="cha")
        ch_b = make_channel(tmp_letterbox_home, name="chb")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch_a, ch_b],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_peer_message(
                    ch_a,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other-a",
                    body="ca",
                )
                make_peer_message(
                    ch_b,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other-b",
                    body="cb",
                )
                await asyncio.sleep(0.3)
            warns = _collision_warns(caplog)
            channel_names_in_warns = {
                name for name in ("cha", "chb")
                for r in warns if name in r.getMessage()
            }
            assert len(warns) == 2, (
                f"expected one WARN per channel, got {len(warns)}"
            )
            assert channel_names_in_warns == {"cha", "chb"}
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_warn_text_includes_remediation_hint(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Vision §3.4.1 — WARN must point at ``--as`` and
        ``LETTERBOX_SENDER`` as remediation paths, and name the channel
        + colliding sender. Substring-level only (full wording is
        implementer's-latitude per §14).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other",
                    body="hint",
                )
                await asyncio.sleep(0.3)
            warns = _collision_warns(caplog)
            assert len(warns) == 1
            msg = warns[0].getMessage()
            assert "--as" in msg
            assert "LETTERBOX_SENDER" in msg
            assert "ch01" in msg
            assert _SELF_SENDER in msg
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_warn_fires_on_polling_path_too(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Both producers (watchdog + polling) traverse the SAME
        ``_on_fs_event`` chokepoint, so filter 5b fires from either
        path. With watchdog disabled (K3 test seam), the polling tick
        delivers the collision and the WARN still fires.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            assert w._observer is None  # K3 seam: no observer.
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_peer_message(
                    ch,
                    peer_sender=_SELF_SENDER,
                    peer_instance="lb-other",
                    body="via-polling",
                )
                # ~3-4 polling ticks.
                await asyncio.sleep(0.4)
            assert len(_collision_warns(caplog)) == 1
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestChannelDirDeletion — 4d K3/K4 (lazy mkdir + per-outage dedupe)
# ──────────────────────────────────────────────────────────────


class TestChannelDirDeletion:
    """4d K3/K4 — polling tick recovers from a deleted channel directory.

    Vision §3.6: "Watcher logs WARN and re-creates the directory on next
    message." 4d implements this on the polling-tick path (the watchdog
    inotify watch is invalidated by the kernel when the inode vanishes,
    so the polling tick is the natural rediscovery trigger). The WARN
    fires once per outage; ``_warned_missing_channels.discard`` re-arms
    when the directory has files again.
    """

    @pytest.mark.asyncio
    async def test_polling_handles_missing_dir_with_warn(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Delete the channel dir → next polling tick logs one WARN
        naming the channel.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                shutil.rmtree(ch.path)
                await asyncio.sleep(0.4)
            warns = _missing_dir_warns(caplog)
            assert len(warns) == 1
            assert "ch01" in warns[0].getMessage()
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_recreates_missing_dir_with_mode_0700(
        self,
        tmp_letterbox_home: Path,
        permissive_umask: None,
    ) -> None:
        """K3 — re-created directory lands at mode ``0o700``.

        Under permissive umask, ``mkdir(mode=0o700)`` would land literal
        ``0o700`` even without ``os.chmod``; the explicit chmod (per 3a
        precedent) is the symmetric belt-and-suspenders guarantee.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            shutil.rmtree(ch.path)
            await wait_for(lambda: ch.path.exists(), timeout=1.0)
            assert oct(ch.path.stat().st_mode & 0o777) == "0o700"
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_recreates_missing_dir_with_watchdog_reschedule(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """G2 — re-arm watchdog on the re-created path.

        ``Observer.schedule`` is wrapped to record post-mkdir
        invocations; assert it is called again with the channel path
        AFTER the deletion (the initial ``start()`` call doesn't count).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            assert w._observer is not None
            # Wrap schedule() to record post-start invocations while
            # still delegating to the real method (watchdog must keep
            # working on the re-created dir for the structural-recovery
            # observable below).
            real_schedule = w._observer.schedule
            calls: list[str] = []

            def recording_schedule(handler, path, **kwargs):
                calls.append(str(path))
                return real_schedule(handler, path, **kwargs)

            w._observer.schedule = recording_schedule  # type: ignore[method-assign]

            shutil.rmtree(ch.path)
            await wait_for(
                lambda: any(str(ch.path) == p for p in calls), timeout=1.0
            )
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_missing_dir_warn_does_not_re_fire_on_second_tick_while_still_missing(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """K4 — fire-once-per-outage. Force ``list_messages`` to keep
        raising ``FileNotFoundError`` across multiple ticks; only ONE
        WARN must land.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            # Patch AFTER start() so the watermark-compute call at
            # start() is unaffected; only polling-tick calls explode.
            def _missing(channel_dir: Path) -> list[Path]:
                raise FileNotFoundError(f"simulated missing dir: {channel_dir}")

            monkeypatch.setattr(watcher_mod, "list_messages", _missing)
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # ~4-5 polling ticks.
                await asyncio.sleep(0.5)
            warns = _missing_dir_warns(caplog)
            assert len(warns) == 1, (
                f"expected exactly one WARN across multiple ticks, "
                f"got {len(warns)}"
            )
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_missing_dir_warn_re_fires_after_recovery_and_redeletion(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """K4 — discard from the dedupe set when files arrive again.

        Delete → tick (WARN+mkdir) → peer writes file (recovery
        confirmed) → delete → tick → second WARN.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                # Outage 1.
                shutil.rmtree(ch.path)
                await wait_for(lambda: ch.path.exists(), timeout=1.0)
                await wait_for(
                    lambda: len(_missing_dir_warns(caplog)) >= 1,
                    timeout=1.0,
                )
                # Recovery: peer writes a message → polling tick should
                # discard channel from _warned_missing_channels.
                make_peer_message(ch, body="recovery")
                await wait_for(lambda: queue.qsize() >= 1, timeout=1.0)
                # Outage 2.
                shutil.rmtree(ch.path)
                await wait_for(
                    lambda: len(_missing_dir_warns(caplog)) >= 2,
                    timeout=1.0,
                )
            assert len(_missing_dir_warns(caplog)) == 2
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_missing_dir_does_not_affect_other_channels(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Per-channel isolation: deleting ch_a's dir does not WARN for
        ch_b, and ch_b keeps working.
        """
        ch_a = make_channel(tmp_letterbox_home, name="cha")
        ch_b = make_channel(tmp_letterbox_home, name="chb")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            [ch_a, ch_b],
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                shutil.rmtree(ch_a.path)
                # ch_b stays alive — peer writes there, message flows.
                make_peer_message(ch_b, body="survivor-on-b")
                await wait_for(lambda: queue.qsize() >= 1, timeout=1.0)
                await wait_for(
                    lambda: len(_missing_dir_warns(caplog)) >= 1,
                    timeout=1.0,
                )
            warns = _missing_dir_warns(caplog)
            # Exactly one WARN, for cha only.
            assert len(warns) == 1
            assert "cha" in warns[0].getMessage()
            assert "chb" not in warns[0].getMessage()
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_polling_only_recovery_works_without_watchdog(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """K3 test seam: with ``_watchdog_enabled=False`` the recovery
        path still runs — mkdir lands, no re-schedule (no observer to
        schedule on), no crash.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            assert w._observer is None
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                shutil.rmtree(ch.path)
                await wait_for(lambda: ch.path.exists(), timeout=1.0)
                await wait_for(
                    lambda: len(_missing_dir_warns(caplog)) >= 1,
                    timeout=1.0,
                )
            # Polling task survived the recovery cycle.
            assert w._polling_task is not None
            assert not w._polling_task.done()
        finally:
            await w.stop()


# ──────────────────────────────────────────────────────────────
# TestRecoveryContracts — Vision §3.6 absence-of-behavior tests
# ──────────────────────────────────────────────────────────────


class TestRecoveryContracts:
    """4d K5/K6 — locks the Vision §3.6 rows that need NO new code.

    These tests assert structural invariants the watcher already
    upholds: future-dated peer timestamps pass through, no rescan fires
    on a fresh ``start()`` over a populated channel, orphaned ``.tmp``
    files stay ignored on the polling path, and the watcher never reads
    ``.read/`` for the start watermark.
    """

    @pytest.mark.asyncio
    async def test_future_dated_peer_timestamp_accepted(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """K5 — Vision §3.6 row: future-dated peer timestamps pass
        through (no validation, no WARN). The ``WatcherEvent.timestamp``
        is the watcher's own ``datetime.now(UTC).isoformat()``, NOT the
        peer's — trusted-source discipline (Vision §6.4 / §13.3).
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            future = datetime.now(timezone.utc) + timedelta(days=400)
            with caplog.at_level(logging.WARNING, logger="letterbox.watcher"):
                make_peer_message(ch, body="from-the-future", timestamp=future)
                await wait_for(lambda: queue.qsize() >= 1, timeout=2.0)
            # No WARN of any kind (collision, missing-dir, [watcher],
            # [polling]) fired in response to the future timestamp.
            assert _collision_warns(caplog) == []
            assert _missing_dir_warns(caplog) == []
            unexpected = [
                r for r in caplog.records
                if r.name == "letterbox.watcher"
                and r.levelno >= logging.WARNING
            ]
            assert unexpected == [], (
                f"unexpected WARN/ERROR records: "
                f"{[r.getMessage() for r in unexpected]}"
            )
            evt = queue.get_nowait()
            # WatcherEvent.timestamp is the watcher's own now-time, not
            # the peer's future-dated stem. We can't assert exact value
            # but it must NOT be the peer's far-future ISO timestamp.
            assert isinstance(evt.timestamp, str)
            assert future.isoformat() != evt.timestamp
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_no_rescan_on_fresh_start_in_populated_channel(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """K6 — restart = fresh start = no backlog flood.

        Channel has 10 pre-existing peer messages; a fresh Watcher's
        ``start()`` picks the lex-last filename as the watermark, so
        nothing pre-existing reaches the queue. Vision §3.6.1 / ADR-024:
        "watcher's job is L3 for new arrivals, not historical replay."
        Rescan = user calls ``check_messages`` (3c), not the watcher.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=10)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.4)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_orphaned_tmp_file_ignored_during_polling(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """An orphaned ``msg-...json.tmp`` file (e.g., from a crashed
        writer) is dropped at filter 1 (basename regex). Polling tick
        does not emit it, even with no other producers around.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
            _watchdog_enabled=False,
        )
        await w.start()
        try:
            stem = make_message_filename(
                datetime(2099, 1, 1, tzinfo=timezone.utc)
            ).removesuffix(".json")
            tmp = ch.path / (stem + ".json.tmp")
            tmp.write_bytes(b'{"id": "orphaned"}')
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.4)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_no_rescan_advances_when_user_read_state_file_exists(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """The watcher NEVER consults ``.read/{label}.json`` for the
        start watermark — ADR-021 / Vision §3.6.1 separation of
        concerns. ``check_messages`` (3c) is the one consumer of
        ``high_water_mark``.

        Set up: 10 pre-existing messages + a ``.read/`` file claiming
        ``high_water_mark=""``. If the watcher consulted ``.read/`` it
        would flood the queue with all 10. Watcher uses the lex-last
        filename instead → zero queue events.
        """
        ch = make_channel(tmp_letterbox_home, name="ch01")
        populate_peer_msgs(ch, count=10)
        read_dir = ch.path / ".read"
        read_dir.mkdir(exist_ok=True)
        (read_dir / f"{_SELF_SENDER}.json").write_text(
            '{"high_water_mark": ""}', encoding="utf-8"
        )
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
            poll_interval=0.1,
        )
        await w.start()
        try:
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.4)
        finally:
            await w.stop()


class TestDirectedAddressing:
    """Filter 7: directed messages notify only the recipient (observable, not notifiable).

    A message carrying a non-empty ``recipient`` that is NOT this endpoint must
    not wake the agent (no queue event) — but it is deliberately NOT hidden: it
    stays on disk and remains readable via ``check_messages``. A broadcast
    (``recipient`` None) or a message directed AT this endpoint notifies normally.
    """

    @staticmethod
    def _write_directed(ch: Channel, *, recipient: str | None) -> None:
        stem = make_message_filename(
            datetime(2099, 1, 1, tzinfo=timezone.utc)
        ).removesuffix(".json")
        msg = new_message(
            id=stem,
            channel=ch.name,
            instance_id=_PEER_INSTANCE,
            sender=_PEER_SENDER,
            body="directed",
            recipient=recipient,
        )
        write_message(ch.path, msg)

    @pytest.mark.asyncio
    async def test_directed_elsewhere_is_not_notified(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            # peer → claude-c; we are claude-a, so no 📬 for us.
            self._write_directed(ch, recipient="claude-c")
            with pytest.raises(TimeoutError):
                await wait_for(lambda: queue.qsize() > 0, timeout=0.8)
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_directed_at_me_is_notified(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            self._write_directed(ch, recipient=_SELF_SENDER)
            await wait_for(lambda: queue.qsize() > 0, timeout=2.0)
            assert queue.qsize() == 1
        finally:
            await w.stop()

    @pytest.mark.asyncio
    async def test_broadcast_is_notified(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = make_channel(tmp_letterbox_home, name="ch01")
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            ch,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=queue,
        )
        await w.start()
        try:
            self._write_directed(ch, recipient=None)
            await wait_for(lambda: queue.qsize() > 0, timeout=2.0)
            assert queue.qsize() == 1
        finally:
            await w.stop()

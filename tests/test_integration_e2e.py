"""End-to-end integration tests — Phase 10b (the channel-system contracts).

8d proved the *headline* two-process exchange (T1) over the **real
``mcp.client.stdio`` transport** — that closed W14 and is NOT re-proven here.
This module is the integration evidence for the channel system's three
remaining end-to-end properties:

* **T2 — channel isolation:** endpoint B on channel ``01`` and endpoint D on
  channel ``02`` never cross-talk, even when the two channels hold
  byte-identical message filenames. Isolation is by *directory* (W6 demux),
  not by id.
* **T3 — burst integrity:** a burst of 100 peer messages on one channel reaches
  the receiver with **no loss** (all 100 notifications fire) and **no reorder on
  the durable read path** (``list_messages`` / ``Channel.list_unread`` return
  them filename-sorted, contiguous, no dupes — W1/W2/W4/W6).
* **T4 — recovery composition:** the four Vision §3.6 recovery entries hold when
  composed in a running system — orphaned ``.tmp`` + startup reaper (a),
  malformed JSON skip + surface (b), deleted channel dir mid-run (c), broken
  read-state (d). The Ironclad Invariant in motion: the system heals without
  ever destroying user data (W1/W2/W3/W4).

**Altitude (K1).** The "peer send" in every test is a **direct
``protocol.write_message`` to the shared channel directory** — the *exact*
bytes the MCP ``send_message`` tool produces (7b builds a ``Message`` via
``new_message`` then ``write_message``), and the precise pattern 8b/8c/8d use.
Spinning a real ``letterbox mcp`` stdio subprocess per send would re-prove the
SATISFIED W14 row and make a 100-message burst brutally slow and flaky. The
W-rows under test (W1/W2/W3/W4/W6) are filesystem/channel/watcher wirings; the
**receiver** is the real wiring — a live ``run_launcher`` (real ``Watcher`` +
injection loop + PTY) where notification delivery is the proof, or a real
``Watcher`` / channel-layer call where the PTY adds no wiring coverage.

The MCP stdio *transport* is covered at 8d/T1 — it is not forgotten, it is
elsewhere.

**Cite the unit, prove the system (T4).** The recovery primitives (2d reaper,
2c ``ParseError`` + ``.tmp`` glob, 4d ``.broken`` rename / dir re-creation, 3b
read-state recovery) are all unit-locked already. T4 does NOT re-prove them — it
cites the owning unit test in a comment and asserts the end-to-end outcome.

Idioms are **cloned, not imported** (the clone-per-file convention 5a–8d follow;
K5). 10b is the *third* consumer of the 8d launcher-e2e toolkit, firing the
"promote on third use" trigger — but promotion would churn green, release-
critical 8c/8d files in the 2026-06-02 freeze window, so the toolkit is cloned
locally and the promotion candidate is logged in IMPLEMENTATION_NOTES as
deferred debt (K5).

Module pinned to the ``watcher`` xdist group (real ``watchdog.Observer`` +
inotify — same pin as ``test_launcher.py`` / ``test_launcher_e2e.py``); without
it, ``-n auto`` can exhaust ``fs.inotify.max_user_instances`` (T2 doubles the
observer count per test).

No ``@pytest.mark.budget`` anywhere — 10b is correctness; 10c owns the e2e
budgets on the same surface.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sys
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import letterbox.adapters.base as base
from letterbox import launcher
from letterbox.adapters.base import Adapter, register_adapter
from letterbox.channel import Channel, read_state
from letterbox.launcher import LauncherSession, run_launcher
from letterbox.protocol import (
    ParseError,
    list_messages,
    make_message_filename,
    new_message,
    read_message,
    write_message,
)
from letterbox.watcher import Watcher, WatcherEvent
from tests.conftest import FakeHarness
from tests.helpers import wait_for

pytestmark = pytest.mark.xdist_group("watcher")

# fake_harness can't interrupt its blocking stdin read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# K3 — the template carries {message_id} so each rendered notification is a
# distinct, countable token. The 8d default "📬 {channel}" renders identically
# for every message, which makes "did all 100 arrive?" unprovable (G4).
_COUNTABLE_TEMPLATE = "📬 {channel} {message_id}"

# A message-id stem is msg-YYYYMMDDTHHMMSSffffff-{uuid4hex} (ADR-028:
# ^msg-\d{8}T\d{6}\d{6}-[0-9a-f]{32}$). This finds the stems the watcher renders
# into the echo so T3 can count the distinct set and T4(a) can prove ONLY the
# good message was notified.
_MID_RE = re.compile(rb"msg-\d{8}T\d{12}-[0-9a-f]{32}")

# Distinct peer identities. A peer message must differ from the receiver on
# BOTH sender AND instance_id to pass the ADR-022 combined own-write OR-filter
# and fire (proven at 8b/8d). The receivers use labels "b"/"d"; the peers use
# "a"/"c" with their own instance ids.
_PEER_A_SENDER = "a"
_PEER_A_INSTANCE = "lb-peer-a"
_PEER_C_SENDER = "c"
_PEER_C_INSTANCE = "lb-peer-c"


# ──────────────────────────────────────────────────────────────────────
# Cloned fixtures / helpers (clone-per-file convention 5a–8d; K5)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def reset_registry(monkeypatch: pytest.MonkeyPatch) -> dict[str, type[Adapter]]:
    """Replace the module-level ``_REGISTRY`` with a fresh empty dict (cloned 8d)."""
    fresh: dict[str, type[Adapter]] = {}
    monkeypatch.setattr(base, "_REGISTRY", fresh)
    return fresh


@pytest.fixture
def fake_adapter(reset_registry: dict[str, type[Adapter]]) -> type[Adapter]:
    """Register a placeholder ``fakeharness`` adapter into the reset registry.

    Its class attrs are deliberately placeholders — the launcher (ADR-041)
    overrides ``command`` / ``default_args`` / ``notification_template`` from
    config at launch, so the spawn never uses these. They exist only to satisfy
    ``register_adapter``'s non-empty validation. (Cloned 8d.)
    """

    @register_adapter
    class _FakeHarnessAdapter(Adapter):
        name = "fakeharness"
        command = "fakeharness-placeholder"
        default_args = ["placeholder"]
        notification_template = "placeholder {channel}"

    return _FakeHarnessAdapter


def _register_fake_adapter(name: str) -> None:
    """Register a placeholder adapter under ``name`` into the active registry.

    T2 needs *two* receivers with distinct echo files, so it needs two harness
    names (one ``[harness.<name>]`` config block + one registry entry each). The
    8d ``fake_adapter`` fixture only registers a single ``fakeharness``; this
    helper builds the same placeholder shape dynamically for an arbitrary name.
    ``Adapter`` has no ``@abstractmethod``s (only ``__new__`` blocks ``cls is
    Adapter``), so a ``type()``-built subclass carrying the four validated class
    attrs is instantiable exactly like the fixture's ``_FakeHarnessAdapter``.
    """
    register_adapter(
        type(
            f"_FakeAdapter_{name}",
            (Adapter,),
            {
                "name": name,
                "command": "placeholder",
                "default_args": ["placeholder"],
                "notification_template": "placeholder {channel}",
            },
        )
    )


def _harness_block(
    name: str, *, command: str, default_args: list[str], template: str
) -> str:
    """Render one ``[harness.<name>]`` TOML block.

    POSIX tmp paths carry no quotes/backslashes, so naive double-quoting is safe
    (cloned 8d ``_write_harness_config`` quoting discipline).
    """
    args_items = ", ".join(f'"{a}"' for a in default_args)
    return (
        f"[harness.{name}]\n"
        f'command = "{command}"\n'
        f"default_args = [{args_items}]\n"
        f'notification_template = "{template}"\n'
    )


def _write_config(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, *blocks: str
) -> None:
    """Write one or more harness blocks and point LETTERBOX_CONFIG at the file.

    LETTERBOX_CONFIG is the project-local config override (config.py K2), the
    only config-file hook that doesn't read the real ``~/.letterbox``. (Cloned
    8d ``_write_harness_config``, generalised to N blocks for T2's two
    receivers.)
    """
    config_path.write_text("\n".join(blocks), encoding="utf-8")
    monkeypatch.setenv("LETTERBOX_CONFIG", str(config_path))


def _patch_sleeper_mcp_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Path:
    """Make ``setup_launcher`` emit a benign sleeper MCP config (cloned 8d).

    ``setup_launcher`` always wires ``--mcp-config`` at a config whose command is
    the ``letterbox`` console script, which isn't on PATH in the test venv. 10b
    cares about the receive path, not the receiver's own MCP-child topology
    (that is 8d/Test 2), so we point each receiver's fake_harness child at a
    harmless sleeper — fake_harness spawns and reaps it without the console
    script. With two receivers (T2) both share this module-wide monkeypatched
    sleeper, which is fine (G7 — neither test cares about its own MCP child).
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
    """Capture each ``LauncherSession`` ``run_launcher`` builds internally (cloned 8d).

    ``run_launcher`` owns its session privately, but tests need its
    ``channel`` / ``state_dir`` / ``instance_id`` (to write peer messages into
    the right dir and run the read-path asserts) and must ``wait_for`` the
    watcher to start before any peer write (the post-start-write rule, ADR-024).
    Two receivers (T2) append two sessions to the list, in setup order — tests
    that need a specific one match on ``session.channel.name``.
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
    """Assert the §2.1 clean-exit contract: no orphan process, no temp file (cloned 8d)."""
    assert session.handle.process.poll() is not None  # harness reaped
    assert not session.mcp_config_path.exists()  # temp MCP config deleted
    assert session.watcher._started is False  # watcher stopped


def _write_peer_message(
    channel: Channel,
    *,
    sender: str,
    instance_id: str,
    body: str = "peer",
    timestamp: datetime | None = None,
) -> Path:
    """Write one peer message into ``channel`` and return its path (cloned 8b).

    These are the exact bytes the MCP ``send_message`` tool writes (7b:
    ``make_message_filename`` → ``new_message`` → ``write_message``). ``sender``
    and ``instance_id`` are required keyword-only here (no peer-default) so each
    isolation/recovery call states its peer identity explicitly. Distinct
    ``sender`` + ``instance_id`` from the receiver's self values let the message
    survive the ADR-022 own-write filter.
    """
    stem = make_message_filename(timestamp=timestamp).removesuffix(".json")
    msg = new_message(
        id=stem,
        channel=channel.name,
        instance_id=instance_id,
        sender=sender,
        body=body,
    )
    return write_message(channel.path, msg)


def _read_echo(path: Path) -> bytes:
    """Return everything a fake_harness has echoed to ``path``, or b"" if untouched.

    For receivers whose echo file isn't the ``fake_harness`` fixture's (T2's
    second receiver), this reads the raw bytes. Reading the echo *file* (not the
    PTY master fd) means the injected ``\\r`` survives literally, so substring
    assertions must tolerate the trailing CR (G6).
    """
    return path.read_bytes() if path.exists() else b""


def _future_ts(seconds: float = 1.0) -> datetime:
    """A UTC timestamp safely *after* a watcher's start watermark.

    A watcher started on an empty channel synthesises its watermark from the
    current wall clock (4b). A peer message written *before* that watermark is
    backlog and is deliberately never injected (ADR-024 / G2). Future-dating the
    write by a margin guarantees it sorts strictly above the watermark and is
    delivered (§3.6 — future-dated timestamps are accepted as-is). Constructing
    the base once and adding monotonic microseconds keeps filename order
    well-defined for the no-reorder proof (K4 / G3).
    """
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


# ══════════════════════════════════════════════════════════════════════
# T2 — Channel isolation (launcher layer; two receivers)
# ══════════════════════════════════════════════════════════════════════


class TestChannelIsolation:
    """T2 — endpoint B(01) and D(02) never cross-talk, even with identical filenames.

    Proves W6 (watcher demultiplexes by channel directory) + W2/W4 (read/own-write
    filter scoped per channel). The two peer messages share a byte-identical
    filename stem written into the two different channel dirs, so the ONLY thing
    that separates them is the directory — isolation is structural, not by id.
    """

    @pytest.mark.asyncio
    async def test_channels_isolate_by_directory_not_filename(
        self,
        fake_harness: FakeHarness,
        reset_registry: dict[str, type[Adapter]],
        tmp_letterbox_home: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Two receivers need two harness names (one echo file + config block
        # each). B → "hb" on channel "01"; D → "hd" on channel "02".
        _register_fake_adapter("hb")
        _register_fake_adapter("hd")
        echo_b = fake_harness.echo_file
        echo_d = tmp_path / "fake_harness_echo_d.bin"
        _write_config(
            tmp_path / "letterbox.toml",
            monkeypatch,
            _harness_block(
                "hb",
                command=sys.executable,
                default_args=[
                    str(fake_harness.script_path),
                    "--echo-to",
                    str(echo_b),
                ],
                template=_COUNTABLE_TEMPLATE,
            ),
            _harness_block(
                "hd",
                command=sys.executable,
                default_args=[
                    str(fake_harness.script_path),
                    "--echo-to",
                    str(echo_d),
                ],
                template=_COUNTABLE_TEMPLATE,
            ),
        )
        _patch_sleeper_mcp_config(monkeypatch, tmp_path)
        captured = _spy_setup_launcher(monkeypatch)

        task_b = asyncio.create_task(
            run_launcher(
                "hb", "01", as_label="b", cwd=tmp_path, teardown_timeout=_FAST_TEARDOWN
            )
        )
        task_d = asyncio.create_task(
            run_launcher(
                "hd", "02", as_label="d", cwd=tmp_path, teardown_timeout=_FAST_TEARDOWN
            )
        )
        try:
            # Post-start write only (ADR-024 / G2): wait for BOTH watchers to
            # start before writing, or a pre-start write is backlog → no inject.
            await wait_for(
                lambda: len(captured) == 2
                and all(s.watcher._started for s in captured),
                timeout=10.0,
            )
            by_name = {s.channel.name: s for s in captured}
            session_b = by_name["01"]
            session_d = by_name["02"]

            # One peer message into 01 (sender "a") and one into 02 (sender "c"),
            # written with the SAME filename stem so isolation cannot lean on id
            # uniqueness — only the directory separates them.
            ts = _future_ts()
            shared_stem = make_message_filename(timestamp=ts).removesuffix(".json")
            write_message(
                session_b.channel.path,
                new_message(
                    id=shared_stem,
                    channel="01",
                    instance_id=_PEER_A_INSTANCE,
                    sender=_PEER_A_SENDER,
                    body="to-01",
                ),
            )
            write_message(
                session_d.channel.path,
                new_message(
                    id=shared_stem,
                    channel="02",
                    instance_id=_PEER_C_INSTANCE,
                    sender=_PEER_C_SENDER,
                    body="to-02",
                ),
            )

            notif_01 = ("📬 01 " + shared_stem).encode("utf-8")
            notif_02 = ("📬 02 " + shared_stem).encode("utf-8")

            # Each receiver sees its own channel's notification exactly once...
            await wait_for(
                lambda: _read_echo(echo_b).count(notif_01) == 1, timeout=10.0
            )
            await wait_for(
                lambda: _read_echo(echo_d).count(notif_02) == 1, timeout=10.0
            )

            # ...and NEVER the other channel's, despite the identical stem (W6).
            assert notif_02 not in _read_echo(echo_b)
            assert notif_01 not in _read_echo(echo_d)

            # Read-path isolation (W2/W4): list_unread on 01 returns only the 01
            # message; on 02 only the 02 message — even though both dirs hold an
            # identically-named file. (Plan §15 stretch, folded in: cheap and it
            # closes success-criterion #1's "read-path returns only its own".)
            ch_01 = Channel.get_or_create(
                "01", "b", "", state_dir=session_b.state_dir
            )
            ch_02 = Channel.get_or_create(
                "02", "d", "", state_dir=session_d.state_dir
            )
            unread_01 = ch_01.list_unread(self_instance_id=session_b.instance_id)
            unread_02 = ch_02.list_unread(self_instance_id=session_d.instance_id)
            assert [m.body for m in unread_01.messages] == ["to-01"]
            assert [m.body for m in unread_02.messages] == ["to-02"]
        finally:
            for task in (task_b, task_d):
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        for session in captured:
            _session_torn_down(session)
        # Stable post-teardown counts: no late cross-channel leak.
        assert _read_echo(echo_b).count(notif_01) == 1
        assert notif_02 not in _read_echo(echo_b)
        assert _read_echo(echo_d).count(notif_02) == 1
        assert notif_01 not in _read_echo(echo_d)


# ══════════════════════════════════════════════════════════════════════
# T3 — Burst of 100: no loss + no reorder (launcher layer + read-path assert)
# ══════════════════════════════════════════════════════════════════════


class TestBurstIntegrity:
    """T3 — 100 peer messages arrive with no loss and no reorder on the read path.

    No-loss is proven at the *notification* layer (all 100 distinct ids fire);
    no-reorder is proven on the *durable read path* (``list_messages`` /
    ``list_unread``), never on notification arrival order — under a burst,
    watchdog/poll event order is not guaranteed to match write order, so the
    only honest ordering guarantee is filename lexical sort = chronological
    (§3.2, K4). Proves W1 (atomic write), W2 (list/read), W4 (unread +
    own-write filter), W6 (event + poll fallback drop nothing).
    """

    _BURST = 100

    @pytest.mark.asyncio
    async def test_hundred_message_burst_no_loss_no_reorder(
        self,
        fake_harness: FakeHarness,
        fake_adapter: type[Adapter],
        tmp_letterbox_home: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_config(
            tmp_path / "letterbox.toml",
            monkeypatch,
            _harness_block(
                "fakeharness",
                command=sys.executable,
                default_args=[
                    str(fake_harness.script_path),
                    "--echo-to",
                    str(fake_harness.echo_file),
                ],
                template=_COUNTABLE_TEMPLATE,
            ),
        )
        _patch_sleeper_mcp_config(monkeypatch, tmp_path)
        captured = _spy_setup_launcher(monkeypatch)

        task = asyncio.create_task(
            run_launcher(
                "fakeharness",
                "burst",
                as_label="b",
                cwd=tmp_path,
                teardown_timeout=_FAST_TEARDOWN,
            )
        )
        try:
            await wait_for(
                lambda: bool(captured) and captured[0].watcher._started,
                timeout=10.0,
            )
            session = captured[0]

            # Burst-write 100 peer messages with explicit monotonically-
            # increasing timestamps (K4 / G3) so filename sort is unambiguous —
            # relying on now(UTC) for 100 tight writes risks same-microsecond
            # ties broken by the random UUID4 tail. One body carries Lithuanian +
            # emoji for the §13.2 UTF-8 round-trip check.
            base_ts = _future_ts()
            written: list[Path] = []
            for i in range(self._BURST):
                body = "ąčęėįš 📨 burst-0" if i == 0 else f"burst-{i}"
                written.append(
                    _write_peer_message(
                        session.channel,
                        sender=_PEER_A_SENDER,
                        instance_id=_PEER_A_INSTANCE,
                        body=body,
                        timestamp=base_ts + timedelta(microseconds=i),
                    )
                )

            # No-loss: every one of the 100 distinct message-ids fires a
            # notification. A dropped watchdog event would be backstopped by the
            # 5s poll fallback (W6) — a genuinely lost message is the exact
            # failure this catches. Generous timeout for poll-fallback headroom.
            await wait_for(
                lambda: len(set(_MID_RE.findall(fake_harness.read_echo())))
                == self._BURST,
                timeout=25.0,
            )

            # No-reorder, durable proof #1 (W1/W2): list_messages returns the
            # exact 100 written files in filename-sorted order, contiguous, no
            # gaps, no dupes. The writes are already in ascending order, so the
            # expected list is just their names sorted.
            expected_names = sorted(p.name for p in written)
            listed_names = [p.name for p in list_messages(session.channel.path)]
            assert listed_names == expected_names
            assert len(listed_names) == self._BURST
            assert len(set(listed_names)) == self._BURST  # no dupes

            # No-reorder, durable proof #2 (W4): list_unread returns all 100 peer
            # messages in filename order with has_more False at limit=100. Pass
            # limit=100 — the default 20 would truncate. The constructed Channel
            # carries sender_label "b" (the self_sender half of the own-write
            # filter); list_unread takes only self_instance_id.
            ch = Channel.get_or_create("burst", "b", "", state_dir=session.state_dir)
            result = ch.list_unread(
                self_instance_id=session.instance_id, limit=self._BURST
            )
            expected_ids = [name.removesuffix(".json") for name in expected_names]
            assert [m.id for m in result.messages] == expected_ids
            assert len(result.messages) == self._BURST
            assert result.has_more is False
            # §13.2 — UTF-8 round-trips end-to-end through write→read.
            assert result.messages[0].body == "ąčęėįš 📨 burst-0"
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        _session_torn_down(captured[0])
        assert len(set(_MID_RE.findall(fake_harness.read_echo()))) == self._BURST


# ══════════════════════════════════════════════════════════════════════
# T4 — Recovery composition (mixed layers; cite the unit, prove the system)
# ══════════════════════════════════════════════════════════════════════


class TestRecoveryComposition:
    """T4 — the four §3.6 recovery entries hold composed in a running system.

    Each sub-test composes an already-unit-locked primitive end-to-end. It does
    NOT re-prove the primitive — it cites the owning unit test in a comment and
    asserts the *system survives*. Proves W1, W2, W3, W4 under composition.
    """

    @pytest.mark.asyncio
    async def test_t4a_orphan_tmp_reaped_on_startup_good_message_still_flows(
        self,
        fake_harness: FakeHarness,
        fake_adapter: type[Adapter],
        tmp_letterbox_home: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """T4(a) — orphaned .tmp + startup reaper (launcher layer).

        Primitives: ``reap_orphan_tmp`` (2d — runs in ``setup_launcher`` startup,
        default 3600s threshold) and the ``.tmp`` glob exclusion (2c — readers
        glob ``msg-*.json``, which structurally excludes ``.tmp``). This composes
        both through a real launch.

        Deviation from plan §5's "plant ... plus one good peer .json" before
        startup: the good message is written *after* the watcher starts, not
        planted pre-launch. A pre-start write is backlog and is deliberately
        never injected (ADR-024 / G2 — which the plan itself flags), so a planted
        good .json could never satisfy success-criterion #3's "good message still
        delivered". Planting the two .tmp files pre-launch (the reaper's subject)
        and writing the good message post-start is the honest composition: it
        proves the reaper's startup work did not break the live path.
        """
        # Create the channel dir and plant the two .tmp files BEFORE launch so
        # the reaper (in setup_launcher startup) acts on them. Filenames end in
        # ".json.tmp" (the writer's real suffix) — the reaper filter is
        # end-anchored on ".json.tmp" (G8); ".tmp.json" would be ignored.
        ch = Channel.get_or_create("reaper", "b", "", state_dir=tmp_letterbox_home)
        stale_tmp = ch.path / (
            make_message_filename().removesuffix(".json") + ".json.tmp"
        )
        stale_tmp.write_bytes(b"stale-incomplete")
        old = time.time() - 7200  # >1h: backdate mtime so the reaper sweeps it
        os.utime(stale_tmp, (old, old))
        fresh_tmp = ch.path / (
            make_message_filename().removesuffix(".json") + ".json.tmp"
        )
        fresh_tmp.write_bytes(b"fresh-incomplete")  # mtime now → preserved

        _write_config(
            tmp_path / "letterbox.toml",
            monkeypatch,
            _harness_block(
                "fakeharness",
                command=sys.executable,
                default_args=[
                    str(fake_harness.script_path),
                    "--echo-to",
                    str(fake_harness.echo_file),
                ],
                template=_COUNTABLE_TEMPLATE,
            ),
        )
        _patch_sleeper_mcp_config(monkeypatch, tmp_path)
        captured = _spy_setup_launcher(monkeypatch)

        task = asyncio.create_task(
            run_launcher(
                "fakeharness",
                "reaper",
                as_label="b",
                cwd=tmp_path,
                teardown_timeout=_FAST_TEARDOWN,
            )
        )
        try:
            await wait_for(
                lambda: bool(captured) and captured[0].watcher._started,
                timeout=10.0,
            )
            session = captured[0]

            # The reaper ran in startup (before the watcher started): stale .tmp
            # swept, fresh .tmp preserved.
            assert not stale_tmp.exists()
            assert fresh_tmp.exists()

            # A good post-start peer message still delivers a notification — the
            # reaper's startup work didn't break the live path.
            good = _write_peer_message(
                session.channel,
                sender=_PEER_A_SENDER,
                instance_id=_PEER_A_INSTANCE,
                body="good",
                timestamp=_future_ts(),
            )
            good_id = good.name.removesuffix(".json").encode("utf-8")
            await wait_for(
                lambda: good_id in fake_harness.read_echo(), timeout=15.0
            )

            # Neither .tmp ever fired a notification: the only message-id token
            # in the echo is the good message's. (.tmp files are structurally
            # invisible to the watcher's msg-*.json glob.)
            assert set(_MID_RE.findall(fake_harness.read_echo())) == {good_id}
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        _session_torn_down(captured[0])

    @pytest.mark.asyncio
    async def test_t4b_malformed_json_skipped_then_surfaced_on_read(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """T4(b) — malformed JSON (watcher layer).

        Primitive: the watcher's filter-5 ``ParseError`` skip (4d K7 / 2c
        ``read_message``→``ParseError``). The watcher layer is the honest level —
        the PTY/adapter adds no W1–W4 coverage here.

        Asserts the *implemented* reality (G5), not the aspirational §3.6 wording:
        a malformed-body file with a valid filename fires NO notification (filter
        5 logs one WARN and skips, so the queue never sees it), is left in place
        (L8), and the good message still produces an event. The ``parse_error``
        surfacing is a separate *read-path* property: ``read_message`` returns a
        ``ParseError`` — i.e. ``check_messages`` would flag it without crashing.
        """
        ch = Channel.get_or_create("malformed", "b", "", state_dir=tmp_letterbox_home)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        watcher = Watcher(
            ch,
            self_sender="b",
            self_instance_id="lb-b",
            queue=queue,
            poll_interval=0.2,
        )
        await watcher.start()
        try:
            base = _future_ts()
            # Valid filename (make_message_filename-shaped) but garbage bytes →
            # read_message returns ParseError → filter 5 skips. Earlier timestamp
            # than the good message so it is scanned first; either way it never
            # enqueues.
            bad_path = ch.path / make_message_filename(timestamp=base)
            bad_path.write_bytes(b"{ this is not valid json ")
            good = _write_peer_message(
                ch,
                sender=_PEER_A_SENDER,
                instance_id="lb-a",
                body="good",
                timestamp=base + timedelta(microseconds=1),
            )
            good_id = good.name.removesuffix(".json")

            await wait_for(lambda: not queue.empty(), timeout=15.0)
            evt = queue.get_nowait()
            assert evt.message_id == good_id
            # The malformed file is NEVER enqueued (filter 5 skips it regardless
            # of scan order), so exactly one event ever arrives: the queue is
            # empty after the good event and stays empty.
            assert queue.empty()
            # Left in place — recovery never deletes user data (L8).
            assert bad_path.exists()
        finally:
            await watcher.stop()

        # Read path surfaces the failure as a ParseError (2c primitive). At the
        # channel/protocol layer this IS the ParseError dataclass (it has no
        # `body` field — `body: null` is the 7c MCP-wire projection, not this
        # layer's return; G5 "assert reality").
        result = read_message(bad_path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json")

    @pytest.mark.asyncio
    async def test_t4c_deleted_channel_dir_recovers_mid_run(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """T4(c) — deleted channel dir mid-run (watcher layer).

        Primitive: ``_handle_missing_channel_dir`` lazy recovery (4d — re-creates
        the dir on the next poll tick). Driven with a bare
        ``Watcher(_watchdog_enabled=False)`` and a short poll interval so recovery
        fires deterministically on the poll tick (the recommended §5 path) rather
        than racing an inotify DELETE/re-CREATE. The observer reschedule in
        ``_handle_missing_channel_dir`` is guarded by ``observer is not None and
        self._watchdog_enabled`` (watcher.py:661), so the disabled-watchdog seam
        is safe.

        Asserts the *survivable outcome* — after the dir is deleted, the next
        peer write lands and is observed, and the watcher does not crash — not
        the internal WARN mechanics (those are 4d's unit territory).
        """
        ch = Channel.get_or_create("deldir", "b", "", state_dir=tmp_letterbox_home)
        queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        watcher = Watcher(
            ch,
            self_sender="b",
            self_instance_id="lb-b",
            queue=queue,
            poll_interval=0.2,
            _watchdog_enabled=False,
        )
        await watcher.start()
        try:
            shutil.rmtree(ch.path)
            # Lazy recovery re-creates the dir on the next poll tick. We must wait
            # for it before writing — write_message never mkdirs (it writes into
            # an existing dir).
            await wait_for(lambda: ch.path.exists(), timeout=10.0)

            good = _write_peer_message(
                ch,
                sender=_PEER_A_SENDER,
                instance_id="lb-a",
                body="survive",
                timestamp=_future_ts(seconds=2.0),
            )
            good_id = good.name.removesuffix(".json")

            await wait_for(lambda: not queue.empty(), timeout=15.0)
            evt = queue.get_nowait()
            assert evt.message_id == good_id  # the conversation survived
        finally:
            await watcher.stop()

    def test_t4d_broken_read_state_preserved_and_backlog_restored(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """T4(d) — broken read-state (channel layer; sync — no PTY/async needed).

        Primitive: ``read_state`` corruption recovery (3b — rename to
        ``.broken.<ts>`` preserving bytes, return fresh state). The channel-layer
        call is the sufficient, robust default (K2 latitude — the heavier MCP
        stdio path adds no W3 coverage).

        Asserts: the corrupt bytes are preserved under a ``.broken.<ts>`` name
        (L8), a fresh state is returned (empty high_water_mark), and ``list_unread``
        consequently returns the full peer backlog.
        """
        ch = Channel.get_or_create(
            "readstate", "b", "", state_dir=tmp_letterbox_home
        )
        # A peer backlog the endpoint has never acknowledged.
        base = datetime(2026, 5, 2, tzinfo=timezone.utc)
        for i in range(3):
            _write_peer_message(
                ch,
                sender=_PEER_A_SENDER,
                instance_id="lb-a",
                body=f"backlog-{i}",
                timestamp=base + timedelta(microseconds=i),
            )

        # Corrupt this endpoint's read-state file with non-JSON bytes.
        read_dir = ch.path / ".read"
        read_dir.mkdir(mode=0o700, exist_ok=True)
        state_path = read_dir / "b.json"
        corrupt = b"{ corrupt not json"
        state_path.write_bytes(corrupt)

        state = read_state(ch, "b")

        # Corrupt bytes preserved under .broken.<ts> (the suffix is a strftime
        # stamp — glob rather than match an exact ts).
        broken = list(read_dir.glob("b.json.broken.*"))
        assert len(broken) == 1
        assert broken[0].read_bytes() == corrupt
        # Fresh state returned — empty high_water_mark.
        assert state.high_water_mark == ""
        # The fresh state means the full peer backlog reads as unread.
        result = ch.list_unread(self_instance_id="lb-b", limit=100)
        assert len(result.messages) == 3
        assert [m.body for m in result.messages] == [
            "backlog-0",
            "backlog-1",
            "backlog-2",
        ]

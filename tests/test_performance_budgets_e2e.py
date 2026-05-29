"""Vision §9.4 end-to-end performance budgets — block IMPLEMENTATION DONE per §13.7.

The three budgets here cover every end-to-end Vision §9.4 row not owned by 2d
(protocol) or 4e (watcher). 2d/4e measured to the watcher queue; 10c measures the
FULL path through ``run_launcher`` + the injection loop + a real PTY to the
rendered notification in the ``fake_harness`` echo file. Per §13.7 these gate
``[IMPLEMENTATION DONE]``; CI failures here are real regressions, not flakes (no
retry).

Structurally a fusion of two established shapes: the **4e budget-file discipline**
(``pytestmark`` marker LIST, ``_BENCH_RUNS=30`` + ``_BENCH_WARMUP=5``, ``_p95``
integer estimator, ``_async_pad_warmup_until_floor``, this Calibration block) and
the **10b launcher-e2e harness** (``run_launcher`` + ``fake_harness`` PTY + the
cloned ``_spy_setup_launcher`` / ``_patch_sleeper_mcp_config`` / ``_write_peer_message``
toolkit). Idioms are **cloned, not imported** (clone-per-file convention 5a–10b;
K5) — 10c is the 4th consumer; promotion to a shared module is deferred to a
post-freeze maintenance vision (logged in IMPLEMENTATION_NOTES).

Calibration block (per PLANNING_NOTES "show your work"):

* **N per benchmark:** 30 measured + 5 warmup.
* **P95 index:** ``ceil(0.95 * 30) - 1 = 28`` (0-indexed) — second-worst of 30
  (cloned 2d/4e). Integer-arithmetic ``_p95``; no numpy.
* **Runtime floor:** 5 s/test (anti-flake). event-path pads via
  ``_async_pad_warmup_until_floor`` (op = write + ``_await_token_in_echo``);
  list-channels pads via the sync ``_pad_warmup_until_floor`` (op =
  ``channel.list_channels``); poll-path naturally exceeds (30 × 0.2 s ≈ 6 s).
* **PLANNING_NOTES "empirical 2 × p95 from ≥10 CI runs":** applies to the RUNTIME
  FLOORS, not the budget thresholds — 500 ms / (poll_interval + 100 ms) / 200 ms
  are fixed by Vision §9.4.
* **Measurement fidelity (K2):** the event/poll arrival is detected by a 1 ms
  inner poll on the echo file (NOT the 50 ms shared ``wait_for``) so the P95
  reflects letterbox latency, not poll granularity. The coarse ``wait_for`` is
  still used for the non-timed "watcher started" gate.
* **list-channels (K4):** measures ``channel.list_channels(state_dir=…)`` directly
  — the O(n) scan the command is built on. The CLI handler's constant config-load
  + print overhead is deliberately excluded; the budget targets the scan that
  scales with channel count.
* **Mixed-population corpus:** 50 channels, per-channel message counts cycling
  ``[0, 1, 20, 100, 500]`` (Vision §9.4 "mixed populations" — includes empty and
  large). Microsecond-offset timestamps keep filename order deterministic.
* **poll-path override:** ``poll_interval=0.2 s`` for tractable wall time (Vision
  §9.4's "bounded by poll interval (≤ 5 s)" is a relationship contract; the
  production default is 5 s). Forced via a ``launcher.Watcher`` monkeypatch —
  ``run_launcher`` exposes no seam (K3).

* **Hardware envelope (Phase 10c author's laptop, 2026-05-29, Linux 6.12.85,
  Python 3.13.5, ext4-backed /tmp), three back-to-back uninstrumented runs:**

  ===========================================  ====================  =============  ==========
  Benchmark                                    Budget                Measured P95   Headroom
  ===========================================  ====================  =============  ==========
  event-path (write→echo, full PTY)            500 ms                2.08–2.31 ms   ≈217–241×
  poll-path (poll=0.2 s, write→echo)           300 ms (0.2 + 0.1)    201.7–201.9 ms ≈1.49×
  list-channels (50 mixed-population chans)    200 ms                19.9–20.7 ms   ≈9.7–10×
  ===========================================  ====================  =============  ==========

  GitHub Actions Linux runners are typically 1–3× slower than this laptop on
  single-thread workloads. The event-path and list-channels rows retain large
  headroom even at the slow end of CI. The poll-path row is intrinsically tight
  because ``poll_interval`` itself is the floor — only the overhead above it can
  shift on CI. Do NOT relax the absolute thresholds — they are user-facing
  promises (Vision §9.4).
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

import pytest

import letterbox.adapters.base as base
from letterbox import channel, launcher
from letterbox.adapters.base import Adapter, register_adapter
from letterbox.channel import Channel
from letterbox.launcher import LauncherSession, run_launcher
from letterbox.protocol import make_message_filename, new_message, write_message
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# budget → un-instrumented CI step (default ``-m 'not budget'`` filter);
# xdist_group("watcher") → bound inotify instances under ``-n auto`` (the
# event-path test allocates a real Observer via run_launcher). Same LIST shape
# as 4e / test_launcher_e2e.py / 10b. (G1 — the pin is load-bearing.)
pytestmark = [pytest.mark.budget, pytest.mark.xdist_group("watcher")]

_BENCH_RUNS = 30
_BENCH_WARMUP = 5
_RUNTIME_FLOOR_SECONDS = 5.0

_POLL_PATH_INTERVAL_SECONDS = 0.2
_POLL_PATH_OVERHEAD_BUDGET_SECONDS = 0.10  # cushion atop poll_interval

# Tight inner-poll interval for the echo-file arrival measurement (K2). The
# shared ``wait_for`` default is 50 ms — far too coarse for a sub-50 ms e2e
# latency. 1 ms quantization keeps the measurement honest.
_ECHO_POLL_INTERVAL_SECONDS = 0.001
_ECHO_TIMEOUT_SECONDS = 2.0  # per-iteration delivery safety net

# fake_harness can't interrupt its blocking stdin read on SIGTERM (PEP 475), so
# teardown pays the full timeout then SIGKILLs — keep it snappy (cloned 10b).
_FAST_TEARDOWN = 1.0

# K3/G4 — the template carries {message_id} so each rendered notification is a
# distinct, countable token (the stem the test wrote). The 8d default
# "📬 {channel}" renders identically every time — unusable for per-iteration
# timing.
_COUNTABLE_TEMPLATE = "📬 {channel} {message_id}"

# Distinct peer identity. A peer message must differ from the receiver on BOTH
# sender AND instance_id to pass the ADR-022 own-write OR-filter and fire. The
# receivers use label "b"; the peer uses "a" with its own instance id.
_PEER_SENDER = "a"
_PEER_INSTANCE = "lb-peer-a"


# ──────────────────────────────────────────────────────────────────────
# Cloned fixtures / helpers (clone-per-file convention 5a–10b; K5)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def reset_registry(monkeypatch: pytest.MonkeyPatch) -> dict[str, type[Adapter]]:
    """Replace the module-level ``_REGISTRY`` with a fresh empty dict (cloned 10b)."""
    fresh: dict[str, type[Adapter]] = {}
    monkeypatch.setattr(base, "_REGISTRY", fresh)
    return fresh


@pytest.fixture
def fake_adapter(reset_registry: dict[str, type[Adapter]]) -> type[Adapter]:
    """Register a placeholder ``fakeharness`` adapter into the reset registry.

    Its class attrs are placeholders — the launcher (ADR-041) overrides
    ``command`` / ``default_args`` / ``notification_template`` from config at
    launch, so the spawn never uses these. They exist only to satisfy
    ``register_adapter``'s non-empty validation. (Cloned 10b.)
    """

    @register_adapter
    class _FakeHarnessAdapter(Adapter):
        name = "fakeharness"
        command = "fakeharness-placeholder"
        default_args = ["placeholder"]
        notification_template = "placeholder {channel}"

    return _FakeHarnessAdapter


def _harness_block(
    name: str, *, command: str, default_args: list[str], template: str
) -> str:
    """Render one ``[harness.<name>]`` TOML block (cloned 10b).

    POSIX tmp paths carry no quotes/backslashes, so naive double-quoting is safe.
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
    """Write harness blocks and point ``LETTERBOX_CONFIG`` at the file (cloned 10b).

    ``LETTERBOX_CONFIG`` is the project-local config override (config.py K2), the
    only config-file hook that doesn't read the real ``~/.letterbox``.
    """
    config_path.write_text("\n".join(blocks), encoding="utf-8")
    monkeypatch.setenv("LETTERBOX_CONFIG", str(config_path))


def _patch_sleeper_mcp_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Path:
    """Make ``setup_launcher`` emit a benign sleeper MCP config (cloned 10b).

    ``setup_launcher`` always wires ``--mcp-config`` at a config whose command is
    the ``letterbox`` console script, which isn't on PATH in the test venv. 10c
    cares about the *receive* path, not the receiver's own MCP-child topology
    (that is 8d/T1), so we point the fake_harness child at a harmless sleeper.
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
    """Capture each ``LauncherSession`` ``run_launcher`` builds internally (cloned 10b).

    ``run_launcher`` owns its session privately, but the benchmark needs its
    ``channel`` (to write peer messages into the right dir) and its ``watcher``
    (to ``wait_for`` ``_started`` before any peer write — the post-start-write
    rule, ADR-024).
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
    """Assert the §2.1 clean-exit contract: no orphan process, no temp file (cloned 10b)."""
    assert session.handle.process.poll() is not None  # harness reaped
    assert not session.mcp_config_path.exists()  # temp MCP config deleted
    assert session.watcher._started is False  # watcher stopped


def _write_peer_message(
    channel_handle: Channel,
    *,
    sender: str = _PEER_SENDER,
    instance_id: str = _PEER_INSTANCE,
    body: str = "peer",
    timestamp: datetime | None = None,
) -> Path:
    """Write one peer message into ``channel_handle`` and return its path (cloned 10b).

    These are the exact bytes the MCP ``send_message`` tool writes (7b:
    ``make_message_filename`` → ``new_message`` → ``write_message``). Distinct
    ``sender`` + ``instance_id`` from the receiver's self values let the message
    survive the ADR-022 own-write filter. The ``make_message_filename`` UUID4
    tail makes every stem unique even at the same microsecond, so each rendered
    ``{message_id}`` token is distinct per iteration (G4).
    """
    stem = make_message_filename(timestamp=timestamp).removesuffix(".json")
    msg = new_message(
        id=stem,
        channel=channel_handle.name,
        instance_id=instance_id,
        sender=sender,
        body=body,
    )
    return write_message(channel_handle.path, msg)


def _future_ts(seconds: float = 1.0) -> datetime:
    """A UTC timestamp safely *after* a watcher's start watermark (cloned 10b).

    A watcher started on an empty channel synthesises its watermark from the
    current wall clock (4b). A peer message written *before* that watermark is
    backlog and is deliberately never injected (ADR-024 / G3). Future-dating the
    write guarantees it sorts strictly above the watermark and is delivered.

    Future-dating does NOT add latency: the watchdog fires on the inode CREATE
    event and the poll loop on the directory scan — neither waits for wall-clock
    to reach the embedded timestamp; the timestamp only governs filename sort
    order vs the watermark (G3).
    """
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


# ──────────────────────────────────────────────────────────────────────
# Budget-discipline helpers (cloned 2d/4e)
# ──────────────────────────────────────────────────────────────────────


def _p95(samples: list[float]) -> float:
    """Return the P95 of ``samples`` via integer arithmetic (cloned 2d/4e).

    For ``N = 30``: index ``ceil(0.95 * 30) - 1 = 28`` (0-indexed in the sorted
    list), i.e. the second-worst of 30. Intentionally duplicated to keep each
    budget file standalone (no cross-file fixture imports).
    """
    s = sorted(samples)
    n = len(s)
    idx = max(0, -(-95 * n // 100) - 1)
    return s[idx]


def _pad_warmup_until_floor(
    op: Callable[[], object],
    start_wall: float,
    floor_seconds: float = _RUNTIME_FLOOR_SECONDS,
) -> None:
    """Run untimed warmup ``op()`` calls until wall time clears the floor (cloned 4e).

    The 30 timed samples remain the P95-estimator's input; this pads *only*
    warmup, so the calibration math stays untouched.
    """
    while time.monotonic() - start_wall < floor_seconds:
        op()


async def _async_pad_warmup_until_floor(
    op: Callable[[], Awaitable[None]],
    start_wall: float,
    floor_seconds: float = _RUNTIME_FLOOR_SECONDS,
) -> None:
    """Async variant of ``_pad_warmup_until_floor`` for awaitable ops (cloned 4e).

    The event-path benchmark needs this because each pad iteration writes a peer
    message *and* awaits the rendered notification's arrival — a real full cycle,
    not a ``time.sleep`` (calling pattern matters for cache stability, G6).
    """
    while time.monotonic() - start_wall < floor_seconds:
        await op()


# ──────────────────────────────────────────────────────────────────────
# New local helpers (10c-specific)
# ──────────────────────────────────────────────────────────────────────


async def _await_token_in_echo(
    read_echo: Callable[[], bytes],
    token: bytes,
    *,
    timeout: float = _ECHO_TIMEOUT_SECONDS,
) -> None:
    """Busy-poll the echo bytes every ~1 ms until ``token`` appears (K2).

    This is the e2e analogue of 4e's ``await asyncio.wait_for(queue.get(), ...)``:
    the timed loop must detect arrival far finer than the 50 ms shared
    ``wait_for`` to avoid quantizing every sample to a 50 ms grid. The
    ``asyncio.wait_for`` safety net surfaces a dropped event as ``TimeoutError``
    rather than hanging pytest (G3). ``token`` is the message stem bytes; the
    rendered notification embeds it verbatim (the trailing ``\\r`` from the CR
    terminator never anchors the match, G5).
    """

    async def _poll() -> None:
        while token not in read_echo():
            await asyncio.sleep(_ECHO_POLL_INTERVAL_SECONDS)

    await asyncio.wait_for(_poll(), timeout=timeout)


def _build_mixed_channels(state_dir: Path, *, count: int = 50) -> None:
    """Create ``count`` channel dirs with varied message populations (K4 corpus).

    Per-channel counts cycle ``[0, 1, 20, 100, 500]`` so the workload is "mixed
    populations" per Vision §9.4 — including empty and large channels. Each
    message uses a microsecond-offset timestamp so filename order is
    deterministic; the random UUID4 tail keeps stems unique. Writes go through
    the real ``new_message`` → ``write_message`` path; the budget then reads the
    filesystem via ``channel.list_channels``.
    """
    populations = [0, 1, 20, 100, 500]
    base = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(count):
        ch = Channel.get_or_create(
            f"chan-{i:02d}", "b", "a", state_dir=state_dir
        )
        n_msgs = populations[i % len(populations)]
        for j in range(n_msgs):
            stem = make_message_filename(
                timestamp=base + timedelta(microseconds=j)
            ).removesuffix(".json")
            write_message(
                ch.path,
                new_message(
                    id=stem,
                    channel=ch.name,
                    instance_id=_PEER_INSTANCE,
                    sender=_PEER_SENDER,
                    body=f"msg-{j}",
                ),
            )


# ══════════════════════════════════════════════════════════════════════
# Budget 1 — event-path write→notification latency (full PTY)
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_event_path_write_to_notification_p95_under_500ms(
    fake_harness: FakeHarness,
    fake_adapter: type[Adapter],
    tmp_letterbox_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vision §9.4: peer-write → rendered notification (full PTY) P95 < 500 ms.

    The full end-to-end path no prior budget phase reached: a real
    ``run_launcher`` receiver (real ``watchdog.Observer`` + injection loop +
    real PTY via ``fake_harness``). Each iteration writes a fresh future-dated
    peer message and measures to the rendered ``{message_id}`` token's first
    appearance in the echo file via the 1 ms ``_await_token_in_echo`` (K2). One
    receiver reused across all iterations (steady-state semantic, 4e K4).
    """
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
            "evt",
            as_label="b",
            cwd=tmp_path,
            teardown_timeout=_FAST_TEARDOWN,
        )
    )
    try:
        # Coarse gate (50 ms wait_for fine here — NOT timed): post-start write
        # only, or a pre-start write is backlog → never injected (ADR-024 / G3).
        await wait_for(
            lambda: bool(captured) and captured[0].watcher._started,
            timeout=10.0,
        )
        session = captured[0]
        start_wall = time.monotonic()

        for _ in range(_BENCH_WARMUP):
            stem = _write_peer_message(
                session.channel, body="warmup", timestamp=_future_ts()
            ).name.removesuffix(".json")
            await _await_token_in_echo(fake_harness.read_echo, stem.encode("utf-8"))

        samples_ms: list[float] = []
        for _ in range(_BENCH_RUNS):
            t0 = time.perf_counter_ns()
            stem = _write_peer_message(
                session.channel, body="timed", timestamp=_future_ts()
            ).name.removesuffix(".json")
            await _await_token_in_echo(fake_harness.read_echo, stem.encode("utf-8"))
            t1 = time.perf_counter_ns()
            samples_ms.append((t1 - t0) / 1_000_000.0)

        async def _pad_op() -> None:
            stem = _write_peer_message(
                session.channel, body="pad", timestamp=_future_ts()
            ).name.removesuffix(".json")
            await _await_token_in_echo(fake_harness.read_echo, stem.encode("utf-8"))

        await _async_pad_warmup_until_floor(_pad_op, start_wall=start_wall)

        p95_ms = _p95(samples_ms)
        assert p95_ms < 500.0, (
            f"event-path write→notification P95 = {p95_ms:.3f} ms exceeds 500 ms "
            f"budget (Vision §9.4). Samples (ms): {sorted(samples_ms)}"
        )
    finally:
        # G5/G6 — cancel-and-await teardown so a failing assertion never leaks
        # the PTY child / polling task / Observer (those warn at GC, and
        # filterwarnings=["error"] would escalate that to a failure).
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    _session_torn_down(session)


# ══════════════════════════════════════════════════════════════════════
# Budget 2 — poll-path write→notification latency (watchdog disabled)
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_poll_path_write_to_notification_bounded_by_poll_interval(
    fake_harness: FakeHarness,
    fake_adapter: type[Adapter],
    tmp_letterbox_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vision §9.4: poll-path write→notification latency bounded by poll interval.

    Identical full PTY path, but the receiver's watcher is forced
    watchdog-disabled (polling-only) at ``poll_interval=0.2 s`` via a
    ``launcher.Watcher`` monkeypatch (K3 — ``run_launcher`` exposes no seam). The
    budget is ``poll_interval + 100 ms`` cushion (Vision §9.4 phrases the
    contract as "bounded by poll interval"). 30 × 0.2 s ≈ 6 s natural cadence
    already clears the 5 s floor — no padding (G8).
    """
    # K3 — swap constructor kwargs only on the real Watcher. setup_launcher calls
    # ``Watcher(channel, self_sender=…, self_instance_id=…, queue=…)`` at call
    # time, so ``channel`` binds positionally and the identity/queue kwargs flow
    # through **kw; we inject the poll-only knobs.
    real_watcher = launcher.Watcher

    def _poll_only(channel_handle: object, **kw: object) -> object:
        return real_watcher(
            channel_handle,
            **kw,
            poll_interval=_POLL_PATH_INTERVAL_SECONDS,
            _watchdog_enabled=False,
        )

    monkeypatch.setattr(launcher, "Watcher", _poll_only)

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
            "poll",
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

        for _ in range(_BENCH_WARMUP):
            stem = _write_peer_message(
                session.channel, body="warmup", timestamp=_future_ts()
            ).name.removesuffix(".json")
            await _await_token_in_echo(fake_harness.read_echo, stem.encode("utf-8"))

        samples_ms: list[float] = []
        for _ in range(_BENCH_RUNS):
            t0 = time.perf_counter_ns()
            stem = _write_peer_message(
                session.channel, body="timed", timestamp=_future_ts()
            ).name.removesuffix(".json")
            await _await_token_in_echo(fake_harness.read_echo, stem.encode("utf-8"))
            t1 = time.perf_counter_ns()
            samples_ms.append((t1 - t0) / 1_000_000.0)

        # 30 × 0.2 s natural cadence exceeds the 5 s floor — no padding (G8).

        budget_ms = (
            _POLL_PATH_INTERVAL_SECONDS + _POLL_PATH_OVERHEAD_BUDGET_SECONDS
        ) * 1000.0
        p95_ms = _p95(samples_ms)
        assert p95_ms < budget_ms, (
            f"poll-path write→notification P95 = {p95_ms:.3f} ms exceeds "
            f"{budget_ms:.1f} ms budget (poll_interval "
            f"{_POLL_PATH_INTERVAL_SECONDS} s + overhead "
            f"{_POLL_PATH_OVERHEAD_BUDGET_SECONDS} s, Vision §9.4). "
            f"Samples (ms): {sorted(samples_ms)}"
        )
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    _session_torn_down(session)


# ══════════════════════════════════════════════════════════════════════
# Budget 3 — list-channels enumeration on 50 mixed-population channels
# ══════════════════════════════════════════════════════════════════════


def test_list_channels_p95_under_200ms_on_50_channels(
    tmp_letterbox_home: Path,
) -> None:
    """Vision §9.4: ``list-channels`` P95 < 200 ms on 50 mixed-population channels.

    Measures ``channel.list_channels(state_dir=…)`` directly (K4) — the O(n)
    scan (``os.scandir`` over ``channels/`` + one ``list_messages`` per channel)
    that scales with channel count. The CLI handler's constant config-load +
    print overhead is deliberately excluded (documented in the Calibration
    block). Sync test (no PTY/async); pads via the sync helper (G8).
    """
    _build_mixed_channels(tmp_letterbox_home, count=50)
    start_wall = time.monotonic()

    for _ in range(_BENCH_WARMUP):
        channel.list_channels(state_dir=tmp_letterbox_home)

    samples_ms: list[float] = []
    for _ in range(_BENCH_RUNS):
        t0 = time.perf_counter_ns()
        channel.list_channels(state_dir=tmp_letterbox_home)
        t1 = time.perf_counter_ns()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    _pad_warmup_until_floor(
        lambda: channel.list_channels(state_dir=tmp_letterbox_home),
        start_wall=start_wall,
    )

    p95_ms = _p95(samples_ms)
    assert p95_ms < 200.0, (
        f"list-channels P95 = {p95_ms:.3f} ms exceeds 200 ms budget on 50 "
        f"mixed-population channels (Vision §9.4). Samples (ms): "
        f"{sorted(samples_ms)}"
    )

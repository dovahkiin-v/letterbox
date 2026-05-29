"""Vision §9.4 watcher-layer performance budgets — block IMPLEMENTATION DONE per §13.7.

The three budgets asserted here cover every watcher-layer row in Vision §9.4.
Per Cross-Cutting §13.7 these tests are gates on ``[IMPLEMENTATION DONE]``
for Phase 4e; CI failures here are real regressions, not flakes (no retry).

Mirrors the 2d / 3c budget-file shape verbatim — same ``_BENCH_RUNS = 30``,
same ``_BENCH_WARMUP = 5``, same ``_RUNTIME_FLOOR_SECONDS = 5.0``, same
``_p95`` integer-arithmetic estimator, same ``_pad_warmup_until_floor``
anti-flake pattern (with an ``_async_pad_warmup_until_floor`` sibling for
the event-path test, whose padding op awaits a queue read). The
module-level ``pytestmark`` is a **list** of two markers — ``budget`` for
the un-instrumented CI step AND ``xdist_group("watcher")`` to keep all
inotify-instance allocations on one worker (4b precedent — without it,
parallel workers would exhaust ``fs.inotify.max_user_instances`` at the
default 128 under ``-n auto``).

Calibration block (per PLANNING_NOTES "show your work"):

* **N per benchmark:** 30 measured + 5 warmup.
* **P95 index:** ``ceil(0.95 * N) - 1 = 28`` (0-indexed) — second-worst
  of 30. Cloned from 2d / 3c — intentionally duplicated to keep budget
  files standalone (no cross-file fixture imports).
* **Runtime floor:** 5 s per test (PLANNING_NOTES anti-flake floor). The
  event-path test pads via untimed warmup (``_async_pad_warmup_until_floor``
  — async variant of the 2d / 3c sync helper, divergence intentional per
  the duplicate-rather-than-import idiom). The poll-path test naturally
  exceeds the floor (30 × 0.2 s ≈ 6 s). The startup-rescan test naturally
  exceeds it (~30 fresh-Watcher allocations on a 1 000-msg corpus); a
  defensive async padding loop fires only if 30 iterations finish under
  the floor.
* **PLANNING_NOTES "empirical 2 × p95 from ≥10 CI runs":** that rule
  applies to *runtime floors* (PLANNING_NOTES flavour (1)), not to the
  budget thresholds themselves. The 500 ms / ``poll_interval + 100 ms`` /
  1 000 ms thresholds are fixed by Vision §9.4; the calibration
  discipline applies to the anti-flake floor. For 4e's three benchmarks,
  ``2 × measured_p95`` is well under 5 s, so the static 5 s floor is the
  correct fixed value. If a future change pushes any P95 close to budget
  under CI, capture ≥10 CI-run P95 readings, multiply by 2, bump the
  floor — 2d / 3c precedent.
* **Poll-path override:** ``poll_interval=0.2 s`` for tractable wall time.
  Vision §9.4's "bounded by poll interval (≤ 5 s)" wording is a
  relationship contract; the budget assertion is ``poll_interval + 100 ms``
  overhead headroom. Production default ``poll_interval`` is 5 s; the
  override is a representative-workload speedup, not a contract relaxation.
* **Hardware envelope (Phase 4e author's laptop, 2026-05-28, Linux 6.12.85,
  Python 3.13.5, ext4-backed /tmp), three back-to-back uninstrumented
  runs:**

  ===========================================  ==================  ====================  ==========
  Benchmark                                    Budget              Measured P95          Headroom
  ===========================================  ==================  ====================  ==========
  ``event-path latency``                       500 ms              0.540–0.580 ms        ≈860–925×
  ``poll-path latency`` (poll=0.2 s)           300 ms              201.03–201.06 ms      ≈1.49×
  ``startup-rescan`` (1 000-msg corpus)        1 000 ms            3.44–4.95 ms          ≈202–290×
  ===========================================  ==================  ====================  ==========

  GitHub Actions Linux runners are typically 1–3× slower than this
  laptop on single-thread workloads with comparable storage. The
  event-path and startup-rescan rows retain massive headroom (200×+
  even at the slow end of CI). The poll-path row is intrinsically
  tight because ``poll_interval`` itself is the floor — the per-iteration
  latency distribution centers on ``poll_interval`` regardless of CPU
  speed; only the ~1 ms overhead headroom above ``poll_interval`` can
  shift on CI. A 99 ms regression in scan-and-dispatch overhead atop
  the 0.2 s interval would still pass; anything more is a real signal,
  not a flake. Do NOT relax the absolute thresholds — they are
  user-facing promises in Vision §9.4.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

import pytest

from letterbox.channel import Channel
from letterbox.protocol import make_message_filename, new_message, write_message
from letterbox.watcher import Watcher, WatcherEvent


# §13.7 marker + 4b's inotify-instance pin: every test in this module
# needs both. ``budget`` puts the test in the un-instrumented CI step
# (default ``-m 'not budget'`` filter in pyproject addopts);
# ``xdist_group("watcher")`` pins all watcher tests to one worker so we
# don't blow ``fs.inotify.max_user_instances`` (default 128) when the
# second CI step (``pytest -m budget --no-cov``) runs under ``-n auto``.
# 4e is the first budget file to combine both markers, so ``pytestmark``
# MUST be a list — pytest applies every marker in the sequence.
pytestmark = [pytest.mark.budget, pytest.mark.xdist_group("watcher")]


_BENCH_RUNS = 30
_BENCH_WARMUP = 5
# Anti-flake floor (PLANNING_NOTES): every test's total wall time
# (warmup + measured + padding) must be >= this many seconds so a
# transient CI hiccup cannot perturb the P95 outside its headroom.
_RUNTIME_FLOOR_SECONDS = 5.0

# Poll-path budget uses a short interval so 30 timed iterations stay
# under ~6 s wall time. Vision §9.4's "bounded by poll interval (≤ 5 s)"
# wording asserts the relationship, not the absolute value; the
# representative-workload override is documented in the Calibration
# block above (K6).
_POLL_PATH_INTERVAL_SECONDS = 0.2
_POLL_PATH_OVERHEAD_BUDGET_SECONDS = 0.10  # cushion atop poll_interval

# Per-iteration delivery safety net (G3). Wraps every ``queue.get()`` so
# a dropped inotify event surfaces as ``TimeoutError`` instead of hanging
# the whole pytest run. Sits between the 500 ms event-path budget (so a
# budget-busting iteration fails fast) and the 5 s production polling
# cadence (so the polling fallback can't mask a watchdog failure during
# the event-path test).
_QUEUE_GET_TIMEOUT_SECONDS = 2.0


# Identity strings (mirror 2d / 3c benchmark identity — ``lb-bench`` self,
# ``lb-peer`` peer — so the budget files share an identity convention).
_SELF_SENDER = "claude-a"
_SELF_INSTANCE = "lb-bench"
_PEER_SENDER = "claude-b"
_PEER_INSTANCE = "lb-peer"


def _p95(samples: list[float]) -> float:
    """Return the P95 of ``samples`` via integer arithmetic (no numpy).

    For ``N = 30``: index ``ceil(0.95 * 30) - 1 = 28`` (0-indexed in the
    sorted list), i.e. the second-worst of 30. Cloned from
    ``tests/test_performance_budgets_protocol.py`` /
    ``tests/test_performance_budgets_channel.py`` — intentionally
    duplicated to keep each budget file standalone (no cross-file
    fixture imports).
    """
    s = sorted(samples)
    n = len(s)
    idx = max(0, -(-95 * n // 100) - 1)
    return s[idx]


def _pad_warmup_until_floor(
    op: Callable[[], None],
    start_wall: float,
    floor_seconds: float = _RUNTIME_FLOOR_SECONDS,
) -> None:
    """Run untimed warmup ``op()`` calls until wall time clears the floor.

    The 30 timed samples remain the P95-estimator's input; this pads
    *only* warmup, so the calibration math stays untouched. Cloned from
    ``tests/test_performance_budgets_protocol.py``. The async sibling
    below covers awaitable ops.
    """
    while time.monotonic() - start_wall < floor_seconds:
        op()


async def _async_pad_warmup_until_floor(
    op: Callable[[], Awaitable[None]],
    start_wall: float,
    floor_seconds: float = _RUNTIME_FLOOR_SECONDS,
) -> None:
    """Async variant of ``_pad_warmup_until_floor`` for awaitable ops.

    The event-path benchmark needs this because each pad iteration writes
    a peer message *and* awaits the watcher's queue (a real full cycle,
    not a ``time.sleep`` — calling pattern matters for cache stability
    per G6). The poll-path and startup-rescan benchmarks naturally exceed
    the floor and do not call this helper.

    The async/sync divergence from the 2d / 3c sync-only helper is
    intentional per the duplicate-rather-than-import idiom: clone the
    *concept* per file, adapt to local needs (Phase 2d / 3c
    IMPLEMENTATION_NOTES).
    """
    while time.monotonic() - start_wall < floor_seconds:
        await op()


def _make_peer_msg(
    channel: Channel,
    *,
    peer_sender: str = _PEER_SENDER,
    peer_instance: str = _PEER_INSTANCE,
    msg_id: str | None = None,
    body: str = "peer",
    timestamp: datetime | None = None,
) -> Path:
    """Write a real peer message; return the resulting file path.

    Defaults populate a *peer* message (``claude-b`` / ``lb-peer``) so
    every per-iteration write is unambiguously peer-side under the
    ``self_sender="claude-a"``, ``self_instance_id="lb-bench"`` benchmark
    identity — filter 5b's collision gate never fires, and filter 6's
    ADR-022 OR semantic correctly classifies the message as peer.
    Cloned shape from ``tests/test_watcher.py::make_peer_message``.
    """
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


def _populate_peer_msgs(
    channel: Channel,
    *,
    count: int,
    base: datetime | None = None,
) -> list[str]:
    """Pre-populate ``channel`` with ``count`` peer messages.

    Microsecond-offset filenames so the lexical sort is deterministic
    and chronological — the corpus IS the start-watermark's input for
    the startup-rescan benchmark. Cloned from
    ``tests/test_watcher.py::populate_peer_msgs``.
    """
    if base is None:
        base = datetime(2026, 5, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    stems: list[str] = []
    for i in range(count):
        stem = make_message_filename(
            base + timedelta(microseconds=i)
        ).removesuffix(".json")
        _make_peer_msg(channel, msg_id=stem, body=f"peer-{i}")
        stems.append(stem)
    return stems


def _make_channel(home: Path) -> Channel:
    """Mint the benchmark channel: sender ``claude-a``, recipient ``claude-b``."""
    return Channel.get_or_create(
        "bench", _SELF_SENDER, _PEER_SENDER, state_dir=home
    )


@pytest.mark.asyncio
async def test_event_path_latency_p95_under_500ms(
    tmp_letterbox_home: Path,
) -> None:
    """Vision §9.4: peer-write → watcher-queue event-path P95 < 500 ms.

    Real ``watchdog.Observer`` over real inotify (K5 — no mocks; the
    contract is the real chokepoint latency, not simulation cost). One
    ``Watcher`` allocated outside the timed loop and reused across all
    iterations (K4 — steady-state event-path latency semantic). Per
    iteration: write a fresh peer message → ``await queue.get()`` with
    an ``asyncio.wait_for`` safety net so a dropped event surfaces as
    ``TimeoutError`` rather than a 30-minute pytest hang (G3).
    """
    channel = _make_channel(tmp_letterbox_home)
    queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
    watcher = Watcher(
        channel,
        self_sender=_SELF_SENDER,
        self_instance_id=_SELF_INSTANCE,
        queue=queue,
    )
    await watcher.start()
    try:
        start_wall = time.monotonic()

        # Warmup — burns cold cache, watchdog-thread startup, allocator,
        # importlib re-resolution before the first timed sample.
        for _ in range(_BENCH_WARMUP):
            _make_peer_msg(channel, body="warmup")
            await asyncio.wait_for(queue.get(), timeout=_QUEUE_GET_TIMEOUT_SECONDS)

        samples_ms: list[float] = []
        for _ in range(_BENCH_RUNS):
            t0 = time.perf_counter_ns()
            _make_peer_msg(channel, body="timed")
            await asyncio.wait_for(queue.get(), timeout=_QUEUE_GET_TIMEOUT_SECONDS)
            t1 = time.perf_counter_ns()
            samples_ms.append((t1 - t0) / 1_000_000.0)

        # Anti-flake floor (G6): a fresh-write-then-queue-get cycle is a
        # representative no-op (NOT a ``time.sleep`` — calling pattern
        # matters for cache stability). 30 sub-ms iterations typically
        # finish in well under 1 s; the floor protects against a CI
        # hiccup perturbing the P95.
        async def _pad_op() -> None:
            _make_peer_msg(channel, body="pad")
            await asyncio.wait_for(queue.get(), timeout=_QUEUE_GET_TIMEOUT_SECONDS)

        await _async_pad_warmup_until_floor(_pad_op, start_wall=start_wall)

        p95_ms = _p95(samples_ms)
        assert p95_ms < 500.0, (
            f"event-path latency P95 = {p95_ms:.3f} ms exceeds 500 ms "
            f"budget (Vision §9.4). Samples (ms): {sorted(samples_ms)}"
        )
    finally:
        # G7 — ``await stop()`` in ``finally`` so a failing assertion
        # never leaks the polling task / Observer thread. Pending-task
        # ``RuntimeWarning`` becomes a test failure under
        # ``filterwarnings = ["error"]`` (1b).
        await watcher.stop()


@pytest.mark.asyncio
async def test_poll_path_latency_bounded_by_poll_interval(
    tmp_letterbox_home: Path,
) -> None:
    """Vision §9.4: poll-path latency bounded by ``poll_interval``.

    Watchdog disabled via the 4c K3 ``_watchdog_enabled=False`` test seam
    so only the polling loop catches the new file. ``poll_interval=0.2 s``
    override (K6) keeps 30 iterations under ~6 s wall time; the budget
    assertion is ``poll_interval + 100 ms`` cushion (Vision §9.4 phrases
    the contract as "bounded by poll interval"). The latency distribution
    is approximately uniform on ``[0, poll_interval]`` — P95 leans toward
    ``poll_interval`` itself (Prior Discovery 4).
    """
    channel = _make_channel(tmp_letterbox_home)
    queue: asyncio.Queue[WatcherEvent] = asyncio.Queue()
    watcher = Watcher(
        channel,
        self_sender=_SELF_SENDER,
        self_instance_id=_SELF_INSTANCE,
        queue=queue,
        poll_interval=_POLL_PATH_INTERVAL_SECONDS,
        _watchdog_enabled=False,
    )
    await watcher.start()
    try:
        # Warmup — same shape as event-path. Each cycle takes 0–0.2 s.
        for _ in range(_BENCH_WARMUP):
            _make_peer_msg(channel, body="warmup")
            await asyncio.wait_for(queue.get(), timeout=_QUEUE_GET_TIMEOUT_SECONDS)

        samples_ms: list[float] = []
        for _ in range(_BENCH_RUNS):
            t0 = time.perf_counter_ns()
            _make_peer_msg(channel, body="timed")
            await asyncio.wait_for(queue.get(), timeout=_QUEUE_GET_TIMEOUT_SECONDS)
            t1 = time.perf_counter_ns()
            samples_ms.append((t1 - t0) / 1_000_000.0)

        # 30 × 0.2 s natural cadence already exceeds the 5 s floor — no
        # padding needed (G6).

        budget_ms = (
            _POLL_PATH_INTERVAL_SECONDS + _POLL_PATH_OVERHEAD_BUDGET_SECONDS
        ) * 1000.0
        p95_ms = _p95(samples_ms)
        assert p95_ms < budget_ms, (
            f"poll-path latency P95 = {p95_ms:.3f} ms exceeds "
            f"{budget_ms:.1f} ms budget "
            f"(poll_interval {_POLL_PATH_INTERVAL_SECONDS} s + overhead "
            f"{_POLL_PATH_OVERHEAD_BUDGET_SECONDS} s, Vision §9.4). "
            f"Samples (ms): {sorted(samples_ms)}"
        )
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_startup_rescan_p95_under_1s_on_1k_messages(
    tmp_letterbox_home: Path,
) -> None:
    """Vision §9.4: watcher startup re-scan P95 < 1 s on 1 000-msg corpus.

    Per-test corpus build (G9; ~0.5–1 s on a laptop). Each iteration
    allocates a fresh ``Watcher`` and measures ``await watcher.start()``
    end-to-end — watermark computation + ``Observer.schedule`` +
    polling-task launch (Vision §9.4 says "watcher startup re-scan" and
    that IS all of ``start()``, per G5). ``stop()`` releases inotify
    before the next iteration so the xdist-group-pinned worker never
    exceeds 1 outstanding ``Observer`` instance (G8 — ``cat
    /proc/sys/fs/inotify/max_user_instances`` is 128 by default).
    """
    channel = _make_channel(tmp_letterbox_home)
    _populate_peer_msgs(channel, count=1000)
    start_wall = time.monotonic()

    # Warmup — burns cold cache, watermark-list scan, Observer allocation
    # before the first timed sample.
    for _ in range(_BENCH_WARMUP):
        q_warm: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w_warm = Watcher(
            channel,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=q_warm,
        )
        await w_warm.start()
        await w_warm.stop()

    samples_ms: list[float] = []
    for _ in range(_BENCH_RUNS):
        q: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w = Watcher(
            channel,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=q,
        )
        t0 = time.perf_counter_ns()
        await w.start()
        t1 = time.perf_counter_ns()
        # Untimed teardown — releases the Observer + polling task before
        # the next iteration. Bounded inotify-instance accounting (G8).
        await w.stop()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    # Defensive anti-flake padding (G6 says "no padding needed"; this loop
    # fires only if 35 iterations finished under the 5 s floor, which would
    # be a regression away from the laptop's measured 50–150 ms per start).
    async def _pad_op() -> None:
        q_pad: asyncio.Queue[WatcherEvent] = asyncio.Queue()
        w_pad = Watcher(
            channel,
            self_sender=_SELF_SENDER,
            self_instance_id=_SELF_INSTANCE,
            queue=q_pad,
        )
        await w_pad.start()
        await w_pad.stop()

    await _async_pad_warmup_until_floor(_pad_op, start_wall=start_wall)

    p95_ms = _p95(samples_ms)
    assert p95_ms < 1000.0, (
        f"startup-rescan P95 = {p95_ms:.3f} ms exceeds 1 000 ms budget "
        f"(Vision §9.4). Samples (ms): {sorted(samples_ms)}"
    )

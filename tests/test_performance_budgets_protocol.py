"""Vision §9.4 protocol-layer performance budgets — block IMPLEMENTATION DONE per §13.7.

The four budgets asserted here cover every protocol-layer row in
Vision §9.4. Per Cross-Cutting §13.7 these tests are gates on
``[IMPLEMENTATION DONE]`` for Phase 2d; CI failures here are real
regressions, not flakes (no retry).

Calibration block (per PLANNING_NOTES "show your work"):

* **N per benchmark:** 30 measured + 5 warmup.
* **P95 index:** ``ceil(0.95 * N) - 1 = 28`` (0-indexed) — the 29th of 30
  sorted samples (second-worst). The integer-arithmetic ``_p95`` helper
  below is stdlib-only (no numpy).
* **Runtime floor:** 5 s per test (PLANNING_NOTES anti-flake floor).
  The two ``check_messages``-equivalent tests exceed 5 s naturally from
  the 10 000-message corpus build (~1-2 s) + measured iterations. The
  two ``write_message`` tests pad via additional warmup (untimed) until
  the floor is met — kept separate from the 30 timed samples so the
  P95 estimator math stays the calibration discipline.
* **Hardware envelope (Phase 2d author's laptop, 2026-05-27, Linux
  6.12.85, Python 3.13.5, ext4-backed ``/tmp``):**

  ===========================  ============  ==================  ===========
  Benchmark                    Budget        Measured P95        Headroom
  ===========================  ============  ==================  ===========
  ``write_message`` no-fsync   50 ms         0.024–0.051 ms      ≈1000–2000×
  ``write_message`` reject     10 ms         ~0.001 ms           ≈13000×
  ``check_messages`` 20        100 ms        28–42 ms (median 32)  ≈2–4×
  ``check_messages`` 100       300 ms        29–36 ms (median 31)  ≈8–10×
  ===========================  ============  ==================  ===========

  The ``check_messages 20`` P95 is dominated by ``list_messages``
  scanning + sorting the full 10 000-entry corpus, not by the 20
  ``read_message`` calls — that's why the 100-message variant is only
  slightly slower than the 20-message variant. The 2–4× headroom on
  this one row is the tightest in 2d; below ~2× the budget would need
  the bisection-trim optimisation deferred from 2c §14 (currently the
  full directory is enumerated even for ``limit=20``). Until that
  optimisation lands the laptop number sits comfortably under budget.

* **GitHub Actions Linux runners** are typically 1–3× slower than this
  laptop on single-thread workloads with comparable storage. The two
  ``write_message`` rows retain 1000×+ headroom — CI passes trivially.
  The two ``check_messages`` rows retain 2–10× headroom — CI passes
  with comfort but is the canary if a future change pessimises
  ``list_messages``. Do NOT relax the absolute thresholds — they are
  user-facing promises in Vision §9.4.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from letterbox.protocol import (
    MAX_BODY_BYTES,
    Message,
    MessageTooLarge,
    list_messages,
    make_message_filename,
    new_message,
    read_message,
    write_message,
)


# Pin every test in this module to the ``budget`` marker. Default
# ``pytest`` invocation runs with ``-m "not budget"`` (see pyproject.toml)
# so the suite stays fast and coverage-clean for everything else; CI runs
# a second pass with ``-m budget --no-cov`` to gate the §13.7 budgets
# against UN-instrumented timings. ``pytest-cov``'s sys.settrace hook adds
# ~1-2 µs per Python statement; ``list_messages`` scanning 10 000 entries
# plus 20 ``read_message`` JSON parses pays that overhead ~25 000× per
# iteration, pushing the laptop P95 from ~33 ms (real) to ~115 ms
# (instrumented) — straight through the 100 ms budget. Measuring under
# instrumentation would gate on tracing cost, not letterbox cost.
pytestmark = pytest.mark.budget


_BENCH_RUNS = 30
_BENCH_WARMUP = 5
# Anti-flake floor (PLANNING_NOTES): every test's total wall time
# (warmup + measured) must be >= this many seconds so a transient
# 50 ms CI hiccup cannot perturb the P95 outside its headroom.
_RUNTIME_FLOOR_SECONDS = 5.0


def _p95(samples: list[float]) -> float:
    """Return the P95 of ``samples`` via integer arithmetic (no numpy).

    For ``N = 30``: index ``ceil(0.95 * 30) - 1 = 28`` (0-indexed in the
    sorted list), i.e. the second-worst of 30. Stdlib-only so the budget
    tests stay light on dependencies (Framework P19).
    """
    s = sorted(samples)
    n = len(s)
    idx = max(0, -(-95 * n // 100) - 1)
    return s[idx]


def _make_bench_msg(body: str = "Sveiki", msg_id: str | None = None) -> Message:
    """Build a production-realistic Message with a regex-valid id.

    Mirrors the local ``_make_real`` helper in ``test_protocol.py`` so the
    budget workloads exercise the exact factory path callers use (Vision
    §9.4: "representative workload").
    """
    if msg_id is None:
        msg_id = make_message_filename().removesuffix(".json")
    return new_message(
        id=msg_id,
        channel="01",
        instance_id="lb-bench",
        sender="claude-a",
        body=body,
    )


def _pad_warmup_until_floor(
    op,
    start_wall: float,
    floor_seconds: float = _RUNTIME_FLOOR_SECONDS,
) -> None:
    """Run untimed warmup ``op()`` calls until the test's wall time clears
    the anti-flake floor.

    The 30 timed samples remain the P95-estimator's input; this pads
    *only* warmup, so the calibration math stays untouched. Returns
    when ``time.monotonic() - start_wall >= floor_seconds``.
    """
    while time.monotonic() - start_wall < floor_seconds:
        op()


@pytest.fixture
def bench_channel_dir(tmp_letterbox_home: Path) -> Path:
    """Per-test channel directory under ``tmp_letterbox_home``."""
    d = tmp_letterbox_home / "channels" / "bench"
    d.mkdir(parents=True)
    return d


@pytest.fixture
def bench_channel_1k(bench_channel_dir: Path) -> Path:
    """Channel pre-populated with 1 000 messages (no fsync).

    G7 — per-test corpus; cost ~0.2-0.5 s on a contemporary laptop.
    """
    base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(1000):
        msg_id = make_message_filename(
            base + timedelta(microseconds=i)
        ).removesuffix(".json")
        write_message(bench_channel_dir, _make_bench_msg(body=f"c{i}", msg_id=msg_id))
    return bench_channel_dir


@pytest.fixture
def bench_channel_10k(bench_channel_dir: Path) -> Path:
    """Channel pre-populated with 10 000 messages (no fsync).

    G7 — per-test corpus; cost ~1-3 s on a contemporary laptop. Module-
    scope rejected per the plan (bleeding state across tests surprises
    the reader). If observed runtime grows past ~60 s for the file,
    revisit per the Implementer's Latitude note in p2d §14.
    """
    base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(10_000):
        msg_id = make_message_filename(
            base + timedelta(microseconds=i)
        ).removesuffix(".json")
        write_message(bench_channel_dir, _make_bench_msg(body=f"c{i}", msg_id=msg_id))
    return bench_channel_dir


def test_write_message_p95_under_50ms(bench_channel_1k: Path) -> None:
    """Vision §9.4: ``write_message`` P95 < 50 ms on a 1 000-message channel."""
    start = time.monotonic()

    # Warmup — burns cold cache, allocator, importlib re-resolution
    # before the first timed sample.
    for _ in range(_BENCH_WARMUP):
        write_message(bench_channel_1k, _make_bench_msg(body="warmup"))

    samples_ms: list[float] = []
    for _ in range(_BENCH_RUNS):
        # ``msg.id`` is regenerated per iteration so writes don't land
        # on the same path (UUID4 collision is effectively impossible,
        # but the test asserts behavior under realistic workload).
        msg = _make_bench_msg(body="timed")
        t0 = time.perf_counter_ns()
        write_message(bench_channel_1k, msg)
        t1 = time.perf_counter_ns()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    # Anti-flake floor: pad with untimed warmup if needed (small N=30 +
    # sub-ms-per-op workloads finish in milliseconds; the floor protects
    # against CI hiccups perturbing the P95).
    _pad_warmup_until_floor(
        lambda: write_message(bench_channel_1k, _make_bench_msg(body="pad")),
        start_wall=start,
    )

    p95_ms = _p95(samples_ms)
    assert p95_ms < 50.0, (
        f"write_message P95 = {p95_ms:.3f} ms exceeds 50 ms budget "
        f"(Vision §9.4). Samples (ms): {sorted(samples_ms)}"
    )


def test_write_message_rejection_p95_under_10ms(bench_channel_dir: Path) -> None:
    """Vision §9.4: oversized rejection P95 < 10 ms — no disk I/O.

    G9 — proves the 2a short-circuit fires: build a Message whose body
    is 5 MB + 1 byte; assert ``MessageTooLarge`` raises AND no ``.tmp``
    file is created. If a future change accidentally serialises before
    checking, the 5 MB JSON construction (~50-100 ms) blows the budget.
    """
    start = time.monotonic()

    # Message constructed OUTSIDE the timed loop so we measure only the
    # ``write_message`` call (which short-circuits via ``to_json_bytes``).
    oversized_body = "x" * (MAX_BODY_BYTES + 1)
    oversized_msg = _make_bench_msg(body=oversized_body)

    for _ in range(_BENCH_WARMUP):
        with pytest.raises(MessageTooLarge):
            write_message(bench_channel_dir, oversized_msg)

    samples_ms: list[float] = []
    for _ in range(_BENCH_RUNS):
        t0 = time.perf_counter_ns()
        try:
            write_message(bench_channel_dir, oversized_msg)
        except MessageTooLarge:
            pass
        else:
            pytest.fail("oversized write_message must raise MessageTooLarge")
        t1 = time.perf_counter_ns()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    # G9 critical assertion: no .tmp file was ever created, because the
    # 2a short-circuit fired before any disk I/O.
    tmp_path = bench_channel_dir / f"{oversized_msg.id}.json.tmp"
    final_path = bench_channel_dir / f"{oversized_msg.id}.json"
    assert not tmp_path.exists(), (
        f".tmp file {tmp_path.name} exists after rejection — "
        f"2a short-circuit is broken; serializer ran before size check"
    )
    assert not final_path.exists(), (
        f"final .json {final_path.name} exists after rejection — "
        f"writer somehow completed the rename on an oversized payload"
    )

    # Pad warmup if needed to clear the anti-flake floor.
    def _pad_op() -> None:
        with pytest.raises(MessageTooLarge):
            write_message(bench_channel_dir, oversized_msg)

    _pad_warmup_until_floor(_pad_op, start_wall=start)

    p95_ms = _p95(samples_ms)
    assert p95_ms < 10.0, (
        f"write_message rejection P95 = {p95_ms:.3f} ms exceeds 10 ms budget "
        f"(Vision §9.4 / G9). Samples (ms): {sorted(samples_ms)}"
    )


def test_check_messages_default_limit_p95_under_100ms(
    bench_channel_10k: Path,
) -> None:
    """Vision §9.4: ``list_messages`` + ``read_message`` × 20 P95 < 100 ms.

    The ``check_messages`` MCP tool (Phase 7c) wraps this protocol-layer
    primitive pair; the budget belongs to the underlying primitives.
    """
    start = time.monotonic()

    def run_one() -> None:
        paths = list_messages(bench_channel_10k)
        for p in paths[-20:]:
            read_message(p)

    for _ in range(_BENCH_WARMUP):
        run_one()

    samples_ms: list[float] = []
    for _ in range(_BENCH_RUNS):
        t0 = time.perf_counter_ns()
        run_one()
        t1 = time.perf_counter_ns()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    _pad_warmup_until_floor(run_one, start_wall=start)

    p95_ms = _p95(samples_ms)
    assert p95_ms < 100.0, (
        f"check_messages (limit=20) P95 = {p95_ms:.3f} ms exceeds 100 ms "
        f"budget (Vision §9.4). Samples (ms): {sorted(samples_ms)}"
    )


def test_check_messages_max_limit_p95_under_300ms(
    bench_channel_10k: Path,
) -> None:
    """Vision §9.4: ``list_messages`` + ``read_message`` × 100 P95 < 300 ms."""
    start = time.monotonic()

    def run_one() -> None:
        paths = list_messages(bench_channel_10k)
        for p in paths[-100:]:
            read_message(p)

    for _ in range(_BENCH_WARMUP):
        run_one()

    samples_ms: list[float] = []
    for _ in range(_BENCH_RUNS):
        t0 = time.perf_counter_ns()
        run_one()
        t1 = time.perf_counter_ns()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    _pad_warmup_until_floor(run_one, start_wall=start)

    p95_ms = _p95(samples_ms)
    assert p95_ms < 300.0, (
        f"check_messages (limit=100) P95 = {p95_ms:.3f} ms exceeds 300 ms "
        f"budget (Vision §9.4). Samples (ms): {sorted(samples_ms)}"
    )

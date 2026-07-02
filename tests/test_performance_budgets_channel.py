"""Vision §9.4 channel-layer performance budgets — blocks IMPLEMENTATION DONE per §13.7.

The budgets asserted here cover three Vision §9.4 rows:
``Channel.acknowledge`` (BUDGET-OWNER for Phase 3c) and the two
``check_messages`` rows (``list_unread`` at ``limit=20`` @ 100 ms and
``limit=100`` @ 300 ms). Per Cross-Cutting §13.7 these tests gate
``[IMPLEMENTATION DONE]``; CI failures here are real regressions, not
flakes (no retry).

The two ``check_messages`` rows moved here from
``tests/test_performance_budgets_protocol.py`` when the full-corpus sort
in ``list_messages`` was eliminated (DECISIONS.md — shared scan primitive
+ lazy heap iterators). They now drive the REAL tool path,
``Channel.list_unread``, rather than a protocol-layer
``list_messages()[-20:]`` stand-in — the honest §9.4 gate is the caller
agents actually hit. Cost model per call: O(N) ``_scan_valid_names`` scan
+ O(N) cursor filter + O(M) ``heapify`` + O((filled+skipped)·log M) lazy
pops, where M is the unread count — no O(N·log N) full sort. On the 10k
fresh-cursor corpus (every message unread) the ``limit=20`` page reads 21
messages / the ``limit=100`` page 101; both clear budget with headroom on
~2× GitHub runners (the old protocol-layer row's P95 landed ~110–116 ms
against 100 ms, the regression this change fixes).

Mirrors the 2d ``tests/test_performance_budgets_protocol.py`` shape
verbatim — same ``_BENCH_RUNS = 30``, same ``_BENCH_WARMUP = 5``, same
``_RUNTIME_FLOOR_SECONDS = 5.0``, same ``_p95`` integer-arithmetic
estimator, same ``_pad_warmup_until_floor`` anti-flake pattern, same
module-level ``pytestmark = pytest.mark.budget``. Intentionally
duplicated (not cross-file-imported) to keep each budget file
standalone — pytest fixture-import-across-files is fragile and the cost
is one ~30-line copy.

Calibration block (per PLANNING_NOTES "show your work"):

* **N per benchmark:** 30 measured + 5 warmup.
* **P95 index:** ``ceil(0.95 * N) - 1 = 28`` (0-indexed) — second-worst
  of 30. Stdlib-only (no numpy) per Framework P19.
* **Runtime floor:** 5 s per test (PLANNING_NOTES anti-flake floor).
  ``Channel.acknowledge`` is one read-state read + one ``max()`` + one
  read-state atomic-rename write — sub-millisecond on warm cache.
  ``_pad_warmup_until_floor`` runs UNTIMED warmup until wall time
  crosses the floor; the 30 timed samples remain the calibration input.
* **Budget threshold (static):** 50 ms, fixed by Vision §9.4.
  ``Channel.acknowledge`` P95 < 50 ms on a 1 000-unarchived-channel
  workload. The 1 000-msg corpus is for representative environment
  state; ``acknowledge`` itself does not scan the channel directory
  (it only touches ``.read/{sender}.json``), so the corpus size mainly
  influences page-cache priming, not algorithmic cost.
* **PLANNING_NOTES "empirical ``2 × p95`` from ≥10 CI runs":** that rule
  applies to *runtime floors* (PLANNING_NOTES flavour (1)), not to the
  budget threshold itself. The 50 ms budget is fixed by Vision §9.4;
  the calibration discipline applies to the anti-flake floor. 2d
  validated this exact approach across 5/5 CI iterations. If a future
  ``Channel.acknowledge`` change pushes the P95 close to budget under
  CI, capture ≥10 CI run P95 readings, multiply by 2, bump the floor.
* **Hardware envelope:** developer's laptop
  (``/home/vinga/projects/letterbox``); NVMe SSD; Linux 6.12.85; Python
  3.13.5. CI envelope is GitHub Actions ``ubuntu-latest`` (per 1b
  workflow). Both have ample headroom for the 50 ms budget — typical
  ``acknowledge`` on warm cache is 0.2–1 ms.

  ============================  ============  ================  ===========
  Benchmark                     Budget        Expected P95      Headroom
  ============================  ============  ================  ===========
  ``Channel.acknowledge``       50 ms         ~0.2–1 ms         ≈50–250×
  ``list_unread`` 20 (10k)      100 ms        ~7–11 ms          ≈9–14×
  ``list_unread`` 100 (10k)     300 ms        ~8–11 ms          ≈27–37×
  ============================  ============  ================  ===========

  GitHub Actions Linux runners are typically 1–3× slower than this
  laptop on single-thread workloads with comparable storage. The
  ``acknowledge`` row retains 50×+ headroom; the two ``list_unread`` rows
  retain multiple-× headroom post full-sort elimination (they are the
  canary if a future change re-pessimises the scan path). Do NOT relax
  the absolute thresholds — they are user-facing promises in Vision §9.4.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from letterbox.channel import Channel, ReadState, write_read_state
from letterbox.protocol import (
    Message,
    make_message_filename,
    new_message,
    write_message,
)


# Pin every test in this module to the ``budget`` marker. Default
# ``pytest`` invocation runs with ``-m "not budget"`` (see pyproject.toml)
# so the suite stays fast and coverage-clean for everything else; CI runs
# a second pass with ``-m budget --no-cov`` to gate the §13.7 budgets
# against UN-instrumented timings. Matches the 2d fresh-eyes deviation
# (``pytest-cov`` tracing inflates microsecond ops via the sys.settrace
# hook — measuring under instrumentation would gate on tracing cost,
# not letterbox cost).
pytestmark = pytest.mark.budget


_BENCH_RUNS = 30
_BENCH_WARMUP = 5
# Anti-flake floor (PLANNING_NOTES): every test's total wall time
# (warmup + measured) must be >= this many seconds so a transient CI
# hiccup cannot perturb the P95 outside its headroom.
_RUNTIME_FLOOR_SECONDS = 5.0


def _p95(samples: list[float]) -> float:
    """Return the P95 of ``samples`` via integer arithmetic (no numpy).

    For ``N = 30``: index ``ceil(0.95 * 30) - 1 = 28`` (0-indexed in the
    sorted list), i.e. the second-worst of 30. Cloned from
    ``tests/test_performance_budgets_protocol.py`` — intentionally
    duplicated to keep budget files standalone (no cross-file fixture
    imports).
    """
    s = sorted(samples)
    n = len(s)
    idx = max(0, -(-95 * n // 100) - 1)
    return s[idx]


def _make_bench_msg(
    *,
    body: str = "Sveiki",
    msg_id: str | None = None,
    channel_name: str = "bench",
    sender: str = "claude-b",
    instance_id: str = "lb-peer",
) -> Message:
    """Build a production-realistic peer Message with a regex-valid id.

    Defaults populate a *peer* message (``claude-b`` / ``lb-peer``) so
    the 1K corpus is unambiguously peer-side under the ``self_sender=
    "claude-a"``, ``self_instance_id="lb-bench"`` benchmark identity.
    Cloned shape from
    ``tests/test_performance_budgets_protocol.py._make_bench_msg``.
    """
    if msg_id is None:
        msg_id = make_message_filename().removesuffix(".json")
    return new_message(
        id=msg_id,
        channel=channel_name,
        instance_id=instance_id,
        sender=sender,
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
    *only* warmup, so the calibration math stays untouched. Cloned from
    ``tests/test_performance_budgets_protocol.py``.
    """
    while time.monotonic() - start_wall < floor_seconds:
        op()


@pytest.fixture
def bench_channel(tmp_letterbox_home: Path) -> Channel:
    """Mint a fresh ``Channel`` for the benchmark.

    Sender ``claude-a`` (this endpoint), recipient ``claude-b`` (peer);
    the 1K-corpus fixture below writes from ``claude-b`` so every
    pre-seeded message is unambiguously peer-side under the ADR-022
    combined own-write filter.
    """
    return Channel.get_or_create(
        "bench", "claude-a", "claude-b", state_dir=tmp_letterbox_home
    )


@pytest.fixture
def bench_channel_1k(bench_channel: Channel) -> tuple[Channel, list[str]]:
    """Channel pre-populated with 1 000 PEER message files (no fsync).

    Returns ``(channel, stems)`` — ``stems`` is the lexically-ordered
    list of message-id stems for use as ``acknowledge`` arguments.

    Mirrors the 2d ``bench_channel_1k`` shape verbatim. Per-test corpus;
    cost ~0.2–0.5 s on a contemporary laptop. Pre-writes a starting
    ``.read/claude-a.json`` with hwm at stems[0] so the first
    ``acknowledge`` is a real read-modify-write (not a no-op write
    against the empty fresh-state sentinel — exercising the monotonic
    clamp path under representative state).
    """
    base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
    stems: list[str] = []
    for i in range(1000):
        stem = make_message_filename(
            base + timedelta(microseconds=i)
        ).removesuffix(".json")
        write_message(
            bench_channel.path,
            _make_bench_msg(body=f"c{i}", msg_id=stem),
        )
        stems.append(stem)
    # Pre-seed read-state at stems[0] so timed acknowledges exercise the
    # full read-modify-write path (read existing file + max() + atomic
    # write), not the first-ever ``.read/`` creation special case.
    write_read_state(
        bench_channel,
        ReadState(
            sender_label="claude-a",
            instance_id="lb-bench-init",
            high_water_mark=stems[0],
            updated_at="2026-05-27T14:00:00+00:00",
        ),
    )
    return bench_channel, stems


@pytest.fixture
def bench_channel_10k(bench_channel: Channel) -> Channel:
    """Channel pre-populated with 10 000 PEER message files (no fsync).

    Deliberately leaves read-state ABSENT (no ``.read/claude-a.json``) so
    the per-agent ``high_water_mark`` resolves to the fresh-endpoint
    sentinel ``""`` — every one of the 10 000 peer messages is therefore
    UNREAD, and ``list_unread(limit=20)`` actually reads its 20-message
    page (N1: a pre-advanced cursor would make ``list_unread`` read ZERO
    and silently reduce the benchmark to a scan-only measurement, dropping
    the 20 JSON parses §9.4 promises to cover). Per-test corpus; cost
    ~1–3 s on a contemporary laptop.
    """
    base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(10_000):
        stem = make_message_filename(
            base + timedelta(microseconds=i)
        ).removesuffix(".json")
        write_message(
            bench_channel.path,
            _make_bench_msg(body=f"c{i}", msg_id=stem),
        )
    return bench_channel


def test_check_messages_default_limit_p95_under_100ms(
    bench_channel_10k: Channel,
) -> None:
    """Vision §9.4: ``Channel.list_unread(limit=20)`` P95 < 100 ms on 10k.

    This is the real ``check_messages`` path (7c → ``list_unread``). It
    replaces the retired protocol-layer ``list_messages()[-20:]`` stand-in:
    the honest gate is the tool callers actually hit. Post the full-sort
    elimination the cost is O(N) scan + O(N) cursor filter + O(M) heapify +
    O((filled+skipped)·log M) — no O(N·log N) sort — so the 100 ms §9.4
    budget passes with comfortable headroom even on ~2× GitHub runners.

    N1 — the in-bench ``assert len == 20 and has_more`` locks in that 20
    messages are truly read (fresh cursor). Without it a mis-seeded cursor
    would silently measure scan-only and the budget would be meaningless.
    """
    channel = bench_channel_10k
    start = time.monotonic()

    def run_one() -> "object":
        return channel.list_unread(self_instance_id="lb-bench", limit=20)

    # N1 — prove the workload actually reads its full 20-message page
    # before timing (guards against a scan-only degenerate benchmark).
    result = run_one()
    assert len(result.messages) == 20 and result.has_more, (
        "list_unread(limit=20) must read a full 20-message page with "
        f"has_more on the 10k fresh-cursor corpus; got "
        f"{len(result.messages)} messages, has_more={result.has_more}"
    )

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
        f"check_messages (list_unread limit=20) P95 = {p95_ms:.3f} ms "
        f"exceeds 100 ms budget (Vision §9.4). Samples (ms): "
        f"{sorted(samples_ms)}"
    )


def test_check_messages_max_limit_p95_under_300ms(
    bench_channel_10k: Channel,
) -> None:
    """Vision §9.4: ``Channel.list_unread(limit=100)`` P95 < 300 ms on 10k.

    The ``limit=100`` sibling of the 100 ms row above — same real
    ``check_messages`` path, same fresh-cursor 10k corpus, larger page.
    Passes with headroom post full-sort elimination.
    """
    channel = bench_channel_10k
    start = time.monotonic()

    def run_one() -> "object":
        return channel.list_unread(self_instance_id="lb-bench", limit=100)

    # N1 — prove the workload reads a full 100-message page before timing.
    result = run_one()
    assert len(result.messages) == 100 and result.has_more, (
        "list_unread(limit=100) must read a full 100-message page with "
        f"has_more on the 10k fresh-cursor corpus; got "
        f"{len(result.messages)} messages, has_more={result.has_more}"
    )

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
        f"check_messages (list_unread limit=100) P95 = {p95_ms:.3f} ms "
        f"exceeds 300 ms budget (Vision §9.4). Samples (ms): "
        f"{sorted(samples_ms)}"
    )


def test_acknowledge_p95_under_50ms(
    bench_channel_1k: tuple[Channel, list[str]],
) -> None:
    """Vision §9.4: ``Channel.acknowledge`` P95 < 50 ms on a 1 000-msg channel.

    The 1 000-msg corpus provides representative on-disk state;
    ``acknowledge`` itself only touches ``.read/{sender}.json`` (does
    NOT scan the channel directory), so the cost is dominated by one
    JSON read + ``max()`` + atomic-rename write. Sub-millisecond on
    warm cache; the ``_pad_warmup_until_floor`` helper keeps the test
    above the 5 s anti-flake floor without inflating the calibration N.
    """
    channel, stems = bench_channel_1k
    start = time.monotonic()

    # Warmup — burns cold cache, allocator, importlib re-resolution
    # before the first timed sample. Use the second stem onwards so
    # warmup itself advances the marker; the timed loop will replay
    # newer stems (max() means re-acknowledging an older one is a
    # no-op on hwm but still does the read-write cycle).
    for i in range(_BENCH_WARMUP):
        channel.acknowledge(stems[1 + i], self_instance_id="lb-bench")

    samples_ms: list[float] = []
    # Iterate through the 1K stems in batches so we exercise a mix of
    # forward-advance and no-op (max() clamp) writes. The timed call is
    # always a full read-modify-write regardless of whether hwm advances.
    for i in range(_BENCH_RUNS):
        stem = stems[100 + (i * 11) % 800]  # stride avoids any cache pattern
        t0 = time.perf_counter_ns()
        channel.acknowledge(stem, self_instance_id="lb-bench")
        t1 = time.perf_counter_ns()
        samples_ms.append((t1 - t0) / 1_000_000.0)

    # Anti-flake floor: pad with untimed warmup if needed (sub-ms-per-op
    # workload finishes in milliseconds; the floor protects against CI
    # hiccups perturbing the P95).
    _pad_warmup_until_floor(
        lambda: channel.acknowledge(stems[500], self_instance_id="lb-bench"),
        start_wall=start,
    )

    p95_ms = _p95(samples_ms)
    assert p95_ms < 50.0, (
        f"Channel.acknowledge P95 = {p95_ms:.3f} ms exceeds 50 ms budget "
        f"(Vision §9.4). Samples (ms): {sorted(samples_ms)}"
    )

"""PTY-Parent runtime — spawns harness + starts watcher + drives notification injection loop.

Tier: 4
May import from: stdlib; Tier 1 (``protocol``, ``channel``, ``config``, ``notifications``);
    Tier 2 (``watcher``, ``adapters.base``, ``adapters.pty_common``, ``adapters.mcp_config``);
    Tier 3 concrete adapters via the registry only (never direct imports of sibling adapter modules).
Must NOT import from: ``letterbox.mcp_server`` or ``letterbox.cli`` (Tier 4 sibling isolation —
    bulkhead §13.5).

Filled in: Phase 8a/8b/8c per PHASE_INDEX.
"""
from __future__ import annotations

import asyncio
import logging
import os
import secrets
import shutil
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from letterbox.adapters import load_builtin_adapters
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.mcp_config import cleanup_mcp_config, generate_mcp_config
from letterbox.channel import Channel, check_state_dir_permissions
from letterbox.config import load_config
from letterbox.notifications import (
    NotificationTemplateError,
    render_notification,
    validate_template,
)
from letterbox.protocol import reap_orphan_tmp
from letterbox.watcher import Watcher, WatcherEvent

if TYPE_CHECKING:
    # Type-only import: ``PTYHandle`` is the static type of ``LauncherSession.handle``.
    # Guarded so the launcher carries no *runtime* dependency on ``pty_common`` — it
    # only ever holds the handle that ``adapter.spawn`` returns (the tier-header
    # permits ``adapters.pty_common``; this keeps the runtime surface minimal).
    from letterbox.adapters.pty_common import PTYHandle

__all__ = [
    "LauncherSession",
    "generate_instance_id",
    "resolve_sender_label",
    "run_launcher",
    "setup_launcher",
]

_LOGGER = logging.getLogger("letterbox.launcher")

# Cadence at which ``_await_process_exit`` polls the harness's ``Popen.poll()``
# (K5). ~0.1 s is imperceptible against the human-scale "agent exited" event and
# keeps the waiter's ``await asyncio.sleep`` a clean cancellation point — unlike
# ``to_thread(process.wait)``, which would linger past a ``task.cancel()``.
_PROCESS_POLL_INTERVAL: float = 0.1


def generate_instance_id() -> str:
    """Mint a fresh ephemeral per-launch instance id (Vision §3.2).

    Shape: ``lb-<UTC-compact-ISO8601>-<6-hex>`` — e.g.
    ``lb-20260527T143000Z-7f3a9c``. The timestamp is wall-clock UTC (ADR-015)
    for human readability; the six random hex characters guarantee uniqueness
    even for two launches inside the same second. Matches the regex
    ``^lb-\\d{8}T\\d{6}Z-[0-9a-f]{6}$``.

    Returns:
        The newly generated instance id string.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"lb-{stamp}-{secrets.token_hex(3)}"


def resolve_sender_label(harness_name: str, *, as_label: str | None) -> str:
    """Resolve this endpoint's identity label by the §3.2 priority order.

    Priority (highest first): an explicit ``--as`` value, then the
    ``LETTERBOX_SENDER`` environment variable, then the harness name as the
    default. Empty strings at any level are treated as "not supplied" and fall
    through to the next source — so ``--as ""`` does not shadow
    ``LETTERBOX_SENDER`` (ADR-026: identity is per-launch, never per-channel).

    The environment is read at call time (mirrors ``config``'s call-time-read
    discipline) so tests can ``monkeypatch.setenv`` without an import-time race.

    Args:
        harness_name: The harness/adapter name, used as the fallback label.
        as_label: The value of the ``--as`` flag, or ``None`` when not given.

    Returns:
        The resolved sender label.
    """
    if as_label:
        return as_label
    env_label = os.environ.get("LETTERBOX_SENDER")
    if env_label:
        return env_label
    return harness_name


@dataclass(frozen=True)
class LauncherSession:
    """A started, ready-to-drive PTY-Parent session (runtime-only).

    Holds the live OS handles the 8b injection loop and 8c teardown will
    operate on. Never crosses a persistence boundary, so it carries no
    ``to_dict``/``from_dict``; ``frozen=True`` is hygiene — the mutable live
    objects it references (the ``PTYHandle``, the ``Watcher``, the
    ``asyncio.Queue``) remain mutable inside the frozen wrapper.

    Attributes:
        harness_name: The harness name this session launched.
        instance_id: The ephemeral per-launch instance id (own-write recognition).
        sender_label: The resolved identity label on this channel.
        state_dir: The resolved letterbox state directory (the ``LETTERBOX_HOME``
            join-key value propagated to the agent and its MCP child).
        channel: The open channel handle.
        adapter: The adapter instance whose ``command``/``default_args`` were
            resolved from config (K6); used by 8b for ``inject`` and 8c for
            ``teardown``.
        handle: The PTY handle for the spawned harness process.
        watcher: The started watcher feeding ``queue``.
        queue: The notification queue the watcher writes and 8b drains.
        mcp_config_path: Path to the generated temp MCP config (8c deletes it).
        notification_template: The config-resolved, validated template 8b renders.
        cwd: The working directory the harness was spawned in.
    """

    harness_name: str
    instance_id: str
    sender_label: str
    state_dir: Path
    channel: Channel
    adapter: Adapter
    handle: "PTYHandle"
    watcher: Watcher
    queue: "asyncio.Queue[WatcherEvent]"
    mcp_config_path: Path
    notification_template: str
    cwd: Path


async def setup_launcher(
    harness_name: str,
    channel_name: str,
    *,
    as_label: str | None = None,
    cwd: Path,
    extra_args: list[str] | None = None,
    cli_overrides: dict[str, object] | None = None,
) -> LauncherSession:
    """Assemble a started, ready-to-drive launcher session (Phase 8a).

    This is the assembly slice of the PTY-Parent runtime: resolve identity and
    config, run the fail-loud startup-validation chain, open the channel, sweep
    stale ``.tmp`` files, write the MCP config, spawn the config-resolved harness
    on a PTY with ``--mcp-config`` wired, and start the watcher feeding a queue.
    It deliberately stops at "a started session" — the notification-injection
    loop is 8b and graceful/signal teardown is 8c (K1). Only assembly *rollback*
    lives here (K5).

    The single most load-bearing line is the spawn env's
    ``LETTERBOX_HOME = str(state_dir)`` (K3 / W18): it pins the agent's MCP child
    to the same channel directory the launcher opened, so all three processes
    coordinate on one filesystem location. Without it the conversation could go
    silently dark — the failure class this framework forbids.

    Args:
        harness_name: The harness to launch (also the registry + config key).
        channel_name: The channel to open / create.
        as_label: Optional ``--as`` identity override (highest priority).
        cwd: Working directory to spawn the harness in.
        extra_args: Optional user passthrough args appended after the mandatory
            ``--mcp-config <path>``.
        cli_overrides: Optional flat dict forwarded to ``load_config`` (e.g.
            ``{"state_dir": ...}``).

    Returns:
        A :class:`LauncherSession` with the spawned process and started watcher.

    Raises:
        FileNotFoundError: If the state directory is missing (vector: run
            ``letterbox init``) or the harness command is not on PATH.
        StatePermissionsError: If the state directory is world-accessible.
        KeyError: If the harness is not a registered adapter, or has no
            ``[harness.<name>]`` config block.
        NotificationTemplateError: If the config-resolved notification template
            is invalid (forbidden variable, attribute access, or malformed).
    """
    config = load_config(cli_overrides)
    state_dir = config.state_dir

    sender_label = resolve_sender_label(harness_name, as_label=as_label)
    instance_id = generate_instance_id()

    # ── Startup-validation chain (K4) — every check fails loud BEFORE spawn. ──
    # (1) State-dir permissions; a missing dir becomes a "run letterbox init"
    #     vector rather than the bare stdlib FileNotFoundError (3a: init creates,
    #     the launcher refuses).
    try:
        check_state_dir_permissions(state_dir)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"letterbox state directory {state_dir} does not exist. "
            f"Run `letterbox init` to create it."
        ) from exc

    # (2) Adapter availability — bootstrap the registry, then resolve. An unknown
    #     harness raises KeyError listing the registered names (get_adapter).
    load_builtin_adapters()
    adapter = get_adapter(harness_name)

    # (3) Config override (K6) — the spawned command/args/template come from
    #     config.harnesses, NOT the adapter class attrs (Vision §6.4). get_adapter
    #     returns a fresh, stateless instance per call, so these instance-attr
    #     assignments safely shadow the ClassVar defaults for this one instance.
    try:
        harness_config = config.harnesses[harness_name]
    except KeyError:
        raise KeyError(
            f"No configuration for harness {harness_name!r}. "
            f"Configured harnesses: {sorted(config.harnesses)}. "
            f"Add a [harness.{harness_name}] block to letterbox.toml."
        ) from None
    adapter.command = harness_config.command
    adapter.default_args = list(harness_config.default_args)
    notification_template = harness_config.notification_template

    # (4) Template validity — fail at launch, not on the agent's first
    #     notification (8b). validate_template raises NotificationTemplateError on
    #     a forbidden var / attribute access, but stdlib ValueError on an unclosed
    #     brace; convert the latter so the caller sees one error domain.
    try:
        validate_template(notification_template)
    except ValueError as exc:
        raise NotificationTemplateError(
            f"notification_template for harness {harness_name!r} is malformed "
            f"in letterbox.toml: {exc}"
        ) from exc

    # (5) Effective command on PATH — vector naming the missing binary.
    if shutil.which(adapter.command) is None:
        raise FileNotFoundError(
            f"harness command {adapter.command!r} (for harness {harness_name!r}) "
            f"is not on PATH. Install it, or set [harness.{harness_name}].command "
            f"in letterbox.toml to its absolute path."
        )

    # ── Open the channel, then sweep its stale .tmp files (§3.6 reaper). ──
    channel = Channel.get_or_create(
        channel_name, sender=sender_label, recipient="", state_dir=state_dir
    )
    # Narrow reaper scope (§14 latitude): sweep only the channel we are launching,
    # not every channel under state_dir — we don't touch channels we didn't open.
    reap_orphan_tmp(channel.path)

    # ── Generate MCP config, then spawn + start the watcher (rollback-guarded). ──
    mcp_config_path = generate_mcp_config(
        harness_name, channel_name, sender_label, instance_id
    )

    handle: "PTYHandle | None" = None
    try:
        # The spawn env is fully specified (not merged by the adapter — 5a K6).
        # LETTERBOX_HOME is the W18 join key pinning the MCP child to this state_dir.
        spawn_env = {**os.environ, "LETTERBOX_HOME": str(state_dir)}
        # Mandatory --mcp-config first (predictable position), user passthrough after.
        launch_extra_args = ["--mcp-config", str(mcp_config_path), *(extra_args or [])]
        handle = await adapter.spawn(launch_extra_args, cwd, spawn_env)

        queue: "asyncio.Queue[WatcherEvent]" = asyncio.Queue()
        watcher = Watcher(
            channel,
            self_sender=sender_label,
            self_instance_id=instance_id,
            queue=queue,
        )
        await watcher.start()
    except BaseException:
        # Assembly rollback (K5 / L6): no leaked PTY child, no orphaned temp file.
        if handle is not None:
            await adapter.teardown(handle)
        cleanup_mcp_config(mcp_config_path)
        raise

    return LauncherSession(
        harness_name=harness_name,
        instance_id=instance_id,
        sender_label=sender_label,
        state_dir=state_dir,
        channel=channel,
        adapter=adapter,
        handle=handle,
        watcher=watcher,
        queue=queue,
        mcp_config_path=mcp_config_path,
        notification_template=notification_template,
        cwd=cwd,
    )


async def _injection_loop(session: LauncherSession) -> None:
    """Drain the watcher's event queue, rendering + injecting each notification.

    The consumer half of the PTY-Parent runtime (Phase 8b) and the literal
    realization of Kernel **L3** ("Wake the Agent, Don't Ask Them to Check"):
    the instant a peer writes a message, the watcher queues a
    :class:`~letterbox.watcher.WatcherEvent` and this loop turns it into a
    rendered notification injected into the agent's PTY input stream — no
    polling. Runs forever as an :func:`asyncio.create_task` task owned by 8c's
    ``run_launcher``; 8c (not this loop) handles cancellation and teardown.

    Terminates only on:

    * **Cancellation** — 8c cancels the task at teardown. The sole
      ``await session.queue.get()`` is the single cancellation point;
      ``CancelledError`` propagates cleanly because the only ``except`` is the
      narrow ``OSError`` around ``inject`` (K4).
    * **A dead PTY** — when ``adapter.inject`` raises ``OSError`` the harness's
      slave fd has closed, i.e. the process is gone. A closed PTY is permanent
      (5a's ``inject_to_pty`` loops on short writes, so backpressure blocks
      rather than raises), so the loop surfaces a visible diagnostic to stderr
      and returns rather than spinning the same error on every later event
      (K3 / Vision §12 — silent injection failure is the forbidden failure
      class; ADR-042).

    Every template variable is drawn from the trusted ``WatcherEvent`` fields,
    never from the peer-written message file on disk — the watcher already
    shaped them from its own context (Vision §6.4 / §13.3). This loop's one job
    on the security boundary is to not undo that guarantee: it never reaches
    back to the file for a "richer" value (K2). The notification queue is
    disposable inference (K5): undrained events at teardown are simply dropped,
    since the messages remain on disk (L1).

    Args:
        session: The started session whose ``queue`` this loop drains and whose
            ``adapter`` / ``handle`` it injects into.

    Returns:
        None. Produces side effects only: bytes into the PTY master fd, and an
        ERROR log line on a dead PTY.
    """
    while True:
        evt = await session.queue.get()
        rendered = render_notification(
            session.notification_template,
            channel=evt.channel_name,
            sender=evt.recipient_label,
            message_id=evt.message_id,
            timestamp=evt.timestamp,
        )
        try:
            await session.adapter.inject(session.handle, rendered)
        except OSError as exc:
            # The slave fd has closed — the harness process is gone (§12).
            # Surface loudly (lastResort → stderr with no handler configured)
            # then stop: continuing would spew identical EIO/EBADF on every
            # subsequent event. 8c observes loop completion and tears down.
            _LOGGER.error(
                "letterbox: failed to inject a notification into harness %r — "
                "the PTY closed (the harness process has exited); stopping the "
                "injection loop. Peer messages remain on disk; run "
                "`check_messages` to read them. (%s)",
                session.harness_name,
                exc,
            )
            return


async def _await_process_exit(handle: "PTYHandle") -> None:
    """Block until the spawned harness process exits (the K2/K5 process waiter).

    The third racer in ``run_launcher``'s three-way wait. Between notifications
    the injection loop is parked on ``await queue.get()``, so a harness that
    exits *quietly* (the common case — the user just quits the agent with no
    peer message pending) would never wake the loop. This waiter observes the
    exit directly so ``run_launcher`` returns to the shell instead of hanging
    until a signal.

    Implemented as a pure-asyncio poll (``Popen.poll()`` + ``asyncio.sleep``)
    rather than ``asyncio.to_thread(process.wait)`` (K5): the ``sleep`` is a
    clean cancellation point, so ``run_launcher``'s teardown cancels this task
    instantly regardless of process state — a ``to_thread`` wait would keep its
    worker thread blocked until the process actually died, lingering past the
    cancel and tripping ``filterwarnings=["error"]``. The ~0.1 s detection
    latency (``_PROCESS_POLL_INTERVAL``) is irrelevant against the human-scale
    "agent exited" event.

    Args:
        handle: The PTY handle whose ``process`` (a :class:`subprocess.Popen`)
            this waiter polls.

    Returns:
        None. Returns when the process reports an exit status.
    """
    while handle.process.poll() is None:
        await asyncio.sleep(_PROCESS_POLL_INTERVAL)


async def _teardown_runtime(
    session: LauncherSession,
    racers: "list[asyncio.Task[object]]",
    *,
    teardown_timeout: float,
) -> None:
    """Run the idempotent teardown ladder for a live session (K3).

    Every ``run_launcher`` exit path — signal, harness exit, returned injection
    loop, or external task cancellation — converges here, so the cleanup is
    identical regardless of *why* the conversation ended. The §2.1 clean-exit
    contract is realized as this sequence; L6 ("never leak") is enforced by
    making each step idempotent and the resource-critical ones reachable even if
    an earlier one raises.

    Ladder:

    1. **Cancel-and-settle every racer.** ``gather(..., return_exceptions=True)``
       never re-raises — a child's ``CancelledError`` or an unexpected loop bug
       comes back as a *result element*, not a propagated raise — so the
       resource-critical steps below always run (the 4c discipline; K3 step 2).
       The ``await`` is mandatory under ``filterwarnings=["error"]`` (a pending
       task becomes a ``RuntimeWarning`` → test failure). A settled non-cancel
       exception is logged, never allowed to skip cleanup.
    2. **Stop the watcher**, then **teardown the harness tree**, then **delete
       the temp MCP config** — wrapped in nested ``finally`` so each
       resource-critical step runs even if an earlier one raises. The harness
       teardown reaps the whole process group including the MCP child via
       ``killpg`` (T10), so there is no separate MCP-child reaping. All three
       are individually idempotent (a second call is a clean no-op).

    Args:
        session: The fully-assembled session whose live resources are reclaimed.
        racers: The tasks the race blocked on (injection loop, process waiter,
            signal waiter) — cancelled and settled here.
        teardown_timeout: SIGTERM→SIGKILL grace forwarded to ``adapter.teardown``.

    Returns:
        None.
    """
    for racer in racers:
        racer.cancel()
    results = await asyncio.gather(*racers, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException) and not isinstance(
            result, asyncio.CancelledError
        ):
            _LOGGER.error(
                "letterbox: a launcher task raised during shutdown; continuing "
                "teardown so no process or temp file leaks. (%r)",
                result,
            )

    try:
        await session.watcher.stop()
    finally:
        try:
            await session.adapter.teardown(
                session.handle, timeout=teardown_timeout
            )
        finally:
            cleanup_mcp_config(session.mcp_config_path)


async def run_launcher(
    harness_name: str,
    channel_name: str,
    *,
    as_label: str | None = None,
    cwd: Path,
    extra_args: list[str] | None = None,
    cli_overrides: dict[str, object] | None = None,
    teardown_timeout: float = 5.0,
) -> int:
    """Run a launcher session to completion, then tear it down cleanly (K1).

    The public composer of the PTY-Parent runtime and the realization of
    ``launcher.py``'s ``Filled in: Phase 8a/8b/8c`` promise. It assembles a
    session (8a's :func:`setup_launcher`), runs the notification-injection loop
    (8b's :func:`_injection_loop`) as a task, and blocks until the **first** of a
    three-way race (K2):

    * the **injection loop** returns (the harness died mid-inject — 8b's §12
      dead-PTY diagnostic already surfaced),
    * the **harness process** exits (the common quiet-exit case — K2's third
      racer, :func:`_await_process_exit`), or
    * **SIGINT/SIGTERM** arrives (the user's Ctrl-C — K4).

    Whatever wakes the race, the ``finally`` runs the §2.1 clean-exit ladder
    (:func:`_teardown_runtime`) — so an external ``task.cancel()`` of this
    coroutine triggers the *identical* teardown (K4; this is how 8d and any
    multi-launcher embedding tear down, since ``add_signal_handler`` is one
    handler per signal per loop).

    This is a coroutine, not a sync wrapper: it must ``await setup_launcher``,
    create tasks, and ``add_signal_handler`` on a *running* loop. The
    ``asyncio.run(...)`` boundary belongs to the caller (9a), which maps the
    returned int to ``sys.exit``. There is deliberately no interactive stdin/
    stdout bridge — the PTY-Parent's duties are spawn / watch / inject / teardown
    only (§2.1); ``run_launcher`` blocks on lifecycle events, not terminal bytes.

    Args:
        harness_name: The harness to launch (registry + config key).
        channel_name: The channel to open / create.
        as_label: Optional ``--as`` identity override (highest priority).
        cwd: Working directory to spawn the harness in.
        extra_args: Optional user passthrough args (after the mandatory
            ``--mcp-config <path>``).
        cli_overrides: Optional flat dict forwarded to ``load_config``.
        teardown_timeout: SIGTERM→SIGKILL grace for ``adapter.teardown`` (the
            production default 5.0 doubles as the test seam — tests pass a small
            value to avoid paying the full fake_harness SIGTERM timeout).

    Returns:
        ``0`` on any clean teardown (harness exit, loop return, or handled
        signal / cancellation).

    Raises:
        Exception: Anything :func:`setup_launcher` raises propagates unwrapped —
            8a rolls back its own partial assembly, so there is nothing for
            ``run_launcher`` to clean up and no session to tear down.
    """
    # setup_launcher may raise; if it does, 8a (K5) already reaped the child and
    # deleted the temp config. The teardown try MUST start only after a *fully
    # assembled* session exists (Gotcha #1) — otherwise we'd double-teardown a
    # session 8a already rolled back.
    session = await setup_launcher(
        harness_name,
        channel_name,
        as_label=as_label,
        cwd=cwd,
        extra_args=extra_args,
        cli_overrides=cli_overrides,
    )

    loop = asyncio.get_running_loop()
    signal_event = asyncio.Event()

    def _on_signal() -> None:
        """Wake the race on SIGINT/SIGTERM by setting the shared event (K4)."""
        signal_event.set()

    # POSIX-only: add_signal_handler is one handler per signal per loop and is
    # Unix-only. Tolerate NotImplementedError (non-POSIX) and the off-main-thread
    # ValueError/RuntimeError — the process waiter and external cancellation still
    # provide teardown, so a missing signal handler degrades gracefully (K4).
    installed_signals: list[int] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except (NotImplementedError, RuntimeError, ValueError) as exc:
            _LOGGER.warning(
                "letterbox: could not install a %s handler (%s); relying on "
                "harness-exit detection and task cancellation for teardown.",
                signal.Signals(sig).name,
                exc,
            )
        else:
            installed_signals.append(sig)

    injection_task = asyncio.create_task(_injection_loop(session))
    proc_task = asyncio.create_task(_await_process_exit(session.handle))
    signal_task = asyncio.create_task(signal_event.wait())
    racers: list[asyncio.Task[object]] = [injection_task, proc_task, signal_task]
    try:
        await asyncio.wait(racers, return_when=asyncio.FIRST_COMPLETED)
    finally:
        # Un-register signals FIRST (K3 step 1) so a second signal during
        # teardown can't re-enter and a later run_launcher in the same process
        # starts clean. Then run the resource ladder — reached on signal,
        # process-exit, loop-return, AND cancellation of this coroutine.
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        await _teardown_runtime(
            session, racers, teardown_timeout=teardown_timeout
        )

    return 0

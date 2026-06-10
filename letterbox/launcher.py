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
import contextlib
import logging
import os
import secrets
import select
import shutil
import signal
import sys
import termios
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from letterbox.adapters import load_builtin_adapters
from letterbox.adapters.base import Adapter, get_adapter
from letterbox.adapters.mcp_config import cleanup_mcp_config, generate_mcp_config
from letterbox.channel import Channel, check_state_dir_permissions
from letterbox.config import load_config
from letterbox.locks import claim_pid_lock, release_pid_lock
from letterbox.notifications import (
    NotificationTemplateError,
    render_notification,
    validate_template,
)
from letterbox.adapters.pty_common import get_winsize, raw_mode, set_winsize
from letterbox.protocol import reap_orphan_tmp
from letterbox.watcher import Watcher, WatcherEvent

if TYPE_CHECKING:
    # Type-only import: ``PTYHandle`` is the static type of ``LauncherSession.handle``.
    # The interactive bridge (remediation r1) added a *runtime* dependency on
    # ``pty_common`` (``get_winsize`` / ``set_winsize`` / ``raw_mode`` above —
    # tier-legal: Tier 4 may import ``adapters.pty_common``). ``PTYHandle`` stays
    # type-only because the launcher only ever holds the handle ``adapter.spawn``
    # returns; it never constructs one.
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
        mcp_config_path: Path to the generated temp MCP config (8c deletes it),
            or ``None`` when the harness loads its letterbox MCP server from
            its own settings rather than a ``--mcp-config`` flag (ADR-054).
        notification_template: The config-resolved, validated template 8b renders.
        cwd: The working directory the harness was spawned in.
        pid_lock_path: The pid lock file path claimed at startup; released by
            ``run_launcher``'s ``finally`` (duplicate-instance guard).
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
    mcp_config_path: "Path | None"
    notification_template: str
    cwd: Path
    pid_lock_path: Path


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
        FileNotFoundError: If the harness command is not on PATH. (A missing
            state directory is NOT an error — it is auto-created at mode 0700;
            see ADR-051.)
        StatePermissionsError: If the state directory exists but is
            world-accessible (the security gate is unchanged).
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
    # (1) State dir: self-heal a missing dir by creating it at mode 0700, rather
    #     than refusing (Framework P5 Self-Healing; ADR-051). This is consistent
    #     with Channel.get_or_create, which already mkdir(parents=True)s the
    #     channel subtree — the launcher refusing while the channel layer
    #     auto-creates was an internal inconsistency that surfaced as a raw
    #     traceback on a first run. An EXISTING world-accessible dir is still
    #     refused by check_state_dir_permissions (the security gate is unchanged).
    try:
        check_state_dir_permissions(state_dir)
    except FileNotFoundError:
        # mkdir's mode is umask-masked, so set 0700 explicitly afterward. This
        # only ever tightens a dir we just created (the branch is reached only
        # when stat() raised FileNotFoundError), mirroring `letterbox init
        # --global` (cli._handle_init).
        state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(state_dir, 0o700)

    # (1.5) Duplicate-identity guard — fail before spawn if the same sender label
    #     is already running. Stale locks (dead pid) are silently overwritten.
    pid_lock_path = claim_pid_lock(state_dir, channel_name, sender_label, harness_name)

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

    # ── Generate the MCP config (flag-wired harnesses only), then spawn +
    #    start the watcher (rollback-guarded). A harness whose CLI has no
    #    ``--mcp-config`` flag (Gemini, Antigravity — ADR-054) loads its
    #    letterbox MCP server from its own settings, so the launcher generates
    #    no temp file and injects no flag for it. ──
    mcp_config_path: "Path | None" = None
    if adapter.mcp_config_via_flag:
        mcp_config_path = generate_mcp_config(
            harness_name, channel_name, sender_label, instance_id
        )

    handle: "PTYHandle | None" = None
    try:
        # The spawn env is fully specified (not merged by the adapter — 5a K6).
        # LETTERBOX_HOME is the W18 join key pinning the MCP child to this
        # state_dir. The CHANNEL / SENDER / INSTANCE_ID vars are the runtime
        # join keys a settings-wired harness (no --mcp-config flag) reads via
        # _parse_args's env fallback — the same mechanism the Forge tower
        # orchestrator uses for FORGE_DIALOGUE_CHANNEL (ADR-055). They are
        # harmless for a flag-wired harness (Claude): the explicit --mcp-config
        # flags take precedence over the env in the MCP child's _parse_args.
        spawn_env = {
            **os.environ,
            "LETTERBOX_HOME": str(state_dir),
            "LETTERBOX_CHANNEL": channel_name,
            "LETTERBOX_SENDER": sender_label,
            "LETTERBOX_INSTANCE_ID": instance_id,
        }
        # Mandatory --mcp-config first (predictable position), user passthrough
        # after — but only for flag-wired harnesses (ADR-054). Others pass
        # through just the user's extra_args; their tools come from settings.
        if mcp_config_path is not None:
            launch_extra_args = [
                "--mcp-config",
                str(mcp_config_path),
                *(extra_args or []),
            ]
        else:
            launch_extra_args = list(extra_args or [])
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
        # Assembly rollback (K5 / L6): no leaked PTY child, no orphaned temp file,
        # no orphaned pid lock.
        if handle is not None:
            await adapter.teardown(handle)
        if mcp_config_path is not None:
            cleanup_mcp_config(mcp_config_path)
        release_pid_lock(pid_lock_path)
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
        pid_lock_path=pid_lock_path,
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
       the temp MCP config** (skipped when ``mcp_config_path`` is ``None`` —
       a settings-wired harness generated no temp file, ADR-054) — wrapped in
       nested ``finally`` so each resource-critical step runs even if an
       earlier one raises. The harness
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
            if session.mcp_config_path is not None:
                cleanup_mcp_config(session.mcp_config_path)


def _resolve_user_fd(explicit: int | None, stream: object) -> int | None:
    """Resolve a user-terminal fd: an explicit override, else the stream's fileno.

    Returns ``None`` when no real fd is available — e.g. pytest has replaced
    ``sys.stdin`` with a capture object whose ``fileno`` raises — so the caller
    treats the launch as non-interactive and skips the bridge.

    Args:
        explicit: An explicit fd passed to :func:`run_launcher` (the pty-pair test
            seam), or ``None`` to fall back to ``stream``.
        stream: The standard stream (``sys.stdin`` / ``sys.stdout``) to query.

    Returns:
        The resolved integer fd, or ``None`` if none is available.
    """
    if explicit is not None:
        return explicit
    try:
        return stream.fileno()  # type: ignore[attr-defined]
    except (AttributeError, ValueError, OSError):
        return None


class _TerminalBridge:
    """Bidirectional byte relay between the controlling tty and the harness PTY.

    The missing interactive link in the PTY-Parent (remediation r1): without it a
    human sees a blank screen and cannot type to the agent. On the ``script(1)`` /
    ``pexpect.interact()`` pattern, it does three things for the duration of an
    interactive session:

    * **Relay** — a single dedicated thread (Vision §2.3: ``os.read``/``os.write``
      on master fds are blocking, so they belong off the event loop) runs a
      ``select`` loop shuttling user keystrokes ``user_stdin_fd → master_fd`` and
      harness output ``master_fd → user_stdout_fd``. The thread is **not** an
      asyncio lifecycle racer — it is pure byte transport; teardown decisions stay
      with ``run_launcher``'s three racers.
    * **Raw mode** — the controlling tty is put in raw mode (via the reusable
      :func:`~letterbox.adapters.pty_common.raw_mode` primitive held in an
      ``ExitStack``) so control chars and escape sequences pass through
      byte-faithfully, and is restored on **every** exit path.
    * **Window size** — the harness PTY is sized to the user terminal at start and
      re-sized on ``SIGWINCH`` (handled on the loop's main thread, never the relay
      thread).

    **Two writers, no lock** (Vision §2.2): both this relay (stdin→master) and the
    injection loop write ``master_fd``. Injected notifications are short (well under
    ``PIPE_BUF``), so an ``os.write`` cannot tear a keystroke burst; and the harness
    echoing injected bytes back through ``master_fd`` is exactly what makes a 📬
    notification visible on the user's screen.

    **Raw-mode Ctrl-C** (Vision §2.3): raw mode disables ``ISIG``, so the user's
    ``Ctrl-C`` (``0x03``) is relayed to the agent (correct ``script(1)`` behavior)
    rather than raising ``SIGINT`` in the launcher. Teardown is driven by harness
    exit (the user quits the agent) or an external signal — both already handled by
    the race; this is intended behavior, not a regression.

    Lifecycle is :meth:`start` / :meth:`stop`; ``stop`` is idempotent and
    partial-start safe (it restores only what ``start`` actually set).
    """

    # Generous join bound: the self-pipe wakes the relay's ``select`` immediately,
    # so the join is normally instant. The daemon thread can never block process
    # exit, so this is belt-and-suspenders only.
    _RELAY_THREAD_JOIN_TIMEOUT: float = 2.0

    def __init__(
        self,
        *,
        user_stdin_fd: int,
        user_stdout_fd: int,
        master_fd: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Record the fds and loop the bridge will operate on (no I/O here).

        Args:
            user_stdin_fd: The controlling terminal's input fd (read by the relay,
                put in raw mode).
            user_stdout_fd: The controlling terminal's output fd (written by the
                relay with harness output).
            master_fd: The harness PTY master fd (``session.handle.master_fd``).
            loop: The running event loop the SIGWINCH handler installs on.
        """
        self._user_stdin_fd = user_stdin_fd
        self._user_stdout_fd = user_stdout_fd
        self._master_fd = master_fd
        self._loop = loop
        self._raw_stack = contextlib.ExitStack()
        self._pipe_r: int | None = None
        self._pipe_w: int | None = None
        self._thread: threading.Thread | None = None
        self._sigwinch_installed = False
        self._stopped = False

    def start(self) -> None:
        """Size the PTY, enter raw mode, start the relay thread, install SIGWINCH.

        Returns:
            None.
        """
        # 1. Initial window size — best-effort; a failure must not abort the launch.
        self._propagate_winsize()
        # 2. Raw mode via the reusable primitive, held in an ExitStack so stop()
        #    restores it idempotently and partial-start-safely.
        self._raw_stack.enter_context(raw_mode(self._user_stdin_fd))
        # 3. Self-pipe shutdown channel for the relay thread.
        self._pipe_r, self._pipe_w = os.pipe()
        # 4. Relay thread (blocking master-fd I/O lives off the event loop — §2.3).
        self._thread = threading.Thread(
            target=self._relay_loop, name="letterbox-pty-relay", daemon=True
        )
        self._thread.start()
        # 5. SIGWINCH on the loop (main thread only), tolerant like SIGINT/SIGTERM.
        try:
            self._loop.add_signal_handler(signal.SIGWINCH, self._on_resize)
        except (NotImplementedError, RuntimeError, ValueError, AttributeError) as exc:
            _LOGGER.warning(
                "letterbox: could not install a SIGWINCH handler (%s); window-resize "
                "propagation is disabled for this session.",
                exc,
            )
        else:
            self._sigwinch_installed = True

    def stop(self) -> None:
        """Tear down the bridge: remove SIGWINCH, stop the relay, restore the tty.

        Idempotent and partial-start safe. The tty is restored **last** and
        unconditionally (in a ``finally``) so it is cooked again before any
        teardown log lines print to the user's screen.

        Returns:
            None.
        """
        if self._stopped:
            return
        self._stopped = True
        try:
            # Remove SIGWINCH FIRST so a resize during teardown can't re-enter.
            if self._sigwinch_installed:
                with contextlib.suppress(
                    ValueError, RuntimeError, NotImplementedError
                ):
                    self._loop.remove_signal_handler(signal.SIGWINCH)
                self._sigwinch_installed = False
            # Wake the relay's select via the self-pipe, then join the thread.
            if self._pipe_w is not None:
                with contextlib.suppress(OSError):
                    os.write(self._pipe_w, b"\x00")
            if self._thread is not None:
                self._thread.join(timeout=self._RELAY_THREAD_JOIN_TIMEOUT)
            # Close the self-pipe (only after the thread that reads it is gone).
            if self._pipe_r is not None:
                with contextlib.suppress(OSError):
                    os.close(self._pipe_r)
            if self._pipe_w is not None:
                with contextlib.suppress(OSError):
                    os.close(self._pipe_w)
        finally:
            # Restore the controlling tty LAST (§2.4 step 4). ExitStack.close() is a
            # no-op if start() never entered the raw_mode context (partial start).
            # The restore MUST NOT raise past here (§2.4): if the controlling
            # terminal has vanished mid-session (SSH drop, emulator killed), the
            # tcsetattr restore raises EIO — log and continue so teardown still
            # reaps the harness and deletes the temp config (Framework P5 / L6).
            try:
                self._raw_stack.close()
            except (termios.error, OSError) as exc:
                _LOGGER.warning(
                    "letterbox: could not restore terminal attributes at teardown "
                    "(%s); the controlling terminal may have closed.",
                    exc,
                )

    def _on_resize(self) -> None:
        """SIGWINCH handler (main thread): re-mirror the user size onto the PTY."""
        self._propagate_winsize()

    def _propagate_winsize(self) -> None:
        """Read the user terminal's size and push it to the harness PTY."""
        try:
            rows, cols = get_winsize(self._user_stdin_fd)
            set_winsize(self._master_fd, rows, cols)
        except OSError as exc:
            _LOGGER.warning(
                "letterbox: could not propagate window size to the harness (%s).",
                exc,
            )

    def _relay_loop(self) -> None:
        """Shuttle bytes both directions until shutdown / EOF (the relay thread)."""
        watch = [self._user_stdin_fd, self._master_fd, self._pipe_r]
        while True:
            try:
                readable, _, _ = select.select(watch, [], [])
            except (OSError, ValueError):
                return  # A watched fd was closed under us — stop relaying.
            if self._pipe_r in readable:
                return  # stop() asked us to exit.
            if self._master_fd in readable:
                try:
                    data = os.read(self._master_fd, 4096)
                except OSError:
                    return  # EIO/EBADF — the harness PTY is gone.
                if not data:
                    return  # master EOF — the harness exited.
                try:
                    self._write_all(self._user_stdout_fd, data)
                except OSError:
                    return
            if self._user_stdin_fd in readable:
                try:
                    data = os.read(self._user_stdin_fd, 4096)
                except OSError:
                    # User end gone; keep relaying harness output to the screen.
                    watch = [fd for fd in watch if fd != self._user_stdin_fd]
                    continue
                if not data:
                    # User stdin EOF; keep relaying harness output.
                    watch = [fd for fd in watch if fd != self._user_stdin_fd]
                    continue
                try:
                    self._write_all(self._master_fd, data)
                except OSError:
                    return  # Harness PTY closed mid-write.

    @staticmethod
    def _write_all(fd: int, data: bytes) -> None:
        """Write all of ``data`` to ``fd``, looping on short writes."""
        written = 0
        while written < len(data):
            written += os.write(fd, data[written:])


async def run_launcher(
    harness_name: str,
    channel_name: str,
    *,
    as_label: str | None = None,
    cwd: Path,
    extra_args: list[str] | None = None,
    cli_overrides: dict[str, object] | None = None,
    user_stdin_fd: int | None = None,
    user_stdout_fd: int | None = None,
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
    returned int to ``sys.exit``.

    When both user fds are real ttys (the interactive case), a
    :class:`_TerminalBridge` relays bytes between the controlling terminal and the
    harness PTY: a dedicated thread runs a ``select`` loop shuttling user
    keystrokes to ``master_fd`` and harness output back to the user's stdout, the
    controlling tty is put in raw mode (restored on every exit path), and
    ``SIGWINCH`` propagates window-size changes to the PTY. The relay is pure byte
    transport and is **not** one of the lifecycle racers — teardown decisions still
    come solely from the three racers below. Two writers touch ``master_fd`` (this
    relay and the injection loop) with no lock: injected notifications are well
    under ``PIPE_BUF`` so an ``os.write`` cannot tear a keystroke burst, and the
    harness echoing injected bytes back through ``master_fd`` is what makes a 📬
    notification visible on the user's screen. In raw mode ``ISIG`` is off, so the
    user's Ctrl-C is relayed to the agent rather than raising ``SIGINT`` here —
    quitting the agent (harness exit) or an external signal drives teardown, both
    already handled by the race. Under pytest, pipes, or redirection the fds are not
    ttys and the bridge is skipped entirely: ``run_launcher`` is then lifecycle-only
    (spawn / watch / inject / teardown), exactly as before.

    Args:
        harness_name: The harness to launch (registry + config key).
        channel_name: The channel to open / create.
        as_label: Optional ``--as`` identity override (highest priority).
        cwd: Working directory to spawn the harness in.
        extra_args: Optional user passthrough args (after the mandatory
            ``--mcp-config <path>``).
        cli_overrides: Optional flat dict forwarded to ``load_config``.
        user_stdin_fd: Optional override for the controlling terminal's input fd
            (defaults to ``sys.stdin``). The pty-pair test harness substitutes a
            fake user terminal here; production leaves it ``None``.
        user_stdout_fd: Optional override for the controlling terminal's output fd
            (defaults to ``sys.stdout``). The interactive bridge engages only when
            both resolved fds are real ttys.
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

    # Interactive terminal bridge (§2.6): engage ONLY when both user fds are real
    # ttys. Under pytest / pipes / redirection (the world the existing suite runs
    # in) the fds are not ttys, so the bridge is skipped and run_launcher behaves
    # exactly as the lifecycle-only original — this guard is what keeps that suite
    # green. ``letterbox claude > log`` is likewise a clean no-op, not a crash.
    in_fd = _resolve_user_fd(user_stdin_fd, sys.stdin)
    out_fd = _resolve_user_fd(user_stdout_fd, sys.stdout)
    bridge: "_TerminalBridge | None" = None
    if (
        in_fd is not None
        and out_fd is not None
        and os.isatty(in_fd)
        and os.isatty(out_fd)
    ):
        bridge = _TerminalBridge(
            user_stdin_fd=in_fd,
            user_stdout_fd=out_fd,
            master_fd=session.handle.master_fd,
            loop=loop,
        )

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
        # Start the bridge inside the try so its stop() is guaranteed by the
        # finally even if start() partially completes (stop() is partial-safe).
        if bridge is not None:
            bridge.start()
        await asyncio.wait(racers, return_when=asyncio.FIRST_COMPLETED)
    finally:
        # Un-register signals FIRST (K3 step 1) so a second signal during
        # teardown can't re-enter and a later run_launcher in the same process
        # starts clean. Then stop the bridge — restoring the controlling tty
        # BEFORE the teardown ladder's logs print (so they aren't rendered raw) —
        # and finally run the resource ladder. Reached on signal, process-exit,
        # loop-return, AND cancellation of this coroutine.
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        if bridge is not None:
            bridge.stop()
        release_pid_lock(session.pid_lock_path)
        await _teardown_runtime(
            session, racers, teardown_timeout=teardown_timeout
        )

    return 0

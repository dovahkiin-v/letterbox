"""argparse top-level dispatch — routes subcommands to launcher / mcp_server / utility handlers.

Tier: 4
May import from: stdlib (including ``argparse``); Tier 1 (``config``, ``protocol``); Tier 2 (``channel``).
Must NOT import from: ``letterbox.launcher`` or ``letterbox.mcp_server`` at module load time —
    those are imported LAZILY inside their respective subcommand handlers (bulkhead §13.5,
    avoids cross-sibling-Tier-4 module-load dependency).

Filled in: Phase 9a/9b/9c/9d per PHASE_INDEX.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.resources
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Tier ≤2 leaves/services — safe at module level (G6). The §13.5 bulkhead
# forbids only the two heavy Tier-4 siblings (``launcher``/``mcp_server``),
# which stay lazy inside their handlers. Mirrors how ``mcp_server`` (also
# Tier 4) imports ``channel``/``config``/``protocol`` at module top.
from letterbox import channel, config, protocol

__all__ = ["main"]

# Harness subcommands route to ``launcher.run_launcher``. The three names match
# the registered adapter keys (W12, 8a) and the ``[harness.*]`` config blocks.
_HARNESSES = ("claude", "gemini", "antigravity")

# Subcommands with a fixed, closed flag surface that must REJECT leftover argv
# with a vector error (Framework P3 / G2). Harnesses plus the two read commands
# 9b lands, ``init`` (9c's closed ``--channel``/``--global`` surface), and
# ``prune`` (9d's closed retention surface). ``mcp`` is the sole subcommand that
# forwards leftover argv raw; every other subcommand now has a real, closed flag
# surface (the transitional utility-stub scaffold is gone — 9d was the last stub
# to graduate).
_REJECTS_UNKNOWN = _HARNESSES + ("tail", "list-channels", "init", "prune")

# Output-format / color flag surfaces shared by ``tail`` and ``list-channels``
# (and clonable by 9c/9d). Plain is the §11.1 default — jq/screen-reader safe.
_FORMAT_CHOICES = ("plain", "rich")
_COLOR_CHOICES = ("auto", "always", "never")

# UX knob, not a budget gate (§13 latitude): how long ``tail --follow`` sleeps
# between polls. A module-private constant is more grep-discoverable than an
# inline literal (4c/5b naming precedent). 1.0s is imperceptible to a human
# reading a live thread and costs one cheap filename-only scandir per tick.
_FOLLOW_POLL_INTERVAL_SECONDS = 1.0

# Minimal ANSI SGR codes — applied ONLY in rich mode (K4). Plain output is
# unconditionally colorless, which is what makes ``--color=never`` trivially
# escape-free and plain the screen-reader / pipe-safe default.
_ANSI_DIM = "2"
_ANSI_BOLD = "1"
_ANSI_RESET = "\x1b[0m"


def main(argv: list[str] | None = None) -> int:
    """Console-script entry point — argparse top-level dispatch (Vision §7.1/§7.2).

    Routes ``claude``/``gemini``/``antigravity`` to :func:`launcher.run_launcher`
    under an :func:`asyncio.run` boundary (K4); forwards ``mcp``'s raw argv
    verbatim to :func:`mcp_server.run` (K2 — never re-parses the join-key flags);
    and routes the ``tail``/``list-channels``/``init``/``prune`` utility commands
    (9b/9c/9d) to their handlers.

    Args:
        argv: The argument vector to dispatch. ``None`` falls back to
            ``sys.argv[1:]`` (the console-script case).

    Returns:
        The process exit code. setuptools' ``console_scripts`` wrapper calls
        ``sys.exit(main())``, so this int becomes the process exit status.
    """
    raw = list(sys.argv[1:] if argv is None else argv)

    # K2 — the load-bearing restraint. ``letterbox mcp ...`` is agent-driven
    # (generate_mcp_config §7.1 always emits ``["mcp", "--channel", ...]`` with
    # ``mcp`` leading), so intercept it BEFORE argparse can touch the join-key
    # flags. The ``--channel``/``--as``/``--instance-id`` spellings live ONLY in
    # ``mcp_server._parse_args``; redeclaring them here is the W13 silent-failure
    # trap — a drifted spelling makes the agent's MCP child parse-error on spawn
    # and the channel goes silent with no error anyone looks for. Forward raw,
    # never re-parse.
    if raw and raw[0] == "mcp":
        return _dispatch_mcp(raw[1:])

    # K1 — manual ``--`` passthrough split (NOT argparse.REMAINDER): everything
    # after the FIRST ``--`` is verbatim harness extra-args (§7.2). Deterministic,
    # and sidesteps argparse's REMAINDER quirks.
    pre, extra_args = _split_passthrough(raw)
    parser = _build_parser()
    # parse_known_args (not parse_args) so ``mcp`` can absorb leftover argv —
    # argparse.REMAINDER refuses leading optional-looking tokens (``--channel``)
    # on 3.13 subparsers, so REMAINDER is unusable here. The ``unknown`` leftovers
    # are forwarded raw by ``mcp``; every other subcommand rejects any leftover
    # below (the transitional stubs were all replaced through 9a-9d).
    args, unknown = parser.parse_known_args(pre)
    # Subcommands with a fixed, closed flag surface — reject stray flags with a
    # vector error (Framework P3 / G2). ``mcp`` is the sole subcommand that
    # deliberately absorbs leftover argv (forwarding it raw to the MCP child);
    # all others are in ``_REJECTS_UNKNOWN``.
    if args.command in _REJECTS_UNKNOWN and unknown:
        parser.error(f"unrecognized arguments: {' '.join(unknown)}")
    return args.handler(args, extra_args, unknown)


def _split_passthrough(raw: list[str]) -> tuple[list[str], list[str]]:
    """Split argv on the FIRST ``--`` into (parse-half, harness-passthrough).

    The ``--`` token itself is dropped. With no ``--`` present, the whole vector
    is the parse-half and the passthrough is empty (Gotcha #2).

    Args:
        raw: The full argument vector (after any ``mcp`` intercept).

    Returns:
        A ``(pre, extra_args)`` pair: ``pre`` is parsed by argparse, ``extra_args``
        is forwarded verbatim to the spawned harness.
    """
    if "--" in raw:
        idx = raw.index("--")
        return raw[:idx], raw[idx + 1 :]
    return raw, []


def _build_parser() -> argparse.ArgumentParser:
    """Build the top-level parser with one subparser per subcommand.

    The harness subparsers carry the shared ``--channel``/``--as``/``--cwd``
    flags; the top-level parser carries the global ``--config`` (§7.3 form
    ``letterbox --config ./my.toml claude ...``). ``mcp`` is registered for
    ``--help`` and invalid-choice coherence (its real dispatch is the
    pre-argparse intercept in :func:`main`, K2); ``init`` carries its closed
    ``--channel``/``--global`` surface (9c); ``prune`` carries its closed
    retention surface (9d — required ``--channel``, a required one-of selection
    rule, and the ``--delete``/``--yes-i-am-sure`` action modifiers).

    Returns:
        The configured :class:`argparse.ArgumentParser`.
    """
    parser = argparse.ArgumentParser(
        prog="letterbox",
        description="File-based real-time comms between two AI agents in separate terminals.",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        help="Path to a letterbox.toml to use (sets LETTERBOX_CONFIG; later sources win, §8.1).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in _HARNESSES:
        sub = subparsers.add_parser(name, help=f"Launch {name} on a channel.")
        _add_harness_flags(sub)
        sub.set_defaults(handler=_handle_harness, harness_name=name)

    # mcp — registered for help/invalid-choice coherence only; the canonical
    # agent invocation (mcp leading) is intercepted before argparse in main().
    # It declares NO flags, so the non-canonical ``letterbox --config x mcp ...``
    # ordering routes the join-key argv through ``parse_known_args`` leftovers,
    # forwarded untouched (K2 — 9a never names --channel/--as/--instance-id).
    mcp_sub = subparsers.add_parser(
        "mcp",
        help="(internal) stdio MCP server spawned by the agent — not for direct human use.",
    )
    mcp_sub.set_defaults(handler=_handle_mcp)

    # tail — the human's window into a channel: streams message JSON (one object
    # per line) to stdout for jq-friendly consumption (§11.1). Read-only.
    tail_sub = subparsers.add_parser(
        "tail", help="Stream a channel's messages (JSONL on stdout)."
    )
    tail_sub.add_argument("--channel", required=True, help="Channel name to read.")
    tail_sub.add_argument(
        "--follow",
        "-f",
        action="store_true",
        help="Keep streaming newly-arriving messages until Ctrl-C.",
    )
    _add_output_flags(tail_sub)
    tail_sub.set_defaults(handler=_handle_tail)

    # list-channels — "what's even running?" Lists channels with last-activity.
    lc_sub = subparsers.add_parser(
        "list-channels", help="List channels with last-activity timestamps."
    )
    _add_output_flags(lc_sub)
    lc_sub.set_defaults(handler=_handle_list_channels)

    # init — the user's on-ramp: scaffolds a starting ``letterbox.toml`` so a
    # newcomer doesn't hand-author TOML (§11.1). Closed flag surface (K4): only
    # ``--channel`` (pre-populate the sample's ``[[channels]]`` entry) and
    # ``--global`` (write ``~/.letterbox/config.toml`` instead of
    # ``./letterbox.toml``). ``dest="is_global"`` because ``global`` is a Python
    # keyword (Gotcha #1, mirrors ``--as`` → ``as_label``). It's in
    # ``_REJECTS_UNKNOWN`` so ``init --bogus`` is a vector error (exit 2).
    init_sub = subparsers.add_parser(
        "init", help="Scaffold a starting letterbox.toml (project-local by default)."
    )
    init_sub.add_argument(
        "--channel",
        default=None,
        help="Pre-populate the scaffolded [[channels]] entry with this name.",
    )
    init_sub.add_argument(
        "--global",
        dest="is_global",
        action="store_true",
        help="Write ~/.letterbox/config.toml instead of ./letterbox.toml.",
    )
    init_sub.set_defaults(handler=_handle_init)

    # prune — the user's only path to reclaim space in an unbounded channel
    # (§3.5), and the codebase's ONLY destructive command (Kernel L8). Closed
    # surface: a required ``--channel``, exactly one selection rule (a
    # ``required=True`` mutually-exclusive group, so a bare ``prune --channel x``
    # is an argparse error — no "prune everything by accident"), and the two
    # action modifiers ``--delete``/``--yes-i-am-sure`` (the K2 safety matrix).
    prune_sub = subparsers.add_parser(
        "prune",
        help="Reclaim channel space (dry-run by default; --yes-i-am-sure to act).",
    )
    prune_sub.add_argument("--channel", required=True, help="Channel name to prune.")
    rule = prune_sub.add_mutually_exclusive_group(required=True)
    rule.add_argument(
        "--older-than",
        type=_parse_duration,
        metavar="DURATION",
        help="Match messages created before now minus DURATION (e.g. 7d, 2h, 30m, 45s).",
    )
    rule.add_argument(
        "--keep-last",
        type=_nonneg_int,
        metavar="N",
        help="Match all but the N newest messages (N >= 0; 0 matches all).",
    )
    rule.add_argument(
        "--acknowledged-by-all",
        action="store_true",
        help="Match messages every known endpoint has acknowledged (fails safe to none).",
    )
    prune_sub.add_argument(
        "--delete",
        action="store_true",
        help="Delete matched files instead of moving them to cold/ (requires --yes-i-am-sure).",
    )
    prune_sub.add_argument(
        "--yes-i-am-sure",
        dest="yes_i_am_sure",
        action="store_true",
        help="Actually perform the action; without it, prune only previews (dry-run).",
    )
    prune_sub.set_defaults(handler=_handle_prune)

    return parser


def _add_output_flags(sub: argparse.ArgumentParser) -> None:
    """Add the shared ``--format``/``--color`` flags to a read-command subparser.

    Both default to the §11.1 plain/auto contract: ``--format=plain`` emits the
    pipe- and screen-reader-safe form (JSONL for ``tail``, tab-separated for
    ``list-channels``); ``--color=auto`` colorizes only when stdout is a TTY and
    only in rich mode (K4). Cloned by 9c/9d as ``init``/``prune`` land.

    Args:
        sub: The subparser to augment.
    """
    sub.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default="plain",
        help="Output format (default: plain — jq/screen-reader friendly).",
    )
    sub.add_argument(
        "--color",
        choices=_COLOR_CHOICES,
        default="auto",
        help="Colorize rich output (default: auto — color only when stdout is a TTY).",
    )


def _add_harness_flags(sub: argparse.ArgumentParser) -> None:
    """Add the shared harness flags (``--channel``/``--as``/``--cwd``) to a subparser.

    ``--as`` maps to ``dest="as_label"`` because ``as`` is a Python keyword
    (Gotcha #1); the value is passed straight through to
    ``run_launcher(as_label=...)`` — 9a resolves no identity itself.

    Args:
        sub: The harness subparser to augment.
    """
    sub.add_argument("--channel", required=True, help="Channel name to open / create.")
    sub.add_argument(
        "--as",
        dest="as_label",
        help="This endpoint's identity label (default: LETTERBOX_SENDER env, else harness name).",
    )
    sub.add_argument(
        "--cwd",
        help="Working directory to spawn the harness in (default: current directory; ~ expands).",
    )


def _handle_harness(
    args: argparse.Namespace, extra_args: list[str], unknown: list[str]
) -> int:
    """Dispatch a harness subcommand to ``launcher.run_launcher`` (K4).

    Owns the ``asyncio.run`` boundary (``run_launcher`` is a coroutine so 9a, not
    the launcher, holds the running loop for ``add_signal_handler``). ``--config``
    rides the ``LETTERBOX_CONFIG`` env lever (K5), not ``cli_overrides`` (whose
    whitelist is ``state_dir``-only). Identity resolution stays in the launcher.

    Args:
        args: Parsed namespace (carries ``harness_name``, ``channel``, ``as_label``,
            ``cwd``, ``config``).
        extra_args: Verbatim passthrough args (everything after ``--``).
        unknown: Leftover argv — always empty here (``main`` rejects stray harness
            flags before dispatching).

    Returns:
        ``run_launcher``'s flat exit code (``0`` on any clean teardown, 8c K5).
    """
    if args.config:
        os.environ["LETTERBOX_CONFIG"] = os.path.abspath(os.path.expanduser(args.config))
    cwd = Path(args.cwd).expanduser() if args.cwd else Path.cwd()

    from letterbox import launcher

    return asyncio.run(
        launcher.run_launcher(
            args.harness_name,
            args.channel,
            as_label=args.as_label,
            cwd=cwd,
            extra_args=extra_args,
            cli_overrides=None,
        )
    )


def _handle_mcp(
    args: argparse.Namespace, extra_args: list[str], unknown: list[str]
) -> int:
    """Forward a subparser-routed ``mcp`` invocation to :func:`mcp_server.run` (K2).

    Only reached for the non-canonical ordering (e.g. ``letterbox --config x mcp
    ...``); the canonical agent invocation is intercepted before argparse in
    :func:`main`. Either way the raw argv is forwarded untouched.

    Args:
        args: Parsed namespace (the mcp subparser declares no flags of its own).
        extra_args: Verbatim passthrough from any ``--`` split (unused by the
            mcp server, which takes only the join-key argv).
        unknown: The join-key argv left over by ``parse_known_args`` — forwarded
            byte-for-byte.

    Returns:
        ``0`` after ``mcp_server.run`` returns (it blocks until SIGTERM).
    """
    return _dispatch_mcp(unknown)


def _dispatch_mcp(mcp_args: list[str]) -> int:
    """Run the stdio MCP server with the verbatim forwarded argv (K2).

    Args:
        mcp_args: Everything after the ``mcp`` token — forwarded byte-for-byte
            to ``mcp_server.run`` with no reinterpretation.

    Returns:
        ``0`` after the server's blocking serve loop returns on SIGTERM.
    """
    from letterbox import mcp_server

    mcp_server.run(mcp_args)
    return 0


# ──────────────────────────────────────────────────────────────────────
# Read-command handlers: tail + list-channels (9b)
# ──────────────────────────────────────────────────────────────────────


def _resolve_state_dir(args: argparse.Namespace) -> Path:
    """Resolve the state dir, honoring the global ``--config`` flag (K5).

    Mirrors ``_handle_harness``'s ``LETTERBOX_CONFIG`` env-lever idiom: a
    ``--config <path>`` sets the env var (so ``load_config`` consults that
    TOML's ``[letterbox] state_dir``), then the resolved ``state_dir`` is read
    via ``load_config`` — NOT ``resolve_state_dir``, which ignores config files
    (§8.1 precedence). Shared by both read handlers and clonable by 9c/9d.

    Args:
        args: Parsed namespace (carries the top-level ``config`` flag).

    Returns:
        The resolved state directory path.
    """
    if args.config:
        os.environ["LETTERBOX_CONFIG"] = os.path.abspath(os.path.expanduser(args.config))
    return config.load_config().state_dir


def _should_use_color(choice: str, stream: object) -> bool:
    """Resolve the ``--color`` tri-state against an output stream (K4).

    Args:
        choice: One of ``"auto"``, ``"always"``, ``"never"``.
        stream: The destination stream (checked for ``.isatty()`` under
            ``auto``); a non-TTY (pipe, file, ``StringIO``, capsys) → no color.

    Returns:
        ``True`` if color escapes should be emitted, ``False`` otherwise.
    """
    if choice == "never":
        return False
    if choice == "always":
        return True
    isatty = getattr(stream, "isatty", None)
    return bool(isatty()) if callable(isatty) else False


def _colorize(text: str, code: str, *, enabled: bool) -> str:
    """Wrap ``text`` in an ANSI SGR ``code`` when ``enabled``, else return it raw.

    Args:
        text: The text to (maybe) colorize.
        code: The SGR parameter (e.g. ``"1"`` bold, ``"2"`` dim).
        enabled: Whether color is active (resolved by :func:`_should_use_color`).

    Returns:
        The escaped string when ``enabled``; the unchanged ``text`` otherwise.
    """
    if not enabled:
        return text
    return f"\x1b[{code}m{text}{_ANSI_RESET}"


def _render_message(msg: protocol.Message, *, fmt: str, use_color: bool) -> str:
    """Render one message for ``tail`` output.

    Plain (default) → the canonical §3.2 JSONL line via
    :func:`protocol.to_json_bytes` (K2 — no hand-rolled serializer, so the
    ``ensure_ascii=False`` discipline holds by delegation). Rich → one
    human-readable line ``[timestamp] sender → recipient: body`` (the arrow is
    omitted when ``recipient`` is ``None``); color applies in rich mode only.

    Args:
        msg: The message to render.
        fmt: ``"plain"`` or ``"rich"``.
        use_color: Whether to emit ANSI escapes (rich mode only).

    Returns:
        A single rendered line (no trailing newline).
    """
    if fmt == "rich":
        ts = _colorize(msg.timestamp, _ANSI_DIM, enabled=use_color)
        sender = _colorize(msg.sender, _ANSI_BOLD, enabled=use_color)
        if msg.recipient is None:
            return f"[{ts}] {sender}: {msg.body}"
        return f"[{ts}] {sender} → {msg.recipient}: {msg.body}"
    return protocol.to_json_bytes(msg).decode("utf-8")


def _tail_once(
    channel_dir: Path,
    since: str | None,
    *,
    fmt: str,
    use_color: bool,
    out: object,
    err: object,
) -> str | None:
    """Scan, render, and emit one batch of messages newer than ``since``.

    Pure poll cycle (no sleep) so the follow loop is testable in isolation.
    Each message renders to ``out`` (stdout — data); a corrupt file produces a
    single WARN to ``err`` (stderr — diagnostic) and is skipped, keeping stdout
    pure JSONL for ``jq`` (K3). The file is never touched (Kernel L8). A
    prune-race ``FileNotFoundError`` between listing and reading is skipped
    silently (the message is simply gone, not broken).

    Args:
        channel_dir: The channel directory to scan (already existence-checked).
        since: The last filename seen, or ``None`` for the full backlog. Passed
            to :func:`protocol.list_messages` as a strictly-greater cursor (G5).
        fmt: ``"plain"`` or ``"rich"``.
        use_color: Whether to emit ANSI escapes (rich mode only).
        out: Destination for message data (stdout).
        err: Destination for diagnostics (stderr).

    Returns:
        The new cursor (the last filename seen this cycle), or ``since``
        unchanged when no new messages arrived.
    """
    paths = protocol.list_messages(channel_dir, since=since)
    for path in paths:
        try:
            result = protocol.read_message(path)
        except FileNotFoundError:
            # Prune-race (IMPLEMENTATION_NOTES 2c): listed, then removed before
            # read. Gone, not corrupt — skip without a diagnostic.
            continue
        if isinstance(result, protocol.ParseError):
            print(
                f"letterbox tail: skipping corrupt message {result.path.name}: "
                f"{result.reason}",
                file=err,
            )
            continue
        print(_render_message(result, fmt=fmt, use_color=use_color), file=out)
    return paths[-1].name if paths else since


def _follow_loop(
    channel_dir: Path,
    cursor: str | None,
    *,
    fmt: str,
    use_color: bool,
    out: object,
    err: object,
) -> int:
    """Poll ``channel_dir`` for new messages until Ctrl-C (G4).

    Sleeps ``_FOLLOW_POLL_INTERVAL_SECONDS`` then re-scans via
    :func:`_tail_once`, advancing the cursor each tick. ``KeyboardInterrupt``
    (SIGINT — the user intentionally stopping ``--follow``) is the normal exit:
    caught, returns ``0`` quietly (matches 8c's flat-``0`` convention). If the
    channel directory is removed out from under a live follow (``rm`` of the
    channel), ``list_messages`` raises ``FileNotFoundError`` — caught here and
    turned into a clear stderr vector + nonzero exit rather than a traceback
    (Framework P3 — no dead ends; the G3 entry pre-check guards startup, this
    guards mid-follow disappearance).

    Args:
        channel_dir: The channel directory to poll.
        cursor: The cursor after the backlog dump (last filename, or ``None``).
        fmt: ``"plain"`` or ``"rich"``.
        use_color: Whether to emit ANSI escapes (rich mode only).
        out: Destination for message data (stdout).
        err: Destination for diagnostics (stderr).

    Returns:
        ``0`` on clean Ctrl-C teardown; ``1`` if the channel dir vanished.
    """
    try:
        while True:
            time.sleep(_FOLLOW_POLL_INTERVAL_SECONDS)
            try:
                cursor = _tail_once(
                    channel_dir, cursor, fmt=fmt, use_color=use_color, out=out, err=err
                )
            except FileNotFoundError:
                print(
                    f"letterbox tail: channel directory vanished, stopping follow "
                    f"({channel_dir})",
                    file=err,
                )
                return 1
    except KeyboardInterrupt:
        return 0


def _handle_tail(
    args: argparse.Namespace, extra_args: list[str], unknown: list[str]
) -> int:
    """Stream a channel's messages to stdout (Vision §11.1).

    Validates the channel name, then checks the directory exists WITHOUT
    creating it (G3 — read-only inspection; never ``Channel.get_or_create``).
    Dumps the backlog as JSONL (or rich lines), then optionally follows
    (``--follow``) until Ctrl-C. Invalid name / missing channel → clear stderr
    vector + nonzero exit, before any backlog dump or poll.

    Args:
        args: Parsed namespace (``channel``, ``follow``, ``format``, ``color``,
            ``config``).
        extra_args: Verbatim passthrough (unused by ``tail``).
        unknown: Leftover argv — always empty (``main`` rejects stray flags).

    Returns:
        ``0`` on success / clean follow teardown; ``1`` on a bad/missing channel.
    """
    name = args.channel
    if not protocol.is_valid_channel_name(name):
        print(
            f"letterbox tail: invalid channel name {name!r} "
            "(expected lowercase alphanumeric, '-'/'_' after the first character)",
            file=sys.stderr,
        )
        return 1
    state_dir = _resolve_state_dir(args)
    channel_dir = state_dir / "channels" / name
    if not channel_dir.is_dir():
        print(
            f"letterbox tail: no such channel {name!r} (looked in {channel_dir})",
            file=sys.stderr,
        )
        return 1
    use_color = _should_use_color(args.color, sys.stdout)
    cursor = _tail_once(
        channel_dir,
        None,
        fmt=args.format,
        use_color=use_color,
        out=sys.stdout,
        err=sys.stderr,
    )
    if args.follow:
        return _follow_loop(
            channel_dir,
            cursor,
            fmt=args.format,
            use_color=use_color,
            out=sys.stdout,
            err=sys.stderr,
        )
    return 0


def _handle_list_channels(
    args: argparse.Namespace, extra_args: list[str], unknown: list[str]
) -> int:
    """List channels with their last-activity timestamps (Vision §11.1).

    Plain (default) → one ``name\\tlast_activity`` line per channel (tab-safe:
    channel names can't contain tabs). Empty channels render a textual
    ``(no messages)`` sentinel (never blank-only — no color-only signaling).
    Rich → an aligned table with a header row. An empty install prints nothing
    to stdout and a brief informational note to stderr (so a pipe stays clean).

    Args:
        args: Parsed namespace (``format``, ``color``, ``config``).
        extra_args: Verbatim passthrough (unused).
        unknown: Leftover argv — always empty (``main`` rejects stray flags).

    Returns:
        ``0`` always (an empty install is informational, not an error).
    """
    state_dir = _resolve_state_dir(args)
    summaries = channel.list_channels(state_dir=state_dir)
    if not summaries:
        print("letterbox list-channels: no channels found", file=sys.stderr)
        return 0
    use_color = _should_use_color(args.color, sys.stdout)
    if args.format == "rich":
        name_width = max(len("CHANNEL"), *(len(s.name) for s in summaries))
        header = (
            f"{_colorize('CHANNEL'.ljust(name_width), _ANSI_BOLD, enabled=use_color)}  "
            f"{_colorize('LAST ACTIVITY', _ANSI_BOLD, enabled=use_color)}"
        )
        print(header, file=sys.stdout)
        for s in summaries:
            activity = s.last_activity or "(no messages)"
            print(f"{s.name.ljust(name_width)}  {activity}", file=sys.stdout)
    else:
        for s in summaries:
            print(f"{s.name}\t{s.last_activity or '(no messages)'}", file=sys.stdout)
    return 0


# ──────────────────────────────────────────────────────────────────────
# Write-command handler: init (9c)
# ──────────────────────────────────────────────────────────────────────


def _build_config_content(channel_name: str | None) -> str:
    """Build the TOML content ``init`` scaffolds, from the 1c bundled sample.

    Reads ``letterbox/data/sample_letterbox.toml`` via :mod:`importlib.resources`
    (so it resolves in both editable-source and installed-wheel modes, K5) with
    explicit UTF-8 so the sample's 📬 ``notification_template`` emoji round-trips.
    Without ``channel_name`` the sample is returned **verbatim** (it ships an
    illustrative ``debate-01`` placeholder channel). With ``channel_name`` (K3),
    the trailing ``[[channels]]`` block is regenerated for that name — the
    sample text up to its sole ``[[channels]]`` token, plus a freshly-templated
    block carrying the per-launch-identity NOTE comment (the rule newcomers most
    often get wrong). TOML array-of-tables conventionally come last, and the
    sample places it last, so splitting on the token is robust, not fragile.

    Args:
        channel_name: The validated channel name to scaffold, or ``None`` for the
            verbatim sample.

    Returns:
        The UTF-8 TOML text to write. Always parses through ``config.load_config``.
    """
    sample = (
        importlib.resources.files("letterbox.data")
        .joinpath("sample_letterbox.toml")
        .read_text(encoding="utf-8")
    )
    # Sample-drift guard (Gotcha): K3's split assumes exactly one trailing
    # array-of-tables. A future 1c sample change fails loudly here, not silently
    # miswriting.
    assert sample.count("[[channels]]") == 1, (
        "sample_letterbox.toml must contain exactly one [[channels]] block "
        "(init's --channel regeneration splits on it)"
    )
    if channel_name is None:
        return sample
    # Validated names are ``[a-z0-9_-]`` with an alphanumeric first char (no
    # quotes/slashes), so a ``name = "{NAME}"`` string template carries zero
    # TOML-injection risk (K3). Take everything before the array-of-tables token
    # and append a regenerated block for ``channel_name``.
    prefix = sample.split("[[channels]]", 1)[0]
    return (
        f"{prefix}[[channels]]\n"
        f'name = "{channel_name}"\n'
        'description = "Channel scaffolded by letterbox init"\n'
        "# NOTE: identity is per-launch (--as or LETTERBOX_SENDER), never per-channel-config.\n"
    )


def _atomic_write_new(path: Path, content: str) -> None:
    """Write ``content`` to ``path`` via write-temp-then-rename (Vision §13.4 / L6).

    Writes the full UTF-8 text to a sibling ``<name>.<pid>.tmp``, ``flush`` +
    ``os.fsync`` the data, then ``os.replace`` it onto ``path`` — so the target
    never appears partially written (a crash mid-write leaves only the temp file,
    never a half-config that ``init`` would then refuse to re-scaffold over). The
    temp file is a sibling of the target (same filesystem → ``os.replace`` is
    atomic) and is cleaned up on any error path.

    This does NOT enforce refuse-overwrite — ``os.replace`` clobbers by design;
    the caller's existence pre-check (K2) is the refuse gate. Parent-dir fsync
    (2c's durability extension) is intentionally skipped for this one-shot human
    command — the file fsync is the core §13.4 requirement.

    Args:
        path: The target file to publish atomically (its parent must exist).
        content: The UTF-8 text to write.

    Returns:
        None.
    """
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as fp:
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        # Clean up the orphan temp on any failure (including KeyboardInterrupt) so
        # a crashed init leaves no litter beside the target (§14 latitude).
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _handle_init(
    args: argparse.Namespace, extra_args: list[str], unknown: list[str]
) -> int:
    """Scaffold a starting ``letterbox.toml`` (Vision §11.1, ADR-010).

    Project-local by default (``./letterbox.toml``); ``--global`` writes
    ``~/.letterbox/config.toml`` (the HOME-derived user-global path
    ``load_config`` consults, K1) and creates ``~/.letterbox/`` at mode ``0700``
    when it was missing (never re-tightening a dir the user already configured).
    The two-step write covenant (K2): an existence pre-check refuses to overwrite
    an existing config (Framework P3 vector citing the path, exit 1), then
    :func:`_atomic_write_new` publishes the content crash-safely. ``--channel``
    pre-populates the scaffolded ``[[channels]]`` entry (validated first, exit 1
    on a bad name — nothing written).

    Args:
        args: Parsed namespace (``channel``, ``is_global``).
        extra_args: Verbatim passthrough (unused by ``init``).
        unknown: Leftover argv — always empty (``main`` rejects stray flags).

    Returns:
        ``0`` on a successful scaffold; ``1`` on a bad ``--channel`` name or a
        refused overwrite.
    """
    channel_name = args.channel
    if channel_name is not None and not protocol.is_valid_channel_name(channel_name):
        print(
            f"letterbox init: invalid channel name {channel_name!r} "
            "(expected lowercase alphanumeric, '-'/'_' after the first character)",
            file=sys.stderr,
        )
        return 1

    if args.is_global:
        target = Path.home() / ".letterbox" / "config.toml"
    else:
        target = Path.cwd() / "letterbox.toml"

    # Refuse-overwrite gate (K2) — the gentle half of the Ironclad Invariant:
    # init only ever creates, never clobbers (Framework P3 vector + a remedy hint).
    if target.exists():
        print(
            f"letterbox init: {target} already exists; refusing to overwrite "
            "(remove it to re-scaffold)",
            file=sys.stderr,
        )
        return 1

    # 0700 only when we create the dir (Gotcha): mkdir's mode is umask-masked, so
    # follow with an explicit chmod — but only when the dir was absent, so we don't
    # re-tighten a dir the user already configured. Project-local's parent is cwd
    # (always exists) → no mkdir there.
    if args.is_global:
        parent = target.parent
        if not parent.exists():
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chmod(parent, 0o700)

    _atomic_write_new(target, _build_config_content(channel_name))
    print(f"letterbox init: wrote {target}", file=sys.stdout)
    return 0


# ──────────────────────────────────────────────────────────────────────
# Retention handler: prune (9d) — the ONLY destructive command (Kernel L8)
# ──────────────────────────────────────────────────────────────────────


# ``\d+[dhms]`` is the §7.1 / Vision-example duration floor (``7d``/``2h``/
# ``30m``/``45s``). Anchored so junk like ``7x``/``abc``/``7d3h``/``""`` is a
# clean ArgumentTypeError → argparse's own exit-2 vector (K-Gotcha §7).
_DURATION_RE = re.compile(r"^(\d+)([dhms])$")
_DURATION_UNITS = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}


def _parse_duration(text: str) -> timedelta:
    """Parse a ``--older-than`` duration (``\\d+[dhms]``) into a :class:`timedelta`.

    Wired as the argparse ``type=`` callable so a malformed value raises
    :class:`argparse.ArgumentTypeError` and surfaces as argparse's standard
    exit-2 vector — the handler body then assumes a validated ``timedelta``.

    Args:
        text: The raw flag value (e.g. ``"7d"``, ``"30m"``).

    Returns:
        The parsed duration.

    Raises:
        argparse.ArgumentTypeError: If ``text`` is not ``<digits><d|h|m|s>``.
    """
    match = _DURATION_RE.match(text)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"invalid duration {text!r} (expected <number><unit> where unit is "
            "one of d/h/m/s, e.g. '7d', '2h', '30m', '45s')"
        )
    value = int(match.group(1))
    return timedelta(**{_DURATION_UNITS[match.group(2)]: value})


def _nonneg_int(text: str) -> int:
    """Parse a ``--keep-last`` value into a non-negative int.

    Wired as the argparse ``type=`` callable (same exit-2 discipline as
    :func:`_parse_duration`): a non-integer or negative value is an
    :class:`argparse.ArgumentTypeError`, so the handler body assumes ``N >= 0``.

    Args:
        text: The raw flag value.

    Returns:
        The parsed non-negative integer.

    Raises:
        argparse.ArgumentTypeError: If ``text`` is not a non-negative integer.
    """
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid value {text!r} for --keep-last (expected a non-negative integer)"
        ) from None
    if value < 0:
        raise argparse.ArgumentTypeError(
            f"--keep-last must be >= 0, got {value}"
        )
    return value


def _message_creation_time(path: Path) -> datetime:
    """Derive a message's creation time from its filename timestamp (K4).

    Age is the microsecond-precision UTC timestamp embedded in the filename, NOT
    ``st_mtime`` — mtime is fragile under ``cp -r``, backup-restore, syncthing,
    and NFS skew, so the whole codebase treats the filename as authoritative
    (§3.2 / ADR-017; mirrors 3d's ``_filename_to_iso_timestamp``). Only call this
    on paths from :func:`protocol.list_messages`, which are already
    ``is_valid_message_filename``-gated, so the ``YYYYMMDDTHHMMSSffffff`` segment
    is guaranteed well-formed. Uses ``strptime`` (4-digit-year safe — avoids the
    glibc ``strftime("%Y")`` zero-pad bug 2b/3d hit).

    Args:
        path: A valid ``msg-<ts>-<uuid>.json`` path.

    Returns:
        The message's creation time as a timezone-aware UTC datetime.
    """
    # ``msg-YYYYMMDDTHHMMSSffffff-<uuid>.json`` — the ts segment carries no ``-``,
    # the uuid hex follows the first ``-``, so split once after the ``msg-`` prefix.
    segment = path.name[len("msg-"):].split("-", 1)[0]
    return datetime.strptime(segment, "%Y%m%dT%H%M%S%f").replace(tzinfo=timezone.utc)


def _read_high_water_mark(path: Path) -> str:
    """Read one endpoint's ``high_water_mark`` from a ``.read/*.json`` file (K5).

    Read-only and fail-safe: any read/parse/shape failure returns ``""`` (the
    fresh-endpoint sentinel, lexically less than any real ``msg-...`` stem), which
    drives the ``--acknowledged-by-all`` ceiling down to "match nothing". The
    ``.read`` file is NEVER rewritten, renamed, or deleted — this is exactly why
    prune reads the JSON directly instead of calling ``channel.read_state`` (whose
    corruption-recovery rename side-effect would mutate read-state, K1/L8).

    Args:
        path: The ``.read/<sender_label>.json`` file to read.

    Returns:
        The endpoint's ``high_water_mark`` stem, or ``""`` on any failure.
    """
    try:
        data = json.loads(path.read_bytes())
        hwm = data["high_water_mark"]
    except (OSError, ValueError, KeyError, TypeError):
        # OSError: unreadable file. ValueError: malformed JSON (JSONDecodeError).
        # KeyError: missing field. TypeError: top-level JSON isn't an object.
        return ""
    return hwm if isinstance(hwm, str) else ""


def _acknowledged_matches(
    candidates: list[Path], channel_dir: Path
) -> tuple[list[Path], str | None]:
    """Select messages every known endpoint has acknowledged (K5).

    The safe ceiling is the **minimum** ``high_water_mark`` across all
    ``.read/*.json`` endpoints; a message matches when its id-stem is ``<=`` that
    ceiling (inclusive — the high-water message itself has been read by everyone).
    Two fail-safes keep uncertainty reducing the prune set, never expanding it:
    (a) zero ``.read/*.json`` files → nothing acknowledged → match nothing with an
    informational note; (b) any endpoint with an empty/unreadable mark drives the
    min to ``""`` → match nothing.

    Args:
        candidates: The ascending message paths from :func:`protocol.list_messages`.
        channel_dir: The channel directory (its ``.read/`` holds the state files).

    Returns:
        A ``(matches, note)`` pair. ``note`` is a rule-specific diagnostic for the
        zero-endpoints case, else ``None``.
    """
    read_dir = channel_dir / ".read"
    # ``*.broken.<ts>`` / ``*.json.tmp`` names don't match ``*.json``, so the glob
    # excludes them. Sorted only for deterministic iteration; min() is unaffected.
    state_files = sorted(read_dir.glob("*.json")) if read_dir.is_dir() else []
    if not state_files:
        return [], "no endpoints have acknowledged yet"
    ceiling = min(_read_high_water_mark(sf) for sf in state_files)
    # Stem-vs-stem comparison (both sides carry NO ``.json``); lexical ==
    # chronological per §3.2. ceiling == "" naturally matches nothing.
    return [p for p in candidates if p.name.removesuffix(".json") <= ceiling], None


def _select_matches(
    args: argparse.Namespace, candidates: list[Path], channel_dir: Path
) -> tuple[list[Path], str | None]:
    """Apply the one selection rule the parser guaranteed is present.

    The ``required=True`` mutually-exclusive group means exactly one of
    ``--older-than`` / ``--keep-last`` / ``--acknowledged-by-all`` is set.

    Args:
        args: Parsed namespace carrying the selection rule.
        candidates: Ascending (oldest→newest) message paths.
        channel_dir: The channel directory (for the acknowledged-by-all rule).

    Returns:
        A ``(matches, note)`` pair; ``note`` is an optional rule-specific
        diagnostic for an empty match set (else ``None``).
    """
    if args.older_than is not None:
        cutoff = datetime.now(timezone.utc) - args.older_than
        return [p for p in candidates if _message_creation_time(p) < cutoff], None
    if args.keep_last is not None:
        # ``list_messages`` is ascending, so "keep the N newest" = drop the tail.
        # ``N == 0`` matches all (the slice ``[:-0]`` is empty, not "all" — the
        # Python footgun the scout flagged), ``N >= count`` matches none.
        if args.keep_last == 0:
            return list(candidates), None
        return candidates[: -args.keep_last], None
    return _acknowledged_matches(candidates, channel_dir)


def _move_to_cold(msg_path: Path, cold_dir: Path) -> None:
    """Move one message file into ``cold/`` via a single atomic rename (K3/§13.4).

    ``os.replace`` of an existing file IS the atomic primitive (no write-temp
    needed — that differs from ``init``'s ``_atomic_write_new``, which exists to
    publish *new content*). ``cold/`` is a child of the channel dir, so the move
    stays on one filesystem and is reversible (``mv cold/* .``). A move is not a
    deletion, so this is L8-safe by construction.

    Args:
        msg_path: The message file to move (in the channel root).
        cold_dir: The ``<channel>/cold/`` destination directory (already created).

    Returns:
        None.
    """
    os.replace(msg_path, cold_dir / msg_path.name)


def _handle_prune(
    args: argparse.Namespace, extra_args: list[str], unknown: list[str]
) -> int:
    """Reclaim channel space — dry-run by default, double-gated to delete (§3.5).

    The K2 action/safety matrix, in branch order:

    - ``--delete`` without ``--yes-i-am-sure`` → **refuse** (P3 vector naming the
      consent flag, exit 1, nothing happens) — the gate standing between a user
      and irreversible loss.
    - selection only → **dry-run**: print matched ids (stdout) + a "would move N
      to cold/" summary (stderr), touch nothing.
    - ``--yes-i-am-sure`` → **move** matched files to ``<channel>/cold/`` (atomic,
      reversible).
    - ``--delete --yes-i-am-sure`` → **DELETE** matched files — the one and only
      message-deletion path in the codebase (Kernel L8).

    Matched ids go to **stdout** (data — so ``prune … | xargs`` works); every
    summary/diagnostic/refusal goes to **stderr** (10a output discipline). No
    ANSI, no ``--format``/``--color``; the dry-run-vs-execute distinction is
    textual ("would move" vs "moved").

    Args:
        args: Parsed namespace (``channel``, the one selection rule, ``delete``,
            ``yes_i_am_sure``, ``config``).
        extra_args: Verbatim passthrough (unused by ``prune``).
        unknown: Leftover argv — always empty (``main`` rejects stray flags).

    Returns:
        ``0`` on a clean dry-run / move / delete (incl. an empty match set); ``1``
        on a bad/missing channel or a ``--delete`` without consent.
    """
    name = args.channel
    if not protocol.is_valid_channel_name(name):
        print(
            f"letterbox prune: invalid channel name {name!r} "
            "(expected lowercase alphanumeric, '-'/'_' after the first character)",
            file=sys.stderr,
        )
        return 1

    # Safety-matrix gate #1 (K2): --delete demands explicit consent. Refuse BEFORE
    # resolving anything — a destructive request without the flag does nothing.
    if args.delete and not args.yes_i_am_sure:
        print(
            "letterbox prune: --delete requires --yes-i-am-sure "
            "(refusing to delete without explicit consent; nothing was removed)",
            file=sys.stderr,
        )
        return 1

    state_dir = _resolve_state_dir(args)
    channel_dir = state_dir / "channels" / name
    if not channel_dir.is_dir():
        print(
            f"letterbox prune: no such channel {name!r} (looked in {channel_dir})",
            file=sys.stderr,
        )
        return 1

    # ``list_messages`` is the candidate source — never re-glob ``msg-*``. It
    # already excludes ``.tmp``/malformed names and (critically) the ``cold/`` and
    # ``.read/`` subdirectories (they're dirs), so already-cold files are never
    # re-pruned.
    candidates = protocol.list_messages(channel_dir)
    matches, note = _select_matches(args, candidates, channel_dir)
    if not matches:
        print(f"letterbox prune: {note or 'nothing to prune'}", file=sys.stderr)
        return 0

    # Matched ids are the command's data → stdout (pipeable). Emit before acting.
    for path in matches:
        print(path.name, file=sys.stdout)
    count = len(matches)

    if not args.yes_i_am_sure:
        # Dry-run preview: --delete-without-consent already refused above, so the
        # only previewed action here is the cold/ move.
        print(
            f"letterbox prune: would move {count} message(s) to cold/ "
            "(dry-run; pass --yes-i-am-sure to act)",
            file=sys.stderr,
        )
        return 0

    if args.delete:
        # THE deletion path (Kernel L8) — only reachable behind --delete AND
        # --yes-i-am-sure. The lone deletion of a user message in the codebase.
        for path in matches:
            os.unlink(path)
        print(f"letterbox prune: deleted {count} message(s)", file=sys.stderr)
        return 0

    cold_dir = channel_dir / "cold"
    if not cold_dir.exists():
        # 0700 only when WE create it (mkdir's mode is umask-masked → explicit
        # chmod), never re-tightening an existing cold/ — the 3a/4d/9c idiom.
        cold_dir.mkdir(mode=0o700)
        os.chmod(cold_dir, 0o700)
    for path in matches:
        _move_to_cold(path, cold_dir)
    print(f"letterbox prune: moved {count} message(s) to cold/", file=sys.stderr)
    return 0

"""stdio MCP server body for ``letterbox mcp`` subcommand. Spawned BY the agent.

Tier: 4
May import from: stdlib; Tier 1+2 (``protocol``, ``channel``, ``config``, and ``notifications``
    if needed for tool error messaging); ``mcp`` SDK.
Must NOT import from: ``letterbox.launcher`` or ``letterbox.cli`` (Tier 4 sibling isolation —
    bulkhead §13.5).

Filled in: Phase 7a/7b/7c/7d per PHASE_INDEX.
"""
from __future__ import annotations

import argparse
import os
import sys

from mcp.server.fastmcp import FastMCP

from letterbox.channel import (
    Channel,
    channel_info as _channel_info,
    list_channels as _list_channels,
)
from letterbox.config import load_config
from letterbox.locks import list_live_participants
from letterbox.protocol import (
    Message,
    is_valid_message_filename,
    list_messages,
    make_message_filename,
    new_message,
    write_message,
)

__all__ = ["run"]

# The agent-facing explanation surfaced both as the dormant ``channel_info``
# detail and as the error every messaging tool raises when called with no
# active bridge (ADR-056). Phrased for the AGENT: it says what is true and what
# a human must do, so the agent can relay accurately instead of guessing.
_NOT_BRIDGED_DETAIL = (
    "letterbox is loaded but no channel is set, so there is no active bridge to "
    "a peer. A human starts one by launching `letterbox <harness> --channel "
    "<name> --as <label>` (and the peer the same way, on the same channel). "
    "Call channel_info at any time to re-check the bridge state."
)


def _message_to_dict(msg: Message) -> dict:
    """Flatten a :class:`~letterbox.protocol.Message` to its §3.2 wire dict.

    The MCP read tools return a plain ``dict`` and let FastMCP serialise it
    to the wire (K2 — ``mcp_server.py`` writes no JSON itself). The key set
    matches what ``to_json_bytes`` emits, so the agent receives a faithful,
    full message object.

    ``msg.metadata`` is a frozen :class:`~letterbox.protocol.Metadata`
    dataclass, NOT a dict — this helper flattens it explicitly to the nested
    ``{"encryption": ..., "ext": ...}`` shape rather than relying on the SDK's
    dataclass coercion (unverified for this layer; explicit matches the
    on-disk shape). Established here for 7b; reused by 7c/7d for the
    clean-message case inside their envelopes.

    Args:
        msg: The message to serialise.

    Returns:
        A plain ``dict`` with the §3.2 key set; ``recipient``/``in_reply_to``
        may be ``None`` (JSON ``null``).
    """
    return {
        "schema_version": msg.schema_version,
        "id": msg.id,
        "channel": msg.channel,
        "address": msg.address,
        "instance_id": msg.instance_id,
        "sender": msg.sender,
        "recipient": msg.recipient,
        "timestamp": msg.timestamp,
        "body": msg.body,
        "in_reply_to": msg.in_reply_to,
        "metadata": {
            "encryption": msg.metadata.encryption,
            "ext": msg.metadata.ext,
        },
    }


def run(argv: list[str] | None = None) -> None:
    """Run the ``letterbox mcp`` stdio MCP server until the agent exits.

    Parses the W13 join-key invocation 5c emits, opens the channel under
    the resolved state directory, builds the six-tool registry, then enters
    the blocking stdio serve loop. The loop ends when the spawning agent
    dies and this child receives SIGTERM — the default disposition is the
    clean exit, because the server owns no state outside the filesystem
    (K4, Vision §6.3).

    Three startup dispositions (ADR-056):

    * **Bridged** — all three join keys resolved (flags or env): open the
      channel and build the full six-tool server.
    * **Dormant** — join keys missing AND stdin is not a TTY, i.e. an MCP host
      spawned us with no channel (the letterbox server left in a harness's
      user-level settings, started by a plain session). Build the server with
      ``channel=None``: it connects cleanly so the harness shows a calm
      "connected", but every messaging tool fails loud the moment it is called
      and ``channel_info`` reports ``{"bridged": false}``. A deliberate plain
      session stays quiet; a genuinely misconfigured bridge surfaces at first
      use rather than going silently dark (Vision §7.1).
    * **Human misuse** — join keys missing AND stdin IS a TTY, i.e. a person
      ran ``letterbox mcp`` by hand (you never do this — the harness spawns it).
      Fail loud to stderr with exit 2 instead of hanging on a stdio handshake
      that will never arrive.

    Args:
        argv: Argument vector after the ``mcp`` token (``--channel``/``--as``/
            ``--instance-id``). ``None`` falls back to ``sys.argv[1:]`` so the
            module is runnable via ``python -c "...run(sys.argv[1:])"`` (the
            shape 9a's dispatch forwards to).

    Returns:
        None. In normal operation the call blocks in ``server.run`` until
        the process is signalled.
    """
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    missing = _missing_join_keys(args)
    if not missing:
        channel = _open_channel(args)
        _align_read_marker(channel, args.instance_id)
        server = _build_server(channel, args.instance_id)
    elif sys.stdin.isatty():
        sys.stderr.write(
            "letterbox mcp: missing join-key value(s): "
            + ", ".join(missing)
            + " — supply each as a flag or its environment variable. You "
            "normally never run `letterbox mcp` yourself; the harness spawns "
            "it. See the README.\n"
        )
        raise SystemExit(2)
    else:
        server = _build_server(None, None)
    server.run("stdio")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the join-key invocation — flags OR environment (K1, ADR-055).

    The three join-key values (``--channel`` / ``--as`` / ``--instance-id``)
    can arrive two ways:

    * **As flags** — the self-contained ``--mcp-config`` path. 5c's
      ``generate_mcp_config`` bakes them into the agent's MCP config, so a
      flag-wired harness (Claude) spawns ``letterbox mcp --channel … --as …
      --instance-id …``. The spellings MUST match what ``generate_mcp_config``
      emits byte-for-byte (locked by 6d Family B).
    * **As environment variables** — ``LETTERBOX_CHANNEL`` /
      ``LETTERBOX_SENDER`` / ``LETTERBOX_INSTANCE_ID``. The launcher exports
      all three into the harness's spawn env (ADR-055), so a settings-wired
      harness whose CLI has no ``--mcp-config`` flag (Gemini, Antigravity) can
      declare ONE fixed, channel-agnostic ``["mcp"]`` server in its settings
      and still receive the per-launch channel/identity at runtime. This
      mirrors the Forge tower orchestrator passing the channel via
      ``FORGE_DIALOGUE_CHANNEL`` — the user never edits settings per channel.

    Flags win over the environment when both are present (an explicit
    invocation is the stronger signal). Resolution is *lenient*: a value absent
    from BOTH sources comes back as ``None``. :func:`run` decides what that
    means — start DORMANT when an MCP host spawned us with no channel (the
    letterbox server left in a harness's user-level settings, a plain session
    with no bridge — ADR-056), or fail loud when a human ran the command
    directly (a TTY). Either way the never-silently-dark contract holds: a
    misconfigured *bridge* surfaces, but a deliberate plain session stays calm.

    The ``--as`` flag maps to ``dest="sender_label"`` because ``as`` is a
    Python keyword — ``args.as`` would be a ``SyntaxError`` (G1).

    Args:
        argv: The argument vector to parse (everything after the ``mcp`` token).

    Returns:
        Namespace with ``channel``, ``sender_label``, and ``instance_id``; each
        is a non-empty ``str`` when supplied, else ``None``.
    """
    parser = argparse.ArgumentParser(
        prog="letterbox mcp",
        description="stdio MCP server for a single letterbox channel (spawned by the agent).",
    )
    parser.add_argument(
        "--channel",
        default=None,
        help="Channel name to open (else $LETTERBOX_CHANNEL).",
    )
    parser.add_argument(
        "--as",
        dest="sender_label",
        default=None,
        help="This endpoint's identity on the channel (else $LETTERBOX_SENDER).",
    )
    parser.add_argument(
        "--instance-id",
        default=None,
        help="Per-launch instance id, the watcher/own-write join key "
        "(else $LETTERBOX_INSTANCE_ID; ADR-011).",
    )
    args = parser.parse_args(argv)

    # Environment fallback (ADR-055): flags win, env fills the gaps, an empty
    # string counts as "not supplied" (mirrors resolve_sender_label's guard).
    args.channel = args.channel or os.environ.get("LETTERBOX_CHANNEL") or None
    args.sender_label = (
        args.sender_label or os.environ.get("LETTERBOX_SENDER") or None
    )
    args.instance_id = (
        args.instance_id or os.environ.get("LETTERBOX_INSTANCE_ID") or None
    )
    return args


# Each join key paired with the human-readable "flag / $ENV" spelling used in
# the dormant-state detail and the human-misuse error (ADR-055/056).
_JOIN_KEY_SPECS: tuple[tuple[str, str], ...] = (
    ("channel", "--channel / $LETTERBOX_CHANNEL"),
    ("sender_label", "--as / $LETTERBOX_SENDER"),
    ("instance_id", "--instance-id / $LETTERBOX_INSTANCE_ID"),
)


def _missing_join_keys(args: argparse.Namespace) -> list[str]:
    """Return the ``flag / $ENV`` spellings of any unresolved join keys."""
    return [spec for attr, spec in _JOIN_KEY_SPECS if not getattr(args, attr)]


def _open_channel(args: argparse.Namespace) -> Channel:
    """Resolve the state dir and open (auto-create) the channel directory.

    Resolves ``state_dir`` via ``config.load_config().state_dir`` — the
    full-precedence resolver the launcher also uses (K5, W18). The narrower
    ``resolve_state_dir()`` is deliberately NOT used: it ignores project-local
    ``letterbox.toml`` ``state_dir`` and would silently diverge from the
    launcher, breaking the shared-directory coordination L1 depends on.

    The peer label (``recipient``) is unknown at launch and informational only
    in v1, so an empty string is passed — ``Channel.get_or_create`` accepts it
    (G6). The per-launch ``instance_id`` is NOT a ``Channel`` field; it is
    carried separately into ``_build_server``.

    Args:
        args: Parsed namespace from :func:`_parse_args`.

    Returns:
        The opened ``Channel`` whose directory now exists at mode ``0o700``.
    """
    state_dir = load_config().state_dir
    return Channel.get_or_create(
        args.channel,
        args.sender_label,
        "",
        state_dir=state_dir,
    )


def _align_read_marker(channel: Channel, instance_id: str) -> None:
    """Advance the read marker to the channel's latest message at launch (ADR-058).

    Aligns this endpoint's ``high_water_mark`` with the watcher's *start
    watermark* (ADR-024): the watcher records the newest filename present at
    init and only notifies about messages that arrive strictly after it, so a
    restart is a clean "fresh start", not a replay of the whole backlog. But
    the read marker (which ``check_messages`` filters on) only ever moved on an
    explicit ``acknowledge`` — so a relaunched session whose marker lagged the
    watermark would surface the entire cross-session backlog as "unread" the
    moment the agent ran a catch-up read, contradicting the calm restart the
    watcher already delivers. This re-syncs the two positions at launch: every
    message already on disk is treated as read, so the inbox starts as
    "messages since I launched". The peer's history stays reachable on demand
    via ``check_messages(since_id=...)``.

    Monotonic and idempotent. :meth:`Channel.acknowledge` clamps to
    ``max(current_hwm, latest)``, so a relaunch never rewinds a marker the
    previous session advanced further (e.g. via ``since_id`` catch-up), and an
    empty channel — no messages yet — is a no-op (nothing to acknowledge). The
    latest stem is taken from ``list_messages`` (filename enumeration only — no
    JSON parse), regardless of sender: own writes are filtered downstream, so
    the marker simply means "everything up to launch is not new".

    Args:
        channel: The opened channel whose read marker is aligned.
        instance_id: The per-launch instance id written into the marker file.

    Returns:
        None.
    """
    paths = list_messages(channel.path, since=None)
    if not paths:
        return
    channel.acknowledge(paths[-1].stem, self_instance_id=instance_id)


def _build_server(
    channel: Channel | None, instance_id: str | None
) -> FastMCP:
    """Build the ``FastMCP("letterbox")`` server with the six §6.1 tools.

    This is the testable seam: it returns the configured server *without*
    entering the blocking stdio loop, so tests can introspect the registry
    (:meth:`FastMCP.list_tools`) and the generated input schemas. The six
    tools were registered here (7a) and their bodies filled across 7b/7c/7d.
    The closures capture ``channel`` and ``instance_id`` so the tool bodies
    have the trusted server-side identity context (§13.3) without module
    globals.

    When ``channel`` is ``None`` the server is **dormant** (ADR-056): it
    registers and connects identically (same tool names, same schemas — so the
    harness shows a calm "connected"), but the four messaging tools fail loud
    via :func:`_require_bridge` the moment they are called, and ``channel_info``
    reports ``{"bridged": false}``. ``list_channels`` still works dormant (it
    is filesystem enumeration, not a per-channel operation), so the agent can
    still see what channels exist.

    Args:
        channel: The opened channel the tools read from and write to, or
            ``None`` for a dormant server (no active bridge).
        instance_id: The per-launch instance id (own-write join key, ADR-011),
            or ``None`` when dormant.

    Returns:
        A ready-to-run ``FastMCP`` instance with all six tools registered.
    """
    server = FastMCP("letterbox")

    def _require_bridge() -> None:
        """Fail loud when a messaging tool is called with no active bridge.

        The dormant-mode counterpart of "never silently dark" (Vision §7.1):
        a plain session never *triggers* this (it just doesn't call the tool),
        but a misconfigured bridge — or an agent that tries to send before the
        human launched the peer — gets a clear, actionable error at first use.
        """
        if channel is None:
            raise RuntimeError(_NOT_BRIDGED_DETAIL)

    @server.tool()
    def send_message(
        body: str, to: str | None = None, in_reply_to: str | None = None
    ) -> dict:
        """Write a message to the current channel and return its message id.

        Leave ``to`` unset to broadcast — every participant on the channel is
        notified (📬) and sees it. Set ``to`` to a participant's label (see
        ``channel_info`` → ``participants``) to direct the message: only that
        participant gets the 📬 notification, while everyone else can still read
        it via ``check_messages`` (observable, but not notified). Directed
        addressing is by convention, not privacy — the message lives in the
        shared channel like any other (§13.3).

        The agent does NOT pass sender — that is populated server-side from the
        launch identity (§3.2). Bodies over 5 MB are rejected with
        MessageTooLarge before any disk I/O. Errors if there is no active
        bridge — call channel_info first if unsure.
        """
        _require_bridge()
        # K4 — sender identity is server-side (from the launcher-resolved
        # channel handle); the signature has no sender parameter, so a spoofed
        # sender is structurally impossible. ``to`` sets the recipient label —
        # an empty string normalizes to None (broadcast), so ``to=""`` never
        # shadows a real label.
        msg_id = make_message_filename().removesuffix(".json")  # stem, no .json (G1)
        msg = new_message(
            id=msg_id,
            channel=channel.name,
            instance_id=instance_id,
            sender=channel.sender_label,
            body=body,
            in_reply_to=in_reply_to,
            recipient=(to or None),
        )
        # K3 — write_message encodes first, so MessageTooLarge propagates
        # before any .tmp is created; no pre-check, no catch-and-wrap.
        write_message(channel.path, msg)
        return {"id": msg.id}

    @server.tool()
    def check_latest_message() -> dict | None:
        """Return the single most recent unread peer message, or null if none.

        The common-case, minimal-context tool: use it when asked "what did
        they say?" — no pagination, no risk of context bombing. Does not
        advance this endpoint's read marker; call acknowledge to do that.
        """
        _require_bridge()
        # K1 — reverse-scan tail accessor (NOT list_unread[-1], which is
        # wrong once unread > 100). latest_unread reuses _is_own_write, so
        # it tracks the K7 reconciliation. Does not advance the marker.
        latest = channel.latest_unread(self_instance_id=instance_id)
        return _message_to_dict(latest) if latest is not None else None

    @server.tool()
    def check_messages(limit: int = 20, since_id: str | None = None) -> dict:
        """Return up to `limit` unread peer messages, oldest unread first.

        Pagination tool for explicit catch-up. "Unread" means newer than this
        endpoint's read marker. A default catch-up read ADVANCES that marker to
        the newest item it returns — reading is acknowledging, so the next call
        continues past this page and nothing re-appears (ADR-058). Pass
        `since_id` to query history from a given id WITHOUT moving the marker.
        Default limit 20 guards against context-window bombing; the hard
        maximum is 100 (higher values are clamped with a warning). The response
        includes a `has_more` flag — keep calling until it is false to drain
        the inbox.
        """
        _require_bridge()
        # K5 — limit type-guard + [1,100] clamp live in list_unread; no
        # redundant guard here. K3 — since_id is an ephemeral read cursor,
        # trusted verbatim (only message_id, which advances the persisted
        # marker, is wire-validated — see acknowledge).
        result = channel.list_unread(
            self_instance_id=instance_id, limit=limit, since_id=since_id
        )
        # K1 — flatten the two ascending lists (clean messages + parse
        # errors) into ONE id-ordered list with per-item parse_error
        # envelopes, so the agent sees true oldest-first order across both.
        # Filenames sort chronologically (ADR-027), so the id stem is the
        # ordering key for both shapes.
        items: list[dict] = [_message_to_dict(msg) for msg in result.messages]
        items.extend(
            {"id": pe.path.stem, "parse_error": pe.reason, "body": None}
            for pe in result.parse_errors
        )
        items.sort(key=lambda item: item["id"])
        # ADR-058 — auto-advance the read marker on a default catch-up read:
        # reading IS acknowledging. Advance to the newest item returned
        # (items are id-sorted ascending, so items[-1] is the max), and
        # because list_unread scans ascending and returns every non-own-write
        # message up to the cap, everything at-or-before that stem has been
        # seen — so the next call resumes cleanly past this page (driving
        # pagination off the marker, no since_id needed). A `since_id` query
        # is an explicit, non-advancing history read (the same falsy guard
        # list_unread uses for its bound: None AND "" fall through to the
        # marker), and check_latest_message stays peek-only. Channel.acknowledge
        # clamps monotonically, so this never rewinds the marker.
        if not since_id and items:
            channel.acknowledge(items[-1]["id"], self_instance_id=instance_id)
        response: dict = {"messages": items, "has_more": result.has_more}
        # Omit "warning" entirely when no clamp fired — don't emit null.
        if result.limit_warning is not None:
            response["warning"] = result.limit_warning
        return response

    @server.tool()
    def acknowledge(message_id: str) -> dict:
        """Advance this endpoint's read marker to clear a message from its inbox.

        Per-agent state: this does not delete the message file or affect the
        peer's view. Advances the high-water mark to max(current, message_id)
        by filename order, so it is idempotent.
        """
        _require_bridge()
        # K2 — validate the wire-format of message_id BEFORE advancing the
        # persisted marker. Channel.acknowledge does max(hwm, message_id)
        # lexically; a hallucinated id that sorts above real ids (e.g.
        # "zzz", since "msg-…" < "zzz") would jump the high-water-mark past
        # every real message and silently blank the agent's whole inbox —
        # the exact silent-failure class the framework forbids (L1, P3).
        # The channel layer defers this check to the MCP boundary (ADR-037).
        # message_id arrives FastMCP-coerced as str, so only its FORMAT
        # needs checking (no type-guard). Validate what writes the marker;
        # trust the read-only since_id (K3).
        if not is_valid_message_filename(f"{message_id}.json"):
            raise ValueError(
                f"invalid message_id {message_id!r}: expected a message-id "
                "stem like msg-YYYYMMDDThhmmssffffff-<32 hex> "
                "(no .json suffix), as returned by check_messages"
            )
        channel.acknowledge(message_id, self_instance_id=instance_id)
        return {"ok": True}

    @server.tool()
    def list_channels() -> list[dict]:
        """Enumerate all channels (registered and auto-created) with last activity.

        Each entry carries the channel name and its most recent activity
        timestamp.
        """
        # K1/ADR-038 — derive state_dir from the open channel handle, NOT by
        # re-resolving config. get_or_create locks the invariant
        # channel.path == state_dir/channels/<name>, so channel.path.parent.parent
        # IS the state_dir this channel lives in — enumerating the same world the
        # open channel belongs to, with no second config load or TOCTOU drift.
        # Dormant (ADR-056): no channel handle, so fall back to the configured
        # state_dir — enumeration is channel-independent, so a dormant agent can
        # still discover what channels exist (no _require_bridge gate here).
        state_dir = (
            channel.path.parent.parent if channel is not None
            else load_config().state_dir
        )
        # K2 — call the aliased free function (a bare `list_channels` would
        # resolve to this closure, not the channel-layer function). Drop
        # ChannelSummary.path — never leak an absolute filesystem path to the
        # agent; the §6.1 wire shape is {name, last_activity} only.
        return [
            {"name": summary.name, "last_activity": summary.last_activity}
            for summary in _list_channels(state_dir=state_dir)
        ]

    @server.tool()
    def channel_info() -> dict:
        """Report the agent's letterbox situation — the bridge-state oracle.

        Call this BEFORE sending if unsure of the state. It answers, server-side
        (the agent never asserts its own identity or backlog — §13.3):

        * ``bridged`` — is a channel actually active at all? When ``false`` (a
          plain session, no bridge), that is the only field, plus a ``detail``
          telling the agent what a human must do; the messaging tools will
          error until then.
        * When ``true``: ``channel`` and ``sender`` (your identity); ``unread``
          (your unread peer count); ``peer`` (who last spoke, observed from the
          most recent message — ``null`` if no one else has spoken);
          ``peer_has_spoken``; ``last_peer_activity`` (when, ISO-8601 — your
          liveness signal: 90 s ago vs 3 days ago vs never); and
          ``participants`` (every label currently running on this channel,
          *including yourself* and anyone who has launched but not yet spoken —
          this is "who is in the room", the set you can direct a message to via
          ``send_message(to=...)``).
        """
        if channel is None:
            # Dormant (ADR-056): no active bridge. Honest, actionable state —
            # not an error — so the agent can relay accurately to the human.
            return {"bridged": False, "detail": _NOT_BRIDGED_DETAIL}
        # K2 — call the aliased free function (the bare name would resolve to
        # this closure). All values server-computed (§13.3): identity from the
        # launcher-resolved channel handle; unread/peer from the filesystem.
        # ``peer`` is the observed peer (sender of the latest peer message) —
        # more useful than v1's always-"" recipient_label. ``participants`` is
        # derived from the live pid-locks (who is RUNNING), not from message
        # history (who has SPOKEN) — so it surfaces a silent lurker too, the
        # signal you need to address a directed message before anyone replies.
        info = _channel_info(channel, self_instance_id=instance_id)
        return {
            "bridged": True,
            "channel": info.channel,
            "sender": info.sender_label,
            "unread": info.unread_count,
            "peer": info.peer_label,
            "peer_has_spoken": info.peer_label is not None,
            "last_peer_activity": info.last_peer_activity,
            # state_dir derived the same way list_channels does (channel.path is
            # state_dir/channels/<name>); channel is non-None past the guard.
            "participants": list_live_participants(
                channel.path.parent.parent, channel.name
            ),
        }

    return server

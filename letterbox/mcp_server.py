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
import sys

from mcp.server.fastmcp import FastMCP

from letterbox.channel import (
    Channel,
    channel_info as _channel_info,
    list_channels as _list_channels,
)
from letterbox.config import load_config
from letterbox.protocol import (
    Message,
    is_valid_message_filename,
    make_message_filename,
    new_message,
    write_message,
)

__all__ = ["run"]


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
    channel = _open_channel(args)
    server = _build_server(channel, args.instance_id)
    server.run("stdio")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the W13 join-key flags — the load-bearing spelling contract (K1).

    The three flag spellings (``--channel``, ``--as``, ``--instance-id``) MUST
    match byte-for-byte what 5c's ``generate_mcp_config`` emits (locked by 6d
    Family B). A divergence makes the agent's MCP child parse-error the instant
    it spawns and the channel goes silent with no error anyone thinks to look
    for (Vision §7.1). All three are required; argparse rejects a missing one
    with a vector message on stderr and exit code 2.

    The ``--as`` flag maps to ``dest="sender_label"`` because ``as`` is a
    Python keyword — ``args.as`` would be a ``SyntaxError`` (G1).

    Args:
        argv: The argument vector to parse (everything after the ``mcp`` token).

    Returns:
        Namespace with ``channel``, ``sender_label``, and ``instance_id``.
    """
    parser = argparse.ArgumentParser(
        prog="letterbox mcp",
        description="stdio MCP server for a single letterbox channel (spawned by the agent).",
    )
    parser.add_argument(
        "--channel",
        required=True,
        help="Channel name to open (the directory under <state_dir>/channels/).",
    )
    parser.add_argument(
        "--as",
        dest="sender_label",
        required=True,
        help="This endpoint's identity on the channel (resolved by the launcher).",
    )
    parser.add_argument(
        "--instance-id",
        required=True,
        help="Per-launch instance id (the watcher/own-write join key, ADR-011).",
    )
    return parser.parse_args(argv)


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


def _build_server(channel: Channel, instance_id: str) -> FastMCP:
    """Build the ``FastMCP("letterbox")`` server with the six §6.1 tools.

    This is the testable seam: it returns the configured server *without*
    entering the blocking stdio loop, so tests can introspect the registry
    (:meth:`FastMCP.list_tools`) and the generated input schemas. The six
    tools were registered here (7a) and their bodies filled across 7b/7c/7d
    (now complete — no stub remains). The closures capture ``channel`` and
    ``instance_id`` so the tool bodies have the trusted server-side identity
    context (§13.3) without module globals.

    Args:
        channel: The opened channel the tools read from and write to.
        instance_id: The per-launch instance id (own-write join key, ADR-011).

    Returns:
        A ready-to-run ``FastMCP`` instance with all six tools registered.
    """
    server = FastMCP("letterbox")

    @server.tool()
    def send_message(body: str, in_reply_to: str | None = None) -> dict:
        """Write a message to the current channel and return its message id.

        The agent does NOT pass sender or recipient — those are populated
        server-side from the launch identity (§3.2). Bodies over 5 MB are
        rejected with MessageTooLarge before any disk I/O.
        """
        # K4 — identity is server-side: sender from the launcher-resolved
        # channel handle, instance_id from the captured launch id. The
        # signature has no identity parameter, so agent-supplied identity
        # is structurally impossible. recipient stays None (v1, §3.2).
        msg_id = make_message_filename().removesuffix(".json")  # stem, no .json (G1)
        msg = new_message(
            id=msg_id,
            channel=channel.name,
            instance_id=instance_id,
            sender=channel.sender_label,
            body=body,
            in_reply_to=in_reply_to,
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
        # K1 — reverse-scan tail accessor (NOT list_unread[-1], which is
        # wrong once unread > 100). latest_unread reuses _is_own_write, so
        # it tracks the K7 reconciliation. Does not advance the marker.
        latest = channel.latest_unread(self_instance_id=instance_id)
        return _message_to_dict(latest) if latest is not None else None

    @server.tool()
    def check_messages(limit: int = 20, since_id: str | None = None) -> dict:
        """Return up to `limit` unread peer messages, oldest unread first.

        Pagination tool for explicit catch-up. "Unread" means newer than this
        endpoint's read marker. `since_id` queries from a given id without
        moving the marker. Default limit 20 guards against context-window
        bombing; the hard maximum is 100 (higher values are clamped with a
        warning). The response includes a `has_more` flag for paging.
        """
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
        state_dir = channel.path.parent.parent
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
        """Return the current channel's metadata for this endpoint.

        Reports the channel name, this endpoint's sender label, the peer's
        recipient label, and this endpoint's unread count — all computed
        server-side so the agent is never trusted to assert its own identity
        or backlog.
        """
        # K2 — call the aliased free function (the bare name would resolve to
        # this closure). All four fields are server-computed (§13.3): identity
        # from the launcher-resolved channel handle, unread_count from the
        # filesystem. recipient_label is "" in v1 (peer label unknown at launch
        # — _open_channel opens with recipient=""); that is the honest answer,
        # not a bug. Flatten field-by-field (not asdict()) for parity with
        # _message_to_dict and to stay robust if ChannelInfo grows.
        info = _channel_info(channel, self_instance_id=instance_id)
        return {
            "channel": info.channel,
            "sender_label": info.sender_label,
            "recipient_label": info.recipient_label,
            "unread_count": info.unread_count,
        }

    return server

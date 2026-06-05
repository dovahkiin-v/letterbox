"""Channel type, per-agent read-state file management, channel listing, unread query.

Tier: 1
May import from: stdlib, ``letterbox.protocol``.
Must NOT import from: ``letterbox.config``, ``letterbox.notifications``, or any Tier 2+ module
    (sibling-Tier-1 isolation — channel.py is the only Tier 1 module permitted to depend on another).

Filled in: Phase 3a/3b/3c/3d per PHASE_INDEX.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from letterbox.protocol import (
    Message,
    ParseError,
    is_valid_channel_name,
    list_messages,
    read_message,
)


__all__ = [
    "Channel",
    "ChannelInfo",
    "ChannelSummary",
    "ReadState",
    "StatePermissionsError",
    "UnreadResult",
    "channel_info",
    "check_state_dir_permissions",
    "list_channels",
    "read_state",
    "write_read_state",
]


# Module logger — mirrors ``letterbox.protocol``'s convention. Phase 3a body
# emits no log lines itself (no I/O failures to soft-warn about), but defining
# the logger now means Phase 3b's ``.read/{label}.json.broken.<ts>`` WARN has
# a place to go without amending the imports later.
_LOGGER = logging.getLogger("letterbox.channel")


# ──────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────


class StatePermissionsError(Exception):
    """Raised by :func:`check_state_dir_permissions` when the state directory
    has any "other" permission bit set (mask ``0o007``).

    The raising site constructs a vector message naming the path and the
    octal mode (Framework P3). Launchers (Phase 8a) and the MCP server
    (Phase 7a) catch this and translate to a clean stderr line + non-zero
    exit per Vision §6.4.
    """


# ──────────────────────────────────────────────────────────────
# Channel — first-class type pairing a name with on-disk path + identity
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Channel:
    """A named relationship between two endpoints, materialised as a directory.

    Fields:
        name: Channel name as registered. Pre-validated by
            :meth:`Channel.get_or_create` (callers using direct
            ``Channel(...)`` construction assert validity themselves —
            the classmethod is the documented entry point).
        path: Absolute path to the channel directory
            (``state_dir/channels/<name>/``).
        sender_label: This endpoint's identity on this channel. Sourced
            at launch time from the priority chain (``--as`` flag >
            ``LETTERBOX_SENDER`` env > harness-name default; Vision §3.2).
            Channel.py does NOT resolve the priority chain — that lives in
            the launcher (8a) and CLI (9a). The dataclass holds whatever
            the caller passed.
        recipient_label: The peer's identity on this channel (informational
            in v1; Vision §3.2 / §4.1). May be empty string when the peer
            label is unknown at launch — downstream consumers
            (:meth:`Channel.list_unread` in 3c, ``channel_info`` in 3d)
            decide what to do with empty.

    Direct ``Channel(...)`` construction skips name validation. The
    classmethod :meth:`get_or_create` is the documented constructor for
    new channels; direct construction is the escape hatch for callers
    that already trust their inputs (e.g., a future phase reading a
    registered config).
    """

    name: str
    path: Path
    sender_label: str
    recipient_label: str

    @classmethod
    def get_or_create(
        cls,
        name: str,
        sender: str,
        recipient: str,
        *,
        state_dir: Path,
    ) -> "Channel":
        """Mint a ``Channel`` handle, auto-creating its directory if missing.

        Idempotent: calling twice with identical arguments yields equal
        ``Channel`` instances and is a no-op against the filesystem
        (beyond the harmless re-chmod). Vision §6.4 mandates mode
        ``0o700`` on the channel directory; the function uses
        ``mkdir(mode=0o700)`` plus an explicit ``os.chmod`` to defeat
        the process umask (Phase 1b convention).

        The ``state_dir`` keyword arg is required because channel.py
        cannot import :mod:`letterbox.config` (tier discipline — Phase
        3a K2). Callers resolve the state dir via
        ``config.resolve_state_dir()`` first, then pass the result.

        Args:
            name: Channel name. Must match ``^[a-z0-9][a-z0-9_-]*$``
                (delegated to :func:`letterbox.protocol.is_valid_channel_name`;
                consumer-raises pattern from Phase 2b K5).
            sender: This endpoint's identity on this channel. Empty string
                is accepted (caller's contract, not channel.py's to enforce
                — the launcher owns the resolution chain).
            recipient: The peer endpoint's identity. May be empty string
                when unknown at launch time.
            state_dir: Absolute path to the letterbox state directory
                (``~/.letterbox/`` by default). Channel directory will
                land at ``state_dir / "channels" / name``.

        Returns:
            A frozen ``Channel`` whose ``path`` is the on-disk directory.

        Raises:
            TypeError: If ``name``, ``sender``, or ``recipient`` is not a
                string. The defensive type guard catches non-string input
                with a vector domain error before
                :func:`is_valid_channel_name` raises a less-informative
                stdlib ``TypeError`` from ``re.Pattern.fullmatch``
                internals (Phase 2b IMPLEMENTATION_NOTES).
            ValueError: If ``name`` does not match the channel-name regex.
        """
        # Type guards before the predicate (G3). The predicate raises a
        # stdlib TypeError from regex internals on non-str input; the
        # consumer site translates to a domain error with a vector
        # message naming the offending arg.
        if not isinstance(name, str):
            raise TypeError(
                f"channel name must be str, got {type(name).__name__}"
            )
        if not isinstance(sender, str):
            raise TypeError(
                f"channel sender must be str, got {type(sender).__name__}"
            )
        if not isinstance(recipient, str):
            raise TypeError(
                f"channel recipient must be str, got {type(recipient).__name__}"
            )

        # Predicate-owns-truth, consumer-owns-error (Phase 2b K5). The
        # error vector names the rejected value AND the rule so the user
        # sees what they sent and what's allowed.
        if not is_valid_channel_name(name):
            raise ValueError(
                f"channel name {name!r} rejected; must match ^[a-z0-9][a-z0-9_-]*$"
            )

        channel_path = state_dir / "channels" / name
        # ``parents=True`` builds the ``channels/`` intermediate parent
        # on fresh state dirs; ``exist_ok=True`` makes the call idempotent.
        # Per G2, only the leaf is re-chmod'd — intermediate ``channels/``
        # inherits from the user's umask, which is acceptable because it
        # contains no secrets, only further channel dirs that ARE chmod'd.
        channel_path.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Defeat umask — mkdir's ``mode`` is masked by the process umask.
        # The explicit chmod makes the resulting mode deterministic
        # regardless of caller environment (Phase 1b conftest pattern).
        os.chmod(channel_path, 0o700)

        return cls(
            name=name,
            path=channel_path,
            sender_label=sender,
            recipient_label=recipient,
        )

    # ── 3c: acknowledge + Unread Query (ADR-012 / ADR-021 / ADR-022) ──

    def acknowledge(
        self, message_id: str, *, self_instance_id: str
    ) -> None:
        """Advance this endpoint's read-state high-water-mark monotonically.

        Composition of 3b's primitives: ``read_state`` (recovers from
        corruption automatically) → ``max()`` clamp → ``write_read_state``
        (atomic-rename, auto-creates ``.read/`` on first call). Idempotent
        — acknowledging the same id twice is a no-op on ``high_water_mark``
        (``updated_at`` always advances because the caller did write).
        Acknowledging an older id is also a no-op on ``high_water_mark``
        because the clamp is monotonic non-decreasing (K6).

        Per-agent isolation (ADR-021): only ``.read/{self.sender_label}.json``
        is touched. The peer's ``.read/<peer>.json`` is byte-identical
        before and after. Message files in the live channel directory are
        never deleted or modified — "acknowledge" is a marker advance,
        not a file mutation.

        The ``message_id`` is trusted verbatim — no regex check, no
        ``.removesuffix(".json")`` rewrite (the MCP boundary at Phase 7c
        owns wire-format validation). A caller bug (passing a filename
        instead of a stem) lands the bad value in the marker file rather
        than silently dropping it; consumer-raises-from-caller pattern
        (3a K3).

        Args:
            message_id: The message-id stem (no ``.json``) to acknowledge.
                Stored as-is.
            self_instance_id: The launcher's process-identity for this
                endpoint (``lb-...`` per Phase 8a). Keyword-only because
                ``Channel`` is the durable half of identity (per
                ``letterbox.toml`` / ``--as``) and ``instance_id`` is the
                ephemeral half (launcher-generated per-process). Storing
                it on ``Channel`` would force the launcher to mint a new
                handle every write; passing it per-call keeps the
                ephemeral/durable split visible (K1, ADR-022).

        Returns:
            ``None``.

        Raises:
            TypeError: If ``self.sender_label`` is not a string (escape
                hatch validation propagates from 3b).
            ValueError: If ``self.sender_label`` does not match
                ``^[a-z0-9][a-z0-9_-]*$``.
            OSError: For underlying filesystem failures during write.
        """
        current = read_state(self, self.sender_label)
        # Lexical max IS chronological max because filenames are
        # microsecond-precise per ADR-027 (K6).
        new_hwm = max(current.high_water_mark, message_id)
        new_state = ReadState(
            sender_label=self.sender_label,
            instance_id=self_instance_id,
            high_water_mark=new_hwm,
            updated_at=_now_iso_utc(),
        )
        write_read_state(self, new_state)

    def list_unread(
        self,
        *,
        self_instance_id: str,
        limit: int = 20,
        since_id: str | None = None,
    ) -> "UnreadResult":
        """Return peer messages this endpoint has not yet acknowledged.

        Composes ``list_messages`` (2c enumeration) + ``read_message``
        (2c defensive read) + the ADR-022 combined own-write filter
        (:func:`_is_own_write`) + 3b's per-agent read-state primitives
        into the canonical inbox-advance query.

        Filter pipeline:

        1. Enumerate every ``msg-*.json`` in the channel directory via
           ``list_messages`` (filename-only — no per-file ``stat`` or
           JSON parse on the rejected path).
        2. Skip paths whose stem ``<=`` the cursor (``since_id`` if
           supplied non-empty, else the per-agent ``high_water_mark``;
           ADR-012 — ``since_id`` is an override, never a floor, and the
           ``high_water_mark`` is NOT updated by this call).
        3. ``read_message`` each survivor. ``FileNotFoundError`` is
           silently skipped (G4 — race with prune is normal). ``ParseError``
           lands in ``UnreadResult.parse_errors`` with a WARN log (G3 —
           per-event, no dedupe).
        4. Apply :func:`_is_own_write` to surviving ``Message`` results;
           drop own writes (ADR-022 combined filter — ``sender`` half
           catches cross-restart self-recognition, ``instance_id`` half
           catches same-harness-deadlock).
        5. Cap at the effective limit (clamped to ``[1, 100]`` with a
           vector ``limit_warning`` populated when the clamp fires);
           ``has_more=True`` if more unread items exist past the cap.

        Parse errors count against the limit because they consume a slot
        in the agent's inbox (the MCP layer at Phase 7c flattens them
        into per-message envelopes with ``parse_error: "<reason>"`` and
        ``body: null``).

        **Performance note:** Channels with more than ~10 000 unarchived
        peer messages may see noticeable latency on this method (the
        underlying ``list_messages`` is O(n) over the channel directory).
        Users running long-lived channels should periodically run
        ``letterbox prune`` (Phase 9d) to keep performance bounded
        (Vision §3.5).

        Args:
            self_instance_id: The launcher's process-identity for this
                endpoint (``lb-...``). Keyword-only — symmetric with
                ``Channel.acknowledge`` (3c K1 rationale) and
                ``channel_info`` (3d K2). Used in the combined own-write
                filter; treated as opaque (no shape validation).
            limit: Maximum number of inbox slots to return. Default 20
                (ADR-012). Clamped to ``[1, 100]`` with a vector
                ``limit_warning`` populated when clamp fires. Non-int
                input raises ``TypeError`` (K5 / 3a K3 consumer-raises
                pattern; ``bool`` is special-cased because
                ``isinstance(True, int)`` is ``True``).
            since_id: Optional override cursor (no ``.json``). When
                supplied non-empty, the filter is ``path.stem > since_id``
                — ``high_water_mark`` is ignored AND not updated (K3 /
                ADR-012). ``None`` and empty string both fall through to
                the per-agent marker (G9 — defensive against MCP callers
                that pass ``""`` instead of ``null``).

        Returns:
            An :class:`UnreadResult` with ``messages`` (peer messages in
            lexical/chronological order, capped at the effective limit),
            ``parse_errors`` (files that failed read_message — ordered as
            encountered during the enumeration), ``has_more`` (True iff
            more peer items exist past the cap), and ``limit_warning``
            (vector message naming the rejected limit AND the rule, or
            ``None`` when no clamp fired).

        Raises:
            TypeError: If ``limit`` is not an integer (bool included).
            FileNotFoundError: If the channel directory does not exist
                (caller-error path for direct ``Channel(...)`` construction
                bypassing ``get_or_create``; D2 in scout brief).
        """
        # K5 — consumer-raises type-guard. bool is a subclass of int in
        # Python, so isinstance(True, int) is True; special-case it
        # (matches protocol._require_type's discipline at protocol.py:199).
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError(
                f"limit must be int, got {type(limit).__name__}"
            )

        # K5 — clamp with vector warning. Inline rather than a helper
        # because there is no shared call site (one consumer for now).
        limit_warning: str | None = None
        if limit < 1:
            limit_warning = (
                f"requested limit {limit} below minimum 1; clamped to 1"
            )
            effective_limit = 1
        elif limit > 100:
            limit_warning = (
                f"requested limit {limit} exceeds maximum 100; clamped to 100"
            )
            effective_limit = 100
        else:
            effective_limit = limit

        # K3 / G9 — since_id override semantics. Empty string and None
        # both fall through to the per-agent marker. Truthy non-empty
        # string is the override that never advances the marker.
        if since_id:
            bound = since_id
        else:
            bound = read_state(self, self.sender_label).high_water_mark

        paths = list_messages(self.path, since=None)
        messages: list[Message] = []
        parse_errors: list[ParseError] = []
        has_more = False

        for path in paths:
            # G1 — stem comparison (no ``.json``), matches 3b's stored hwm shape.
            if path.stem <= bound:
                continue
            try:
                result = read_message(path)
            except FileNotFoundError:
                # G4 — race with prune is normal; silently skip, no WARN.
                continue
            if isinstance(result, ParseError):
                # Consumes an inbox slot. Check the cap BEFORE adding so
                # the (limit+1)-th item triggers has_more correctly.
                if len(messages) + len(parse_errors) >= effective_limit:
                    has_more = True
                    break
                _LOGGER.warning(
                    "parse_error during list_unread on %s: %s",
                    path,
                    result.reason,
                )
                parse_errors.append(result)
                continue
            # Surviving Message — apply ADR-022 combined own-write filter.
            if _is_own_write(result, self.sender_label, self_instance_id):
                continue
            if len(messages) + len(parse_errors) >= effective_limit:
                has_more = True
                break
            messages.append(result)

        return UnreadResult(
            messages=messages,
            parse_errors=parse_errors,
            has_more=has_more,
            limit_warning=limit_warning,
        )

    def latest_unread(self, *, self_instance_id: str) -> Message | None:
        """Return the single most-recent unread peer message, or ``None``.

        The common-case peek behind the ``check_latest_message`` MCP tool
        (7b / K1). Distinct from :meth:`list_unread`, which is a *forward
        page* capped at 100 oldest-first — that primitive cannot answer
        "the single newest unread" once the backlog exceeds the cap (and
        because ``check_latest_message`` never advances the marker, a
        peek-only agent accumulates unbounded unread, so the naive
        ``list_unread(...).messages[-1]`` would return an ever-staler
        ~100th-oldest message forever). This method instead does a
        **reverse scan from the newest filename**, stopping at the first
        unread peer message — O(trailing own-writes) reads, usually one.

        Filter pipeline (newest-first):

        1. ``read_state`` recovers this endpoint's ``high_water_mark``
           (the per-agent cursor; empty ``""`` for a fresh endpoint).
        2. Walk ``reversed(list_messages(...))`` — newest filename first.
        3. ``break`` at the first path whose stem ``<=`` the cursor:
           because filenames sort chronologically (ADR-027), every
           remaining (older) path is also acknowledged, so the scan is
           done (G2 — do NOT ``continue`` past it).
        4. ``read_message`` each survivor. ``FileNotFoundError`` (prune
           race) is skipped silently. ``ParseError`` is skipped silently
           too — **no per-file WARN** (K5): the reverse scan is unbounded,
           so logging every malformed file risks WARN spam; malformed
           files are the thorough path's concern (``check_messages``
           surfaces them as ``parse_error``/``body: null`` envelopes).
        5. The first surviving non-own-write ``Message`` is returned —
           own writes are dropped via the ADR-022 combined filter
           (:func:`_is_own_write`), so this method automatically tracks
           whatever the K7 OR/AND reconciliation resolves to.

        Does NOT advance ``high_water_mark`` — peeking is read-only
        (Vision §6.1). Call :meth:`acknowledge` to move the marker.

        Args:
            self_instance_id: The launcher's process-identity for this
                endpoint (``lb-...``). Keyword-only — symmetric with
                :meth:`acknowledge` / :meth:`list_unread` / :func:`channel_info`.
                Used in the combined own-write filter; treated as opaque
                (no shape validation).

        Returns:
            The newest unread peer :class:`Message`, or ``None`` when the
            channel has no unread peer messages.

        Raises:
            FileNotFoundError: If the channel directory does not exist —
                propagates from ``list_messages`` (caller-error path for
                direct ``Channel(...)`` construction bypassing
                ``get_or_create``; G4). A per-file ``FileNotFoundError``
                from ``read_message`` is the prune race and is skipped.
            TypeError: If ``self.sender_label`` is not a string
                (propagates from 3b's :func:`read_state`).
            ValueError: If ``self.sender_label`` does not match
                ``^[a-z0-9][a-z0-9_-]*$``.
        """
        bound = read_state(self, self.sender_label).high_water_mark
        # G2 — reverse scan with early break is correct because filenames
        # sort chronologically (ADR-027): the first stem <= bound means
        # every remaining (older) path is also acknowledged.
        for path in reversed(list_messages(self.path, since=None)):
            if path.stem <= bound:
                break
            try:
                result = read_message(path)
            except FileNotFoundError:
                # G4 — race with prune is normal; silently skip.
                continue
            if isinstance(result, ParseError):
                # K5 — malformed file skipped silently (no WARN in an
                # unbounded scan); check_messages (7c) surfaces it.
                continue
            # ADR-022 combined own-write filter — drop own writes.
            if _is_own_write(result, self.sender_label, self_instance_id):
                continue
            return result
        return None


# ──────────────────────────────────────────────────────────────
# State-directory permissions check (first "refuse to start" gate)
# ──────────────────────────────────────────────────────────────


def check_state_dir_permissions(state_dir: Path) -> None:
    """Refuse to proceed if ``state_dir`` is world-accessible.

    Vision §6.4 prose says "world-readable"; this implementation widens to
    ANY "other" permission bit (mask ``0o007``) — Phase 3a K5 defense in
    depth. A world-writable ``~/.letterbox/`` lets another local user
    inject arbitrary message files, which is strictly worse than
    world-readable; a world-traversable directory leaks structure even
    when individual file modes are tight. Rejecting any ``0o007`` bit
    costs nothing legitimate and closes the strictly-worse failure modes.

    This function is the **template** for future startup-validation
    checks. Phase 8a (launcher startup chain) will compose this with
    adapter-availability checks, MCP-config writability checks, etc.,
    using the same shape: take a ``Path`` (or other context), raise a
    domain error on fail, return ``None`` on accept.

    The function does NOT attempt to auto-fix bad permissions (Framework
    P5 cascade-test — silent mutation of user filesystem state is the
    wrong cure). It reports; the user repairs.

    A missing ``state_dir`` is NOT a permissions error — ``Path.stat()``
    raises ``FileNotFoundError``, which propagates. The caller (CLI
    ``init`` will create; launcher will refuse) decides the response.

    TOCTOU: a local attacker who can ``chmod`` ``~/.letterbox/`` between
    this check and the caller's next action already has filesystem
    access to the messages directly. Vision §6.4 explicitly does NOT
    defend against the compromised local user account.

    Args:
        state_dir: Absolute path to the letterbox state directory.

    Returns:
        ``None`` on accept.

    Raises:
        FileNotFoundError: If ``state_dir`` does not exist.
        StatePermissionsError: If any "other" permission bit (``0o007``)
            is set. The error message names the path, the offending
            octal mode, and the ``chmod 0700`` fix.
    """
    stat_result = state_dir.stat()
    mode = stat_result.st_mode & 0o777
    if mode & 0o007:
        raise StatePermissionsError(
            f"{state_dir} has mode 0o{mode:03o} (world-accessible); "
            f"chmod 0700 to continue"
        )


# ──────────────────────────────────────────────────────────────
# Per-agent read-state (Phase 3b — Vision §3.4 / §3.6 / ADR-021)
# ──────────────────────────────────────────────────────────────


# The exact set of keys that may appear in a read-state JSON file. Drives
# the strict-parse discipline in ReadState.from_dict (mirrors 2a K4's
# reserved-field guard on the Message payload).
_READ_STATE_FIELDS: frozenset[str] = frozenset(
    {"sender_label", "instance_id", "high_water_mark", "updated_at"}
)


def _now_iso_utc() -> str:
    """Return the current UTC time as an ISO-8601 string.

    Format: ``datetime.now(timezone.utc).isoformat()``. Microsecond
    precision is variable-length — when ``microsecond == 0`` the
    ``.000000`` segment is omitted (2a IMPLEMENTATION_NOTES G3).
    """
    return datetime.now(timezone.utc).isoformat()


def _broken_suffix_timestamp() -> str:
    """Return a filename-safe ISO-8601 UTC timestamp for ``.broken.<ts>``.

    Format: ``YYYYMMDDTHHMMSSffffffZ`` — colon-stripped (filename
    portability across filesystems that disallow ``:``; Vision §3.2
    rationale), microsecond-precise (sort-monotonic across consecutive
    corruptions in the same second), ``Z`` suffix for UTC.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + "Z"


@dataclass(frozen=True)
class ReadState:
    """A single endpoint's read-state on one channel (Vision §3.4).

    On-disk shape: JSON at ``<channel.path>/.read/<sender_label>.json``,
    one file per endpoint per channel. Per-agent isolation (ADR-021) —
    two peers sharing one channel each maintain their own ``ReadState``;
    one peer's acknowledge never advances the other peer's marker.

    Fields:
        sender_label: This endpoint's identity (also the filename stem).
            Validated at the I/O surface by :func:`read_state` and
            :func:`write_read_state`. The dataclass itself does not
            enforce the regex — direct construction is the escape hatch
            for callers that already trust their inputs (same pattern
            as ``Channel``).
        instance_id: The launcher's process identity at the most recent
            write. Free-form string; channel.py does NOT validate the
            shape (cross-tier coupling — the launcher in Phase 8a owns
            the ``lb-{ISO8601-no-punct}-{6-hex}`` format). Empty string
            allowed for the synthesised fresh-state placeholder.
        high_water_mark: Message-id stem (no ``.json``) of the most
            recent acknowledged peer message. Free-form at this layer
            (3c is the consumer that knows the msg-id shape). Empty
            string is the fresh-endpoint sentinel — lexically less than
            any real ``msg-...`` stem (Vision §3.6).
        updated_at: ISO-8601 UTC string of the last write. Free-form at
            the dataclass level; writers populate via
            :func:`_now_iso_utc`.
    """

    sender_label: str
    instance_id: str
    high_water_mark: str
    updated_at: str

    def to_dict(self) -> dict[str, str]:
        """Return the four fields as a plain dict for JSON serialisation.

        Order is irrelevant — the writer applies ``sort_keys=True``
        (ADR-030), so the on-disk bytes are alphabetically ordered.
        """
        return {
            "sender_label": self.sender_label,
            "instance_id": self.instance_id,
            "high_water_mark": self.high_water_mark,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReadState":
        """Construct a ``ReadState`` from a parsed JSON dict (strict).

        Mirrors 2a K4 reserved-field discipline:

        * Rejects unknown keys.
        * Rejects missing keys.
        * Rejects non-string values.

        Does NOT validate field *content* — semantic checks for
        ``high_water_mark`` shape (msg-id format) and ``instance_id``
        shape (``lb-...``) live at the consumer sites (3c / 4b / 8a).
        ``from_dict`` enforces SHAPE; consumers enforce SEMANTICS.

        Errors raised here propagate to :func:`read_state`'s recovery
        branch (rename-to-``.broken.<ts>`` + WARN + fresh state). The
        caller of ``read_state`` never sees the strict-parse exception
        directly.

        Args:
            data: A dict produced by ``json.loads`` on a read-state file.

        Returns:
            A constructed ``ReadState``.

        Raises:
            ValueError: On any shape mismatch. Vector messages name the
                offending key and the rule that failed.
        """
        if not isinstance(data, dict):
            raise ValueError(
                f"read-state JSON must be a dict, "
                f"got {type(data).__name__}"
            )
        extras = sorted(set(data) - _READ_STATE_FIELDS)
        if extras:
            raise ValueError(
                f"unknown key {extras[0]!r} in read-state JSON; "
                f"expected {sorted(_READ_STATE_FIELDS)}"
            )
        for key in sorted(_READ_STATE_FIELDS):
            if key not in data:
                raise ValueError(
                    f"missing key {key!r} in read-state JSON"
                )
            value = data[key]
            # isinstance(True, str) is False, so bool values are
            # naturally rejected here without special-casing — verified
            # by TestReadStateFromDict.test_rejects_bool_value.
            if not isinstance(value, str):
                raise ValueError(
                    f"key {key!r} must be str in read-state JSON, "
                    f"got {type(value).__name__}"
                )
        return cls(
            sender_label=data["sender_label"],
            instance_id=data["instance_id"],
            high_water_mark=data["high_water_mark"],
            updated_at=data["updated_at"],
        )


def read_state(channel: Channel, sender_label: str) -> ReadState:
    """Load this endpoint's per-channel read-state, recovering on corruption.

    Three paths:

    * **Missing file** — return a fresh ``ReadState`` with empty
      ``instance_id``/``high_water_mark`` and a current ``updated_at``.
      The ``.read/`` directory is NOT created on read — that is the
      writer's responsibility (K6).
    * **Existing valid file** — JSON-parse, strict-validate via
      :meth:`ReadState.from_dict`, return the constructed state.
    * **Corrupted file** (JSON parse error or strict-shape mismatch) —
      rename to ``.read/<sender_label>.json.broken.<ts>`` to preserve
      the bytes for postmortem (Kernel L8 Ironclad Invariant), log a
      WARN, return a fresh state. *Never* deletes user data, even when
      the user's data is corrupted.

    The ``sender_label`` is validated against
    :func:`letterbox.protocol.is_valid_channel_name` because the label
    becomes a filename component (K2, ADR-028 path safety). The same
    regex protects channel directory names and sender-label filenames —
    one path-safety boundary, one predicate.

    Args:
        channel: A ``Channel`` minted by :meth:`Channel.get_or_create`.
        sender_label: This endpoint's identity (also the filename stem).

    Returns:
        A ``ReadState`` — never raises on missing or corrupted file.

    Raises:
        TypeError: If ``sender_label`` is not a string.
        ValueError: If ``sender_label`` does not match
            ``^[a-z0-9][a-z0-9_-]*$``.
    """
    # Type guard before the predicate (G3 — same pattern as
    # Channel.get_or_create). is_valid_channel_name calls
    # re.Pattern.fullmatch which raises a less-informative stdlib
    # TypeError on non-str input; we translate to a domain error.
    if not isinstance(sender_label, str):
        raise TypeError(
            f"sender_label must be str, "
            f"got {type(sender_label).__name__}"
        )
    if not is_valid_channel_name(sender_label):
        raise ValueError(
            f"sender_label {sender_label!r} rejected; "
            f"must match ^[a-z0-9][a-z0-9_-]*$"
        )

    state_path = channel.path / ".read" / f"{sender_label}.json"

    if not state_path.exists():
        return ReadState(
            sender_label=sender_label,
            instance_id="",
            high_water_mark="",
            updated_at=_now_iso_utc(),
        )

    try:
        data = json.loads(state_path.read_bytes())
        return ReadState.from_dict(data)
    except (json.JSONDecodeError, ValueError) as exc:
        broken_path = state_path.with_name(
            f"{sender_label}.json.broken.{_broken_suffix_timestamp()}"
        )
        # os.rename is atomic on POSIX — either the original or the
        # .broken copy exists at any moment, never both half-written.
        os.rename(state_path, broken_path)
        _LOGGER.warning(
            "read-state %s corrupted (%s); preserved as %s; "
            "treating endpoint as fresh",
            state_path,
            exc,
            broken_path.name,
        )
        return ReadState(
            sender_label=sender_label,
            instance_id="",
            high_water_mark="",
            updated_at=_now_iso_utc(),
        )


def write_read_state(
    channel: Channel,
    state: ReadState,
    *,
    fsync: bool = False,
) -> Path:
    """Atomically write ``state`` to ``<channel.path>/.read/<state.sender_label>.json``.

    Auto-creates ``.read/`` with mode ``0o700`` on first write (K6 — 3a
    owns "channel exists"; 3b owns ".read/ inside it"). Uses
    write-temp-then-rename mirroring 2c's ``write_message``, so readers
    only ever see complete files (Vision §3.3 / Cross-Cutting §13.4 /
    Kernel L6). JSON is encoded with ``ensure_ascii=False`` (§13.2) and
    ``sort_keys=True`` (ADR-030); identical input states produce
    byte-identical files.

    The temp suffix is ``.json.tmp`` (note order — NOT ``.tmp.json``),
    matching 2c's ``write_message`` shape. The reaper in Phase 2d is
    scoped to ``msg-*.json.tmp`` in channel directories and will NOT
    sweep ``.read/<label>.json.tmp`` orphans; the file is overwritten
    on every successful write, so orphans are bounded by sender_label
    count × channel count (negligible at letterbox scale; documented as
    out-of-scope per §15).

    Args:
        channel: A ``Channel`` minted by :meth:`Channel.get_or_create`.
            ``channel.path`` must exist; this function does not create
            it.
        state: A fully-populated ``ReadState``. The caller (e.g., 3c's
            ``Channel.acknowledge``) owns synthesis of ``instance_id``,
            ``high_water_mark``, and ``updated_at``; this function
            writes verbatim.
        fsync: If ``True``, fsync the data file AND the parent
            directory after the rename so a crash cannot lose the state
            (K3). Default ``False`` matches Vision §9.4 (no fsync by
            default; POSIX rename semantics carry durability for the
            common case). Exposed only as this keyword argument — there
            is no CLI flag.

    Returns:
        The final :class:`Path` to the renamed read-state file.

    Raises:
        TypeError: If ``state.sender_label`` is not a string.
        ValueError: If ``state.sender_label`` does not match
            ``^[a-z0-9][a-z0-9_-]*$``.
        OSError: For underlying filesystem failures (permission,
            ENOSPC, ENOENT on ``channel.path``, etc.). On a failure
            between opening the ``.tmp`` and the ``os.replace``, the
            orphan ``.tmp`` is left behind — no defensive ``finally``
            unlink (mirrors 2c K2).
    """
    if not isinstance(state.sender_label, str):
        raise TypeError(
            f"sender_label must be str, "
            f"got {type(state.sender_label).__name__}"
        )
    if not is_valid_channel_name(state.sender_label):
        raise ValueError(
            f"sender_label {state.sender_label!r} rejected; "
            f"must match ^[a-z0-9][a-z0-9_-]*$"
        )

    read_dir = channel.path / ".read"
    # parents=False — channel.path was created by Channel.get_or_create
    # (3a); its absence means a caller error, not something
    # write_read_state should auto-fix. Defeat umask with the explicit
    # chmod (G1) so the mode is deterministic across caller environments.
    read_dir.mkdir(mode=0o700, parents=False, exist_ok=True)
    os.chmod(read_dir, 0o700)

    encoded = json.dumps(
        state.to_dict(), ensure_ascii=False, sort_keys=True
    ).encode("utf-8")

    final_path = read_dir / f"{state.sender_label}.json"
    tmp_path = read_dir / f"{state.sender_label}.json.tmp"

    with open(tmp_path, "wb") as fp:
        fp.write(encoded)
        if fsync:
            fp.flush()
            os.fsync(fp.fileno())

    # os.replace gives correct overwrite semantics on Windows (no-op
    # difference from os.rename on POSIX). The 2c↔3b parallel is
    # exact — same suffix order, same fsync placement.
    os.replace(tmp_path, final_path)

    if fsync:
        # Parent-directory fsync AFTER rename so the name->inode
        # binding is durable across a crash (K3).
        dirfd = os.open(str(read_dir), os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)

    return final_path


# ──────────────────────────────────────────────────────────────
# Phase 3c — UnreadResult + own-write filter (ADR-022)
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class UnreadResult:
    """The four-field return shape of :meth:`Channel.list_unread`.

    Symmetric with the rest of the channel-layer dataclasses
    (``Message``, ``ReadState``, ``Channel`` — all ``frozen=True``).
    Runtime-only — does NOT cross a persistence boundary; the MCP wire
    transformation at Phase 7c flattens this into a JSON envelope with
    ``messages: [...]``, per-message ``parse_error: "<reason>"`` /
    ``body: null`` for failures, ``has_more: bool``, and (when set)
    ``warning: "..."``.

    Fields:
        messages: Peer messages in lexical/chronological order, capped
            at the effective limit (after own-write filtering and cursor
            trimming).
        parse_errors: Files whose ``read_message`` returned
            :class:`letterbox.protocol.ParseError`, in encounter order.
            Each consumed an inbox slot (counts against the limit) so
            ``len(messages) + len(parse_errors)`` is the number of items
            the agent paid attention to.
        has_more: ``True`` iff more peer items (messages OR parse_errors)
            exist past the effective limit. Used by the MCP layer's
            pagination logic — the agent can re-call with a higher
            ``since_id`` to drain the backlog.
        limit_warning: Vector message naming the rejected ``limit`` AND
            the rule when the K5 clamp fired. ``None`` when ``limit`` is
            within ``[1, 100]``. Single-sentence log-line shape (no
            trailing period, G6) — the MCP layer may surface it verbatim
            or wrap it.
    """

    messages: list[Message]
    parse_errors: list[ParseError]
    has_more: bool
    limit_warning: str | None = None


def _is_own_write(
    msg: Message, self_sender: str, self_instance_id: str
) -> bool:
    """Return ``True`` iff ``msg`` was written by this endpoint (ADR-022).

    The combined own-write filter: ``(msg.sender == self_sender) OR
    (msg.instance_id == self_instance_id)``. Both halves are necessary
    to cover all four cells of the own-vs-peer matrix:

    * **Sender half** catches **cross-restart self-recognition**: a
      restarted letterbox process has a fresh ``instance_id`` but the
      same ``sender_label`` (durable, from ``letterbox.toml`` or
      ``--as``). Without this half the watcher would flood the agent
      with notifications for its own historical writes (Vision §3.2).
    * **Instance_id half** catches the **same-harness configuration-error
      case**: two endpoints on a channel that both default to the same
      ``sender`` (e.g., the user forgot ``--as`` on the second terminal)
      would deadlock under sender-only filtering. ``instance_id`` is
      always distinct per process, so OR-ing it in keeps mechanical
      filtering correct even when human-readable identity is misconfigured.

    **Defensive empty-string guard:** if either half's matched pair is
    the empty string (configuration error caught at a higher layer),
    that half does NOT count as a match — the filter must not silently
    classify every message as own and starve the agent's inbox.

    **Module-private friend-import contract:** the leading underscore
    declares "this is intentional internal API, not a public surface".
    Two consumers in v1: :meth:`Channel.list_unread` (same module) and
    Phase 4b's ``Watcher`` (Tier-2 sibling that will import via
    ``from letterbox.channel import _is_own_write``). Promoting to
    ``__all__`` would overstate the API; inlining the OR in
    ``list_unread`` would force the watcher to duplicate the rule and
    risk drift.

    Args:
        msg: The peer-or-own ``Message`` to classify.
        self_sender: This endpoint's sender label (from
            ``Channel.sender_label`` — the launcher-resolved durable
            identity). ASCII case-sensitive comparison (no normalization).
        self_instance_id: This endpoint's ephemeral per-process identity
            (launcher-generated, ``lb-...``). ASCII case-sensitive.

    Returns:
        ``True`` if either half of the combined filter matches with a
        non-empty value; ``False`` otherwise.
    """
    sender_match = self_sender != "" and msg.sender == self_sender
    instance_match = (
        self_instance_id != "" and msg.instance_id == self_instance_id
    )
    return sender_match or instance_match


# ──────────────────────────────────────────────────────────────
# Phase 3d — Channel Listing + Channel Info
# ──────────────────────────────────────────────────────────────


def _filename_to_iso_timestamp(filename: str) -> str:
    """Convert a strict message filename to an ISO-8601 UTC timestamp.

    Slices the 21-char timestamp segment (chars ``[4:25]``) from a
    filename matching ADR-028's regex
    ``^msg-[0-9]{8}T[0-9]{6}[0-9]{6}-[0-9a-f]{32}\\.json$`` and rebuilds
    it as an ISO-8601 UTC string (``YYYY-MM-DDTHH:MM:SS.ffffff+00:00``).

    K5 — filenames are the source of truth for activity timestamps
    because ``mtime`` is fragile (``cp -r`` without ``-p``, syncthing,
    rsync, NFS clock skew, ``tar`` extraction all reset or shuffle
    ``mtime``; filenames are immutable and travel with the file). This
    function is the bridge from the filename's lexically-sortable
    microsecond-precise form to the ISO-8601 wire form used everywhere
    else in letterbox (``Message.timestamp``, ``ReadState.updated_at``).

    Caller's contract: ``filename`` MUST already match the strict regex
    above. ``list_messages`` (2c) returns only validated names, so the
    only in-vision caller (``list_channels``) satisfies the contract by
    construction. The slice math is anchored to ADR-028; G7's
    ``test_filename_to_iso_roundtrip`` will fail loudly if the regex
    ever changes.

    Args:
        filename: A message filename (must match ADR-028).

    Returns:
        ISO-8601 UTC string with microsecond precision and ``+00:00``
        offset.
    """
    # filename[4:25] is the 21-char ``YYYYMMDDTHHMMSSffffff`` segment.
    # ``strptime`` does NOT have glibc's strftime("%Y") zero-pad bug
    # (per Phase 2b IMPLEMENTATION_NOTES — strftime emits 3-char years
    # for year < 1000; strptime correctly parses 4-char years). The
    # production input domain is ``now(UTC)`` (year 2026+), so the
    # ambient invariant is trivial.
    ts = datetime.strptime(filename[4:25], "%Y%m%dT%H%M%S%f").replace(
        tzinfo=timezone.utc
    )
    return ts.isoformat()


@dataclass(frozen=True)
class ChannelSummary:
    """Per-channel summary returned by :func:`list_channels`.

    Frozen dataclass; runtime-only — does NOT cross a persistence
    boundary. The MCP wire transformation at Phase 7d flattens this to
    ``{name, last_activity}`` per Vision §6.1.

    Fields:
        name: Channel name (verbatim from the directory name; already
            ``is_valid_channel_name``-validated by ``list_channels``).
        path: Absolute path to the channel directory
            (``state_dir/channels/<name>``).
        last_activity: ISO-8601 UTC string of the lexically-last
            ``msg-*.json`` filename's embedded timestamp (K5 — derived
            from the filename, NOT from ``mtime``), or ``None`` when
            the channel directory contains zero message files (K3 —
            honest empty-channel semantics; consumers that want a
            sortable sentinel use ``key=lambda c: c.last_activity or ""``).
    """

    name: str
    path: Path
    last_activity: str | None = None


@dataclass(frozen=True)
class ChannelInfo:
    """Per-channel info returned by :func:`channel_info`.

    Frozen dataclass; runtime-only. The MCP wire transformation at
    Phase 7d flattens this to
    ``{channel, sender_label, recipient_label, unread_count}`` per
    Vision §6.1.

    All four fields are server-computed from the launcher-resolved
    ``Channel`` handle (``name`` / ``sender_label`` / ``recipient_label``)
    and the filesystem (``unread_count``). Agent-supplied strings
    NEVER reach these fields — Join-Key Discipline §13.3 by composition.

    Fields:
        channel: Channel name (mirrors ``Channel.name``).
        sender_label: This endpoint's identity (mirrors
            ``Channel.sender_label`` — launcher-resolved per Vision §3.2
            priority chain ``--as`` > ``LETTERBOX_SENDER`` > harness
            name).
        recipient_label: The peer endpoint's identity (mirrors
            ``Channel.recipient_label``).
        unread_count: True count of unread peer items for this endpoint
            after applying the ADR-022 combined own-write filter and
            the 3b ``high_water_mark`` cursor. Parse errors count toward
            the total (they consume an inbox slot the agent must attend
            to; K4). NOT capped at 100 — honest count beats capped
            count for the agent's affordances ("47 unread" is
            decision-relevant; "100+ unread" is not).
        peer_label: The peer's identity as *observed from real traffic* —
            the ``sender`` of the most recent peer message — or ``None``
            when the peer has never spoken on this channel (ADR-056). This
            is the honest answer to "who am I talking to?": ``recipient_label``
            is unknown at launch (``""`` in v1), but the peer announces
            itself the moment it writes. Peer-sourced, so informational
            for the agent (same trust class as ``check_messages`` bodies),
            never fed into a trusted notification.
        last_peer_activity: ISO-8601 UTC timestamp of the most recent peer
            message (from its filename, the server-generated sortable
            stamp — ADR-027), or ``None`` when the peer has never spoken.
            The agent's liveness signal: "peer last spoke 90 s ago" reads
            very differently from "3 days ago" or "never".
    """

    channel: str
    sender_label: str
    recipient_label: str
    unread_count: int
    peer_label: str | None = None
    last_peer_activity: str | None = None


def list_channels(*, state_dir: Path) -> list[ChannelSummary]:
    """Enumerate channels under ``state_dir/channels/`` and summarise each.

    Walks ``state_dir/channels/`` one level only, returning one
    :class:`ChannelSummary` per directory whose name passes
    :func:`letterbox.protocol.is_valid_channel_name`. Non-directory
    entries, hidden files, and invalid-name directories are silently
    skipped (G1 — directory enumeration is best-effort, not a security
    boundary; the security boundary is the regex gate inside
    :meth:`Channel.get_or_create`).

    Output is sorted by ``name`` (K6/G8 — ``os.scandir`` iteration order
    is non-deterministic on Linux per 2d IMPLEMENTATION_NOTES). Stable
    output matters for both CLI human consumption and ``jq``-style
    scripting.

    Each ``last_activity`` is derived from the lexically-last
    ``msg-*.json`` filename's embedded timestamp (K5 / G3). Empty
    channels (zero message files) surface ``last_activity=None`` (K3 —
    honest semantics over fabricated sentinels).

    The ``state_dir`` parameter is keyword-only (K1) — channel.py
    cannot import :mod:`letterbox.config`, so the launcher (8a) / CLI
    (9b) / MCP server (7d) resolve via
    :func:`letterbox.config.resolve_state_dir` and pass the absolute
    path in.

    Missing ``state_dir/channels/`` returns ``[]`` (G2 — fresh
    ``~/.letterbox/``, post-init but pre-first-channel-write, has no
    ``channels/`` subdir). A missing ``state_dir`` itself ALSO returns
    ``[]`` for symmetry — ``letterbox list-channels`` on a brand-new
    install should print an empty list rather than a stack trace
    (§14 implementer's-latitude — both branches collapse into the same
    ``Path.exists()`` short-circuit).

    Scope clarification (K7): channels declared in ``letterbox.toml``
    but never written to (no on-disk directory) are NOT surfaced.
    ``list_channels`` is the filesystem-only primitive; the config-union
    shape (if needed) lives at the CLI/MCP composition layer (9b / 7d),
    where importing :mod:`letterbox.config` is permitted.

    **Performance note:** at v1 scale (50 channels of mixed populations)
    the per-channel ``list_messages`` enumeration is dominant. The 10c
    end-to-end ``list-channels`` budget (P95 < 200 ms on representative
    workloads) covers this from the CLI surface; 3d itself adds no
    BUDGET-OWNER gate.

    Args:
        state_dir: Absolute path to the letterbox state directory
            (``~/.letterbox/`` by default).

    Returns:
        A list of :class:`ChannelSummary` sorted by ``name``. Empty
        list when ``state_dir/channels/`` (or ``state_dir`` itself)
        does not exist.
    """
    channels_root = state_dir / "channels"
    # G2 — missing channels/ (or missing state_dir entirely) collapses
    # to []. Path.exists() short-circuits on any missing intermediate,
    # so a single check covers both cases.
    if not channels_root.exists():
        return []

    summaries: list[ChannelSummary] = []
    # os.scandir requires explicit context-manager close to release the
    # OS-level directory handle promptly (matches protocol.list_messages'
    # discipline at protocol.py:808).
    with os.scandir(channels_root) as it:
        for entry in it:
            # G1 — non-directories silently skipped. ``.DS_Store``,
            # stray lock files, future archive sidecar files, etc.
            if not entry.is_dir():
                continue
            # G1 — invalid channel-name directories silently skipped.
            # ``is_valid_channel_name`` matches the regex used by
            # ``Channel.get_or_create``; uppercase/dotfile/leading-dash
            # dirs (hand-edited state, unrelated tools) cannot be
            # surfaced as channels and therefore are not.
            if not is_valid_channel_name(entry.name):
                continue
            entry_path = Path(entry.path)
            # K5/G3 — last_activity from the lexically-last msg-*.json
            # filename's embedded timestamp. list_messages already
            # validates names against ADR-028 and lexically sorts; the
            # final element is the chronologically newest message.
            # No per-file stat or read_message — filename enumeration
            # is the entire cost.
            paths = list_messages(entry_path, since=None)
            if paths:
                last_activity: str | None = _filename_to_iso_timestamp(
                    paths[-1].name
                )
            else:
                # K3 — honest None on empty channels.
                last_activity = None
            summaries.append(
                ChannelSummary(
                    name=entry.name,
                    path=entry_path,
                    last_activity=last_activity,
                )
            )

    # K6/G8 — explicit name-sort. os.scandir iteration order is NOT
    # guaranteed alphabetical on Linux (verified in 2d's per-file-error
    # test under pytest-xdist), so the explicit sort is load-bearing.
    summaries.sort(key=lambda c: c.name)
    return summaries


def channel_info(
    channel: Channel, *, self_instance_id: str
) -> ChannelInfo:
    """Return this endpoint's view of one channel — identity + unread count.

    Composes :func:`letterbox.protocol.list_messages` + 2c
    :func:`letterbox.protocol.read_message` + the ADR-022 combined
    own-write filter (:func:`_is_own_write`) + 3b's :func:`read_state`
    into the canonical per-channel metadata query.

    The returned fields are all sourced server-side:

    * ``channel`` / ``sender_label`` / ``recipient_label`` come from the
      launcher-resolved :class:`Channel` handle.
    * ``unread_count`` comes from filesystem enumeration of
      ``msg-*.json`` files past this endpoint's ``high_water_mark``,
      filtered by ADR-022 (sender half catches cross-restart
      self-recognition; instance half catches the same-harness
      configuration-error case).
    * ``peer_label`` / ``last_peer_activity`` (ADR-056) are the agent's
      situational-awareness signals, read from the most recent peer
      message (a reverse scan that stops at the first non-own-write):
      who the peer is and when it last spoke, or ``None``/``None`` when
      the peer has never written. Peer-sourced and informational — the
      same trust class as ``check_messages`` bodies, never injected into
      a notification.

    Per Vision §6.4 + §13.3 (Join-Key Discipline), the agent NEVER
    asserts identity or unread-count; the server computes both. A
    malicious peer's ``sender`` or ``instance_id`` payload field cannot
    drive any of the returned values.

    Parse errors (malformed JSON, oversized — anything that surfaces as
    :class:`letterbox.protocol.ParseError`) count toward
    ``unread_count`` — they consume an inbox slot the agent must
    attend to (K4). Per-event WARN log (mirrors 3c
    :meth:`Channel.list_unread`). Races with prune
    (``FileNotFoundError`` from ``read_message``) are silently skipped
    — no WARN, no count contribution (G5).

    The ``unread_count`` is the **true count**, NOT capped at 100. Plan
    K4: honest count beats capped count for the agent's affordances —
    "you have 47 unread" is decision-relevant; "you have 100+ unread"
    forces a second tool call to find the actual figure. Vision §6.1
    names ``unread_count`` as a single integer with no ``has_more``.

    **Performance note:** Channels with more than ~10 000 unarchived
    peer messages may see noticeable latency on this call (one JSON
    parse per unread peer message). Users running long-lived channels
    should periodically run ``letterbox prune`` (Phase 9d) to keep
    performance bounded (Vision §3.5).

    Args:
        channel: A :class:`Channel` minted by
            :meth:`Channel.get_or_create` (or otherwise constructed
            with a path that exists on disk).
        self_instance_id: The launcher's process-identity for this
            endpoint (``lb-...``). Keyword-only (K2 — mirrors
            :meth:`Channel.acknowledge` / :meth:`Channel.list_unread`).
            Treated as opaque; no shape validation (the launcher in
            Phase 8a owns the format).

    Returns:
        A :class:`ChannelInfo` with the four fields populated. All
        sourced server-side (§13.3).

    Raises:
        FileNotFoundError: If ``channel.path`` does not exist — propagates
            from ``list_messages``. Caller bug — direct ``Channel(...)``
            construction with a nonexistent path bypasses
            ``get_or_create``'s mkdir (G9). The function does NOT add
            a defensive ``.exists()`` check (consumer-raises pattern).
        TypeError: If ``channel.sender_label`` is not a string
            (propagates from 3b's :func:`read_state`).
        ValueError: If ``channel.sender_label`` does not match
            ``^[a-z0-9][a-z0-9_-]*$``.
    """
    # 3b read_state — recovers from missing/corrupted .read/ file
    # automatically. Validates channel.sender_label against the
    # channel-name regex (path-safety boundary).
    current = read_state(channel, channel.sender_label)
    bound = current.high_water_mark

    # G9 — list_messages raises FileNotFoundError if channel.path
    # doesn't exist. Documented caller-bug path; let it propagate.
    paths = list_messages(channel.path, since=None)
    count = 0
    for path in paths:
        # G1-from-3c — stem comparison (no .json); matches 3b's stored
        # high_water_mark shape ("msg-..." with no extension).
        if path.stem <= bound:
            continue
        try:
            result = read_message(path)
        except FileNotFoundError:
            # G5 — race with prune is normal; silently skip, no WARN.
            continue
        if isinstance(result, ParseError):
            # K4 — parse error consumes an inbox slot. Mirror 3c's
            # per-event WARN (no dedupe — corruption is per-event,
            # not per-novel-name).
            _LOGGER.warning(
                "parse_error during channel_info on %s: %s",
                path,
                result.reason,
            )
            count += 1
            continue
        # ADR-022 combined own-write filter — drop own writes.
        if _is_own_write(result, channel.sender_label, self_instance_id):
            continue
        count += 1

    # AX state (ADR-056): the most recent peer message gives the agent two
    # honest situational signals with no presence mechanism — WHO the peer is
    # (its sender label, observed from real traffic, not the launch-time
    # recipient_label which is "" in v1) and WHEN it last spoke. Reverse-scan
    # from the newest filename and stop at the first peer message: the common
    # case (a recent peer message) reads one or a few files; it degrades to a
    # full read only when the peer has NEVER spoken — itself the decision-
    # relevant "peer absent" signal, on a channel that is small by definition.
    peer_label: str | None = None
    last_peer_activity: str | None = None
    for path in reversed(paths):
        try:
            recent = read_message(path)
        except FileNotFoundError:
            continue
        if isinstance(recent, ParseError):
            continue
        if _is_own_write(recent, channel.sender_label, self_instance_id):
            continue
        peer_label = recent.sender
        last_peer_activity = _filename_to_iso_timestamp(path.name)
        break

    return ChannelInfo(
        channel=channel.name,
        sender_label=channel.sender_label,
        recipient_label=channel.recipient_label,
        unread_count=count,
        peer_label=peer_label,
        last_peer_activity=last_peer_activity,
    )

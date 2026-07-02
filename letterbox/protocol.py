"""Message format, serialization, filename generation, atomic-rename write primitives, strict filename validation.

Tier: 1
May import from: stdlib only.
Must NOT import from: any other ``letterbox.*`` module (Tier 1 leaf — see PLANNING_FRAMEWORK P7 bulkhead rule).

Filled in: Phase 2a/2b/2c/2d per PHASE_INDEX.
"""
from __future__ import annotations

import heapq
import json
import logging
import os
import re
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


__all__ = [
    "MAX_BODY_BYTES",
    "SCHEMA_VERSION",
    "Message",
    "Metadata",
    "MessageTooLarge",
    "ParseError",
    "from_json_bytes",
    "is_valid_channel_name",
    "is_valid_message_filename",
    "list_messages",
    "make_message_filename",
    "new_message",
    "read_message",
    "reap_orphan_tmp",
    "to_json_bytes",
    "write_message",
]


# ──────────────────────────────────────────────────────────────
# Constants (ADR-014 — 5 MB ceiling; v1 schema version)
# ──────────────────────────────────────────────────────────────


MAX_BODY_BYTES: int = 5 * 1024 * 1024
SCHEMA_VERSION: int = 1


# ──────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────


class MessageTooLarge(Exception):
    """Raised by :func:`to_json_bytes` when the serialized payload exceeds
    :data:`MAX_BODY_BYTES`.

    The cap is on the *output bytes* of ``to_json_bytes`` (ADR-014); we
    short-circuit before serialising when the body alone already exceeds
    the limit (K2). Error message includes the field, the limit, and the
    actual byte count so the caller has a vector to act on (Framework P3).
    """


# ──────────────────────────────────────────────────────────────
# Dataclasses (Vision §3.2)
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Metadata:
    """Reserved metadata block for forward-compatibility.

    ``encryption`` is reserved for future encryption-at-rest (v1 always
    ``None``). ``ext`` is the open extension point preserved verbatim by
    the protocol (v1 factory writes ``{}`` when no value is supplied).
    """

    encryption: dict | None
    ext: dict


@dataclass(frozen=True)
class Message:
    """A single message on the letterbox wire (Vision §3.2).

    All fields are present in v1; ``recipient`` and ``in_reply_to`` may
    be ``None`` but the JSON keys are always emitted (the parser rejects
    their absence). The dataclass is ``frozen=True`` so two equal Messages
    serialise to byte-identical output (K5).
    """

    schema_version: int
    id: str
    channel: str
    address: str
    instance_id: str
    sender: str
    recipient: str | None
    timestamp: str
    body: str
    in_reply_to: str | None
    metadata: Metadata


# ──────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────


_ALLOWED_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "schema_version",
        "id",
        "channel",
        "address",
        "instance_id",
        "sender",
        "recipient",
        "timestamp",
        "body",
        "in_reply_to",
        "metadata",
    }
)
_REQUIRED_TOP_LEVEL_KEYS: frozenset[str] = _ALLOWED_TOP_LEVEL_KEYS
_ALLOWED_METADATA_KEYS: frozenset[str] = frozenset({"encryption", "ext"})


def _normalise_ext_value(value: Any) -> Any:
    """Recursively replace tuples with lists so persisted JSON round-trips
    cleanly.

    JSON has no tuple type — ``json.loads`` always returns lists for arrays.
    Normalising at factory time prevents the silent invariant break of
    ``write -> read -> assert equality`` failing because one side carries a
    tuple and the other a list (Framework P3: errors are vectors; the friendly
    vector here is "we normalised, so your round-trip succeeds").
    """
    if isinstance(value, tuple):
        return [_normalise_ext_value(v) for v in value]
    if isinstance(value, list):
        return [_normalise_ext_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalise_ext_value(v) for k, v in value.items()}
    return value


def _format_utc_timestamp(dt: datetime) -> str:
    """Validate a datetime is UTC (or zero-offset alias) and emit ISO-8601.

    Accepts ``timezone.utc`` and any aware ``datetime`` whose
    ``utcoffset()`` is ``timedelta(0)``. Rejects naive datetimes and
    aware datetimes whose offset is non-zero (ADR-015). Local time is
    forbidden anywhere in the message format.
    """
    offset = dt.utcoffset()
    if offset != timedelta(0):
        raise ValueError(
            "timestamp must be a UTC-aware datetime "
            "(offset must be exactly +00:00; pass datetime.now(timezone.utc) "
            "or a datetime with tzinfo=timezone.utc). "
            "Naive and non-UTC datetimes are forbidden (ADR-015)."
        )
    return dt.isoformat()


def _require_key(table: dict, key: str, context: str) -> Any:
    """Return ``table[key]`` or raise a vector ``ValueError``."""
    if key not in table:
        raise ValueError(
            f"missing required field {key!r} in {context}. "
            f"Every field declared in schema_version=1 must be present."
        )
    return table[key]


def _reject_extra_keys(table: dict, allowed: frozenset[str], context: str) -> None:
    """Raise ``ValueError`` if ``table`` carries keys outside ``allowed``."""
    extras = sorted(set(table) - allowed)
    if not extras:
        return
    raise ValueError(
        f"unknown field(s) in {context}: {', '.join(repr(k) for k in extras)}. "
        f"Only metadata.ext is an open extension point in schema_version=1; "
        f"other unknown fields indicate schema drift."
    )


def _require_type(value: Any, expected: type, field_name: str) -> None:
    """Raise ``ValueError`` if ``value`` is not an instance of ``expected``.

    ``bool`` is special-cased: ``isinstance(True, int)`` is True in Python,
    which would let a JSON ``true`` slip past an ``int`` type check.
    """
    if expected is int and isinstance(value, bool):
        raise ValueError(
            f"field {field_name!r} must be an integer, got bool. "
            "JSON true/false are not accepted for integer fields."
        )
    if not isinstance(value, expected):
        raise ValueError(
            f"field {field_name!r} must be {expected.__name__}, "
            f"got {type(value).__name__}."
        )


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────


def new_message(
    *,
    id: str,
    channel: str,
    instance_id: str,
    sender: str,
    body: str,
    recipient: str | None = None,
    in_reply_to: str | None = None,
    ext: dict | None = None,
    timestamp: datetime | None = None,
) -> Message:
    """Build a :class:`Message` with reserved fields populated.

    The factory is the *only* path that fills ``schema_version``,
    ``address``, ``metadata.encryption``, and ``metadata.ext``. Reserved
    fields cannot be overridden by the caller (no ``address`` parameter,
    no ``encryption`` parameter); this is deliberate per Vision §1.2's
    Retrofit-Test wiring promise.

    Args:
        id: Message id string. Format validation lives in Phase 2b; this
            factory accepts any string verbatim.
        channel: Channel name string. Validation lives in Phase 2b.
        instance_id: Per-launch identifier (set by the launcher, Phase 8a).
        sender: Sender label (set by the launcher from ``--as`` /
            ``LETTERBOX_SENDER`` / harness name, ADR-026). Must be non-empty.
        body: Message body, any UTF-8 string up to ~5 MB serialised.
        recipient: Optional peer label; ``None`` by default since v1 launchers
            rarely know the peer's symmetric identity.
        in_reply_to: Optional ancestor message id; trusted blindly (ADR-020).
        ext: Optional open-extension dict; defaults to a fresh empty dict.
            Tuples in nested values are normalised to lists at factory time.
        timestamp: Optional UTC-aware datetime; ``None`` means "now in UTC".
            Naive and non-UTC datetimes are rejected with a vector ValueError.

    Returns:
        A frozen :class:`Message` with every field populated.

    Raises:
        ValueError: If ``sender`` is empty, or ``timestamp`` is naive /
            non-UTC.
    """
    if not sender:
        raise ValueError(
            "sender must be a non-empty string. "
            "The launcher resolves identity from --as / LETTERBOX_SENDER / "
            "harness name (ADR-026)."
        )

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    ts_str = _format_utc_timestamp(timestamp)

    ext_value: dict
    if ext is None:
        ext_value = {}
    else:
        ext_value = _normalise_ext_value(ext)

    return Message(
        schema_version=SCHEMA_VERSION,
        id=id,
        channel=channel,
        address="file://local",
        instance_id=instance_id,
        sender=sender,
        recipient=recipient,
        timestamp=ts_str,
        body=body,
        in_reply_to=in_reply_to,
        metadata=Metadata(encryption=None, ext=ext_value),
    )


def to_json_bytes(msg: Message) -> bytes:
    """Serialise a :class:`Message` to UTF-8 JSON bytes.

    The output:

    * Has sorted keys at every level (K5 — equal Messages produce identical
      bytes; round-trip ``to_json_bytes(from_json_bytes(...))`` is the
      identity at the bytes level).
    * Uses ``ensure_ascii=False`` so Lithuanian, CJK, and emoji appear as
      themselves rather than ``\\uXXXX`` escapes (Vision §13.2 / Phase 1b lint).
    * Has no indentation, no trailing newline, no BOM — Phase 2c writes
      exactly these bytes to disk via atomic-rename.

    Args:
        msg: A :class:`Message` to serialise.

    Returns:
        UTF-8 encoded JSON bytes.

    Raises:
        MessageTooLarge: If the serialised payload exceeds
            :data:`MAX_BODY_BYTES`. We short-circuit before serialising
            when the body alone (UTF-8 byte count) already exceeds the
            limit, so pathological inputs are cheap to reject (K2).
    """
    # K2 / Gotcha 4: cheap O(1) early-out using codepoint count; if the
    # codepoint count already exceeds the byte cap, the UTF-8 byte count
    # can only be larger.
    if len(msg.body) > MAX_BODY_BYTES:
        raise MessageTooLarge(
            f"body codepoint count {len(msg.body)} exceeds {MAX_BODY_BYTES} "
            f"(5 MB ceiling, ADR-014). Reduce the body or split the message."
        )
    body_bytes_len = len(msg.body.encode("utf-8"))
    if body_bytes_len > MAX_BODY_BYTES:
        raise MessageTooLarge(
            f"body UTF-8 byte count {body_bytes_len} exceeds {MAX_BODY_BYTES} "
            f"(5 MB ceiling, ADR-014). Reduce the body or split the message."
        )

    payload = {
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
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_BODY_BYTES:
        raise MessageTooLarge(
            f"serialised body bytes {len(encoded)} exceed {MAX_BODY_BYTES} "
            f"(5 MB ceiling, ADR-014). The envelope adds ~250 bytes; "
            f"reduce the body to fit."
        )
    return encoded


def from_json_bytes(data: bytes | bytearray | memoryview) -> Message:
    """Parse JSON bytes back into a :class:`Message`.

    Strict by design (K4): missing required fields, unknown extra top-level
    keys, type mismatches, and unsupported ``schema_version`` values all
    raise :class:`ValueError` with a vector message naming the field and
    the rule. The only open extension point is ``metadata.ext``.

    Args:
        data: A bytes-like object containing UTF-8 encoded JSON.

    Returns:
        A :class:`Message` reconstructed from the JSON.

    Raises:
        ValueError: On any structural deviation from the schema_version=1
            shape (missing field, unknown field, wrong type, unsupported
            version).
    """
    try:
        parsed = json.loads(bytes(data))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"malformed JSON: {exc}. "
            f"The on-disk shape is schema_version=1 (see DECISIONS.md ADR-014)."
        ) from exc

    if not isinstance(parsed, dict):
        raise ValueError(
            f"message root must be a JSON object, got {type(parsed).__name__}."
        )

    _reject_extra_keys(parsed, _ALLOWED_TOP_LEVEL_KEYS, "message root")
    for key in _REQUIRED_TOP_LEVEL_KEYS:
        if key not in parsed:
            raise ValueError(
                f"missing required field {key!r} in message root. "
                f"Every field declared in schema_version=1 must be present."
            )

    schema_version = parsed["schema_version"]
    _require_type(schema_version, int, "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported schema_version={schema_version}. "
            f"This letterbox version recognises only schema_version=1."
        )

    _require_type(parsed["id"], str, "id")
    _require_type(parsed["channel"], str, "channel")
    _require_type(parsed["address"], str, "address")
    _require_type(parsed["instance_id"], str, "instance_id")
    _require_type(parsed["sender"], str, "sender")
    if not parsed["sender"]:
        raise ValueError(
            "field 'sender' must be a non-empty string "
            "(identity comes from --as / LETTERBOX_SENDER / harness name)."
        )

    recipient = parsed["recipient"]
    if recipient is not None:
        _require_type(recipient, str, "recipient")

    _require_type(parsed["timestamp"], str, "timestamp")
    _require_type(parsed["body"], str, "body")

    in_reply_to = parsed["in_reply_to"]
    if in_reply_to is not None:
        _require_type(in_reply_to, str, "in_reply_to")

    metadata_raw = parsed["metadata"]
    _require_type(metadata_raw, dict, "metadata")
    _reject_extra_keys(metadata_raw, _ALLOWED_METADATA_KEYS, "metadata")
    encryption = _require_key(metadata_raw, "encryption", "metadata")
    if encryption is not None:
        _require_type(encryption, dict, "metadata.encryption")
    ext = _require_key(metadata_raw, "ext", "metadata")
    _require_type(ext, dict, "metadata.ext")

    return Message(
        schema_version=schema_version,
        id=parsed["id"],
        channel=parsed["channel"],
        address=parsed["address"],
        instance_id=parsed["instance_id"],
        sender=parsed["sender"],
        recipient=recipient,
        timestamp=parsed["timestamp"],
        body=parsed["body"],
        in_reply_to=in_reply_to,
        metadata=Metadata(encryption=encryption, ext=ext),
    )


# ──────────────────────────────────────────────────────────────
# Filename generation + name validation (Phase 2b)
# ──────────────────────────────────────────────────────────────


# Strict message-filename regex (ADR-028). Literal ``[0-9]`` rather than
# ``\d`` so Unicode decimal digits (Gujarati ૪, Arabic-Indic, ...) cannot
# pass the validator and reach the notification renderer (Phase 2b G3).
# Explicit ``^...$`` anchors combine with ``re.fullmatch`` at the call site
# as belt-and-suspenders against a future deletion of either guard (K2).
_MESSAGE_FILENAME_RE: re.Pattern[str] = re.compile(
    r"^msg-[0-9]{8}T[0-9]{6}[0-9]{6}-[0-9a-f]{32}\.json$"
)

# Strict channel-name regex. Tightens Vision §6.4's prose (``[a-z0-9_-]+``)
# to require the first character to be alphanumeric, blocking leading ``-``
# (would mis-parse as a CLI flag) and leading ``_`` (conventionally private).
# See Phase 2b K3 / G5 for the rationale.
_CHANNEL_NAME_RE: re.Pattern[str] = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def make_message_filename(timestamp: datetime | None = None) -> str:
    """Generate a canonical letterbox message filename.

    Shape: ``msg-YYYYMMDDTHHMMSSffffff-{uuid4hex32}.json`` — 63 characters
    total (4 + 21 + 1 + 32 + 5). The microsecond-resolved timestamp segment
    is fixed-width zero-padded so that filename lexical sort equals
    chronological sort (Vision §3.2). The 32-character UUID4 hex tail
    carries 128 bits of randomness — birthday-paradox 50% collision at
    ~2^64 messages, effectively impossible at any realistic scale (ADR-027).

    The microsecond segment is emitted via ``strftime("%f")`` (always six
    digits) rather than ``isoformat()`` (omits ``.000000`` for whole-second
    timestamps). This deliberate divergence from the on-wire timestamp
    shape (Phase 2a G1) is what makes "ls + sort by name" equal
    "messages in chronological order" — without it, every consumer would
    have to parse JSON to recover the order.

    Args:
        timestamp: Optional UTC-aware datetime; ``None`` (default) means
            ``datetime.now(timezone.utc)``. Naive and non-UTC datetimes
            are rejected with a vector ``ValueError`` (ADR-015 — reuses
            :func:`_format_utc_timestamp` for the discipline check).

    Returns:
        A filename string of length 63.

    Raises:
        ValueError: If ``timestamp`` is naive or has a non-UTC offset.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    # Side-effect call: raises ValueError on naive/non-UTC. ISO string
    # discarded — the filename uses strftime("%f") below for fixed-width
    # microseconds (G1 — divergent from the JSON timestamp shape on purpose).
    _format_utc_timestamp(timestamp)
    # Year is f-string formatted because glibc's strftime("%Y") does NOT
    # zero-pad years < 1000 (year 999 → "999", not "0999"), which would
    # produce a 7-char date segment that fails the [0-9]{8} validator
    # regex. Production calls always pass `now(UTC)` (year 2026+), so this
    # is a defense-in-depth correctness guarantee for the full datetime
    # domain rather than a runtime behavior change. strftime is retained
    # for month/day/time/microsecond (K4) where its zero-padding is
    # robust across years.
    ts_part = f"{timestamp.year:04d}{timestamp.strftime('%m%dT%H%M%S%f')}"
    return f"msg-{ts_part}-{uuid.uuid4().hex}.json"


def is_valid_message_filename(name: str) -> bool:
    """Return True iff ``name`` matches the strict message-filename shape.

    Strict regex ``^msg-[0-9]{8}T[0-9]{6}[0-9]{6}-[0-9a-f]{32}\\.json$``
    (ADR-028). This predicate is the **load-bearing defense against
    template-injection-via-filename**: even if a malicious peer wrote
    ``msg-123-$(rm -rf /).json`` into the channel directory, callers
    that gate template substitution on this predicate cannot smuggle
    shell text into the notification stream (Vision §3.2 / §6.4).

    Returns ``bool`` rather than raising — Phase 2c ``list_messages``
    filters silently and logs novel rejections at WARN (per ADR-028);
    Phase 3a ``Channel.get_or_create`` and Phase 7b ``send_message``
    wrap this and raise domain errors with vector messages. The
    "predicate owns the truth, consumer owns the error" split (K5)
    keeps each call site free to choose its own surface.

    Args:
        name: Filename string (e.g., from ``os.scandir(channel_dir)``).

    Returns:
        ``True`` if ``name`` is a syntactically valid message filename;
        ``False`` otherwise.
    """
    return _MESSAGE_FILENAME_RE.fullmatch(name) is not None


def is_valid_channel_name(name: str) -> bool:
    """Return True iff ``name`` is a safe channel-name string.

    Regex ``^[a-z0-9][a-z0-9_-]*$`` — lowercase ASCII alphanumeric, with
    ``-`` and ``_`` allowed only after the first character. The
    leading-character constraint is slightly stricter than Vision §6.4's
    prose (``[a-z0-9_-]+``); it blocks leading ``-`` (which a downstream
    argparse consumer could mis-parse as a flag like ``-rf``) and leading
    ``_`` (conventionally private). Tightens an existing path-safety
    decision (ADR-028's defense-in-depth spirit) rather than introducing
    a new architectural call (Phase 2b K3 / G5).

    Returns ``bool`` — Phase 3a ``Channel.get_or_create`` wraps this and
    raises a domain ``ValueError`` with a vector message on rejection.

    Args:
        name: Channel-name string.

    Returns:
        ``True`` if ``name`` is a syntactically valid channel name;
        ``False`` otherwise.
    """
    return _CHANNEL_NAME_RE.fullmatch(name) is not None


# ──────────────────────────────────────────────────────────────
# Atomic-rename write / defensive read / filename-only listing
# (Phase 2c — Vision §3.3 / §3.6 / §9.4 / Kernel L6)
# ──────────────────────────────────────────────────────────────


_LOGGER = logging.getLogger("letterbox.protocol")

# Module-level dedupe set for novel malformed-filename WARN logs (K6,
# ADR-028 "logged at WARN if novel"). ``set.add`` is GIL-atomic so no
# lock is needed for the dedupe semantics we want; the set is cleared
# only by process restart, matching the "novel within this run" framing.
# First module-level mutable state in protocol.py — every prior bit of
# module-level state has been an immutable frozenset.
_WARNED_BAD_NAMES: set[str] = set()


@dataclass(frozen=True)
class ParseError:
    """Sum-type return from :func:`read_message` for payload failures.

    NOT an :class:`Exception` subclass — this is a *return value*. The
    caller pattern-matches via ``isinstance(result, ParseError)`` on the
    ``Message | ParseError`` union. Inheriting from ``Exception`` would
    let ``raise ParseError(...)`` compile and silently drift the union
    semantics; the load-bearing intent of ADR-020 + Vision §3.6 is that
    a payload error is a *value the agent receives*, not an exception
    the system swallows.

    Two canonical ``reason`` prefixes are used by v1:

    * ``"oversized"`` — file >5 MB; the limit is fixed (no detail).
    * ``"malformed_json: <ValueError message>"`` — JSON parse or schema
      check failed.

    The file is NEVER deleted on a ``ParseError`` — Kernel L8 Ironclad
    Invariant: preserve corrupted content for postmortem.
    """

    path: Path
    reason: str


def write_message(
    channel_dir: Path,
    msg: Message,
    *,
    fsync: bool = False,
) -> Path:
    """Atomically write ``msg`` to ``channel_dir``.

    Writes to a sibling ``msg-*.json.tmp`` then ``os.replace`` to the
    final ``msg-*.json`` name (Vision §3.3 / Cross-Cutting §13.4 /
    Kernel L6). Readers only ever see a complete file, never the
    in-flight ``.tmp``.

    The caller is responsible for building ``msg`` with ``msg.id`` set
    to the filename stem (no ``.json``). The function writes to
    ``channel_dir / f"{msg.id}.json"`` — this contract keeps the chain
    ``make_message_filename() -> new_message(id=stem) -> write_message()``
    visible in one block of code (K1). ``write_message`` never mutates
    ``msg`` and never generates a filename itself.

    The temp suffix is ``.json.tmp`` (note order — NOT ``.tmp.json``),
    so the strict ``msg-*.json`` validator regex structurally excludes
    in-flight files (G4 / Vision §3.6). Reversing the suffix order
    would allow ``list_messages`` to return mid-write garbage.

    Args:
        channel_dir: Directory the message lands in (e.g.,
            ``~/.letterbox/channels/01/``). Must exist; ``write_message``
            does not create it (``Channel.get_or_create`` in Phase 3a
            owns directory creation).
        msg: A populated :class:`Message`. ``msg.id`` is the filename
            stem.
        fsync: If ``True``, fsync the data file AND the parent directory
            after the rename so a crash after the call returns cannot
            lose the message (G5). Default ``False`` matches Vision §9.4
            (no fsync by default; POSIX rename semantics carry durability
            for the common case). The durability seam is exposed only as
            this direct keyword argument — there is no CLI flag and no
            config consultation (K3 — no global state).

    Returns:
        The final :class:`Path` to the renamed ``msg-*.json`` file.

    Raises:
        MessageTooLarge: If :func:`to_json_bytes` rejects the payload
            (propagated; no ``.tmp`` file is created when the body is
            oversized — the 2a serialiser short-circuits before any
            disk I/O).
        OSError: For underlying filesystem failures (permission, ENOSPC,
            ENOENT on ``channel_dir``, etc.). On a failure between
            opening the ``.tmp`` and the ``os.replace``, the orphan
            ``.tmp`` is left behind — Phase 2d's startup-only reaper
            deletes ``.tmp`` files older than 1h. No defensive
            ``finally`` unlink lives here; that would fight the reaper
            contract (K2).
    """
    # Encode FIRST — to_json_bytes raises MessageTooLarge before any
    # disk I/O when the payload would exceed the 5 MB ceiling (2a
    # contract). No .tmp file is created in that case.
    encoded = to_json_bytes(msg)

    final_path = channel_dir / f"{msg.id}.json"
    tmp_path = channel_dir / f"{msg.id}.json.tmp"

    # Write the entire payload in one open/close. Binary mode because
    # to_json_bytes already produced UTF-8 bytes.
    with open(tmp_path, "wb") as fp:
        fp.write(encoded)
        if fsync:
            fp.flush()
            os.fsync(fp.fileno())

    # os.replace (vs os.rename) is a no-op difference on POSIX but
    # gives correct overwrite semantics on Windows. UUID4 32-hex
    # filenames make a collision effectively impossible (ADR-027), so
    # we don't pre-check final_path.exists() — that would race with a
    # concurrent process and add noise against a scenario ADR-027
    # declares impossible (G1).
    os.replace(tmp_path, final_path)

    if fsync:
        # Parent-directory fsync AFTER rename so the name->inode
        # binding is durable across a crash. POSIX guarantees rename
        # is atomic for *visibility*, but durability across a crash
        # requires fsync'ing the directory (K3). Skipping this would
        # mean a crash 50 ms after write_message returns could leave
        # the rename in the page cache only.
        dirfd = os.open(str(channel_dir), os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)

    return final_path


def read_message(path: Path) -> Message | ParseError:
    """Defensively read a message file.

    Returns :class:`Message` on success or :class:`ParseError` on any
    payload failure. The file is left in place even on ``ParseError`` —
    Kernel L8 Ironclad Invariant: recovery must be reversible. The
    higher layers (``check_messages`` in Phase 7c) translate the
    ``ParseError`` into a JSON wire envelope with ``body: null`` and a
    ``parse_error`` field so the agent learns of the failure without
    losing the data point.

    The function ``stat``\\s the file *before* reading bytes — a 5 GB
    adversarial input costs one syscall, not 5 GB of allocation (G3).
    The 2a serialiser already prevents a letterbox-generated message
    from exceeding the cap; this read-side check defends against
    hand-edited files or files from a different schema version.

    Args:
        path: Path to a file expected to be ``msg-*.json``. The function
            does not validate the filename shape; :func:`list_messages`
            and higher layers (watcher, channel) already filter.

    Returns:
        :class:`Message` if the file parses cleanly; otherwise
        :class:`ParseError` with one of:

        * ``ParseError(path, "oversized")`` — file size >5 MB.
        * ``ParseError(path, f"malformed_json: {detail}")`` — JSON
          parse or schema check failed (any :class:`ValueError` from
          :func:`from_json_bytes`).

    Raises:
        FileNotFoundError: If the file does not exist (raced with
            ``letterbox prune`` or a manual ``rm`` between
            :func:`list_messages` and :func:`read_message`). This is
            the only exception path — consumers (``check_messages``,
            ``Channel.list_unread``) catch and skip at their layer
            (K4 / G6). Folding ``FileNotFoundError`` into
            ``ParseError("not_found", path)`` would conflate "missing"
            with "broken" — two different remediations.
    """
    # G3 — stat before read_bytes so an adversarial multi-GB file
    # costs one syscall, not multi-GB of allocation.
    if path.stat().st_size > MAX_BODY_BYTES:
        return ParseError(path=path, reason="oversized")

    data = path.read_bytes()
    try:
        return from_json_bytes(data)
    except ValueError as exc:
        return ParseError(path=path, reason=f"malformed_json: {exc}")


def list_messages(
    channel_dir: Path,
    since: str | None = None,
) -> list[Path]:
    """Enumerate valid message files in ``channel_dir``.

    Filename-only — no JSON parse, no ``stat`` per entry, no mtime read
    (K5 / Vision §9.4 default). Iterates via :func:`os.scandir`, filters
    via :func:`is_valid_message_filename` (which structurally excludes
    ``.tmp`` files because the strict regex anchors on ``\\.json$``),
    sorts lexically, and optionally trims to entries strictly greater
    than ``since``.

    By the microsecond-precision filename invariant (Vision §3.2),
    lexical sort equals chronological sort, so no mtime is needed.
    mtime is fragile under ``cp -r``, syncthing, NFS skew, etc. —
    sorting by filename is the §3.2 / ADR-017 canonical choice.

    Novel malformed filenames are logged at ``WARN`` level once per
    process (ADR-028 / K6). Subsequent encounters of the same name are
    silent; this avoids flooding the log if a stale ``.broken`` file
    persists.

    Args:
        channel_dir: Directory to enumerate.
        since: Optional filename string (e.g.
            ``"msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6f.json"``)
            used as a **strictly greater** cursor — entries with
            ``entry.name <= since`` are skipped (G8). ``None`` returns
            all matching entries. The cursor is a *filename*, not a
            bare message id; the channel layer (Phase 3c) translates a
            ``high_water_mark`` to/from a filename as needed.

    Returns:
        A list of :class:`Path` objects in ascending lexical order
        (equivalent to chronological order per §3.2). Empty list if
        no entries match.

    Raises:
        FileNotFoundError: If ``channel_dir`` does not exist
            (consistent with :func:`os.scandir` semantics). Higher
            layers create the directory eagerly via
            ``Channel.get_or_create`` (Phase 3a).
    """
    return [
        channel_dir / name
        for name in sorted(_scan_valid_names(channel_dir, since))
    ]


def _scan_valid_names(
    channel_dir: Path,
    since: str | None = None,
) -> list[str]:
    """Scan ``channel_dir`` once, returning valid message filenames (unsorted).

    The single home of the enumeration invariants shared by
    :func:`list_messages` and the channel-layer hot readers
    (``Channel.list_unread`` / ``Channel.latest_unread``): the
    :func:`os.scandir` walk, the ``is_dir()`` skip, the
    :func:`is_valid_message_filename` validation, the ADR-028 warn-once
    side effect, the ``.tmp`` structural exclusion (via the ``\\.json$``
    regex anchor), and the optional strict-greater ``since`` cursor
    filter. Validation is a side effect of *scanning* — every entry is
    checked and warned-on-once regardless of whether the caller keeps
    it — so no early-termination cleverness is permitted here (it would
    break ADR-028 warn-once, which must see every entry).

    Returns bare ``entry.name`` strings (not :class:`Path` objects) so
    that lazy directional selection over the corpus operates on cheap
    ``str`` comparisons; callers that want ``Path`` objects reconstruct
    them via ``channel_dir / name`` (equivalent to ``Path(entry.path)``
    since ``entry.path == os.path.join(channel_dir, name)``).

    Args:
        channel_dir: Directory to enumerate.
        since: Optional filename string used as a **strictly greater**
            cursor — entries with ``name <= since`` are skipped (G8).
            ``None`` returns all matching entries. The cursor is a
            *filename*, not a bare message id.

    Returns:
        A list of valid message filenames in scan (arbitrary) order.
        Empty list if no entries match.

    Raises:
        FileNotFoundError: If ``channel_dir`` does not exist (consistent
            with :func:`os.scandir` semantics).
    """
    out: list[str] = []
    with os.scandir(channel_dir) as it:
        for entry in it:
            # Skip directories — only files can be messages. We never
            # recurse; multi-level layouts (e.g., ``.read/``) are owned
            # by the channel layer, not the protocol layer. N4 — this
            # skip stays BEFORE validation so ``.read/``/``cold/`` never
            # emit spurious ADR-028 warnings.
            if entry.is_dir():
                continue
            name = entry.name
            if not is_valid_message_filename(name):
                # ADR-028 "logged at WARN if novel". Dedupe across the
                # life of the process (K6 — set.add is GIL-atomic, so
                # no lock is needed for the semantics we want).
                if name not in _WARNED_BAD_NAMES:
                    _WARNED_BAD_NAMES.add(name)
                    _LOGGER.warning(
                        "ignoring file %r in channel dir %s: "
                        "does not match strict message-filename shape "
                        "(ADR-028). File left in place.",
                        name,
                        channel_dir,
                    )
                continue
            if since is not None and name <= since:
                continue
            out.append(name)
    return out


def _iter_names_ascending(names: list[str]) -> Iterator[str]:
    """Yield ``names`` in ascending lexical order, lazily via a min-heap.

    ``heapify`` is O(N) (≈ the unavoidable scan cost); each ``heappop`` is
    O(log N). A reader that stops after *k* pops pays O(N + k·log N), not
    the O(N·log N) of a full sort. Ascending lexical order equals
    chronological order per §3.2 / ADR-017.

    Args:
        names: The filenames to iterate (consumed into a private heap;
            the caller's list is copied, not mutated).

    Yields:
        Filenames in ascending lexical order.
    """
    heap = list(names)
    heapq.heapify(heap)
    while heap:
        yield heapq.heappop(heap)


class _Rev:
    """Reverse-comparator wrapper: inverts ``<`` so a min-heap pops max-first.

    Wrapping each name in ``_Rev`` turns :mod:`heapq`'s min-heap into a
    max-heap over the underlying names without a negated key (names are
    strings, not numbers) — the public-API-only idiom for lazy descending
    selection on a frozen artifact. ``__slots__`` keeps the per-name
    wrapper allocation minimal at 10k corpus sizes.
    """

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __lt__(self, other: "_Rev") -> bool:
        return self.name > other.name


def _iter_names_descending(names: list[str]) -> Iterator[str]:
    """Yield ``names`` in descending lexical (newest-first) order, lazily.

    The reverse of :func:`_iter_names_ascending`, via the :class:`_Rev`
    wrapper so :mod:`heapq`'s min-heap pops the maximum name first. Same
    O(N + k·log N) cost profile: a reader that stops after *k* pops (e.g.
    ``latest_unread`` stopping at the first unread peer) never pays the
    full-corpus sort.

    Args:
        names: The filenames to iterate.

    Yields:
        Filenames in descending lexical order (newest first).
    """
    heap = [_Rev(n) for n in names]
    heapq.heapify(heap)
    while heap:
        yield heapq.heappop(heap).name


# ──────────────────────────────────────────────────────────────
# Startup-only ``.tmp`` reaper (Phase 2d — Vision §3.6 / ADR-016 / Kernel L6)
# ──────────────────────────────────────────────────────────────


def reap_orphan_tmp(
    channel_dir: Path,
    mtime_threshold_seconds: float = 3600.0,
) -> int:
    """Sweep stale ``.json.tmp`` files from ``channel_dir`` (ADR-016).

    The lifecycle counterpart to :func:`write_message`'s atomic-rename
    contract: 2c's writer deliberately does NOT clean up orphan ``.tmp``
    files on crash (no defensive ``finally`` unlink — that would fight
    this reaper). 2d closes the loop: writes are atomic AND eventually
    swept, satisfying Kernel L6 (Ironclad Invariant) jointly across the
    two phases.

    **Designed to run at letterbox startup only** (Phase 8a launcher
    loops over ``~/.letterbox/channels/*`` and calls this on each).
    The 1-hour default threshold is the ADR-016 safety buffer — short
    enough that orphans don't accumulate across long-running channels,
    long enough that a slow legitimate write (``write_message`` keeps
    a ``.tmp`` open for microseconds before the atomic rename) is
    never racially unlinked.

    Per-file ``OSError`` on ``unlink`` is logged at WARN and the sweep
    continues to the next entry (K4). Startup must not block on a single
    locked or read-only file; the unreaped file is retried on next
    startup, eventually ages out, eventually reaps.

    Filter is ``entry.name.endswith(".json.tmp")`` (K2 — exact end-anchor):
    this matches every ``.tmp`` :func:`write_message` produces and ignores
    foreign suffixes like ``.tmp.json`` (G2 — those are not the writer's
    output, so leave for human inspection). A single ``time.time()``
    snapshot at function entry (K5) ensures all entries are compared
    against the same "now" — a slow sweep cannot drift mid-loop.

    Args:
        channel_dir: Directory to sweep. One channel per call; the
            launcher (Phase 8a) is the only production caller and owns
            the cross-channel loop.
        mtime_threshold_seconds: Files with ``mtime`` more than this
            many seconds before the call started are deleted. Default
            ``3600.0`` (1 hour, ADR-016). Tests may pass small positive
            values (e.g. ``0.1``) to observe the boundary fast. Must be
            strictly positive — zero or negative would race with active
            writes (K3).

    Returns:
        Count of ``.json.tmp`` files successfully unlinked. Per-file
        failures (caught and logged at WARN) are NOT counted; the
        return value reflects real successful deletions only.

    Raises:
        FileNotFoundError: If ``channel_dir`` does not exist (mirrors
            :func:`list_messages` / :func:`os.scandir` semantics — K4
            of p2c).
        ValueError: If ``mtime_threshold_seconds <= 0`` (K3 — defensive
            against race-with-writer).
    """
    if mtime_threshold_seconds <= 0:
        raise ValueError(
            f"mtime_threshold_seconds must be > 0, got {mtime_threshold_seconds}. "
            f"A zero or negative threshold would race with in-flight writes "
            f"(write_message keeps a .json.tmp open for microseconds before "
            f"atomic rename; ADR-016)."
        )

    # K5 — single snapshot of "now" so all entries in this sweep are
    # compared against the same instant. Avoids the race where a
    # borderline file is classified differently depending on its
    # position in the scandir order.
    now = time.time()
    deleted = 0
    with os.scandir(channel_dir) as it:
        for entry in it:
            # Skip directories — only files can be orphan .tmp's. We
            # never recurse; ``.read/`` and any other subdirs are owned
            # by the channel layer (Phase 3b), not the protocol layer.
            if entry.is_dir():
                continue
            # K2 — strict end-anchor: matches every .json.tmp the writer
            # can produce; ignores foreign suffixes like .tmp.json (G2).
            if not entry.name.endswith(".json.tmp"):
                continue
            # G1 — DirEntry.stat() is cached per entry; one syscall, not
            # two. follow_symlinks=False because symlinks in a channel
            # dir are user-introduced anomalies, not letterbox state.
            mtime = entry.stat(follow_symlinks=False).st_mtime
            if now - mtime <= mtime_threshold_seconds:
                continue
            try:
                os.unlink(entry.path)
            except OSError as exc:
                # K4 — one stubborn file (locked by antivirus, permission
                # change, vanished mid-sweep) must not block the rest of
                # the sweep. Launcher startup relies on partial progress;
                # the unreaped file retries on next startup.
                _LOGGER.warning(
                    "failed to unlink orphan .tmp file %s: %s. "
                    "Will retry on next startup.",
                    entry.path,
                    exc,
                )
                continue
            _LOGGER.info(
                "reaped orphan .tmp file %s (age %.1fs, threshold %.1fs)",
                entry.path,
                now - mtime,
                mtime_threshold_seconds,
            )
            deleted += 1
    return deleted

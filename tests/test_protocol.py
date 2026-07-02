"""Tests for Phase 2a — Message dataclass + JSON serialization.

Behavioral TDD per the plan's §9.1 grouping. No mocks — every test runs
synchronously against real objects. Phase 2a/2b classes use no fixtures
from ``conftest.py`` (those layers are pure: no I/O, no env reads, no
async). Phase 2c classes take ``tmp_letterbox_home`` (the channel-dir
cradle) — first protocol-test use of the fixture; satisfies Wiring
Ledger entry-001's 2c half.

Test classes:

* ``TestMessageShape`` — frozen dataclasses, public constants.
* ``TestFactoryNewMessage`` — defaults, UTC enforcement, mutable-default
  hygiene, tuple normalisation, empty-sender rejection.
* ``TestSerialization`` — round-trip equality + byte-equality, sorted
  keys, UTF-8 sovereignty, strict parse rejections.
* ``TestSizeCeiling`` — 5 MB ceiling on serialized bytes; UTF-8 byte
  count (not codepoint count); pre-serialize short-circuit performance.
* ``TestUtcDiscipline`` — factory rejects naive/non-UTC, timestamp ends
  ``+00:00``, process-TZ flip doesn't perturb output.
* ``TestSchemaVersion`` — schema_version=1 enforced on parse.
* ``TestReservedFields`` — ``address`` / ``metadata.encryption`` /
  ``metadata.ext`` are written by the factory and preserved verbatim.
* ``TestProperty`` — hypothesis round-trip properties (small bodies).
* ``TestChannelNameValidation`` / ``TestFilenameValidation`` /
  ``TestFilenameGeneration`` — Phase 2b predicates + generator.
* ``TestWriteMessage`` / ``TestReadMessage`` / ``TestListMessages`` —
  Phase 2c atomic-rename, defensive read, filename-only listing.
* ``TestReapOrphanTmp`` — Phase 2d startup-only ``.tmp`` reaper
  (ADR-016 / Vision §3.6 / Kernel L6 lifecycle counterpart to 2c).
"""
from __future__ import annotations

import dataclasses
import json
import logging
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

import letterbox.protocol as protocol
from letterbox.protocol import (
    MAX_BODY_BYTES,
    SCHEMA_VERSION,
    Message,
    Metadata,
    MessageTooLarge,
    ParseError,
    from_json_bytes,
    is_valid_channel_name,
    is_valid_message_filename,
    list_messages,
    make_message_filename,
    new_message,
    read_message,
    reap_orphan_tmp,
    to_json_bytes,
    write_message,
)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def _make(**overrides):
    """Build a Message via the factory with sensible defaults."""
    kwargs: dict = {
        "id": "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6f",
        "channel": "01",
        "instance_id": "lb-20260527T143000Z-7f3a9c",
        "sender": "claude-a",
        "body": "Sveiki",
    }
    kwargs.update(overrides)
    return new_message(**kwargs)


# ──────────────────────────────────────────────────────────────
# TestMessageShape
# ──────────────────────────────────────────────────────────────


class TestMessageShape:
    """The Message + Metadata dataclasses have the documented shape."""

    def test_message_is_frozen(self) -> None:
        msg = _make()
        with pytest.raises(dataclasses.FrozenInstanceError):
            msg.body = "mutated"  # type: ignore[misc]

    def test_metadata_is_frozen(self) -> None:
        meta = Metadata(encryption=None, ext={})
        with pytest.raises(dataclasses.FrozenInstanceError):
            meta.ext = {"foo": "bar"}  # type: ignore[misc]

    def test_message_has_all_expected_fields(self) -> None:
        names = {f.name for f in dataclasses.fields(Message)}
        assert names == {
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

    def test_metadata_has_expected_fields(self) -> None:
        names = {f.name for f in dataclasses.fields(Metadata)}
        assert names == {"encryption", "ext"}

    def test_max_body_bytes_constant(self) -> None:
        assert MAX_BODY_BYTES == 5 * 1024 * 1024 == 5_242_880

    def test_schema_version_constant(self) -> None:
        assert SCHEMA_VERSION == 1

    def test_dataclasses_replace_returns_new_value(self) -> None:
        # Sanity: frozen != immutable transformation. replace() composes cleanly.
        msg = _make(body="original")
        tweaked = dataclasses.replace(msg, body="tweaked")
        assert msg.body == "original"
        assert tweaked.body == "tweaked"
        assert msg is not tweaked


# ──────────────────────────────────────────────────────────────
# TestFactoryNewMessage
# ──────────────────────────────────────────────────────────────


class TestFactoryNewMessage:
    """new_message() fills reserved fields and enforces UTC + empty-sender."""

    def test_defaults_fill_schema_version_address_metadata(self) -> None:
        msg = _make()
        assert msg.schema_version == SCHEMA_VERSION
        assert msg.address == "file://local"
        assert msg.metadata == Metadata(encryption=None, ext={})

    def test_default_timestamp_is_close_to_now(self) -> None:
        before = datetime.now(timezone.utc)
        msg = _make()
        after = datetime.now(timezone.utc)
        parsed = datetime.fromisoformat(msg.timestamp)
        # Allow a 5-second window for slow CI scheduling.
        assert before - timedelta(seconds=5) <= parsed <= after + timedelta(seconds=5)

    def test_explicit_timestamp_formats_with_plus_zero_zero_suffix(self) -> None:
        ts = datetime(2026, 5, 27, 14, 30, 15, 123456, tzinfo=timezone.utc)
        msg = _make(timestamp=ts)
        assert msg.timestamp == "2026-05-27T14:30:15.123456+00:00"

    def test_naive_timestamp_rejected(self) -> None:
        naive = datetime(2026, 5, 27, 14, 30, 15)
        with pytest.raises(ValueError, match=r"(?i)utc"):
            _make(timestamp=naive)

    def test_non_utc_timestamp_rejected(self) -> None:
        eastern = datetime(2026, 5, 27, 14, 30, 15, tzinfo=timezone(timedelta(hours=3)))
        with pytest.raises(ValueError, match=r"(?i)utc"):
            _make(timestamp=eastern)

    def test_zero_offset_alias_for_utc_accepted(self) -> None:
        # timezone(timedelta(0)) is equal to timezone.utc; the factory must accept both.
        ts = datetime(2026, 5, 27, 14, 30, 15, tzinfo=timezone(timedelta(0)))
        msg = _make(timestamp=ts)
        assert msg.timestamp == "2026-05-27T14:30:15+00:00"

    def test_default_ext_is_empty_dict(self) -> None:
        msg = _make()
        assert msg.metadata.ext == {}

    def test_ext_none_yields_independent_empty_dicts(self) -> None:
        # Mutable-default footgun regression: two None-ext calls must NOT share a dict.
        m1 = _make(ext=None)
        m2 = _make(ext=None)
        assert m1.metadata.ext == {}
        assert m2.metadata.ext == {}
        assert m1.metadata.ext is not m2.metadata.ext

    def test_ext_tuple_value_normalised_to_list(self) -> None:
        msg = _make(ext={"path": (1, 2, 3)})
        assert msg.metadata.ext == {"path": [1, 2, 3]}
        assert isinstance(msg.metadata.ext["path"], list)

    def test_ext_nested_tuples_normalised_recursively(self) -> None:
        msg = _make(ext={"outer": {"inner": (4, 5)}, "list": [(6, 7), 8]})
        assert msg.metadata.ext == {"outer": {"inner": [4, 5]}, "list": [[6, 7], 8]}

    def test_recipient_defaults_to_none(self) -> None:
        msg = _make()
        assert msg.recipient is None

    def test_in_reply_to_defaults_to_none(self) -> None:
        msg = _make()
        assert msg.in_reply_to is None

    def test_recipient_can_be_provided(self) -> None:
        msg = _make(recipient="claude-b")
        assert msg.recipient == "claude-b"

    def test_in_reply_to_trusted_blindly(self) -> None:
        # ADR-020: any string is accepted; the factory does not validate the reference.
        msg = _make(in_reply_to="msg-does-not-exist-anywhere")
        assert msg.in_reply_to == "msg-does-not-exist-anywhere"

    def test_empty_sender_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"(?i)sender"):
            _make(sender="")

    def test_no_address_parameter_exposed(self) -> None:
        # The factory must NOT accept an address kwarg — address is server-reserved.
        with pytest.raises(TypeError):
            new_message(  # type: ignore[call-arg]
                id="x",
                channel="01",
                instance_id="lb-x",
                sender="claude-a",
                body="hi",
                address="https://evil.example.com",
            )


# ──────────────────────────────────────────────────────────────
# TestSerialization
# ──────────────────────────────────────────────────────────────


class TestSerialization:
    """to_json_bytes / from_json_bytes round-trip and stay strict on parse."""

    def test_round_trip_equality(self) -> None:
        msg = _make(body="Round-trip test")
        assert from_json_bytes(to_json_bytes(msg)) == msg

    def test_round_trip_byte_equality(self) -> None:
        msg = _make(body="Bytes are bytes", ext={"phase": "2a", "n": 7})
        once = to_json_bytes(msg)
        twice = to_json_bytes(from_json_bytes(once))
        assert once == twice

    def test_output_is_valid_utf8(self) -> None:
        msg = _make(body="café 🚀 шалом")
        to_json_bytes(msg).decode("utf-8")  # raises on failure

    def test_top_level_keys_alphabetical(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        assert list(parsed.keys()) == sorted(parsed.keys())

    def test_metadata_sub_keys_alphabetical(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        assert list(parsed["metadata"].keys()) == ["encryption", "ext"]

    def test_lithuanian_body_no_unicode_escapes(self) -> None:
        body = "Sveiki, šaltibarščiai ąčęėįšųūž"
        msg = _make(body=body)
        out = to_json_bytes(msg).decode("utf-8")
        assert body in out
        assert "\\u" not in out

    def test_cjk_body_no_unicode_escapes(self) -> None:
        body = "こんにちは世界"
        msg = _make(body=body)
        out = to_json_bytes(msg).decode("utf-8")
        assert body in out
        assert "\\u" not in out

    def test_emoji_body_no_unicode_escapes(self) -> None:
        body = "📬✨🚀"
        msg = _make(body=body)
        out = to_json_bytes(msg).decode("utf-8")
        assert body in out
        assert "\\u" not in out

    def test_mixed_unicode_body_round_trips(self) -> None:
        body = "Sveiki + こんにちは + 📬 + ąčęėįšųūž"
        msg = _make(body=body)
        out = to_json_bytes(msg)
        assert from_json_bytes(out).body == body
        assert "\\u" not in out.decode("utf-8")

    def test_control_characters_round_trip(self) -> None:
        body = "tab:\there\nnewline\x01\x02end"
        msg = _make(body=body)
        assert from_json_bytes(to_json_bytes(msg)).body == body

    def test_no_trailing_newline(self) -> None:
        out = to_json_bytes(_make())
        assert not out.endswith(b"\n")

    def test_no_leading_bom(self) -> None:
        out = to_json_bytes(_make())
        assert not out.startswith(b"\xef\xbb\xbf")

    def test_compact_no_indent(self) -> None:
        # No spaces around ',' or ':' separators (the default of indent=None plus
        # explicit compact separators would both yield this; we just assert absence).
        out = to_json_bytes(_make()).decode("utf-8")
        assert ", " not in out
        assert ": " not in out

    def test_unknown_top_level_field_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["signature"] = "deadbeef"
        bytes_with_extra = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
        with pytest.raises(ValueError, match="signature"):
            from_json_bytes(bytes_with_extra)

    def test_missing_recipient_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed.pop("recipient")
        with pytest.raises(ValueError, match="recipient"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_missing_metadata_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed.pop("metadata")
        with pytest.raises(ValueError, match="metadata"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_missing_metadata_encryption_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["metadata"].pop("encryption")
        with pytest.raises(ValueError, match="encryption"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_missing_metadata_ext_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["metadata"].pop("ext")
        with pytest.raises(ValueError, match="ext"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_metadata_string_version_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["schema_version"] = "1"
        with pytest.raises(ValueError, match="schema_version"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_bool_schema_version_rejected(self) -> None:
        # In Python, ``isinstance(True, int)`` is True — guard explicitly so
        # ``"schema_version": true`` on the wire does not slip past the int check.
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["schema_version"] = True
        with pytest.raises(ValueError, match="schema_version"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_json_array_root_rejected(self) -> None:
        with pytest.raises(ValueError, match="root"):
            from_json_bytes(b"[1, 2, 3]")

    def test_non_string_recipient_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["recipient"] = 42
        with pytest.raises(ValueError, match="recipient"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_non_string_in_reply_to_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["in_reply_to"] = [1, 2]
        with pytest.raises(ValueError, match="in_reply_to"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_metadata_ext_must_be_a_dict_not_list(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["metadata"]["ext"] = [1, 2, 3]
        with pytest.raises(ValueError, match="ext"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_metadata_encryption_dict_parses_for_future_use(self) -> None:
        # Forward-compat hook: an "encryption: {...}" object in the wire parses
        # successfully even though v1 always writes null. Letterbox doesn't yet
        # interpret encryption metadata, but the parser must not refuse it.
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["metadata"]["encryption"] = {"algo": "future-scheme", "kid": "k1"}
        bytes_in = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
        out = from_json_bytes(bytes_in)
        assert out.metadata.encryption == {"algo": "future-scheme", "kid": "k1"}

    def test_extra_metadata_key_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["metadata"]["signature"] = "abc"
        with pytest.raises(ValueError, match="signature"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_empty_sender_on_wire_rejected(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["sender"] = ""
        with pytest.raises(ValueError, match="sender"):
            from_json_bytes(json.dumps(parsed, ensure_ascii=False).encode("utf-8"))

    def test_from_json_bytes_accepts_bytearray(self) -> None:
        # Latitude in §14: accept any bytes-like the json module digests.
        msg = _make()
        as_bytearray = bytearray(to_json_bytes(msg))
        assert from_json_bytes(as_bytearray) == msg

    def test_invalid_json_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            from_json_bytes(b"{ this is not json")

    def test_recipient_can_be_null_on_wire(self) -> None:
        msg = _make(recipient=None)
        parsed = json.loads(to_json_bytes(msg))
        assert "recipient" in parsed
        assert parsed["recipient"] is None
        assert from_json_bytes(to_json_bytes(msg)).recipient is None

    def test_in_reply_to_can_be_null_on_wire(self) -> None:
        msg = _make(in_reply_to=None)
        parsed = json.loads(to_json_bytes(msg))
        assert "in_reply_to" in parsed
        assert parsed["in_reply_to"] is None


# ──────────────────────────────────────────────────────────────
# TestSizeCeiling
# ──────────────────────────────────────────────────────────────


class TestSizeCeiling:
    """5 MB ceiling on the *serialized* output (ADR-014)."""

    def test_5mb_ascii_body_rejected(self) -> None:
        # 5 MB of ASCII plus the envelope clears the 5 MB ceiling.
        body = "a" * MAX_BODY_BYTES
        msg = _make(body=body)
        with pytest.raises(MessageTooLarge, match=str(MAX_BODY_BYTES)):
            to_json_bytes(msg)

    def test_just_under_5mb_body_serialises(self) -> None:
        # Leave a generous envelope buffer (1 KB > our framing of ~250 bytes).
        body = "a" * (MAX_BODY_BYTES - 1024)
        msg = _make(body=body)
        out = to_json_bytes(msg)
        assert len(out) <= MAX_BODY_BYTES

    def test_5mb_plus_one_body_rejected(self) -> None:
        body = "a" * (MAX_BODY_BYTES + 1)
        msg = _make(body=body)
        with pytest.raises(MessageTooLarge, match=str(MAX_BODY_BYTES)):
            to_json_bytes(msg)

    def test_utf8_byte_count_not_codepoint_count(self) -> None:
        # Gotcha 4: a body whose len() is < 5 MB but encoded UTF-8 is > 5 MB
        # must still be rejected. "ą" is 2 UTF-8 bytes.
        body = "ą" * 3_000_000  # len = 3M; encoded = 6M > 5M.
        assert len(body) < MAX_BODY_BYTES
        assert len(body.encode("utf-8")) > MAX_BODY_BYTES
        msg = _make(body=body)
        with pytest.raises(MessageTooLarge):
            to_json_bytes(msg)

    def test_pre_serialise_short_circuit_is_fast(self) -> None:
        # K2: a 10 MB body must be rejected without serialising 10 MB of JSON
        # first. We assert wall-time for 100 rejections — a real serialize
        # would take ~10s+; the short-circuit completes in well under 1s.
        body = "a" * 10_000_000
        msg = _make(body=body)
        iterations = 100
        start = time.perf_counter()
        for _ in range(iterations):
            with pytest.raises(MessageTooLarge):
                to_json_bytes(msg)
        elapsed = time.perf_counter() - start
        # 2s envelope for xdist + CI load. A non-short-circuited path would
        # take ~10s+ here; this is the regression guard, not a tight budget.
        assert elapsed < 2.0, f"{iterations} rejections took {elapsed:.3f}s — short-circuit missing?"

    def test_error_message_names_the_field_and_limit(self) -> None:
        body = "a" * (MAX_BODY_BYTES + 1)
        msg = _make(body=body)
        with pytest.raises(MessageTooLarge) as exc_info:
            to_json_bytes(msg)
        text = str(exc_info.value)
        assert str(MAX_BODY_BYTES) in text or "5 MB" in text or "5242880" in text
        assert "body" in text.lower()


# ──────────────────────────────────────────────────────────────
# TestUtcDiscipline
# ──────────────────────────────────────────────────────────────


class TestUtcDiscipline:
    """ADR-015 — UTC everywhere; the factory enforces it on the way in."""

    def test_factory_timestamp_always_ends_plus_zero_zero(self) -> None:
        msg = _make()
        assert msg.timestamp.endswith("+00:00")

    def test_wire_timestamp_always_ends_plus_zero_zero(self) -> None:
        out = to_json_bytes(_make())
        parsed = json.loads(out)
        assert parsed["timestamp"].endswith("+00:00")

    def test_parsed_back_offset_is_zero(self) -> None:
        msg = _make()
        parsed = datetime.fromisoformat(msg.timestamp)
        assert parsed.utcoffset() == timedelta(0)

    def test_factory_consults_utc_not_local_after_tz_flip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if not hasattr(time, "tzset"):
            pytest.skip("POSIX-only (no time.tzset on this platform)")
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        time.tzset()
        try:
            msg = _make()
            assert msg.timestamp.endswith("+00:00")
            parsed = datetime.fromisoformat(msg.timestamp)
            assert parsed.utcoffset() == timedelta(0)
        finally:
            # Restore C library state so we don't pollute later tests in the
            # same worker. monkeypatch handles the env var; we re-call tzset
            # ourselves once the next test loads (the env will already be
            # back by then since monkeypatch teardown runs after this fn).
            pass

    def test_microsecond_ordering_preserved(self) -> None:
        # Three timestamps 1 µs apart — lexical sort matches chronological.
        base = datetime(2026, 5, 27, 14, 30, 15, 100000, tzinfo=timezone.utc)
        msgs = [
            _make(timestamp=base + timedelta(microseconds=i)) for i in range(3)
        ]
        timestamps = [m.timestamp for m in msgs]
        assert sorted(timestamps) == timestamps


# ──────────────────────────────────────────────────────────────
# TestSchemaVersion
# ──────────────────────────────────────────────────────────────


class TestSchemaVersion:
    """Parser fails loudly on unsupported schema_version (Vision §3.2)."""

    def test_schema_version_2_rejected_mentions_both_versions(self) -> None:
        msg = _make()
        parsed = json.loads(to_json_bytes(msg))
        parsed["schema_version"] = 2
        bytes_in = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
        with pytest.raises(ValueError) as exc_info:
            from_json_bytes(bytes_in)
        text = str(exc_info.value)
        assert "2" in text
        assert "1" in text

    def test_factory_always_writes_schema_version_1(self) -> None:
        msg = _make()
        assert msg.schema_version == 1
        out = json.loads(to_json_bytes(msg))
        assert out["schema_version"] == 1


# ──────────────────────────────────────────────────────────────
# TestReservedFields  (Vision §1.2 — Retrofit-Test wiring)
# ──────────────────────────────────────────────────────────────


class TestReservedFields:
    """Reserved slots (address, encryption, ext) are wired in v1."""

    def test_address_always_file_local(self) -> None:
        msg = _make()
        assert msg.address == "file://local"

    def test_encryption_always_null(self) -> None:
        msg = _make()
        assert msg.metadata.encryption is None

    def test_ext_default_is_empty(self) -> None:
        msg = _make()
        assert msg.metadata.ext == {}

    def test_ext_survives_arbitrary_json_shape(self) -> None:
        ext = {
            "string": "value",
            "int": 42,
            "float": 3.14,
            "bool_true": True,
            "bool_false": False,
            "null": None,
            "nested_dict": {"deep": {"deeper": "leaf"}},
            "list_of_mixed": [1, "two", 3.0, True, None, {"k": "v"}],
        }
        msg = _make(ext=ext)
        round_tripped = from_json_bytes(to_json_bytes(msg))
        assert round_tripped.metadata.ext == ext

    def test_ext_unrecognised_keys_preserved(self) -> None:
        # Vision §3.2: "Letterbox preserves but never interprets metadata.ext".
        ext = {"phase": "2a", "future_field": [1, 2, 3], "x-vendor": "planning-loop"}
        msg = _make(ext=ext)
        assert from_json_bytes(to_json_bytes(msg)).metadata.ext == ext

    def test_wire_metadata_has_encryption_and_ext_keys(self) -> None:
        out = json.loads(to_json_bytes(_make()))
        assert set(out["metadata"].keys()) == {"encryption", "ext"}
        assert out["metadata"]["encryption"] is None
        assert out["metadata"]["ext"] == {}


# ──────────────────────────────────────────────────────────────
# TestProperty  (hypothesis)
# ──────────────────────────────────────────────────────────────


# Use a strategy that excludes surrogate codepoints (U+D800-U+DFFF) — those
# are invalid in JSON strings and unrepresentable in UTF-8. ``st.text()``
# without ``alphabet`` *does* generate surrogates by default, which is what
# we exclude here.
_BODY_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    max_size=1024,
)


class TestProperty:
    """Property-based round-trip checks on arbitrary small bodies."""

    @given(body=_BODY_TEXT)
    @settings(max_examples=200, deadline=2000)
    def test_round_trip_equality(self, body: str) -> None:
        msg = _make(body=body)
        assert from_json_bytes(to_json_bytes(msg)) == msg

    @given(body=_BODY_TEXT)
    @settings(max_examples=200, deadline=2000)
    def test_output_is_valid_json(self, body: str) -> None:
        msg = _make(body=body)
        json.loads(to_json_bytes(msg))  # raises on failure

    @given(body=_BODY_TEXT)
    @settings(max_examples=200, deadline=2000)
    def test_output_is_valid_utf8(self, body: str) -> None:
        msg = _make(body=body)
        to_json_bytes(msg).decode("utf-8")  # raises on failure

    @given(body=_BODY_TEXT)
    @settings(max_examples=200, deadline=2000)
    def test_byte_level_idempotence(self, body: str) -> None:
        msg = _make(body=body)
        once = to_json_bytes(msg)
        twice = to_json_bytes(from_json_bytes(once))
        assert once == twice


# ──────────────────────────────────────────────────────────────
# TestChannelNameValidation  (Phase 2b — Vision §6.4 + K3)
# ──────────────────────────────────────────────────────────────


class TestChannelNameValidation:
    """``is_valid_channel_name`` — Vision §6.4 path safety + Phase 2b K3.

    The plan tightens Vision §6.4's prose (``[a-z0-9_-]+``) to
    ``^[a-z0-9][a-z0-9_-]*$`` — the first-character constraint blocks
    leading ``-`` (which a downstream argparse consumer could misread as
    ``-rf``) and leading ``_`` (conventionally private).
    """

    @pytest.mark.parametrize(
        "name",
        [
            "01",
            "claude-a",
            "debate_01",
            "a",
            "0",
            "c-h-a-n",
        ],
    )
    def test_accepts_valid_channel_names(self, name: str) -> None:
        assert is_valid_channel_name(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "../etc",       # path traversal
            "foo/bar",      # path separator
            "FOO",          # uppercase
            "foo bar",      # whitespace
            "",             # empty
            "-rf",          # K3 — leading dash
            "_internal",    # K3 — leading underscore
            "claude.a",     # dot
            "lt-ąčę",       # non-ASCII
        ],
    )
    def test_rejects_invalid_channel_names(self, name: str) -> None:
        assert is_valid_channel_name(name) is False


# ──────────────────────────────────────────────────────────────
# TestFilenameValidation  (Phase 2b — ADR-028 + K2 + G3)
# ──────────────────────────────────────────────────────────────


# A canonical 2b-format filename used across the validation tests. The
# 32-char uuid hex matches the regex; the µs-resolved timestamp matches the
# strftime("%Y%m%dT%H%M%S%f") shape. Also used as the round-trip input for
# the suffix-append and prefix-prepend defenses (K2).
_VALID_FILENAME = "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json"


class TestFilenameValidation:
    """``is_valid_message_filename`` — ADR-028 strict regex + K2/G3 defenses.

    The validator is the load-bearing defense against
    template-injection-via-filename (Vision §3.2 / §6.4): the watcher and
    every reader rejects malformed names BEFORE substituting them into the
    notification template, so a peer that writes ``msg-123-$(rm -rf /).json``
    into the channel directory cannot smuggle shell text into a notification.
    """

    def test_accepts_canonical_filename(self) -> None:
        assert is_valid_message_filename(_VALID_FILENAME) is True

    @pytest.mark.parametrize(
        "name",
        [
            "msg-bad.json",                                                                     # missing timestamp + uuid
            "msg-20260527T143015123456-short.json",                                             # uuid too short
            "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5eF.json",                  # uppercase in uuid hex
            "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json.evil",             # K2 — suffix appended
            "msg-20260527T143015123456-૮d8e3a1f2c4b5d6e7f8091a2b3c4d5e.json",                   # non-ASCII in uuid hex tail
            "msg-20260527T143015-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json",                        # missing µs segment
            "msg-123-$(ls).json",                                                               # shell metacharacters
            "../outside.json",                                                                  # path traversal
            "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.JSON",                  # uppercase extension
            "MSG-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json",                  # uppercase prefix
            "",                                                                                 # empty string
            "   ",                                                                              # whitespace-only
        ],
    )
    def test_rejects_malformed_filenames(self, name: str) -> None:
        assert is_valid_message_filename(name) is False

    def test_rejects_suffix_append_k2(self) -> None:
        # K2 — fullmatch on a regex with explicit ``^...$`` anchors. Belt:
        # anchors in the literal; suspender: fullmatch in the call. A name
        # with a valid prefix but extra trailing bytes must reject so that a
        # future deletion of the ``$`` anchor does not silently widen the
        # validator.
        assert is_valid_message_filename(_VALID_FILENAME + ".evil") is False

    def test_rejects_prefix_prepend(self) -> None:
        # Mirror of the K2 defense at the front — explicit ``^`` plus
        # fullmatch rejects any leading garbage even when the tail is
        # canonical.
        assert is_valid_message_filename("evil-" + _VALID_FILENAME) is False

    def test_rejects_unicode_digit_in_timestamp_g3(self) -> None:
        # G3 — Python's ``\d`` matches Unicode decimal digits by default
        # (Gujarati ૪, Arabic-Indic, Devanagari, ...). The implementation
        # uses literal ``[0-9]`` to constrain to ASCII digits. This proof
        # catches any future drift back to ``\d``.
        gujarati_two = "૪"  # ૪ (Gujarati 4)
        name = (
            f"msg-{gujarati_two * 8}T143015123456-"
            f"7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json"
        )
        assert is_valid_message_filename(name) is False

    def test_rejects_uppercase_hex(self) -> None:
        # ``uuid.uuid4().hex`` is always lowercase per the stdlib; the
        # validator regex matches ``[0-9a-f]`` only. If a future Python
        # changed case, the self-validation round-trip test would catch
        # it; this is the explicit complementary assertion.
        upper_hex = "7D8E3A1F2C4B5D6E7F8091A2B3C4D5E6"
        name = f"msg-20260527T143015123456-{upper_hex}.json"
        assert is_valid_message_filename(name) is False


# ──────────────────────────────────────────────────────────────
# TestFilenameGeneration  (Phase 2b — ADR-027 + ADR-015)
# ──────────────────────────────────────────────────────────────


class TestFilenameGeneration:
    """``make_message_filename`` — full UUID4 hex (ADR-027) + UTC (ADR-015).

    Filename shape: ``msg-YYYYMMDDTHHMMSSffffff-{uuid4hex32}.json``.
    Lexical sort equals chronological sort because the timestamp segment
    is fixed-width zero-padded down to microseconds. The 32-char UUID4
    hex tail keeps collision probability negligible at any realistic
    scale (birthday paradox 50% at ~2^64 messages, per ADR-027).
    """

    def test_default_uses_utc_now(self) -> None:
        # No timestamp arg → datetime.now(timezone.utc). The shape is
        # consistent with the validator regex regardless of the exact wall
        # clock; we just check self-consistency here since the µs portion
        # is volatile.
        assert is_valid_message_filename(make_message_filename())

    def test_explicit_timestamp_round_trips(self) -> None:
        ts = datetime(2026, 5, 27, 14, 30, 15, 123456, tzinfo=timezone.utc)
        name = make_message_filename(ts)
        # The timestamp segment is fixed by strftime("%Y%m%dT%H%M%S%f").
        assert name.startswith("msg-20260527T143015123456-")
        assert name.endswith(".json")
        assert is_valid_message_filename(name)

    def test_filename_length_is_63(self) -> None:
        # 4 ("msg-") + 21 (timestamp) + 1 ("-") + 32 (uuid hex) + 5 (".json") = 63.
        assert len(make_message_filename()) == 63

    def test_microsecond_zero_pads_to_six_digits(self) -> None:
        # strftime("%f") always emits 6 digits — even when microsecond=0,
        # the segment is "000000". This differs from datetime.isoformat,
        # which omits ".000000" for whole-second timestamps. The fixed
        # width is what makes filename lexical sort equal chronological
        # sort (G1 — deliberate divergence from the JSON timestamp shape).
        ts = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        name = make_message_filename(ts)
        assert name.startswith("msg-20260527T143015000000-")

    def test_rejects_naive_datetime(self) -> None:
        naive = datetime(2026, 5, 27, 14, 30, 15, 123456)
        with pytest.raises(ValueError, match=r"(?i)utc"):
            make_message_filename(naive)

    def test_rejects_non_utc_datetime(self) -> None:
        pacific = timezone(timedelta(hours=-7))
        ts = datetime(2026, 5, 27, 14, 30, 15, 123456, tzinfo=pacific)
        with pytest.raises(ValueError, match=r"(?i)utc"):
            make_message_filename(ts)

    def test_accepts_zero_offset_alias(self) -> None:
        # _format_utc_timestamp accepts any aware datetime whose
        # utcoffset() == timedelta(0); mirror that here.
        zero = timezone(timedelta(0))
        ts = datetime(2026, 5, 27, 14, 30, 15, 123456, tzinfo=zero)
        assert is_valid_message_filename(make_message_filename(ts))

    def test_microsecond_precision_lexical_sort_is_chronological(self) -> None:
        # 1000 filenames at 1µs increments. Because the timestamp segment
        # is fixed-width and zero-padded, lexical sort must equal generation
        # order. Shuffle the list and re-sort to prove the property is on
        # the strings themselves, not on the original ordering.
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        names = [
            make_message_filename(base + timedelta(microseconds=i))
            for i in range(1000)
        ]
        assert sorted(names) == names
        shuffled = names[:]
        random.Random(0).shuffle(shuffled)
        assert sorted(shuffled) == names

    def test_1m_filenames_have_zero_collisions(self) -> None:
        # ADR-027 — full UUID4 hex = 128 bits of randomness. Birthday-paradox
        # 50% collision at ~2^64 messages; 1M generations must all be
        # distinct. Wall-clock target ~1-2s on a modern laptop; if this ever
        # exceeds 30s on CI, investigate (Gotcha G6) — a casual slow-mark
        # would be a calibration miss, not a fix.
        n = 1_000_000
        names: set[str] = set()
        for _ in range(n):
            names.add(make_message_filename())
        assert len(names) == n

    def test_mtime_independence(self, tmp_path: Path) -> None:
        # The chronological order is encoded in the filename itself
        # (the µs-resolved timestamp segment). Touching mtimes — as
        # ``cp -r`` (without ``-p``), syncthing, rsync, NFS clock skew,
        # or ``tar`` extraction can do — must not perturb sort-by-name.
        # Generate N filenames at known timestamps, create empty files,
        # scramble each mtime to a random Unix-time in [1, 2e9], then
        # assert filename-sort still matches generation order.
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        names = [
            make_message_filename(base + timedelta(microseconds=i))
            for i in range(100)
        ]
        for name in names:
            (tmp_path / name).touch()
        rng = random.Random(1)
        for path in tmp_path.iterdir():
            os.utime(path, (rng.uniform(1.0, 2e9), rng.uniform(1.0, 2e9)))
        listed = sorted(p.name for p in tmp_path.iterdir())
        assert listed == names

    @given(ts=st.datetimes(timezones=st.just(timezone.utc)))
    @settings(max_examples=200, deadline=2000)
    def test_self_validation_roundtrip(self, ts: datetime) -> None:
        # G2 — uuid.uuid4().hex is lowercase per the Python stdlib; the
        # validator regex accepts only [0-9a-f]. This property test
        # catches any drift between generator and validator across the
        # full domain of UTC-aware datetimes hypothesis can construct.
        assert is_valid_message_filename(make_message_filename(ts))


# ──────────────────────────────────────────────────────────────
# Phase 2c — Atomic-rename write / defensive read / listing
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def channel_dir(tmp_letterbox_home: Path) -> Path:
    """Per-test channel directory under ``tmp_letterbox_home``.

    First protocol-test use of ``tmp_letterbox_home`` — satisfies the
    2c half of Wiring Ledger entry-001.
    """
    d = tmp_letterbox_home / "channels" / "test"
    d.mkdir(parents=True)
    return d


@pytest.fixture
def reset_warn_dedupe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset ``protocol._WARNED_BAD_NAMES`` to a fresh empty set per test.

    Without this, the module-level dedupe set bleeds across tests in the
    same pytest process (G7). Tests that assert WARN-emission semantics
    should depend on this fixture; tests that just assert filtering
    behavior can skip it.
    """
    monkeypatch.setattr(protocol, "_WARNED_BAD_NAMES", set())


def _valid_id() -> str:
    """Return a fresh, regex-valid message id (filename stem, no .json).

    Avoids the off-by-one ``_make()`` default id (33 chars, not 32 — see
    Phase 2b IMPLEMENTATION_NOTES). Every 2c test that puts a file on
    disk uses this so the resulting filename passes
    ``is_valid_message_filename``.
    """
    return make_message_filename().removesuffix(".json")


def _make_real(**overrides) -> Message:
    """Build a Message with a regex-valid id (unless overridden)."""
    overrides.setdefault("id", _valid_id())
    return _make(**overrides)


# ──────────────────────────────────────────────────────────────
# TestWriteMessage
# ──────────────────────────────────────────────────────────────


class TestWriteMessage:
    """Atomic-rename write — Vision §3.3, Cross-Cutting §13.4, Kernel L6."""

    def test_roundtrip(self, channel_dir: Path) -> None:
        msg = _make_real()
        path = write_message(channel_dir, msg)
        assert path.read_bytes() == to_json_bytes(msg)
        assert from_json_bytes(path.read_bytes()) == msg

    def test_returns_final_path_not_tmp(self, channel_dir: Path) -> None:
        msg = _make_real()
        path = write_message(channel_dir, msg)
        assert path.name == f"{msg.id}.json"
        assert path == channel_dir / f"{msg.id}.json"
        assert not path.name.endswith(".tmp")

    def test_tmp_not_persisted_after_success(self, channel_dir: Path) -> None:
        msg = _make_real()
        write_message(channel_dir, msg)
        assert not (channel_dir / f"{msg.id}.json.tmp").exists()

    def test_tmp_uses_json_tmp_suffix_order(self, channel_dir: Path) -> None:
        # G4 — the suffix is ``.json.tmp``, not ``.tmp.json``. Reverse
        # order would match the strict ``msg-*.json`` regex and slip
        # mid-write garbage into list_messages results. We assert the
        # order by intercepting os.replace and inspecting the pending
        # tmp path on disk.
        msg = _make_real()
        seen_tmp: dict = {}

        real_replace = os.replace

        def spy_replace(src, dst, /):
            seen_tmp["src"] = Path(src)
            return real_replace(src, dst)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(os, "replace", spy_replace)
            write_message(channel_dir, msg)

        assert seen_tmp["src"].name == f"{msg.id}.json.tmp"
        # And the structural exclusion holds: the tmp name fails the
        # strict regex (anchored on ``\.json$``).
        assert not is_valid_message_filename(seen_tmp["src"].name)

    def test_partial_write_invisibility(
        self,
        monkeypatch: pytest.MonkeyPatch,
        channel_dir: Path,
        reset_warn_dedupe: None,
    ) -> None:
        # THE load-bearing test: simulate os.replace failing AFTER the
        # .tmp is written. The .tmp must persist (for the Phase 2d
        # reaper); the final .json must NOT exist; list_messages must
        # NOT surface the .tmp (structural exclusion via the
        # ``.json$``-anchored regex).
        msg = _make_real()

        def failing_replace(src, dst, /):
            raise RuntimeError("simulated rename failure")

        monkeypatch.setattr(os, "replace", failing_replace)
        with pytest.raises(RuntimeError, match="simulated rename failure"):
            write_message(channel_dir, msg)

        tmp_path = channel_dir / f"{msg.id}.json.tmp"
        final_path = channel_dir / f"{msg.id}.json"
        assert tmp_path.exists(), ".tmp must persist for the Phase 2d reaper"
        assert not final_path.exists(), "final .json must not appear on failure"
        # Structural exclusion: the .tmp does not appear in list output.
        assert list_messages(channel_dir) == []

    def test_concurrent_writers(self, channel_dir: Path) -> None:
        # Vision §9.2 — 100 concurrent writers, all messages present,
        # no overwrite, no corruption. UUID4 32-hex + atomic-rename
        # guarantees no collision.
        n = 100

        def writer(i: int) -> Path:
            msg = _make_real(body=f"concurrent-message-{i}")
            return write_message(channel_dir, msg)

        with ThreadPoolExecutor(max_workers=20) as pool:
            paths = list(pool.map(writer, range(n)))

        assert len(paths) == n
        assert len({p.name for p in paths}) == n, "filenames must be unique"
        for path in paths:
            result = read_message(path)
            assert isinstance(result, Message)
        listed = list_messages(channel_dir)
        assert {p.name for p in listed} == {p.name for p in paths}

    def test_fsync_true_succeeds(self, channel_dir: Path) -> None:
        # Cannot directly observe fsync syscalls from Python without
        # ptrace. The contract is "the code path doesn't raise and
        # produces the correct file"; that's what we assert.
        msg = _make_real()
        path = write_message(channel_dir, msg, fsync=True)
        assert path.exists()
        result = read_message(path)
        assert result == msg

    def test_message_too_large_propagates(self, channel_dir: Path) -> None:
        # Oversized body — to_json_bytes raises before any disk I/O.
        # Assert: exception propagates AND no .tmp/.json appears.
        oversized = "x" * (MAX_BODY_BYTES + 1)
        msg = _make_real(body=oversized)
        with pytest.raises(MessageTooLarge):
            write_message(channel_dir, msg)
        assert list(channel_dir.iterdir()) == []

    def test_in_reply_to_blind_trust(self, channel_dir: Path) -> None:
        # ADR-020 — in_reply_to is written verbatim; no scan of the
        # channel directory. Even a syntactically nonsensical reference
        # must round-trip cleanly.
        bogus = "msg-does-not-exist-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz.json"
        msg = _make_real(in_reply_to=bogus)
        path = write_message(channel_dir, msg)
        result = read_message(path)
        assert isinstance(result, Message)
        assert result.in_reply_to == bogus

    def test_filename_passes_validator(self, channel_dir: Path) -> None:
        # The join-key contract: when msg.id comes from
        # make_message_filename(), the on-disk filename is regex-valid.
        msg = _make_real()
        path = write_message(channel_dir, msg)
        assert is_valid_message_filename(path.name)

    def test_filename_matches_msg_id(self, channel_dir: Path) -> None:
        msg = _make_real()
        path = write_message(channel_dir, msg)
        assert path.name == f"{msg.id}.json"
        # And msg.id equals the stem (filename minus ".json").
        assert msg.id == path.name.removesuffix(".json")

    def test_does_not_mutate_message(self, channel_dir: Path) -> None:
        # write_message must not call dataclasses.replace or otherwise
        # change msg.id. The dataclass is frozen anyway; this test
        # protects against a "helpful" future refactor that builds a
        # new Message with a generated id.
        msg = _make_real()
        original_id = msg.id
        write_message(channel_dir, msg)
        assert msg.id == original_id

    def test_all_exports_present(self) -> None:
        # Tier-discipline doesn't break: __all__ carries the four new
        # public symbols (the lint and downstream wiring depend on it).
        assert "ParseError" in protocol.__all__
        assert "write_message" in protocol.__all__
        assert "read_message" in protocol.__all__
        assert "list_messages" in protocol.__all__


# ──────────────────────────────────────────────────────────────
# TestReadMessage
# ──────────────────────────────────────────────────────────────


class TestReadMessage:
    """Defensive read — Vision §3.6, K4, G3, L8."""

    def test_roundtrip(self, channel_dir: Path) -> None:
        msg = _make_real(body="ąčęėįšųūž 你好 🌊")
        path = write_message(channel_dir, msg)
        result = read_message(path)
        assert result == msg

    def test_oversized_file_returns_parse_error(
        self,
        channel_dir: Path,
    ) -> None:
        # G3 — file size check fires BEFORE read_bytes so adversarial
        # multi-GB files don't allocate. We can't safely create a
        # multi-GB file in tests, so the canary is MAX_BODY_BYTES + 1.
        msg_id = _valid_id()
        path = channel_dir / f"{msg_id}.json"
        # Use truncate to create a sparse file of size MAX_BODY_BYTES+1
        # without allocating that many bytes. That tests the stat()
        # short-circuit (G3) — if read_message ever read the bytes,
        # we'd get malformed_json, not oversized.
        with open(path, "wb") as fp:
            fp.truncate(MAX_BODY_BYTES + 1)
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason == "oversized"
        assert result.path == path
        # L8 — file is NOT deleted.
        assert path.exists()

    def test_oversized_at_exact_limit_still_reads(
        self,
        channel_dir: Path,
    ) -> None:
        # Boundary: a file at exactly MAX_BODY_BYTES is allowed (the
        # serialiser uses ``> MAX_BODY_BYTES``). The file is unparseable
        # so we get a malformed_json ParseError, not oversized.
        msg_id = _valid_id()
        path = channel_dir / f"{msg_id}.json"
        with open(path, "wb") as fp:
            fp.truncate(MAX_BODY_BYTES)
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json:"), (
            "exact-limit file is unparseable JSON, not oversized"
        )

    def test_malformed_json_returns_parse_error(
        self,
        channel_dir: Path,
    ) -> None:
        msg_id = _valid_id()
        path = channel_dir / f"{msg_id}.json"
        path.write_bytes(b"{not valid json")
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json:")
        assert result.path == path
        # L8 — file preserved.
        assert path.exists()

    def test_schema_deviation_missing_id(self, channel_dir: Path) -> None:
        msg = _make_real()
        payload = json.loads(to_json_bytes(msg).decode())
        del payload["id"]
        path = channel_dir / f"{msg.id}.json"
        path.write_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json:")
        assert "id" in result.reason

    def test_unsupported_schema_version(self, channel_dir: Path) -> None:
        msg = _make_real()
        payload = json.loads(to_json_bytes(msg).decode())
        payload["schema_version"] = 99
        path = channel_dir / f"{msg.id}.json"
        path.write_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json:")

    def test_missing_file_raises_filenotfounderror(
        self,
        channel_dir: Path,
    ) -> None:
        # K4 — the only exception path. Consumers (check_messages,
        # Channel.list_unread) catch and skip at their layer.
        msg_id = _valid_id()
        missing = channel_dir / f"{msg_id}.json"
        with pytest.raises(FileNotFoundError):
            read_message(missing)

    def test_empty_file_returns_parse_error(self, channel_dir: Path) -> None:
        msg_id = _valid_id()
        path = channel_dir / f"{msg_id}.json"
        path.write_bytes(b"")
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json:")
        assert path.exists()

    def test_extra_top_level_key_returns_parse_error(
        self,
        channel_dir: Path,
    ) -> None:
        # Strict parse — unknown top-level keys are rejected (2a K4).
        msg = _make_real()
        payload = json.loads(to_json_bytes(msg).decode())
        payload["unexpected_field"] = "smuggled"
        path = channel_dir / f"{msg.id}.json"
        path.write_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
        result = read_message(path)
        assert isinstance(result, ParseError)
        assert result.reason.startswith("malformed_json:")

    def test_parse_error_is_not_an_exception(self) -> None:
        # K4 — ParseError is a sum-type return value, NOT an Exception
        # subclass. Inheriting from Exception would let
        # ``raise ParseError(...)`` compile and silently drift semantics.
        pe = ParseError(path=Path("/tmp/x"), reason="oversized")
        assert not isinstance(pe, Exception)
        # And it's a frozen dataclass.
        assert dataclasses.is_dataclass(pe)
        with pytest.raises(dataclasses.FrozenInstanceError):
            pe.reason = "mutated"  # type: ignore[misc]


# ──────────────────────────────────────────────────────────────
# TestListMessages
# ──────────────────────────────────────────────────────────────


class TestListMessages:
    """Filename-only enumeration — Vision §3.6 / §9.4 / ADR-017 / ADR-028."""

    def test_empty_directory(self, channel_dir: Path) -> None:
        assert list_messages(channel_dir) == []

    def test_single_message(self, channel_dir: Path) -> None:
        msg = _make_real()
        path = write_message(channel_dir, msg)
        assert list_messages(channel_dir) == [path]

    def test_multiple_messages_chronological_order(
        self,
        channel_dir: Path,
    ) -> None:
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        paths: list[Path] = []
        for i in range(10):
            msg_id = make_message_filename(
                base + timedelta(microseconds=i)
            ).removesuffix(".json")
            msg = _make_real(id=msg_id, body=f"m{i}")
            paths.append(write_message(channel_dir, msg))

        listed = list_messages(channel_dir)
        # Lexical sort == chronological sort (Vision §3.2 / ADR-017).
        assert listed == paths

    def test_tmp_glob_exclusion(self, channel_dir: Path) -> None:
        # THE load-bearing structural exclusion (Vision §3.6). Plant a
        # syntactically-plausible .tmp file directly; assert it's not
        # returned. Then write a real message and assert that one is.
        bogus_tmp_name = (
            "msg-20260527T143015123456-"
            "7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6f.json.tmp"
        )
        (channel_dir / bogus_tmp_name).write_bytes(
            b'{"would": "look like a message"}'
        )
        # The .tmp doesn't pass the validator: the regex anchors on
        # ``\.json$`` and the trailing ``.tmp`` breaks that.
        assert not is_valid_message_filename(bogus_tmp_name)
        # Now a real message lands alongside it.
        msg = _make_real()
        real_path = write_message(channel_dir, msg)
        listed = list_messages(channel_dir)
        assert listed == [real_path]
        assert bogus_tmp_name not in {p.name for p in listed}

    def test_malformed_filenames_filtered_with_warn(
        self,
        channel_dir: Path,
        caplog: pytest.LogCaptureFixture,
        reset_warn_dedupe: None,
    ) -> None:
        # ADR-028: malformed names are silently filtered but logged at
        # WARN (once per name per process). Plant a unique bad name to
        # dodge cross-test G7 set pollution.
        bad_name = f"msg-bad-{uuid.uuid4().hex}.json"
        (channel_dir / bad_name).write_bytes(b"{}")

        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            result = list_messages(channel_dir)

        assert result == []
        warns = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and bad_name in r.getMessage()
        ]
        assert len(warns) == 1, (
            f"expected exactly one WARN for {bad_name!r}, got {len(warns)}"
        )

    def test_warn_dedupe_within_process(
        self,
        channel_dir: Path,
        caplog: pytest.LogCaptureFixture,
        reset_warn_dedupe: None,
    ) -> None:
        # K6: second call sees the same bad name is silent.
        bad_name = f"msg-bad-{uuid.uuid4().hex}.json"
        (channel_dir / bad_name).write_bytes(b"{}")

        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            list_messages(channel_dir)
        first = [
            r for r in caplog.records if bad_name in r.getMessage()
        ]
        assert len(first) == 1

        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            list_messages(channel_dir)
        second = [
            r for r in caplog.records if bad_name in r.getMessage()
        ]
        assert len(second) == 0, "second encounter must be deduped"

        # A different bad name surfaces its own WARN.
        bad_name_2 = f"msg-bad-{uuid.uuid4().hex}.json"
        (channel_dir / bad_name_2).write_bytes(b"{}")
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            list_messages(channel_dir)
        third = [
            r for r in caplog.records if bad_name_2 in r.getMessage()
        ]
        assert len(third) == 1

    def test_since_strictly_greater(self, channel_dir: Path) -> None:
        # G8 — strictly greater. `since=f3` returns f4, f5 (not f3).
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        paths: list[Path] = []
        for i in range(5):
            msg_id = make_message_filename(
                base + timedelta(microseconds=i)
            ).removesuffix(".json")
            paths.append(write_message(channel_dir, _make_real(id=msg_id)))

        # since = f2 → expect f3, f4 (indices 3..4).
        result = list_messages(channel_dir, since=paths[2].name)
        assert result == paths[3:]

    def test_since_excludes_exact_match(self, channel_dir: Path) -> None:
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        paths: list[Path] = []
        for i in range(3):
            msg_id = make_message_filename(
                base + timedelta(microseconds=i)
            ).removesuffix(".json")
            paths.append(write_message(channel_dir, _make_real(id=msg_id)))

        # since = f1 (the middle one) — strictly greater excludes f1.
        result = list_messages(channel_dir, since=paths[1].name)
        assert paths[1] not in result
        assert result == [paths[2]]

    def test_since_above_all_returns_empty(self, channel_dir: Path) -> None:
        msg = _make_real()
        write_message(channel_dir, msg)
        # A name lexically greater than anything real returns empty.
        too_high = (
            "msg-99999999T999999999999-"
            "ffffffffffffffffffffffffffffffff.json"
        )
        assert list_messages(channel_dir, since=too_high) == []

    def test_since_below_all_returns_everything(
        self,
        channel_dir: Path,
    ) -> None:
        paths = [
            write_message(channel_dir, _make_real()) for _ in range(3)
        ]
        too_low = (
            "msg-00000000T000000000000-"
            "00000000000000000000000000000000.json"
        )
        listed = list_messages(channel_dir, since=too_low)
        assert {p.name for p in listed} == {p.name for p in paths}

    def test_subdirectory_not_recursed(
        self,
        channel_dir: Path,
        reset_warn_dedupe: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # os.scandir does not recurse, and entry.is_dir() short-circuits
        # BEFORE the validator — so a subdirectory name never reaches
        # the WARN code path. A msg-*.json nested inside the subdir is
        # invisible to list_messages.
        sub = channel_dir / "subdir"
        sub.mkdir()
        msg = _make_real()
        write_message(sub, msg)

        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            listed = list_messages(channel_dir)
        assert listed == []
        # No WARN about the subdirectory itself.
        assert not any(
            "subdir" in r.getMessage() for r in caplog.records
        )

    def test_missing_directory_raises(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        # Consistent with os.scandir semantics — no special-casing.
        missing = tmp_letterbox_home / "channels" / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            list_messages(missing)

    def test_returns_only_paths(self, channel_dir: Path) -> None:
        # Belt-and-suspenders: the public surface promises list[Path],
        # not list[os.DirEntry]. A DirEntry would mostly look like a
        # Path but downstream pickling / str() differs.
        write_message(channel_dir, _make_real())
        listed = list_messages(channel_dir)
        for p in listed:
            assert isinstance(p, Path)

    def test_roundtrip_property(self, channel_dir: Path) -> None:
        # Plan §9 hypothesis property — for any factory-built Message,
        # read_message(write_message(...)) == msg. Run a generous batch
        # of bodies (with surrogates filtered) without invoking
        # hypothesis (which complains about function-scoped fixtures).
        rng = random.Random(0xCAFE)
        for _ in range(50):
            body_len = rng.randint(0, 4096)
            chars: list[str] = []
            while len(chars) < body_len:
                cp = rng.randint(32, 0x10FFFF)
                # Skip surrogate range (scout brief: hypothesis pattern
                # excludes ``Cs``).
                if 0xD800 <= cp <= 0xDFFF:
                    continue
                chars.append(chr(cp))
            body = "".join(chars)
            msg = _make_real(body=body)
            path = write_message(channel_dir, msg)
            result = read_message(path)
            assert result == msg


# ──────────────────────────────────────────────────────────────
# Phase 2d — Startup-only ``.tmp`` reaper (ADR-016 / Vision §3.6 / L6)
# ──────────────────────────────────────────────────────────────


class TestReapOrphanTmp:
    """``reap_orphan_tmp`` — the lifecycle counterpart to 2c's writer.

    Pinned contract: deletes only ``.json.tmp`` siblings whose ``mtime`` is
    older than the threshold; leaves ``msg-*.json`` untouched; tolerates
    per-file errors; rejects zero/negative thresholds (K3).
    """

    def test_empty_directory_returns_zero(
        self,
        channel_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Plan §9 test 1: empty dir → 0, no errors, no log lines.
        with caplog.at_level(logging.INFO, logger="letterbox.protocol"):
            assert reap_orphan_tmp(channel_dir) == 0
        assert caplog.records == []

    def test_all_fresh_preserved(self, channel_dir: Path) -> None:
        # Plan §9 test 2: all .tmp files fresh (mtime ≈ now) → 0 deletions,
        # all preserved at default 3600 s threshold.
        names = [f"{_valid_id()}.json.tmp" for _ in range(3)]
        for name in names:
            (channel_dir / name).touch()
        assert reap_orphan_tmp(channel_dir) == 0
        for name in names:
            assert (channel_dir / name).exists()

    def test_all_old_reaped_with_info_log(
        self,
        channel_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Plan §9 test 3: three aged .tmp files → 3 deletions, 3 INFO logs
        # (one per file, with filename in the message).
        names = [f"{_valid_id()}.json.tmp" for _ in range(3)]
        now = time.time()
        for name in names:
            p = channel_dir / name
            p.touch()
            os.utime(p, (now - 7200, now - 7200))  # 2 h old

        with caplog.at_level(logging.INFO, logger="letterbox.protocol"):
            assert reap_orphan_tmp(channel_dir) == 3

        for name in names:
            assert not (channel_dir / name).exists()
        infos = [
            r for r in caplog.records if r.levelno == logging.INFO
        ]
        assert len(infos) == 3
        # Each filename appears in exactly one INFO line.
        for name in names:
            matching = [r for r in infos if name in r.getMessage()]
            assert len(matching) == 1, (
                f"expected exactly one INFO line for {name!r}, got {len(matching)}"
            )

    def test_mixed_ages_boundary(self, channel_dir: Path) -> None:
        # Plan §9 test 4: 3 old + 2 fresh. Reaper keeps the fresh, deletes
        # the old. Tests the boundary on a single sweep.
        now = time.time()
        old_names = [f"{_valid_id()}.json.tmp" for _ in range(3)]
        fresh_names = [f"{_valid_id()}.json.tmp" for _ in range(2)]
        for name in old_names:
            p = channel_dir / name
            p.touch()
            os.utime(p, (now - 7200, now - 7200))
        for name in fresh_names:
            (channel_dir / name).touch()

        assert reap_orphan_tmp(channel_dir) == 3
        for name in old_names:
            assert not (channel_dir / name).exists()
        for name in fresh_names:
            assert (channel_dir / name).exists()

    def test_custom_threshold(self, channel_dir: Path) -> None:
        # Plan §9 test 5: custom small threshold. mtime = now-0.5 reaps;
        # mtime = now-0.05 preserves at threshold=0.1.
        now = time.time()
        old_name = f"{_valid_id()}.json.tmp"
        fresh_name = f"{_valid_id()}.json.tmp"
        old_p = channel_dir / old_name
        fresh_p = channel_dir / fresh_name
        old_p.touch()
        fresh_p.touch()
        os.utime(old_p, (now - 0.5, now - 0.5))
        os.utime(fresh_p, (now - 0.05, now - 0.05))

        assert reap_orphan_tmp(channel_dir, mtime_threshold_seconds=0.1) == 1
        assert not old_p.exists()
        assert fresh_p.exists()

    def test_does_not_touch_valid_messages(self, channel_dir: Path) -> None:
        # Plan §9 test 6 (G3): a valid msg-*.json next to an aged .tmp.
        # After sweep, the .tmp is gone but the message survives AND is
        # still parseable via read_message.
        msg = _make_real()
        msg_path = write_message(channel_dir, msg)

        now = time.time()
        tmp_name = f"{_valid_id()}.json.tmp"
        tmp_path = channel_dir / tmp_name
        tmp_path.touch()
        os.utime(tmp_path, (now - 7200, now - 7200))

        assert reap_orphan_tmp(channel_dir) == 1
        assert not tmp_path.exists()
        assert msg_path.exists()
        # And the surviving message still parses cleanly.
        result = read_message(msg_path)
        assert result == msg

    def test_per_file_unlink_failure_continues(
        self,
        channel_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Plan §9 test 7 (K4): two aged .tmps; first os.unlink call raises
        # PermissionError; second succeeds. Function returns 1, WARN names
        # the failed file, second file is gone, first still exists.
        # ``os.scandir`` does NOT guarantee alphabetical order, so we
        # identify "first" / "second" by the order ``failing_unlink`` was
        # invoked, not by filename.
        now = time.time()
        names = [f"{_valid_id()}.json.tmp" for _ in range(2)]
        for name in names:
            p = channel_dir / name
            p.touch()
            os.utime(p, (now - 7200, now - 7200))

        real_unlink = os.unlink
        calls: list[str] = []

        def failing_unlink(path, *args, **kwargs):
            calls.append(str(path))
            if len(calls) == 1:
                raise PermissionError(f"simulated permission denial on {path}")
            return real_unlink(path, *args, **kwargs)

        monkeypatch.setattr(os, "unlink", failing_unlink)

        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            result = reap_orphan_tmp(channel_dir)

        assert result == 1
        assert len(calls) == 2, "reaper must attempt both files even after one fails"
        failed_path = Path(calls[0])
        succeeded_path = Path(calls[1])
        assert failed_path.exists(), (
            "file whose unlink raised PermissionError must persist"
        )
        assert not succeeded_path.exists(), (
            "the second (successful) unlink must remove the file"
        )
        # WARN names the failed file (and only the failed file).
        warns = [
            r for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert len(warns) == 1
        assert failed_path.name in warns[0].getMessage()

    def test_missing_channel_dir_raises(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        # Plan §9 test 8: matches list_messages / os.scandir convention.
        missing = tmp_letterbox_home / "channels" / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            reap_orphan_tmp(missing)

    def test_zero_or_negative_threshold_raises(self, channel_dir: Path) -> None:
        # Plan §9 test 9 (K3): defensive against race-with-writer.
        with pytest.raises(ValueError, match="must be > 0"):
            reap_orphan_tmp(channel_dir, mtime_threshold_seconds=0)
        with pytest.raises(ValueError, match="must be > 0"):
            reap_orphan_tmp(channel_dir, mtime_threshold_seconds=-1.0)

    def test_does_not_recurse_into_subdirs(self, channel_dir: Path) -> None:
        # Plan §9 test 10: .read/ and other subdirs are the channel layer's
        # domain (Phase 3b); the reaper never walks them.
        sub = channel_dir / "subdir"
        sub.mkdir()
        now = time.time()
        nested_name = f"{_valid_id()}.json.tmp"
        nested_p = sub / nested_name
        nested_p.touch()
        os.utime(nested_p, (now - 7200, now - 7200))

        assert reap_orphan_tmp(channel_dir) == 0
        assert nested_p.exists()

    def test_does_not_delete_wrong_suffix_order(self, channel_dir: Path) -> None:
        # Plan §9 test 11 (G2): ``.tmp.json`` is NOT the writer's suffix
        # order. The reaper's endswith(".json.tmp") strict end-anchor
        # ignores foreign names regardless of their mtime.
        now = time.time()
        wrong = channel_dir / f"{_valid_id()}.tmp.json"
        wrong.touch()
        os.utime(wrong, (now - 7200, now - 7200))

        assert reap_orphan_tmp(channel_dir) == 0
        assert wrong.exists()

    def test_all_exports_present(self) -> None:
        # __all__ carries the new symbol so downstream wiring (Phase 8a
        # launcher) can ``from letterbox.protocol import reap_orphan_tmp``.
        assert "reap_orphan_tmp" in protocol.__all__


# ──────────────────────────────────────────────────────────────
# TestScanValidNames + lazy heap iterators (full-sort elimination)
# ──────────────────────────────────────────────────────────────


class TestScanValidNames:
    """The shared scan primitive underneath ``list_messages`` and the
    channel-layer hot readers — DESIGN §1a / §7 / DECISIONS.md."""

    def test_returns_bare_names_unsorted_set(self, channel_dir: Path) -> None:
        # Same valid corpus as ``list_messages`` sees, but as bare
        # ``entry.name`` strings and in arbitrary (scan) order. The set of
        # names must equal the set ``list_messages`` returns.
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        expected: set[str] = set()
        for i in range(20):
            msg_id = make_message_filename(
                base + timedelta(microseconds=i)
            ).removesuffix(".json")
            p = write_message(channel_dir, _make_real(id=msg_id))
            expected.add(p.name)

        names = protocol._scan_valid_names(channel_dir)
        assert isinstance(names, list)
        assert all(isinstance(n, str) for n in names)
        assert set(names) == expected

    def test_list_messages_is_sorted_scan(self, channel_dir: Path) -> None:
        # The golden equivalence that guarantees byte-identical output for
        # all 8 existing callers: ``list_messages`` == the scan primitive
        # sorted and lifted back to ``Path`` objects.
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        for i in range(20):
            msg_id = make_message_filename(
                base + timedelta(microseconds=i)
            ).removesuffix(".json")
            write_message(channel_dir, _make_real(id=msg_id))

        listed = list_messages(channel_dir)
        rebuilt = [
            channel_dir / n
            for n in sorted(protocol._scan_valid_names(channel_dir))
        ]
        assert listed == rebuilt

    def test_skips_dirs_tmp_and_bad_names_warn_once(
        self,
        channel_dir: Path,
        caplog: pytest.LogCaptureFixture,
        reset_warn_dedupe: None,
    ) -> None:
        # Validation is a side effect of *scanning*: subdirs are skipped
        # silently (is_dir BEFORE validation, N4 — never reach the WARN
        # path), a ``.tmp`` is excluded from RESULTS (\.json$ anchor) but,
        # being a non-dir file that fails the validator, still warns once
        # like any other malformed name (unchanged from ``list_messages``);
        # a novel bad name warns once too.
        (channel_dir / "subdir").mkdir()
        (channel_dir / "cold").mkdir()  # N4 — no spurious ADR-028 warn
        tmp_name = f"{_valid_id()}.json.tmp"
        (channel_dir / tmp_name).write_bytes(b"{}")
        bad_name = f"msg-bad-{uuid.uuid4().hex}.json"
        (channel_dir / bad_name).write_bytes(b"{}")
        good = write_message(channel_dir, _make_real())

        with caplog.at_level(logging.WARNING, logger="letterbox.protocol"):
            names = protocol._scan_valid_names(channel_dir)

        # Only the valid message survives; subdirs and .tmp are excluded.
        assert names == [good.name]
        assert tmp_name not in names
        warned_names = {
            r.args[0]
            for r in caplog.records
            if r.levelno == logging.WARNING
        }
        # The two malformed FILES warn once each; the subdirs never do.
        assert warned_names == {bad_name, tmp_name}
        assert not any(
            "subdir" in r.getMessage() or "cold" in r.getMessage()
            for r in caplog.records
        )

    def test_since_strictly_greater(self, channel_dir: Path) -> None:
        # The cursor filter is full-name strict-greater, identical to
        # ``list_messages``' contract (§3.2 / G8).
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        names_written: list[str] = []
        for i in range(5):
            msg_id = make_message_filename(
                base + timedelta(microseconds=i)
            ).removesuffix(".json")
            names_written.append(write_message(channel_dir, _make_real(id=msg_id)).name)
        names_written.sort()

        result = sorted(
            protocol._scan_valid_names(channel_dir, since=names_written[2])
        )
        assert result == names_written[3:]

    def test_missing_directory_raises(self, tmp_letterbox_home: Path) -> None:
        missing = tmp_letterbox_home / "channels" / "nope"
        with pytest.raises(FileNotFoundError):
            protocol._scan_valid_names(missing)


class TestLazyNameIterators:
    """``_iter_names_ascending`` / ``_iter_names_descending`` / ``_Rev`` —
    DESIGN §1c. Lazy heap selection, O(N + k·log N) for a k-pop reader."""

    def _sample_names(self) -> list[str]:
        base = datetime(2026, 5, 27, 14, 30, 15, 0, tzinfo=timezone.utc)
        # Deliberately construct in shuffled order so we prove the heap
        # orders them, not the input.
        order = [4, 0, 9, 2, 7, 1, 8, 3, 6, 5]
        return [
            make_message_filename(base + timedelta(microseconds=i))
            for i in order
        ]

    def test_ascending_equals_sorted(self) -> None:
        names = self._sample_names()
        assert list(protocol._iter_names_ascending(names)) == sorted(names)

    def test_descending_equals_reverse_sorted(self) -> None:
        names = self._sample_names()
        assert list(protocol._iter_names_descending(names)) == sorted(
            names, reverse=True
        )

    def test_does_not_mutate_input(self) -> None:
        names = self._sample_names()
        snapshot = list(names)
        list(protocol._iter_names_ascending(names))
        list(protocol._iter_names_descending(names))
        assert names == snapshot

    def test_lazy_partial_consumption_descending(self) -> None:
        # A reader that stops after k pops gets the k largest (newest) in
        # order — the ``latest_unread`` stop-early property.
        names = self._sample_names()
        it = protocol._iter_names_descending(names)
        first_three = [next(it) for _ in range(3)]
        assert first_three == sorted(names, reverse=True)[:3]

    def test_empty(self) -> None:
        assert list(protocol._iter_names_ascending([])) == []
        assert list(protocol._iter_names_descending([])) == []

    def test_rev_inverts_ordering(self) -> None:
        lo = protocol._Rev("a")
        hi = protocol._Rev("b")
        # ``_Rev`` inverts ``<`` so the min-heap surfaces the max name.
        assert hi < lo
        assert not lo < hi

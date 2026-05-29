"""Tests for Phase 3a + 3b + 3c — Channel type, per-agent read-state files,
state-dir permissions, ``acknowledge`` + unread query.

Behavioral TDD per the plan's §9 grouping. No mocks — tests run synchronously
against real filesystem state via ``tmp_letterbox_home`` (1b). The phase opens
the channel layer above the now-sealed protocol core; tests here cover the
``Channel`` frozen dataclass, ``Channel.get_or_create`` (auto-creation +
idempotency + name validation), ``check_state_dir_permissions``
(world-accessible mask rejection — the first "refuse to start" gate), the
per-agent read-state mechanism (``ReadState`` + ``read_state`` +
``write_read_state``) that ADR-021 introduced as the replacement for shared
``archive/`` semantics, and the 3c inbox-advance API (``Channel.acknowledge``
+ ``Channel.list_unread`` + ``UnreadResult`` + module-private
``_is_own_write`` predicate that locks ADR-022's combined own-write filter).

Test classes:

* ``TestChannel`` — frozen-dataclass shape, equality, hashability, direct
  construction (escape hatch — name validation deferred to ``get_or_create``).
* ``TestGetOrCreate`` — creation idempotency, mode-0o700 with umask defense,
  ``parents=True`` for missing ``channels/`` parent, name validation
  (delegates to 2b's predicate, raises ``ValueError`` here), type guards
  on ``name``/``sender``/``recipient``, integration with
  ``protocol.write_message`` (the 2c↔3a contract).
* ``TestCheckStateDirPermissions`` — accept matrix (``0o700``, ``0o750``,
  ``0o770``), reject matrix (any ``0o007`` bit), missing dir raises
  ``FileNotFoundError`` (not ``StatePermissionsError`` — documented contract),
  vector-error message includes path + octal mode.
* ``TestReadState`` — frozen-dataclass shape, equality, hashability,
  ``to_dict`` round-trip prep, empty-sentinel field acceptance.
* ``TestReadStateFromDict`` — strict-parse roundtrip; rejects unknown,
  missing, non-string, and bool values; vector error messages.
* ``TestReadStateRead`` — missing-file fresh-state path, no side effects on
  read, valid-file round-trip, corruption recovery (rename to
  ``.broken.<ts>`` + WARN + fresh state), sender-label validation gates.
* ``TestReadStateWrite`` — ``.read/`` auto-creation with mode ``0o700``,
  byte-identical sort-keys output, atomic-rename partial-write invisibility,
  multi-endpoint isolation, restart-with-same-label durability,
  sender-label validation gates.
* ``TestIsOwnWrite`` — the ADR-022 combined filter four-cell matrix
  (own/peer × same-session/cross-restart) plus defensive empty-string guards.
* ``TestAcknowledge`` — read-modify-write idempotency, hwm monotonicity,
  ``.read/`` auto-creation on first use, per-agent isolation (peer
  read-state untouched), message files never deleted/modified.
* ``TestListUnread`` — peer-only filtering, hwm cursor, ``since_id``
  override, ``limit`` clamping with vector ``limit_warning``, ParseError
  surfacing, ``FileNotFoundError`` race silently skipped, ``has_more``
  pagination correctness.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from letterbox.channel import (
    Channel,
    ChannelInfo,
    ChannelSummary,
    ReadState,
    StatePermissionsError,
    UnreadResult,
    _is_own_write,
    channel_info,
    check_state_dir_permissions,
    list_channels,
    read_state,
    write_read_state,
)
from letterbox.protocol import (
    MAX_BODY_BYTES,
    Message,
    Metadata,
    make_message_filename,
    new_message,
    write_message,
)


# ──────────────────────────────────────────────────────────────
# Local fixtures + helpers
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def permissive_umask():
    """Save/restore the process umask around the test body.

    Setting ``os.umask(0)`` lets ``mkdir(mode=0o700)`` land its full mode
    without the default ``0o022`` mask stripping group/other bits. Tests
    that assert created-dir modes depend on it; tests that only assert
    structural properties don't.

    Yields nothing — used as a barrier, not a value.
    """
    old = os.umask(0)
    try:
        yield
    finally:
        os.umask(old)


def _make_channel(
    home: Path,
    name: str = "test",
    sender: str = "claude-a",
    recipient: str = "claude-b",
) -> Channel:
    """Shorthand for ``Channel.get_or_create`` against a tmp letterbox home."""
    return Channel.get_or_create(name, sender, recipient, state_dir=home)


# ──────────────────────────────────────────────────────────────
# TestChannel  (frozen dataclass shape)
# ──────────────────────────────────────────────────────────────


class TestChannel:
    """Frozen-dataclass shape — fields, immutability, equality, hashability."""

    def test_fields_populated(self) -> None:
        ch = Channel(
            name="ch01",
            path=Path("/tmp/whatever"),
            sender_label="alice",
            recipient_label="bob",
        )
        assert ch.name == "ch01"
        assert ch.path == Path("/tmp/whatever")
        assert ch.sender_label == "alice"
        assert ch.recipient_label == "bob"

    def test_is_frozen(self) -> None:
        ch = Channel(name="ch01", path=Path("/x"), sender_label="a", recipient_label="b")
        with pytest.raises(dataclasses.FrozenInstanceError):
            ch.name = "ch02"  # type: ignore[misc]

    def test_equality(self) -> None:
        a = Channel(name="ch01", path=Path("/x"), sender_label="a", recipient_label="b")
        b = Channel(name="ch01", path=Path("/x"), sender_label="a", recipient_label="b")
        assert a == b

    def test_inequality_on_field_drift(self) -> None:
        a = Channel(name="ch01", path=Path("/x"), sender_label="a", recipient_label="b")
        b = Channel(name="ch02", path=Path("/x"), sender_label="a", recipient_label="b")
        assert a != b

    def test_hashable_goes_in_set(self) -> None:
        a = Channel(name="ch01", path=Path("/x"), sender_label="a", recipient_label="b")
        b = Channel(name="ch01", path=Path("/x"), sender_label="a", recipient_label="b")
        c = Channel(name="ch02", path=Path("/x"), sender_label="a", recipient_label="b")
        assert {a, b, c} == {a, c}

    def test_direct_construction_skips_validation(self, tmp_path: Path) -> None:
        """``Channel(...)`` is documented escape-hatch — no name regex check.

        Validation lives in ``get_or_create``. Direct construction is for
        callers that already trust their inputs (e.g., reading from a
        registered config in a future phase).
        """
        ch = Channel(
            name="../etc",
            path=tmp_path,
            sender_label="a",
            recipient_label="b",
        )
        assert ch.name == "../etc"


# ──────────────────────────────────────────────────────────────
# TestGetOrCreate  (classmethod constructor + filesystem effects)
# ──────────────────────────────────────────────────────────────


class TestGetOrCreate:
    """``Channel.get_or_create`` — auto-creation, idempotency, validation."""

    def test_returns_channel_with_expected_fields(self, tmp_letterbox_home: Path) -> None:
        ch = Channel.get_or_create(
            "debate-01", "claude-a", "claude-b", state_dir=tmp_letterbox_home
        )
        assert ch.name == "debate-01"
        assert ch.sender_label == "claude-a"
        assert ch.recipient_label == "claude-b"
        assert ch.path == tmp_letterbox_home / "channels" / "debate-01"

    def test_creates_directory_on_disk(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        assert ch.path.is_dir()

    def test_creates_intermediate_channels_parent(self, tmp_letterbox_home: Path) -> None:
        """``parents=True`` builds ``state_dir/channels/`` if absent."""
        assert not (tmp_letterbox_home / "channels").exists()
        _make_channel(tmp_letterbox_home, name="ch01")
        assert (tmp_letterbox_home / "channels").is_dir()

    def test_creates_dir_with_mode_0o700(
        self,
        tmp_letterbox_home: Path,
        permissive_umask: None,
    ) -> None:
        """Under permissive umask, the channel dir must still land at 0o700.

        This is the assertion that proves the explicit ``os.chmod`` ran —
        without it, ``mkdir(mode=0o700)`` under ``umask(0)`` lands the
        mode literally, and the test passes for the wrong reason.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        assert oct(ch.path.stat().st_mode & 0o777) == "0o700"

    def test_idempotent_returns_equal_channels(self, tmp_letterbox_home: Path) -> None:
        a = _make_channel(tmp_letterbox_home, name="ch01")
        b = _make_channel(tmp_letterbox_home, name="ch01")
        assert a == b

    def test_idempotent_does_not_change_existing_mode(
        self,
        tmp_letterbox_home: Path,
        permissive_umask: None,
    ) -> None:
        """Second call re-chmod's to 0o700; if a test manually loosened the
        mode between calls, ``get_or_create`` restores it.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        os.chmod(ch.path, 0o755)
        _make_channel(tmp_letterbox_home, name="ch01")
        assert oct(ch.path.stat().st_mode & 0o777) == "0o700"

    def test_idempotent_no_error_on_existing_dir(self, tmp_letterbox_home: Path) -> None:
        """``exist_ok=True`` semantics — second call doesn't raise."""
        _make_channel(tmp_letterbox_home, name="ch01")
        # Should not raise.
        _make_channel(tmp_letterbox_home, name="ch01")

    @pytest.mark.parametrize(
        "name",
        [
            "../etc",       # path traversal
            "foo/bar",      # path separator
            "FOO",          # uppercase
            "foo bar",      # whitespace
            "",             # empty
            "-rf",          # leading dash
            "_internal",    # leading underscore
            "claude.a",     # dot
            "lt-ąčę",       # non-ASCII
        ],
    )
    def test_rejects_invalid_name_with_value_error(
        self,
        tmp_letterbox_home: Path,
        name: str,
    ) -> None:
        with pytest.raises(ValueError) as exc:
            Channel.get_or_create(name, "a", "b", state_dir=tmp_letterbox_home)
        # Vector message: rejected name appears AND the regex rule cites the form.
        msg = str(exc.value)
        assert repr(name) in msg
        assert "[a-z0-9]" in msg

    def test_rejects_invalid_name_before_filesystem_touch(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """A malicious name must not even reach ``mkdir`` — validation gates first."""
        with pytest.raises(ValueError):
            Channel.get_or_create("../etc", "a", "b", state_dir=tmp_letterbox_home)
        # The ``channels/`` parent should NOT have been created as a side effect.
        assert not (tmp_letterbox_home / "channels").exists()

    @pytest.mark.parametrize("bad_name", [42, None, b"ch01", ["ch"]])
    def test_rejects_non_str_name_with_type_error(
        self,
        tmp_letterbox_home: Path,
        bad_name,
    ) -> None:
        """Defense in depth (G3) — the predicate's stdlib ``TypeError`` from
        ``re.Pattern.fullmatch`` is swapped for a domain ``TypeError`` with
        a vector message.
        """
        with pytest.raises(TypeError) as exc:
            Channel.get_or_create(bad_name, "a", "b", state_dir=tmp_letterbox_home)  # type: ignore[arg-type]
        assert "name" in str(exc.value)
        assert "str" in str(exc.value)

    @pytest.mark.parametrize("bad_sender", [42, None, b"a"])
    def test_rejects_non_str_sender_with_type_error(
        self,
        tmp_letterbox_home: Path,
        bad_sender,
    ) -> None:
        with pytest.raises(TypeError) as exc:
            Channel.get_or_create("ch01", bad_sender, "b", state_dir=tmp_letterbox_home)  # type: ignore[arg-type]
        assert "sender" in str(exc.value)

    @pytest.mark.parametrize("bad_recipient", [42, None, b"b"])
    def test_rejects_non_str_recipient_with_type_error(
        self,
        tmp_letterbox_home: Path,
        bad_recipient,
    ) -> None:
        with pytest.raises(TypeError) as exc:
            Channel.get_or_create("ch01", "a", bad_recipient, state_dir=tmp_letterbox_home)  # type: ignore[arg-type]
        assert "recipient" in str(exc.value)

    def test_accepts_empty_recipient_label(self, tmp_letterbox_home: Path) -> None:
        """Vision §3.2 / §4.1 — peer label may be unknown at launch."""
        ch = Channel.get_or_create("ch01", "a", "", state_dir=tmp_letterbox_home)
        assert ch.recipient_label == ""

    def test_state_dir_is_keyword_only(self, tmp_letterbox_home: Path) -> None:
        """K2 — ``state_dir`` is keyword-only; positional call must fail.

        This guards the explicit caller-injection design — callers that
        forget to thread ``state_dir`` through are forced to notice.
        """
        with pytest.raises(TypeError):
            Channel.get_or_create("ch01", "a", "b", tmp_letterbox_home)  # type: ignore[misc]

    def test_round_trip_with_protocol_write_message(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """Success criterion 6: ``write_message(channel.path, msg)`` lands.

        Wires the 2c↔3a contract — ``Channel.get_or_create`` owns directory
        creation; ``write_message`` consumes a pre-existing directory.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        msg = new_message(
            id=make_message_filename().removesuffix(".json"),
            channel=ch.name,
            instance_id="inst-1",
            sender=ch.sender_label,
            recipient=ch.recipient_label,
            body="hello",
        )
        path = write_message(ch.path, msg)
        assert path.exists()
        assert path.parent == ch.path


# ──────────────────────────────────────────────────────────────
# TestCheckStateDirPermissions  (startup-validation gate)
# ──────────────────────────────────────────────────────────────


class TestCheckStateDirPermissions:
    """``check_state_dir_permissions`` — the first "refuse to start" gate.

    Vision §6.4 prose says "world-readable"; K5 widens to ANY world-accessible
    bit (mask ``0o007``) — defense in depth. The test matrix exercises both
    halves: modes that lack any ``0o007`` bit accept silently; modes that
    have any ``0o007`` bit reject with a vector ``StatePermissionsError``.
    """

    @pytest.mark.parametrize("mode", [0o700, 0o750, 0o770, 0o740, 0o710, 0o000])
    def test_accepts_clean_modes(
        self,
        tmp_letterbox_home: Path,
        mode: int,
    ) -> None:
        try:
            os.chmod(tmp_letterbox_home, mode)
            # No raise; returns None.
            assert check_state_dir_permissions(tmp_letterbox_home) is None
        finally:
            # Restore 0o700 so pytest's tmp_path teardown can traverse the
            # directory — mode 0o000 strips the owner's own traverse bit.
            os.chmod(tmp_letterbox_home, 0o700)

    @pytest.mark.parametrize("mode", [0o704, 0o701, 0o702, 0o707, 0o777, 0o705])
    def test_rejects_world_accessible_modes(
        self,
        tmp_letterbox_home: Path,
        mode: int,
    ) -> None:
        os.chmod(tmp_letterbox_home, mode)
        with pytest.raises(StatePermissionsError) as exc:
            check_state_dir_permissions(tmp_letterbox_home)
        msg = str(exc.value)
        # Vector-error contract: name the path and the offending mode.
        assert str(tmp_letterbox_home) in msg
        assert f"0o{mode:03o}" in msg
        # Fix-hint included.
        assert "chmod" in msg

    def test_missing_dir_raises_file_not_found(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """Documented contract: missing state_dir is NOT a permissions error;
        the caller (CLI ``init``, launcher) decides how to react.
        """
        missing = tmp_letterbox_home / "nonexistent-subdir"
        with pytest.raises(FileNotFoundError):
            check_state_dir_permissions(missing)

    def test_error_is_state_permissions_error_subclass_of_exception(self) -> None:
        """Public taxonomy — launchers catch ``StatePermissionsError`` and
        translate to stderr + non-zero exit (8a).
        """
        assert issubclass(StatePermissionsError, Exception)

    def test_error_args_contains_path_and_mode(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """Framework P3 vector check — ``exc.args[0]`` carries the full
        diagnostic, not just ``"bad mode"``.
        """
        os.chmod(tmp_letterbox_home, 0o704)
        with pytest.raises(StatePermissionsError) as exc:
            check_state_dir_permissions(tmp_letterbox_home)
        assert str(tmp_letterbox_home) in exc.value.args[0]
        assert "0o704" in exc.value.args[0]


# ──────────────────────────────────────────────────────────────
# Import-shape smoke
# ──────────────────────────────────────────────────────────────


class TestPublicSurface:
    """Guards that the public names in ``__all__`` stay exported.

    Post-3d the exported set is eleven names — three from 3a (``Channel``,
    ``StatePermissionsError``, ``check_state_dir_permissions``), three
    from 3b (``ReadState``, ``read_state``, ``write_read_state``), one
    from 3c (``UnreadResult``), and four from 3d (``ChannelInfo``,
    ``ChannelSummary``, ``channel_info``, ``list_channels``). The 3c
    helper ``_is_own_write`` is deliberately NOT in ``__all__`` — it is
    a module-private friend-import contract (K4) for Phase 4b's watcher,
    not a public API. The 3d helper ``_filename_to_iso_timestamp`` is
    likewise module-private (no in-vision consumer beyond ``list_channels``).
    """

    def test_public_exports(self) -> None:
        import letterbox.channel as ch_mod

        assert set(ch_mod.__all__) == {
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
        }

    def test_is_own_write_is_module_private(self) -> None:
        """K4 — ``_is_own_write`` is the watcher's friend-import target;
        the leading underscore signals "intentional internal API".
        """
        import letterbox.channel as ch_mod

        assert "_is_own_write" not in ch_mod.__all__


# ──────────────────────────────────────────────────────────────
# Phase 3b — Per-Agent Read-State Files
# ──────────────────────────────────────────────────────────────


def _make_state(
    sender: str = "claude-a",
    instance: str = "lb-test",
    hwm: str = "",
    updated_at: str | None = None,
) -> ReadState:
    """Shorthand builder for ``ReadState`` test fixtures.

    Mirrors ``_make_channel``'s ergonomics. If ``updated_at`` is None,
    populates with a current UTC ISO-8601 string so tests don't have to
    hand-craft a timestamp for every state.
    """
    if updated_at is None:
        updated_at = "2026-05-27T14:31:02.456789+00:00"
    return ReadState(
        sender_label=sender,
        instance_id=instance,
        high_water_mark=hwm,
        updated_at=updated_at,
    )


# Reused bad-label catalog. Mirrors 2b's TestChannelNameValidation cases
# verbatim — sender labels share the channel-name path-safety boundary
# (K2, ADR-028), so the rejected set is identical. Drift between 3a/3b
# rejection contracts would confuse future readers.
_BAD_SENDER_LABELS: list[str] = [
    "../etc",       # path traversal
    "foo/bar",      # path separator
    "FOO",          # uppercase
    "foo bar",      # whitespace
    "",             # empty
    "-rf",          # leading dash
    "_internal",    # leading underscore
    "claude.a",     # dot
    "lt-ąčę",       # non-ASCII
]


class TestReadState:
    """Frozen-dataclass shape — fields, immutability, equality, hashability,
    ``to_dict`` shape, empty-sentinel acceptance.
    """

    def test_fields_populated(self) -> None:
        st = ReadState(
            sender_label="claude-a",
            instance_id="lb-20260527T143000Z-7f3a9c",
            high_water_mark="msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6",
            updated_at="2026-05-27T14:31:02.456789+00:00",
        )
        assert st.sender_label == "claude-a"
        assert st.instance_id == "lb-20260527T143000Z-7f3a9c"
        assert st.high_water_mark.startswith("msg-")
        assert st.updated_at.endswith("+00:00")

    def test_is_frozen(self) -> None:
        st = _make_state()
        with pytest.raises(dataclasses.FrozenInstanceError):
            st.sender_label = "claude-b"  # type: ignore[misc]

    def test_equality(self) -> None:
        a = _make_state()
        b = _make_state()
        assert a == b

    def test_inequality_on_field_drift(self) -> None:
        a = _make_state(hwm="msg-a")
        b = _make_state(hwm="msg-b")
        assert a != b

    def test_hashable_goes_in_set(self) -> None:
        a = _make_state()
        b = _make_state()
        c = _make_state(hwm="msg-x")
        assert {a, b, c} == {a, c}

    def test_to_dict_has_exactly_four_keys(self) -> None:
        st = _make_state()
        d = st.to_dict()
        assert set(d) == {"sender_label", "instance_id", "high_water_mark", "updated_at"}

    def test_to_dict_values_match_fields(self) -> None:
        st = _make_state(sender="claude-b", instance="lb-x", hwm="msg-y", updated_at="t")
        d = st.to_dict()
        assert d == {
            "sender_label": "claude-b",
            "instance_id": "lb-x",
            "high_water_mark": "msg-y",
            "updated_at": "t",
        }

    def test_empty_high_water_mark_allowed(self) -> None:
        """Fresh-endpoint sentinel — empty hwm is the "no acknowledge yet" value."""
        st = ReadState(sender_label="a", instance_id="i", high_water_mark="", updated_at="t")
        assert st.high_water_mark == ""

    def test_empty_instance_id_allowed(self) -> None:
        """Synthesized fresh state — instance_id empty until first write."""
        st = ReadState(sender_label="a", instance_id="", high_water_mark="", updated_at="t")
        assert st.instance_id == ""

    def test_direct_construction_skips_label_validation(self) -> None:
        """Validation lives at the I/O surface (read_state/write_read_state),
        not the dataclass — same pattern as ``Channel`` (direct construction
        is the documented escape hatch).
        """
        st = ReadState(sender_label="../etc", instance_id="", high_water_mark="", updated_at="t")
        assert st.sender_label == "../etc"


class TestReadStateFromDict:
    """Strict from_dict — reject unknown / missing / non-string keys with
    vector errors. Errors propagate to ``read_state``'s recovery branch.
    """

    def test_roundtrip_populated(self) -> None:
        st = _make_state(sender="claude-a", instance="lb-x", hwm="msg-y", updated_at="t")
        assert ReadState.from_dict(st.to_dict()) == st

    def test_roundtrip_all_empty(self) -> None:
        """Fresh-sentinel — all four fields empty strings reconstruct identically."""
        st = ReadState(sender_label="", instance_id="", high_water_mark="", updated_at="")
        assert ReadState.from_dict(st.to_dict()) == st

    def test_rejects_non_dict_input(self) -> None:
        with pytest.raises(ValueError) as exc:
            ReadState.from_dict("not a dict")  # type: ignore[arg-type]
        assert "must be a dict" in str(exc.value)

    def test_rejects_unknown_key(self) -> None:
        data = _make_state().to_dict()
        data["extra"] = "x"
        with pytest.raises(ValueError) as exc:
            ReadState.from_dict(data)
        msg = str(exc.value)
        assert "'extra'" in msg
        assert "unknown key" in msg

    @pytest.mark.parametrize(
        "key", ["sender_label", "instance_id", "high_water_mark", "updated_at"]
    )
    def test_rejects_missing_key(self, key: str) -> None:
        data = _make_state().to_dict()
        del data[key]
        with pytest.raises(ValueError) as exc:
            ReadState.from_dict(data)
        msg = str(exc.value)
        assert f"missing key '{key}'" in msg

    @pytest.mark.parametrize(
        "key,bad_value",
        [
            ("sender_label", 42),
            ("instance_id", None),
            ("high_water_mark", 3.14),
            ("updated_at", ["list"]),
        ],
    )
    def test_rejects_non_string_value(self, key: str, bad_value) -> None:
        data = _make_state().to_dict()
        data[key] = bad_value  # type: ignore[assignment]
        with pytest.raises(ValueError) as exc:
            ReadState.from_dict(data)
        msg = str(exc.value)
        assert f"'{key}'" in msg
        assert "must be str" in msg
        assert type(bad_value).__name__ in msg

    def test_rejects_bool_value(self) -> None:
        """``isinstance(True, str)`` is False — bools naturally rejected
        without special-casing, but the contract is worth pinning down.
        """
        data = _make_state().to_dict()
        data["high_water_mark"] = True  # type: ignore[assignment]
        with pytest.raises(ValueError) as exc:
            ReadState.from_dict(data)
        msg = str(exc.value)
        assert "'high_water_mark'" in msg
        assert "bool" in msg

    def test_vector_error_includes_expected_keys(self) -> None:
        """Framework P3 — unknown-key error names the rule (the expected set)."""
        data = _make_state().to_dict()
        data["weird"] = "x"
        with pytest.raises(ValueError) as exc:
            ReadState.from_dict(data)
        for expected in ("sender_label", "instance_id", "high_water_mark", "updated_at"):
            assert expected in str(exc.value)


class TestReadStateRead:
    """``read_state`` — missing-file fresh-state, no read side effects,
    valid-file roundtrip, corruption recovery, sender-label validation.
    """

    def test_missing_read_dir_returns_fresh(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        # Confirm the precondition: .read/ does not exist yet.
        assert not (ch.path / ".read").exists()

        st = read_state(ch, "claude-a")

        assert st.sender_label == "claude-a"
        assert st.high_water_mark == ""
        assert st.instance_id == ""
        # updated_at is a parseable ISO-8601 string in UTC.
        parsed = datetime.fromisoformat(st.updated_at)
        assert parsed.utcoffset().total_seconds() == 0

    def test_missing_read_dir_does_not_create_it(self, tmp_letterbox_home: Path) -> None:
        """G5 — read NEVER creates the .read/ subdirectory."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        read_state(ch, "claude-a")
        assert not (ch.path / ".read").exists()

    def test_missing_file_in_existing_read_dir_returns_fresh(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        (ch.path / ".read").mkdir(mode=0o700)
        # No claude-a.json file inside .read/ — should still return fresh.
        st = read_state(ch, "claude-a")
        assert st.high_water_mark == ""
        assert st.sender_label == "claude-a"

    def test_existing_valid_file_roundtrip(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        original = _make_state(
            sender="claude-a",
            instance="lb-20260527T143000Z-7f3a9c",
            hwm="msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6",
            updated_at="2026-05-27T14:31:02.456789+00:00",
        )
        write_read_state(ch, original)
        loaded = read_state(ch, "claude-a")
        assert loaded == original

    def test_corrupted_non_json_renames_to_broken_and_logs_warn(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        read_dir = ch.path / ".read"
        read_dir.mkdir(mode=0o700)
        state_file = read_dir / "claude-a.json"
        state_file.write_text("not json", encoding="utf-8")

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            st = read_state(ch, "claude-a")

        assert st.high_water_mark == ""
        assert st.sender_label == "claude-a"
        # Original file was renamed away, not deleted.
        assert not state_file.exists()
        broken_files = list(read_dir.glob("claude-a.json.broken.*"))
        assert len(broken_files) == 1
        # L8 — original bytes preserved on disk.
        assert broken_files[0].read_text(encoding="utf-8") == "not json"
        assert "corrupted" in caplog.text

    def test_corrupted_missing_key_renames_to_broken(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Valid JSON but failing strict from_dict shape check also triggers recovery."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        read_dir = ch.path / ".read"
        read_dir.mkdir(mode=0o700)
        state_file = read_dir / "claude-a.json"
        # Valid JSON but missing 'updated_at'.
        state_file.write_text(
            '{"sender_label": "claude-a", "instance_id": "x", "high_water_mark": "y"}',
            encoding="utf-8",
        )

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            st = read_state(ch, "claude-a")

        assert st.high_water_mark == ""
        assert not state_file.exists()
        broken_files = list(read_dir.glob("claude-a.json.broken.*"))
        assert len(broken_files) == 1

    def test_broken_filename_format_portable(self, tmp_letterbox_home: Path) -> None:
        """Suffix is colon-stripped ISO-8601 with Z, microsecond-precise."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        read_dir = ch.path / ".read"
        read_dir.mkdir(mode=0o700)
        (read_dir / "claude-a.json").write_text("garbage", encoding="utf-8")
        read_state(ch, "claude-a")
        broken = next(iter(read_dir.glob("claude-a.json.broken.*")))
        # Format: claude-a.json.broken.YYYYMMDDTHHMMSSffffffZ — no colons.
        suffix = broken.name.split(".broken.", 1)[1]
        assert ":" not in suffix
        assert re.fullmatch(r"\d{8}T\d{6}\d{6}Z", suffix), suffix

    def test_consecutive_corruption_produces_sortable_broken_names(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """Two corruptions on the same endpoint produce sortable filenames."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        read_dir = ch.path / ".read"
        read_dir.mkdir(mode=0o700)
        state_file = read_dir / "claude-a.json"

        state_file.write_text("garbage 1", encoding="utf-8")
        read_state(ch, "claude-a")
        # Small sleep guarantees microsecond drift between the two recoveries.
        # (datetime.now(timezone.utc) microseconds may collide if same wall clock tick.)
        import time as _t

        _t.sleep(0.001)
        state_file.write_text("garbage 2", encoding="utf-8")
        read_state(ch, "claude-a")

        broken_files = sorted(read_dir.glob("claude-a.json.broken.*"))
        assert len(broken_files) == 2
        # Sorted order = chronological order (lex sort works on the suffix format).
        assert broken_files[0].read_text(encoding="utf-8") == "garbage 1"
        assert broken_files[1].read_text(encoding="utf-8") == "garbage 2"

    @pytest.mark.parametrize("label", _BAD_SENDER_LABELS)
    def test_rejects_invalid_sender_label(
        self,
        tmp_letterbox_home: Path,
        label: str,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        with pytest.raises(ValueError) as exc:
            read_state(ch, label)
        msg = str(exc.value)
        assert repr(label) in msg
        assert "[a-z0-9]" in msg

    @pytest.mark.parametrize("bad", [42, None, b"claude-a", ["x"]])
    def test_rejects_non_str_sender_label(
        self,
        tmp_letterbox_home: Path,
        bad,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        with pytest.raises(TypeError) as exc:
            read_state(ch, bad)  # type: ignore[arg-type]
        assert "sender_label" in str(exc.value)
        assert "str" in str(exc.value)

    def test_validation_fires_before_filesystem_touch(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """A malicious label must not reach any disk operation."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        with pytest.raises(ValueError):
            read_state(ch, "../etc")
        # .read/ should not have been side-effected into existence.
        assert not (ch.path / ".read").exists()


class TestReadStateWrite:
    """``write_read_state`` — .read/ auto-creation with 0o700 + umask defense,
    sort-keys + ensure_ascii byte-identity, atomic-rename partial-write
    invisibility, multi-endpoint isolation, restart-with-same-label.
    """

    def test_creates_read_dir_with_mode_0o700(
        self,
        tmp_letterbox_home: Path,
        permissive_umask: None,
    ) -> None:
        """Under permissive umask, .read/ must still land at 0o700.

        Proves the explicit ``os.chmod`` ran — without it,
        ``mkdir(mode=0o700)`` under ``umask(0)`` lands the mode literally
        and the test passes for the wrong reason.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        write_read_state(ch, _make_state())
        read_dir = ch.path / ".read"
        assert read_dir.is_dir()
        assert oct(read_dir.stat().st_mode & 0o777) == "0o700"

    def test_re_write_does_not_change_read_dir_mode(
        self,
        tmp_letterbox_home: Path,
        permissive_umask: None,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        write_read_state(ch, _make_state())
        # Manually loosen the mode; next write should restore 0o700.
        read_dir = ch.path / ".read"
        os.chmod(read_dir, 0o755)
        write_read_state(ch, _make_state())
        assert oct(read_dir.stat().st_mode & 0o777) == "0o700"

    def test_writes_under_correct_path(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        path = write_read_state(ch, _make_state(sender="claude-a"))
        assert path == ch.path / ".read" / "claude-a.json"
        assert path.exists()

    def test_byte_identical_for_identical_input(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """ADR-030 — sort_keys=True makes the on-disk bytes deterministic."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        st = _make_state()
        path = write_read_state(ch, st)
        bytes_a = path.read_bytes()
        write_read_state(ch, st)
        bytes_b = path.read_bytes()
        assert bytes_a == bytes_b

    def test_keys_sorted_alphabetically_on_disk(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        path = write_read_state(ch, _make_state())
        text = path.read_text(encoding="utf-8")
        # Sorted alphabetically: high_water_mark, instance_id, sender_label, updated_at.
        idx_hwm = text.find('"high_water_mark"')
        idx_inst = text.find('"instance_id"')
        idx_sender = text.find('"sender_label"')
        idx_updated = text.find('"updated_at"')
        assert 0 <= idx_hwm < idx_inst < idx_sender < idx_updated

    def test_partial_write_invisibility(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_letterbox_home: Path,
    ) -> None:
        """Atomic-rename — failure mid-write leaves .json.tmp behind, never
        the final .json. Mirrors 2c's TestWriteMessage.test_partial_write_invisibility.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")

        def failing_replace(src, dst, /):
            raise RuntimeError("simulated rename failure")

        monkeypatch.setattr(os, "replace", failing_replace)
        with pytest.raises(RuntimeError, match="simulated rename failure"):
            write_read_state(ch, _make_state(sender="claude-a"))

        read_dir = ch.path / ".read"
        tmp_path = read_dir / "claude-a.json.tmp"
        final_path = read_dir / "claude-a.json"
        assert tmp_path.exists(), ".tmp must persist on failure"
        assert not final_path.exists(), "final .json must not appear on failure"

    def test_tmp_suffix_order_is_json_tmp(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_letterbox_home: Path,
    ) -> None:
        """Suffix is .json.tmp (NOT .tmp.json) — matches 2c's write_message."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")

        def failing_replace(src, dst, /):
            raise RuntimeError("simulated rename failure")

        monkeypatch.setattr(os, "replace", failing_replace)
        with pytest.raises(RuntimeError):
            write_read_state(ch, _make_state(sender="claude-a"))

        leftover = list((ch.path / ".read").iterdir())
        assert len(leftover) == 1
        assert re.fullmatch(r"claude-a\.json\.tmp", leftover[0].name)

    def test_round_trip_write_then_read(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        st = _make_state(hwm="msg-y", updated_at="2026-05-27T14:31:02.456789+00:00")
        write_read_state(ch, st)
        loaded = read_state(ch, st.sender_label)
        assert loaded == st

    def test_fsync_true_succeeds(self, tmp_letterbox_home: Path) -> None:
        """Smoke — can't observe fsync from userspace, but the path must not raise."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        path = write_read_state(ch, _make_state(), fsync=True)
        assert path.exists()

    def test_multi_endpoint_isolation(self, tmp_letterbox_home: Path) -> None:
        """ADR-021 made real — two endpoints share one channel, each owns
        their own file; one peer's write does not touch the other's state.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        st_a = _make_state(sender="claude-a", instance="lb-a", hwm="msg-a")
        st_b = _make_state(sender="claude-b", instance="lb-b", hwm="msg-b")
        write_read_state(ch, st_a)
        write_read_state(ch, st_b)

        loaded_a = read_state(ch, "claude-a")
        loaded_b = read_state(ch, "claude-b")
        assert loaded_a == st_a
        assert loaded_b == st_b

        # Two separate files on disk.
        read_dir = ch.path / ".read"
        assert (read_dir / "claude-a.json").exists()
        assert (read_dir / "claude-b.json").exists()

    def test_restart_with_same_label(self, tmp_letterbox_home: Path) -> None:
        """Across-restart durability — same sender_label keyed file is
        overwritten cleanly by a new instance_id and new high_water_mark.

        Proves the durable-identity boundary (sender_label) is independent
        of the process-identity boundary (instance_id).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        old = _make_state(
            sender="claude-a",
            instance="lb-old-aaa",
            hwm="msg-old",
            updated_at="2026-05-27T10:00:00+00:00",
        )
        write_read_state(ch, old)

        new = _make_state(
            sender="claude-a",
            instance="lb-new-bbb",
            hwm="msg-new",
            updated_at="2026-05-27T15:00:00+00:00",
        )
        write_read_state(ch, new)

        loaded = read_state(ch, "claude-a")
        assert loaded.instance_id == "lb-new-bbb"
        assert loaded.high_water_mark == "msg-new"
        assert loaded.updated_at == "2026-05-27T15:00:00+00:00"

    def test_sequential_writes_atomic(self, tmp_letterbox_home: Path) -> None:
        """Two sequential writes — second wins; file is byte-for-byte the
        second state. No half-merged content (atomic-rename guarantee).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        first = _make_state(hwm="first")
        second = _make_state(hwm="second")
        write_read_state(ch, first)
        path = write_read_state(ch, second)
        on_disk = json.loads(path.read_bytes())
        assert on_disk == second.to_dict()

    @pytest.mark.parametrize("label", _BAD_SENDER_LABELS)
    def test_rejects_invalid_sender_label_in_state(
        self,
        tmp_letterbox_home: Path,
        label: str,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        st = ReadState(sender_label=label, instance_id="i", high_water_mark="", updated_at="t")
        with pytest.raises(ValueError) as exc:
            write_read_state(ch, st)
        msg = str(exc.value)
        assert repr(label) in msg
        assert "[a-z0-9]" in msg

    def test_rejects_non_str_sender_label(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        # Bypass the dataclass annotation to inject a non-str; the
        # frozen dataclass does not enforce types at runtime.
        st = ReadState(
            sender_label=42,  # type: ignore[arg-type]
            instance_id="i",
            high_water_mark="",
            updated_at="t",
        )
        with pytest.raises(TypeError) as exc:
            write_read_state(ch, st)
        assert "sender_label" in str(exc.value)
        assert "str" in str(exc.value)

    def test_writing_empty_high_water_mark_valid(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """A freshly-acknowledged endpoint that has caught up to nothing
        yet — useful boundary for 3c's first-call behaviour.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        st = _make_state(hwm="")
        path = write_read_state(ch, st)
        assert path.exists()
        assert read_state(ch, st.sender_label) == st

    def test_validation_fires_before_read_dir_creation(
        self,
        tmp_letterbox_home: Path,
    ) -> None:
        """A bad label must not reach mkdir — .read/ stays absent."""
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        st = ReadState(sender_label="../etc", instance_id="i", high_water_mark="", updated_at="t")
        with pytest.raises(ValueError):
            write_read_state(ch, st)
        assert not (ch.path / ".read").exists()


# ──────────────────────────────────────────────────────────────
# Phase 3c — acknowledge + Unread Query Logic
# ──────────────────────────────────────────────────────────────


def _make_msg(
    *,
    sender: str,
    instance_id: str,
    msg_id: str = "msg-bench",
    body: str = "hi",
    channel: str = "ch01",
) -> Message:
    """Build a minimal ``Message`` for ``_is_own_write`` unit tests.

    Bypasses :func:`new_message` (which rejects empty sender) so the
    defensive empty-string matrix cases can be exercised at the unit
    level. The four fields ``_is_own_write`` reads are ``sender`` and
    ``instance_id``; the rest are populated with literal sentinels.
    """
    return Message(
        schema_version=1,
        id=msg_id,
        channel=channel,
        address="file://local",
        instance_id=instance_id,
        sender=sender,
        recipient=None,
        timestamp="2026-05-27T00:00:00+00:00",
        body=body,
        in_reply_to=None,
        metadata=Metadata(encryption=None, ext={}),
    )


def _populate_peer_msgs(
    channel: Channel,
    *,
    peer_sender: str = "claude-b",
    peer_instance: str = "lb-peer",
    count: int = 5,
    base: datetime | None = None,
    start_offset_us: int = 0,
) -> list[str]:
    """Write ``count`` peer messages to ``channel``; return ordered id stems.

    Mirrors the 2d ``bench_channel_1k`` shape verbatim — uses
    :func:`make_message_filename` with microsecond offsets from ``base``
    so filename lexical sort equals chronological order (ADR-017). Bodies
    are ``"peer-{i}"`` for cheap inspection.

    Returns the list of stems in the order they were written (also the
    lexical-sort order). Callers can index this list to pick a specific
    high-water-mark boundary.
    """
    if base is None:
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
    stems: list[str] = []
    for i in range(count):
        stem = make_message_filename(
            base + timedelta(microseconds=start_offset_us + i)
        ).removesuffix(".json")
        msg = new_message(
            id=stem,
            channel=channel.name,
            instance_id=peer_instance,
            sender=peer_sender,
            body=f"peer-{i}",
        )
        write_message(channel.path, msg)
        stems.append(stem)
    return stems


class TestIsOwnWrite:
    """ADR-022 combined own-write filter: ``(sender == self_sender) OR (instance_id == self_instance_id)``.

    The four-cell matrix (own × peer × same-session × cross-restart) is
    the heart of letterbox's dialogue-mechanics correctness — getting
    any cell wrong either floods the agent with its own writes
    (cross-restart half missing) or deadlocks two same-label endpoints
    (same-harness half missing). The trailing two tests are defensive:
    empty identity strings (configuration error) must NOT wildcard-match.
    """

    def test_same_session_same_label(self) -> None:
        """Same sender, same instance → own (both halves match)."""
        msg = _make_msg(sender="alice", instance_id="lb-X")
        assert _is_own_write(msg, "alice", "lb-X") is True

    def test_same_session_diff_label(self) -> None:
        """Different sender, same instance → own (instance_id half catches
        the same-harness configuration-error case — two tabs that both
        sent ``--as`` defaults must not deadlock).
        """
        msg = _make_msg(sender="bob", instance_id="lb-X")
        assert _is_own_write(msg, "alice", "lb-X") is True

    def test_cross_restart_same_label(self) -> None:
        """Same sender, different instance → own (sender half catches
        cross-restart self-recognition — a restarted letterbox process
        must still recognise its own historical writes).
        """
        msg = _make_msg(sender="alice", instance_id="lb-OLD")
        assert _is_own_write(msg, "alice", "lb-NEW") is True

    def test_cross_session_diff_label(self) -> None:
        """Different sender, different instance → peer (neither half matches)."""
        msg = _make_msg(sender="bob", instance_id="lb-OLD")
        assert _is_own_write(msg, "alice", "lb-NEW") is False

    def test_empty_self_sender_does_not_match(self) -> None:
        """G5 defensive — empty sender on both sides must NOT match.

        If both endpoints accidentally have empty sender labels (config
        error caught at a higher layer), ``_is_own_write`` must not
        silently classify every message as own and starve the agent.
        """
        msg = _make_msg(sender="", instance_id="lb-X")
        assert _is_own_write(msg, "", "lb-OTHER") is False

    def test_empty_self_instance_does_not_match(self) -> None:
        """G5 defensive — empty instance_id on both sides must NOT match."""
        msg = _make_msg(sender="alice", instance_id="")
        assert _is_own_write(msg, "bob", "") is False

    def test_case_sensitive_sender(self) -> None:
        """ADR-022 data-level join keys — no case folding, no whitespace strip."""
        msg = _make_msg(sender="alice", instance_id="lb-X")
        assert _is_own_write(msg, "Alice", "lb-OTHER") is False


class TestAcknowledge:
    """``Channel.acknowledge`` — read-modify-write idempotency, monotonic hwm,
    ``.read/`` auto-creation, per-agent isolation, message-file immutability.
    """

    def test_advances_hwm_when_message_id_is_newer(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        write_read_state(
            ch,
            _make_state(
                sender="claude-a",
                instance="lb-prev",
                hwm="msg-001",
                updated_at="2026-05-27T10:00:00+00:00",
            ),
        )
        ch.acknowledge("msg-002", self_instance_id="lb-X")
        loaded = read_state(ch, "claude-a")
        assert loaded.high_water_mark == "msg-002"
        assert loaded.instance_id == "lb-X"
        # updated_at advanced (no longer the pre-write literal).
        assert loaded.updated_at != "2026-05-27T10:00:00+00:00"

    def test_does_not_lower_hwm_when_message_id_is_older(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Idempotency for replays: hwm is monotonic non-decreasing.

        K6 — ``max(current, message_id)`` clamp. ``updated_at`` and
        ``instance_id`` still refresh on a no-op write (the caller did
        write, and the file's "last writer wins" — but the hwm is locked).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        write_read_state(
            ch, _make_state(sender="claude-a", hwm="msg-005")
        )
        ch.acknowledge("msg-002", self_instance_id="lb-X")
        loaded = read_state(ch, "claude-a")
        assert loaded.high_water_mark == "msg-005"

    def test_idempotent_same_id(self, tmp_letterbox_home: Path) -> None:
        """Acknowledge twice with same id: file shape stable, hwm unchanged."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        ch.acknowledge("msg-X", self_instance_id="lb-X")
        first = read_state(ch, "claude-a")
        ch.acknowledge("msg-X", self_instance_id="lb-X")
        second = read_state(ch, "claude-a")
        assert first.high_water_mark == second.high_water_mark == "msg-X"
        assert first.instance_id == second.instance_id == "lb-X"

    def test_first_ever_acknowledge_creates_read_state_file(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Fresh endpoint: no ``.read/`` directory pre-call; file present after."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        assert not (ch.path / ".read").exists()
        ch.acknowledge("msg-A", self_instance_id="lb-X")
        state_file = ch.path / ".read" / "claude-a.json"
        assert state_file.exists()
        on_disk = json.loads(state_file.read_bytes())
        assert on_disk["high_water_mark"] == "msg-A"
        assert on_disk["instance_id"] == "lb-X"
        assert on_disk["sender_label"] == "claude-a"

    def test_creates_read_dir_on_first_use(
        self,
        tmp_letterbox_home: Path,
        permissive_umask: None,
    ) -> None:
        """G7 — ``.read/`` lands at 0o700 even under permissive umask
        (3b's ``write_read_state`` re-chmods after mkdir).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        ch.acknowledge("msg-A", self_instance_id="lb-X")
        read_dir = ch.path / ".read"
        assert read_dir.is_dir()
        assert oct(read_dir.stat().st_mode & 0o777) == "0o700"

    def test_uses_provided_instance_id_verbatim(
        self, tmp_letterbox_home: Path
    ) -> None:
        """3c does NOT validate instance_id shape — launcher's contract."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        ch.acknowledge("msg-A", self_instance_id="lb-1")
        assert read_state(ch, "claude-a").instance_id == "lb-1"
        ch.acknowledge("msg-B", self_instance_id="anything-the-launcher-wants")
        assert (
            read_state(ch, "claude-a").instance_id
            == "anything-the-launcher-wants"
        )

    def test_message_id_with_json_extension_is_caller_bug_not_silent_drop(
        self, tmp_letterbox_home: Path
    ) -> None:
        """3c trusts the caller; MCP layer 7c validates inputs.

        If a caller passes ``"msg-X.json"`` (a filename, not a stem), the
        marker stores it verbatim. Documented contract — the MCP boundary
        in 7c is where regex validation lives.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        ch.acknowledge("msg-X.json", self_instance_id="lb-X")
        assert read_state(ch, "claude-a").high_water_mark == "msg-X.json"

    def test_does_not_touch_peer_read_state(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-021 per-agent isolation: A's acknowledge never touches B.json."""
        ch_a = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        # Pre-write claude-b's read-state.
        write_read_state(
            ch_a,
            _make_state(
                sender="claude-b",
                instance="lb-bob",
                hwm="msg-PEER",
                updated_at="2026-05-27T12:00:00+00:00",
            ),
        )
        peer_file = ch_a.path / ".read" / "claude-b.json"
        before = peer_file.read_bytes()

        ch_a.acknowledge("msg-XX", self_instance_id="lb-X")

        after = peer_file.read_bytes()
        assert before == after, "claude-b.json must be byte-identical"
        assert json.loads(after)["sender_label"] == "claude-b"

    def test_does_not_delete_or_modify_message_files(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-021: acknowledge is a marker advance, not a file mutation.

        Message files stay in the live channel directory for the peer's view.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        before_bytes = {
            stem: (ch.path / f"{stem}.json").read_bytes() for stem in stems
        }
        for stem in stems:
            ch.acknowledge(stem, self_instance_id="lb-X")
        for stem, expected in before_bytes.items():
            path = ch.path / f"{stem}.json"
            assert path.exists()
            assert path.read_bytes() == expected

    def test_self_instance_id_is_keyword_only(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K1 — ``self_instance_id`` must be keyword-only; positional fails."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        with pytest.raises(TypeError):
            ch.acknowledge("msg-X", "lb-X")  # type: ignore[misc]


class TestListUnread:
    """``Channel.list_unread`` — peer-only filtering via combined own-write
    filter, ``high_water_mark`` cursor, ``since_id`` override semantics,
    ``limit`` clamping with vector ``limit_warning``, ParseError surfacing,
    FileNotFoundError race silently skipped, ``has_more`` pagination.
    """

    # --- Filter logic ------------------------------------------------------

    def test_empty_channel_returns_empty_result(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        result = ch.list_unread(self_instance_id="lb-X")
        assert isinstance(result, UnreadResult)
        assert result.messages == []
        assert result.parse_errors == []
        assert result.has_more is False
        assert result.limit_warning is None

    def test_returns_only_peer_messages(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        # 5 own writes (sender + instance match).
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a",
            peer_instance="lb-X",
            count=5,
            base=base,
            start_offset_us=0,
        )
        # 5 peer writes (interleaved by timestamp).
        peer_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=5,
            base=base,
            start_offset_us=100,
        )
        result = ch.list_unread(self_instance_id="lb-X")
        assert [m.id for m in result.messages] == peer_stems
        assert all(m.sender == "claude-b" for m in result.messages)

    def test_filters_own_writes_by_sender_half(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Cross-restart self-recognition — different instance_id, same sender."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        # 3 own from a past process (sender match, different instance).
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a",
            peer_instance="lb-OLD",
            count=3,
            base=base,
            start_offset_us=0,
        )
        peer_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=2,
            base=base,
            start_offset_us=100,
        )
        result = ch.list_unread(self_instance_id="lb-NEW")
        assert [m.id for m in result.messages] == peer_stems

    def test_filters_own_writes_by_instance_id_half(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Same-harness deadlock prevention — different sender, same instance."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        # 3 own — same instance_id, different sender label.
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a2",
            peer_instance="lb-X",
            count=3,
            base=base,
            start_offset_us=0,
        )
        peer_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=2,
            base=base,
            start_offset_us=100,
        )
        result = ch.list_unread(self_instance_id="lb-X")
        assert [m.id for m in result.messages] == peer_stems

    def test_returns_messages_in_lexical_order(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Filename-as-chronological-sort (ADR-017) — output is always sorted."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=4
        )
        result = ch.list_unread(self_instance_id="lb-X")
        observed = [m.id for m in result.messages]
        assert observed == sorted(stems)

    # --- high_water_mark filtering ----------------------------------------

    def test_excludes_messages_at_or_below_hwm(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=5
        )
        # Pre-write hwm to the 3rd stem; expect msgs at indices 3..4.
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[2]))
        result = ch.list_unread(self_instance_id="lb-X")
        assert [m.id for m in result.messages] == stems[3:]

    def test_hwm_comparison_uses_stem_not_filename(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G1 — ``path.stem`` (no ``.json``) compared to ``hwm`` (stored stem)."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=3
        )
        # If we accidentally compared ``path.name`` to the stem (which
        # has no ``.json``), then ``"msg-X.json" > "msg-X"`` lexically
        # and ALL messages including the boundary would appear unread.
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[0]))
        result = ch.list_unread(self_instance_id="lb-X")
        observed = [m.id for m in result.messages]
        # Boundary stem itself is excluded (strict greater).
        assert stems[0] not in observed
        assert observed == stems[1:]

    def test_no_read_state_file_returns_full_peer_backlog(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Fresh endpoint (hwm == "") — every peer message is unread."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=4
        )
        assert not (ch.path / ".read").exists()
        result = ch.list_unread(self_instance_id="lb-X")
        assert [m.id for m in result.messages] == stems

    # --- since_id override (K3) -------------------------------------------

    def test_since_id_overrides_hwm_lower(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K3 — ``since_id`` is an override, not a floor.

        hwm=stems[3]; since_id=stems[0] → returns stems[1:], i.e. messages
        the marker would have excluded.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=5
        )
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[3]))
        result = ch.list_unread(
            self_instance_id="lb-X", since_id=stems[0]
        )
        assert [m.id for m in result.messages] == stems[1:]

    def test_since_id_overrides_hwm_higher(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=5
        )
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[0]))
        result = ch.list_unread(
            self_instance_id="lb-X", since_id=stems[3]
        )
        assert [m.id for m in result.messages] == stems[4:]

    def test_since_id_does_not_update_marker(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-012: ``since_id`` query NEVER advances the marker."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=5
        )
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[3]))
        ch.list_unread(self_instance_id="lb-X", since_id=stems[0])
        loaded = read_state(ch, "claude-a")
        assert loaded.high_water_mark == stems[3]

    def test_since_id_empty_string_falls_through_to_hwm(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G9 — ``since_id=""`` behaves identically to ``since_id=None``."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=5
        )
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[2]))
        result = ch.list_unread(self_instance_id="lb-X", since_id="")
        assert [m.id for m in result.messages] == stems[3:]

    def test_since_id_none_uses_hwm(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=5
        )
        write_read_state(ch, _make_state(sender="claude-a", hwm=stems[2]))
        result = ch.list_unread(self_instance_id="lb-X", since_id=None)
        assert [m.id for m in result.messages] == stems[3:]

    # --- limit clamping (K5) ----------------------------------------------

    def test_default_limit_is_20(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=50)
        result = ch.list_unread(self_instance_id="lb-X")
        assert len(result.messages) == 20
        assert result.has_more is True
        assert result.limit_warning is None

    def test_exact_limit_returns_requested_count(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=20)
        result = ch.list_unread(self_instance_id="lb-X", limit=20)
        assert len(result.messages) == 20
        assert result.has_more is False

    def test_under_limit_returns_all(self, tmp_letterbox_home: Path) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=5)
        result = ch.list_unread(self_instance_id="lb-X", limit=20)
        assert len(result.messages) == 5
        assert result.has_more is False

    def test_limit_above_max_clamps_to_100(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=200)
        result = ch.list_unread(self_instance_id="lb-X", limit=500)
        assert len(result.messages) == 100
        assert result.has_more is True
        assert result.limit_warning is not None
        assert "500" in result.limit_warning
        assert "100" in result.limit_warning
        # G6 — no terminating period.
        assert not result.limit_warning.endswith(".")

    @pytest.mark.parametrize("low_limit", [0, -3])
    def test_limit_below_one_clamps_to_one(
        self,
        tmp_letterbox_home: Path,
        low_limit: int,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=5)
        result = ch.list_unread(self_instance_id="lb-X", limit=low_limit)
        assert len(result.messages) == 1
        assert result.has_more is True
        assert result.limit_warning is not None
        assert str(low_limit) in result.limit_warning
        assert "1" in result.limit_warning

    @pytest.mark.parametrize("limit", [1, 20, 100])
    def test_limit_at_boundaries_no_warning(
        self,
        tmp_letterbox_home: Path,
        limit: int,
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=limit)
        result = ch.list_unread(self_instance_id="lb-X", limit=limit)
        assert result.limit_warning is None
        assert len(result.messages) == limit
        assert result.has_more is False

    @pytest.mark.parametrize("bad_limit", ["20", 20.0, True, None])
    def test_non_int_limit_raises_typeerror(
        self,
        tmp_letterbox_home: Path,
        bad_limit,
    ) -> None:
        """K5 / 3a K3 — predicate-owns-truth, consumer-owns-error.

        ``isinstance(True, int)`` is ``True`` in Python, so ``True`` must
        be special-cased (matches 2a ``_require_type`` shape).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        with pytest.raises(TypeError) as exc:
            ch.list_unread(self_instance_id="lb-X", limit=bad_limit)  # type: ignore[arg-type]
        assert "limit" in str(exc.value)

    # --- Robustness (G3/G4) -----------------------------------------------

    def test_parse_error_message_surfaces_in_parse_errors_field(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """G3 — malformed JSON file lands in ``parse_errors``, logs WARN
        (one event = one log line; no dedupe set).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        # One valid peer + one malformed-JSON file with a regex-valid name.
        valid_stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=1)
        bad_stem = make_message_filename(
            datetime(2026, 5, 27, 15, 0, 0, 0, tzinfo=timezone.utc)
        ).removesuffix(".json")
        (ch.path / f"{bad_stem}.json").write_bytes(b"{not valid json")

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            result = ch.list_unread(self_instance_id="lb-X")

        assert [m.id for m in result.messages] == valid_stems
        assert len(result.parse_errors) == 1
        assert result.parse_errors[0].reason.startswith("malformed_json:")
        assert result.parse_errors[0].path.name == f"{bad_stem}.json"
        assert "parse_error" in caplog.text

    def test_file_deleted_mid_pass_is_silently_skipped(
        self,
        tmp_letterbox_home: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """G4 — race with prune: ``FileNotFoundError`` skipped silently.

        Patched ``read_message`` raises ``FileNotFoundError`` for the 2nd
        path in the lexically-sorted list; the remaining messages must
        still surface, with no WARN logged for the skipped one.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        victim_path = ch.path / f"{stems[1]}.json"

        from letterbox import channel as channel_mod

        real_read = channel_mod.read_message

        def fake_read(path: Path):
            if path == victim_path:
                raise FileNotFoundError(str(path))
            return real_read(path)

        monkeypatch.setattr(channel_mod, "read_message", fake_read)

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            result = ch.list_unread(self_instance_id="lb-X")

        observed = [m.id for m in result.messages]
        assert observed == [stems[0], stems[2]]
        assert result.parse_errors == []
        # G4 — no WARN for a normal race.
        assert "parse_error" not in caplog.text

    def test_oversized_file_surfaces_as_parse_error(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Sparse-file pattern (2c IMPLEMENTATION_NOTES) — surfaces as
        ``ParseError(reason="oversized")`` in ``parse_errors``.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stem = make_message_filename(
            datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        ).removesuffix(".json")
        path = ch.path / f"{stem}.json"
        with open(path, "wb") as fp:
            fp.truncate(MAX_BODY_BYTES + 1)
        result = ch.list_unread(self_instance_id="lb-X")
        assert result.messages == []
        assert len(result.parse_errors) == 1
        assert result.parse_errors[0].reason == "oversized"

    # --- has_more correctness ---------------------------------------------

    def test_has_more_true_when_more_unread_exist(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=25)
        result = ch.list_unread(self_instance_id="lb-X", limit=20)
        assert len(result.messages) == 20
        assert result.has_more is True

    def test_has_more_false_when_exactly_limit(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=20)
        result = ch.list_unread(self_instance_id="lb-X", limit=20)
        assert len(result.messages) == 20
        assert result.has_more is False

    def test_has_more_excludes_own_writes(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Own writes never count against the limit OR has_more.

        20 own + 5 peer + limit=10 → all 5 peer messages, has_more=False
        (5 < 10, no more unread once own writes are filtered).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a",
            peer_instance="lb-X",
            count=20,
            base=base,
            start_offset_us=0,
        )
        peer_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=5,
            base=base,
            start_offset_us=100,
        )
        result = ch.list_unread(self_instance_id="lb-X", limit=10)
        assert [m.id for m in result.messages] == peer_stems
        assert result.has_more is False

    def test_has_more_includes_parse_errors_in_count(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Parse errors count against the limit (consume an inbox slot).

        18 valid + 5 malformed peer files, limit=20 → messages=18,
        parse_errors=2 (positions 19, 20), has_more=True (3 errors left).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            count=18,
            base=base,
            start_offset_us=0,
        )
        # 5 malformed peer files — lexically AFTER the valid messages.
        bad_base = base + timedelta(microseconds=1000)
        for i in range(5):
            bad_stem = make_message_filename(
                bad_base + timedelta(microseconds=i)
            ).removesuffix(".json")
            (ch.path / f"{bad_stem}.json").write_bytes(b"{not valid json")

        result = ch.list_unread(self_instance_id="lb-X", limit=20)
        assert len(result.messages) == 18
        assert len(result.parse_errors) == 2
        assert result.has_more is True

    # --- UnreadResult shape -----------------------------------------------

    def test_unread_result_is_frozen_dataclass(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K2 — ``UnreadResult`` is a frozen dataclass; immutable by design."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        result = ch.list_unread(self_instance_id="lb-X")
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.has_more = True  # type: ignore[misc]

    def test_list_unread_kwargs_are_keyword_only(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``self_instance_id``, ``limit``, and ``since_id`` are keyword-only.

        Symmetry with ``Channel.acknowledge`` (3c K1 — rationale) and
        ``channel_info`` (3d K2). The 3c plan's signature line lapsed into
        positional ``self_instance_id`` while K1's rationale text called
        it "required keyword-only"; the Phase 3 integration checkpoint
        reconciled the drift in favour of K1's rationale and 3d's mirror.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        with pytest.raises(TypeError):
            ch.list_unread("lb-X")  # type: ignore[misc]
        with pytest.raises(TypeError):
            ch.list_unread("lb-X", 20)  # type: ignore[misc]


class TestLatestUnread:
    """``Channel.latest_unread`` (7b / K1) — the reverse-scan tail accessor
    behind ``check_latest_message``.

    Distinct from ``list_unread`` (a forward page capped at 100): this
    returns the single NEWEST unread peer message via a reverse scan,
    correct even when unread exceeds 100. Reuses ``_is_own_write`` (so it
    tracks the K7 reconciliation), skips own-writes / parse-errors /
    prune-race-missing files, and never advances ``high_water_mark``.
    K5 diverges from ``list_unread``: parse errors are skipped SILENTLY
    (no per-file WARN in an unbounded scan).
    """

    def test_empty_channel_returns_none(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        assert ch.latest_unread(self_instance_id="lb-X") is None

    def test_returns_none_when_all_own_writes(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Both own-write halves (sender match + instance match) → None."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        # Own by sender half (sender match, different instance).
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a",
            peer_instance="lb-OLD",
            count=2,
            base=base,
            start_offset_us=0,
        )
        # Own by instance half (different sender, instance match).
        _populate_peer_msgs(
            ch,
            peer_sender="claude-other",
            peer_instance="lb-X",
            count=2,
            base=base,
            start_offset_us=100,
        )
        assert ch.latest_unread(self_instance_id="lb-X") is None

    def test_returns_none_when_all_acknowledged(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Every message stem ``<= high_water_mark`` → None (early break)."""
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        # Acknowledge up to the newest — nothing remains unread.
        write_read_state(
            ch, _make_state(sender="claude-a", instance="lb-prev", hwm=stems[-1])
        )
        assert ch.latest_unread(self_instance_id="lb-X") is None

    def test_returns_newest_unread_peer(
        self, tmp_letterbox_home: Path
    ) -> None:
        """The distinguishing test vs ``list_unread``: returns the LAST
        written peer message (newest), not the first.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=5)
        result = ch.latest_unread(self_instance_id="lb-X")
        assert isinstance(result, Message)
        assert result.id == stems[-1]
        assert result.sender == "claude-b"

    def test_returns_newest_with_over_100_unread(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Backlog correctness: ``list_unread(limit=100).messages[-1]`` would
        return the ~100th-oldest; the reverse scan returns the absolute newest.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=101)
        result = ch.latest_unread(self_instance_id="lb-X")
        assert result is not None
        assert result.id == stems[-1]

    def test_skips_own_write_by_sender_half(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Newest is own (sender match, different instance) → returns the
        older peer (reverse scan skips own-writes).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        peer_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=1,
            base=base,
            start_offset_us=0,
        )
        # Newest: own write — same sender label, different (old) instance.
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a",
            peer_instance="lb-OLD",
            count=1,
            base=base,
            start_offset_us=100,
        )
        result = ch.latest_unread(self_instance_id="lb-NEW")
        assert result is not None
        assert result.id == peer_stems[0]

    def test_skips_own_write_by_instance_half(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Newest is own (instance match, different sender) → returns the
        older peer (ADR-022 OR semantic via ``_is_own_write``).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        peer_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=1,
            base=base,
            start_offset_us=0,
        )
        # Newest: own write — different sender, same instance id.
        _populate_peer_msgs(
            ch,
            peer_sender="claude-other",
            peer_instance="lb-X",
            count=1,
            base=base,
            start_offset_us=100,
        )
        result = ch.latest_unread(self_instance_id="lb-X")
        assert result is not None
        assert result.id == peer_stems[0]

    def test_newest_malformed_returns_next_valid_peer_silently(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """K5 — newest file is malformed JSON; the next valid peer is
        returned and NO WARN is logged (contrast ``list_unread``, which
        WARNs per parse error).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        valid_stems = _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            count=1,
            base=base,
            start_offset_us=0,
        )
        # Newest: a regex-valid filename holding malformed JSON.
        bad_stem = make_message_filename(
            base + timedelta(microseconds=100)
        ).removesuffix(".json")
        (ch.path / f"{bad_stem}.json").write_bytes(b"{not valid json")

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            result = ch.latest_unread(self_instance_id="lb-X")

        assert result is not None
        assert result.id == valid_stems[0]
        # K5 — silent skip: no WARN from the channel logger.
        channel_warnings = [
            r
            for r in caplog.records
            if r.name == "letterbox.channel" and r.levelno >= logging.WARNING
        ]
        assert channel_warnings == []

    def test_file_pruned_mid_scan_is_skipped(
        self,
        tmp_letterbox_home: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """G4 — ``read_message`` → ``FileNotFoundError`` for the newest path
        (prune race) is skipped; the next valid peer is returned.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=2)
        victim_path = ch.path / f"{stems[-1]}.json"

        from letterbox import channel as channel_mod

        real_read = channel_mod.read_message

        def fake_read(path: Path):
            if path == victim_path:
                raise FileNotFoundError(str(path))
            return real_read(path)

        monkeypatch.setattr(channel_mod, "read_message", fake_read)

        result = ch.latest_unread(self_instance_id="lb-X")
        assert result is not None
        assert result.id == stems[0]

    def test_does_not_advance_high_water_mark(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Peeking is read-only — the read-state file is byte-identical
        before and after (mirrors ``since_id`` non-advance discipline).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        write_read_state(
            ch, _make_state(sender="claude-a", instance="lb-prev", hwm="")
        )
        state_path = ch.path / ".read" / "claude-a.json"
        before = state_path.read_bytes()

        result = ch.latest_unread(self_instance_id="lb-X")

        assert result is not None  # there ARE unread peers
        assert state_path.read_bytes() == before

    def test_no_read_state_file_is_not_created_on_peek(
        self, tmp_letterbox_home: Path
    ) -> None:
        """A peek on a fresh endpoint does not create the ``.read/`` marker
        (``read_state`` does not write on read; ``latest_unread`` never does).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=1)
        ch.latest_unread(self_instance_id="lb-X")
        assert not (ch.path / ".read" / "claude-a.json").exists()

    def test_latest_unread_kwargs_are_keyword_only(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``self_instance_id`` is keyword-only (mirrors ``acknowledge`` /
        ``list_unread`` / ``channel_info``).
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01", sender="claude-a")
        with pytest.raises(TypeError):
            ch.latest_unread("lb-X")  # type: ignore[misc]


# ──────────────────────────────────────────────────────────────
# Phase 3d — Channel Listing + Channel Info
# ──────────────────────────────────────────────────────────────


class TestChannelSummary:
    """Frozen-dataclass shape — fields, immutability, default."""

    def test_is_frozen(self) -> None:
        summary = ChannelSummary(
            name="ch01",
            path=Path("/tmp/x"),
            last_activity="2026-05-27T14:30:15.123456+00:00",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            summary.name = "ch02"  # type: ignore[misc]

    def test_field_types(self) -> None:
        summary = ChannelSummary(
            name="ch01",
            path=Path("/x/channels/ch01"),
            last_activity="2026-05-27T14:30:15.123456+00:00",
        )
        assert isinstance(summary.name, str)
        assert isinstance(summary.path, Path)
        assert isinstance(summary.last_activity, str)
        # The None-typed branch is exercised by test_last_activity_none_default
        # below; isinstance checking against ``str | None`` requires Python
        # 3.10+ union syntax which is awkward in isinstance.

    def test_last_activity_none_default(self) -> None:
        """K3 — channel-with-zero-msgs surfaces ``None`` rather than fabricating.

        The ``= None`` default lets ``list_channels`` skip the keyword on
        empty channels and lets direct test construction stay minimal.
        """
        summary = ChannelSummary(name="ch01", path=Path("/x"))
        assert summary.last_activity is None


class TestChannelInfo:
    """Frozen-dataclass shape for the ``channel_info`` return type.

    (The function ``channel_info`` itself is covered below in
    ``TestChannelInfoFunction`` — the two-class split disambiguates the
    dataclass shape from the function behavior.)
    """

    def test_is_frozen(self) -> None:
        info = ChannelInfo(
            channel="ch01",
            sender_label="alice",
            recipient_label="bob",
            unread_count=0,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            info.unread_count = 5  # type: ignore[misc]

    def test_field_types(self) -> None:
        info = ChannelInfo(
            channel="ch01",
            sender_label="alice",
            recipient_label="bob",
            unread_count=7,
        )
        assert isinstance(info.channel, str)
        assert isinstance(info.sender_label, str)
        assert isinstance(info.recipient_label, str)
        assert isinstance(info.unread_count, int)

    def test_no_default_values(self) -> None:
        """All four fields are required — no defaults. Constructing without
        ``unread_count`` (the natural typo) raises ``TypeError``.
        """
        with pytest.raises(TypeError):
            ChannelInfo(  # type: ignore[call-arg]
                channel="ch01",
                sender_label="alice",
                recipient_label="bob",
            )


class TestListChannels:
    """``list_channels`` — enumeration, ``last_activity`` derivation, ordering.

    Covers G1 (filtering), G2 (missing dir → empty), G3 (lexically-last
    filename), G7 (helper round-trip), G8 (explicit name-sort), K3 (None
    on empty channel), K5 (filename-derived, NOT mtime), K6 (name-sorted
    output).
    """

    # --- Empty / missing -------------------------------------------------

    def test_missing_channels_dir_returns_empty(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G2 — fresh ``~/.letterbox/`` with no ``channels/`` subdir; ``[]``.

        ``tmp_letterbox_home`` creates ``.letterbox/`` only (per conftest);
        ``channels/`` is materialised lazily by ``Channel.get_or_create``.
        Until then ``list_channels`` must return an empty list, NOT raise.
        """
        assert not (tmp_letterbox_home / "channels").exists()
        assert list_channels(state_dir=tmp_letterbox_home) == []

    def test_empty_channels_dir_returns_empty(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``channels/`` exists but is empty."""
        (tmp_letterbox_home / "channels").mkdir(mode=0o700)
        assert list_channels(state_dir=tmp_letterbox_home) == []

    def test_nonexistent_state_dir_returns_empty(
        self, tmp_path: Path
    ) -> None:
        """§14 implementer's-latitude — ``state_dir`` itself missing also
        returns ``[]`` for symmetry with G2.

        Rationale: the ``letterbox list-channels`` CLI (9b) on a brand-new
        install (``~/.letterbox/`` not yet created) should print an empty
        list, not a stack trace. The G2 check ``(state_dir / "channels").exists()``
        returns ``False`` for a missing ``state_dir`` because ``Path.exists``
        is short-circuit on any missing intermediate.
        """
        nonexistent = tmp_path / "does_not_exist"
        assert list_channels(state_dir=nonexistent) == []

    # --- Single channel --------------------------------------------------

    def test_single_channel_empty_returns_none_last_activity(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K3 — channel directory with zero msg files; ``last_activity`` is ``None``."""
        _make_channel(tmp_letterbox_home, name="ch01")
        result = list_channels(state_dir=tmp_letterbox_home)
        assert len(result) == 1
        assert result[0].name == "ch01"
        assert result[0].last_activity is None

    def test_single_channel_with_msgs_returns_iso_timestamp(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K5 — ``last_activity`` is the ISO form of the newest filename's
        embedded timestamp slice.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        result = list_channels(state_dir=tmp_letterbox_home)
        assert len(result) == 1
        # Reconstruct the expected ISO form independently of the helper.
        expected = (
            datetime.strptime(stems[-1][4:25], "%Y%m%dT%H%M%S%f")
            .replace(tzinfo=timezone.utc)
            .isoformat()
        )
        assert result[0].last_activity == expected

    def test_last_activity_is_filename_derived_not_mtime(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K5 anti-regression — backdate every msg file's mtime and verify
        ``last_activity`` is unchanged.

        Locks the rationale Vision §3.2 documents: ``mtime`` is fragile
        under ``cp -r`` / syncthing / rsync / NFS clock skew / ``tar``;
        filenames are durable.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        # Backdate every msg file's atime+mtime to 1990 — if the
        # implementation accidentally used mtime, last_activity would
        # drift to a 1990 timestamp.
        old = datetime(1990, 1, 1, tzinfo=timezone.utc).timestamp()
        for stem in stems:
            os.utime(ch.path / f"{stem}.json", (old, old))
        result = list_channels(state_dir=tmp_letterbox_home)
        assert result[0].last_activity is not None
        # Filename-derived timestamp: 2026 base (per _populate_peer_msgs default).
        assert result[0].last_activity.startswith("2026-")

    # --- Multiple channels ----------------------------------------------

    def test_multiple_channels_returned_name_sorted(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K6 + G8 — ``os.scandir`` order is non-deterministic on Linux
        (per 2d IMPLEMENTATION_NOTES); ``list_channels`` MUST explicit-sort
        by ``name`` before returning.
        """
        _make_channel(tmp_letterbox_home, name="zebra")
        _make_channel(tmp_letterbox_home, name="alpha")
        _make_channel(tmp_letterbox_home, name="middle")
        result = list_channels(state_dir=tmp_letterbox_home)
        assert [c.name for c in result] == ["alpha", "middle", "zebra"]

    def test_each_channel_has_correct_path(
        self, tmp_letterbox_home: Path
    ) -> None:
        for name in ("ch01", "ch02"):
            _make_channel(tmp_letterbox_home, name=name)
        result = list_channels(state_dir=tmp_letterbox_home)
        for summary in result:
            assert summary.path == (
                tmp_letterbox_home / "channels" / summary.name
            )

    def test_per_channel_last_activity_independent(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Two channels — each summary's ``last_activity`` reflects ITS
        own newest msg.
        """
        ch_a = _make_channel(tmp_letterbox_home, name="ch-a")
        ch_b = _make_channel(tmp_letterbox_home, name="ch-b")
        old_base = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        new_base = datetime(2026, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        _populate_peer_msgs(
            ch_a, peer_sender="claude-b", count=2, base=old_base
        )
        _populate_peer_msgs(
            ch_b, peer_sender="claude-b", count=1, base=new_base
        )
        result = list_channels(state_dir=tmp_letterbox_home)
        by_name = {c.name: c for c in result}
        assert by_name["ch-a"].last_activity is not None
        assert by_name["ch-a"].last_activity.startswith("2025-")
        assert by_name["ch-b"].last_activity is not None
        assert by_name["ch-b"].last_activity.startswith("2026-")

    # --- Filtering (G1) --------------------------------------------------

    def test_skips_non_directory_entries(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G1 — stray files at the ``channels/`` root are not surfaced."""
        _make_channel(tmp_letterbox_home, name="ch01")
        (tmp_letterbox_home / "channels" / ".DS_Store").write_bytes(b"")
        result = list_channels(state_dir=tmp_letterbox_home)
        assert [c.name for c in result] == ["ch01"]

    def test_skips_invalid_channel_names_silently(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G1 — directories whose names fail ``is_valid_channel_name`` are
        silently skipped (no log line, no error).

        ``Channel.get_or_create`` refuses these names at creation time;
        directly mkdir-ing them simulates a hand-edited state dir or a
        stray dir from another tool.
        """
        _make_channel(tmp_letterbox_home, name="valid")
        (tmp_letterbox_home / "channels" / "Foo").mkdir(mode=0o700)
        (tmp_letterbox_home / "channels" / ".hidden").mkdir(mode=0o700)
        (tmp_letterbox_home / "channels" / "-bad").mkdir(mode=0o700)
        result = list_channels(state_dir=tmp_letterbox_home)
        assert [c.name for c in result] == ["valid"]

    def test_cold_subdir_is_listed_as_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Forward-pointing — ``cold`` passes the channel-name regex
        (lowercase, no separator), so a future 9d ``letterbox prune``
        that uses ``state_dir/channels/cold/`` as a sibling archive dir
        would appear here as a "channel".

        This test locks current behaviour; 9d will need to design around
        it (e.g., archive at ``state_dir/cold/`` outside ``channels/``,
        or use a name that fails the regex like ``_cold``).
        """
        (tmp_letterbox_home / "channels" / "cold").mkdir(
            mode=0o700, parents=True
        )
        result = list_channels(state_dir=tmp_letterbox_home)
        assert "cold" in [c.name for c in result]

    # --- Channel-internal directories are NOT walked --------------------

    def test_channel_with_read_subdir_still_listed(
        self, tmp_letterbox_home: Path
    ) -> None:
        """``.read/`` is INSIDE the channel dir, not a sibling at the
        ``channels/`` root; ``list_channels`` iterates one level only.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        # Force .read/<sender>.json into existence via 3b's writer.
        write_read_state(ch, _make_state(sender="claude-a"))
        result = list_channels(state_dir=tmp_letterbox_home)
        assert [c.name for c in result] == ["ch01"]

    def test_channel_with_msg_tmp_files_uses_only_msg_json(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Stale ``.tmp`` orphans are filtered by ``list_messages`` (2c
        regex anchor on ``\\.json$``); ``last_activity`` reflects only
        the valid ``.json`` files even if a ``.tmp`` looks newer by name.
        """
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        _populate_peer_msgs(ch, peer_sender="claude-b", count=2)
        # A "future" .tmp orphan that, if mis-counted, would dominate the
        # lexical-last computation.
        future_base = datetime(2099, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        tmp_name = make_message_filename(future_base) + ".tmp"
        (ch.path / tmp_name).write_bytes(b"junk")
        result = list_channels(state_dir=tmp_letterbox_home)
        assert result[0].last_activity is not None
        assert result[0].last_activity.startswith("2026-")

    # --- G7 lock: filename-to-iso helper round-trip ---------------------

    def test_filename_to_iso_roundtrip(self) -> None:
        """G7 — ``_filename_to_iso_timestamp`` slice indices [4:25] are
        anchored to the ADR-028 regex.

        If 2b's filename format ever drifts, this assertion fires loud
        rather than producing silently-corrupted ``last_activity``
        timestamps in production.
        """
        from letterbox.channel import _filename_to_iso_timestamp

        ts = datetime(2026, 5, 27, 14, 30, 15, 123456, tzinfo=timezone.utc)
        filename = make_message_filename(ts)
        assert _filename_to_iso_timestamp(filename) == ts.isoformat()


class TestChannelInfoFunction:
    """``channel_info`` — true unread count + identity surface.

    Class is intentionally named ``TestChannelInfoFunction`` (not
    ``TestChannelInfo``) to disambiguate from the dataclass-shape class
    above. Plan §9 explicitly calls out the two-class coexistence.
    """

    # --- Identity surface (cheap part) ----------------------------------

    def test_returns_channel_name_from_handle(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(
            tmp_letterbox_home,
            name="debate-01",
            sender="claude-a",
            recipient="claude-b",
        )
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.channel == "debate-01"

    def test_returns_sender_label_from_handle(
        self, tmp_letterbox_home: Path
    ) -> None:
        """§13.3 Join-Key Discipline — ``sender_label`` is server-side
        from the launcher-resolved Channel handle, NEVER from agent input.
        """
        ch = _make_channel(
            tmp_letterbox_home,
            name="ch01",
            sender="alice",
            recipient="bob",
        )
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.sender_label == "alice"

    def test_returns_recipient_label_from_handle(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(
            tmp_letterbox_home,
            name="ch01",
            sender="alice",
            recipient="bob",
        )
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.recipient_label == "bob"

    # --- Unread count (substantive part) --------------------------------

    def test_unread_count_zero_for_empty_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(tmp_letterbox_home, name="ch01")
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 0

    def test_unread_count_all_own_writes_returns_zero(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-022 — own writes (sender match) drop out of the count."""
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        _populate_peer_msgs(
            ch, peer_sender="claude-a", peer_instance="lb-X", count=5
        )
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 0

    def test_unread_count_all_peer_writes_returns_count(
        self, tmp_letterbox_home: Path
    ) -> None:
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        _populate_peer_msgs(ch, peer_sender="claude-b", count=7)
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 7

    def test_unread_count_respects_hwm(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Pre-written ``high_water_mark`` clamps the count.

        10 peer msgs; hwm at ``stems[3]`` → 6 unread (indices 4..9).
        """
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        stems = _populate_peer_msgs(
            ch, peer_sender="claude-b", count=10
        )
        write_read_state(
            ch, _make_state(sender="claude-a", hwm=stems[3])
        )
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 6

    def test_unread_count_filters_own_writes_combined(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-022 combined filter cells together: 3 cross-restart-own
        (sender half) + 2 same-harness-own (instance half) + 4 peer →
        4 unread.
        """
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        # Sender-half match (cross-restart) — different instance_id.
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a",
            peer_instance="lb-OLD",
            count=3,
            base=base,
            start_offset_us=0,
        )
        # Instance-half match (same-harness) — different sender_label.
        _populate_peer_msgs(
            ch,
            peer_sender="claude-a2",
            peer_instance="lb-X",
            count=2,
            base=base,
            start_offset_us=100,
        )
        # Peer — neither half matches.
        _populate_peer_msgs(
            ch,
            peer_sender="claude-b",
            peer_instance="lb-peer",
            count=4,
            base=base,
            start_offset_us=200,
        )
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 4

    def test_unread_count_includes_parse_errors(
        self,
        tmp_letterbox_home: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """G5/K4 — a malformed file with a regex-valid name occupies an
        inbox slot the agent must attend to, so it counts toward unread.
        Mirrors 3c ``list_unread``'s WARN-on-parse-error pattern.
        """
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        bad_stem = make_message_filename(
            datetime(2026, 5, 27, 15, 0, 0, 0, tzinfo=timezone.utc)
        ).removesuffix(".json")
        (ch.path / f"{bad_stem}.json").write_bytes(b"{not valid json")

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            info = channel_info(ch, self_instance_id="lb-X")

        assert info.unread_count == 4  # 3 valid + 1 parse error
        assert "parse_error" in caplog.text

    def test_unread_count_skips_filenotfound_race(
        self,
        tmp_letterbox_home: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """G5 — race with prune (``read_message`` → ``FileNotFoundError``)
        silently skipped, no WARN.

        Per scout brief: patch the ``read_message`` name on the channel
        module (where it is bound at import), NOT on ``protocol`` (the
        channel module already captured the symbol locally).
        """
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        stems = _populate_peer_msgs(ch, peer_sender="claude-b", count=3)
        victim_path = ch.path / f"{stems[1]}.json"

        from letterbox import channel as channel_mod

        real_read = channel_mod.read_message

        def fake_read(path: Path):
            if path == victim_path:
                raise FileNotFoundError(str(path))
            return real_read(path)

        monkeypatch.setattr(channel_mod, "read_message", fake_read)

        with caplog.at_level(logging.WARNING, logger="letterbox.channel"):
            info = channel_info(ch, self_instance_id="lb-X")

        assert info.unread_count == 2
        assert "parse_error" not in caplog.text

    def test_unread_count_includes_oversized(
        self, tmp_letterbox_home: Path
    ) -> None:
        """Oversized files surface as ``ParseError(reason="oversized")``;
        same K4 treatment as malformed JSON — they count toward unread.

        Sparse-file pattern from 2c IMPLEMENTATION_NOTES — no actual 5MB
        buffer is allocated; ``truncate`` makes ``stat().st_size`` report
        the truncated size.
        """
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        _populate_peer_msgs(ch, peer_sender="claude-b", count=2)
        bad_stem = make_message_filename(
            datetime(2026, 5, 27, 15, 0, 0, 0, tzinfo=timezone.utc)
        ).removesuffix(".json")
        with open(ch.path / f"{bad_stem}.json", "wb") as fp:
            fp.truncate(MAX_BODY_BYTES + 1)
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 3

    def test_two_endpoints_see_independent_counts_on_same_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        """ADR-021 per-agent isolation + ADR-022 per-endpoint own-write
        filter. Mirrors the PHASE_INDEX TDD bullet verbatim.

        Setup: 10 from-bob + 3 from-alice on one channel. Alice
        acknowledged bob's first 5 (``hwm = bob_stems[4]``).

        - ``channel_info(alice_handle, "lb-alice")``: bob's last 5 are
          unread (peer, past hwm); alice's 3 own are filtered → 5.
        - ``channel_info(bob_handle, "lb-bob")``: bob's 10 are own
          (filtered); alice's 3 are peer; bob's fresh ``.read/bob.json``
          has empty hwm → 3.
        """
        ch_alice = Channel.get_or_create(
            "ch01", "alice", "bob", state_dir=tmp_letterbox_home
        )
        ch_bob = Channel.get_or_create(
            "ch01", "bob", "alice", state_dir=tmp_letterbox_home
        )
        base = datetime(2026, 5, 27, 14, 0, 0, 0, tzinfo=timezone.utc)
        bob_stems = _populate_peer_msgs(
            ch_bob,
            peer_sender="bob",
            peer_instance="lb-bob",
            count=10,
            base=base,
            start_offset_us=0,
        )
        _populate_peer_msgs(
            ch_alice,
            peer_sender="alice",
            peer_instance="lb-alice",
            count=3,
            base=base,
            start_offset_us=2000,
        )
        write_read_state(
            ch_alice, _make_state(sender="alice", hwm=bob_stems[4])
        )

        alice_info = channel_info(ch_alice, self_instance_id="lb-alice")
        bob_info = channel_info(ch_bob, self_instance_id="lb-bob")
        assert alice_info.unread_count == 5
        assert bob_info.unread_count == 3

    # --- No-cap discipline (K4) -----------------------------------------

    def test_unread_count_returns_true_count_not_capped_at_100(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K4 anti-regression — count is the TRUE count, not
        ``list_unread``-style limited to 100.

        Implementing ``channel_info`` as ``list_unread(limit=10000)``
        would silently return 100 + ``has_more=True``, losing the true
        figure. This test fires loud if that ever happens.
        """
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        _populate_peer_msgs(ch, peer_sender="claude-b", count=150)
        info = channel_info(ch, self_instance_id="lb-X")
        assert info.unread_count == 150

    # --- Failure modes (G9) ---------------------------------------------

    def test_channel_info_raises_filenotfound_for_uncreated_channel(
        self, tmp_letterbox_home: Path
    ) -> None:
        """G9 — direct ``Channel(...)`` construction with a path that does
        not exist (bypassing ``get_or_create``) raises ``FileNotFoundError``
        from ``list_messages`` per the 2c contract.

        Consumer-raises-from-caller pattern (3a K3): the function does NOT
        add a defensive ``.exists()`` check; constructing a Channel with
        a missing path is a caller bug.
        """
        ch = Channel(
            name="missing",
            path=tmp_letterbox_home / "channels" / "missing",
            sender_label="alice",
            recipient_label="bob",
        )
        with pytest.raises(FileNotFoundError):
            channel_info(ch, self_instance_id="lb-X")

    def test_channel_info_self_instance_id_is_keyword_only(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K2 — ``self_instance_id`` is keyword-only (mirrors 3c K1)."""
        ch = _make_channel(
            tmp_letterbox_home, name="ch01", sender="claude-a"
        )
        with pytest.raises(TypeError):
            channel_info(ch, "lb-X")  # type: ignore[misc]

    def test_channel_info_list_channels_state_dir_is_keyword_only(
        self, tmp_letterbox_home: Path
    ) -> None:
        """K1 — ``state_dir`` is keyword-only on ``list_channels``."""
        with pytest.raises(TypeError):
            list_channels(tmp_letterbox_home)  # type: ignore[misc]

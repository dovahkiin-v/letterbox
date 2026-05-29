"""TDD coverage for ``letterbox.notifications`` (Phase 4a).

Four behaviour classes per p4a §9:

* ``TestRenderNotification`` — substitution mechanics, keyword-only signature,
  defense-in-depth re-validation, type guards.
* ``TestValidateTemplate`` — accept/reject matrix for whitelist enforcement.
* ``TestNotificationTemplateError`` — vector-error quality per Framework P3.
* ``TestPublicSurface`` — ``__all__`` lock + module-private discipline (K7).

Pure logic phase: no filesystem, no subprocess, no async. No fixtures from
``conftest.py`` needed. Tests import ``letterbox.notifications`` only.
"""
from __future__ import annotations

import pytest

import letterbox.notifications as notifications_mod
from letterbox.notifications import (
    NotificationTemplateError,
    render_notification,
    validate_template,
)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def _valid_kwargs() -> dict[str, str]:
    """The four standard kwargs for ``render_notification`` — avoids copy-paste."""
    return {
        "channel": "debate-01",
        "sender": "claude-b",
        "message_id": "msg-20260528T010203456789-0123456789abcdef0123456789abcdef",
        "timestamp": "2026-05-28T01:02:03.456789+00:00",
    }


# ──────────────────────────────────────────────────────────────
# TestRenderNotification — substitution roundtrip
# ──────────────────────────────────────────────────────────────


class TestRenderNotification:
    """``render_notification`` substitutes the four whitelisted variables only.

    The signature itself IS the §13.3 Join-Key enforcement (K4) — keyword-only,
    no defaults, no positional path. A peer payload dict cannot be splatted in
    because extra keys raise ``TypeError`` from the function-call protocol.
    """

    def test_substitutes_all_four_variables(self) -> None:
        kw = _valid_kwargs()
        template = "📬 {sender} on {channel} at {timestamp} (id={message_id})"
        result = render_notification(template, **kw)
        assert result == (
            f"📬 {kw['sender']} on {kw['channel']} at {kw['timestamp']} "
            f"(id={kw['message_id']})"
        )

    def test_substitutes_subset_of_variables(self) -> None:
        """The other two kwargs are still required (K4) but may be unused."""
        kw = _valid_kwargs()
        result = render_notification("{sender} → {channel}", **kw)
        assert result == f"{kw['sender']} → {kw['channel']}"

    def test_pure_literal_template_returns_verbatim(self) -> None:
        result = render_notification("Peer wrote on the channel.", **_valid_kwargs())
        assert result == "Peer wrote on the channel."

    def test_empty_template_returns_empty_string(self) -> None:
        assert render_notification("", **_valid_kwargs()) == ""

    def test_escaped_braces_render_as_single_braces(self) -> None:
        """G5 — ``{{`` / ``}}`` are literal-text per ``str.format``, NOT substitution."""
        result = render_notification("{{channel}}", **_valid_kwargs())
        assert result == "{channel}"

    def test_unicode_in_variables_round_trips(self) -> None:
        """Framework P9 multi-language sovereignty — Lithuanian, Cyrillic, CJK,
        emoji must survive verbatim through the renderer (Vision §11.1).
        """
        kw = {
            "channel": "ąčęėįšųūž",
            "sender": "клавде-а",
            "message_id": "msg-中文-emoji-🌍",
            "timestamp": "2026-05-28T01:02:03+00:00",
        }
        result = render_notification("📬 {sender} → {channel} ({message_id})", **kw)
        assert result == f"📬 {kw['sender']} → {kw['channel']} ({kw['message_id']})"

    def test_kwargs_are_keyword_only(self) -> None:
        """K4 — positional invocation forbidden. The signature IS the join-key
        enforcement at the call site; ``TypeError`` is the structural defense.
        """
        with pytest.raises(TypeError):
            render_notification(  # type: ignore[misc]
                "template", "ch", "se", "id", "ts"
            )

    def test_missing_required_kwarg_raises(self) -> None:
        """All four kwargs are required — no defaults (K4)."""
        with pytest.raises(TypeError):
            render_notification(  # type: ignore[call-arg]
                "{channel}", channel="x", sender="y"
            )

    def test_extra_kwarg_raises_type_error(self) -> None:
        """The keyword-only signature rejects extras — a peer payload dict
        cannot be splatted in (``render_notification(t, **msg.to_dict())``
        would carry ``body``, ``id``, ``address``, … and fail loudly).
        """
        with pytest.raises(TypeError):
            render_notification(  # type: ignore[call-arg]
                "{channel}", **_valid_kwargs(), body="malicious peer content"
            )

    def test_render_validates_template_internally(self) -> None:
        """G7 / K3 — re-validate on every call. A caller that skips
        ``validate_template`` upstream is still structurally protected.
        """
        with pytest.raises(NotificationTemplateError):
            render_notification("{body}", **_valid_kwargs())

    def test_render_validates_before_substitution(self) -> None:
        """K3 — validation fires BEFORE ``str.format`` runs. Reject templates
        with attribute access (which ``str.format`` would otherwise silently
        traverse on the trusted-context string, returning e.g.
        ``"<built-in method upper of str object at 0x…>"``).
        """
        with pytest.raises(NotificationTemplateError):
            render_notification("{channel.upper}", **_valid_kwargs())

    def test_render_rejects_non_str_template(self) -> None:
        """G9 — type-guard the template before ``string.Formatter().parse()``
        would raise a cryptic ``AttributeError``.
        """
        with pytest.raises(TypeError):
            render_notification(123, **_valid_kwargs())  # type: ignore[arg-type]

    @pytest.mark.parametrize("field", ["channel", "sender", "message_id", "timestamp"])
    def test_render_rejects_non_str_variable(self, field: str) -> None:
        """G10 — silently coercing non-str via ``__format__`` is the wrong
        cure on a load-bearing security boundary. A ``Path`` object would
        ``__format__`` as a filesystem path; integers as digits. Both are
        surprises the trusted-context contract does not authorize.
        """
        kw = _valid_kwargs()
        kw[field] = 123  # type: ignore[assignment]
        with pytest.raises(TypeError):
            render_notification("{" + field + "}", **kw)


# ──────────────────────────────────────────────────────────────
# TestValidateTemplate — accept/reject matrix
# ──────────────────────────────────────────────────────────────


class TestValidateTemplate:
    """``validate_template`` is the startup-validation gate (K2).

    Mirrors 3a's ``check_state_dir_permissions`` shape: take a thing, raise a
    domain error on fail, return ``None`` on accept. The launcher (Phase 8a)
    composes this with adapter-availability + MCP-config-writability checks.
    """

    # Accept paths ─────────────────────────────────────────────

    def test_accepts_template_with_all_four_variables(self) -> None:
        assert (
            validate_template("{channel} {sender} {message_id} {timestamp}") is None
        )

    def test_accepts_template_with_subset(self) -> None:
        assert validate_template("{channel}") is None

    def test_accepts_pure_literal(self) -> None:
        assert validate_template("Peer wrote.") is None

    def test_accepts_empty_template(self) -> None:
        assert validate_template("") is None

    def test_accepts_escaped_braces(self) -> None:
        """G5 — ``{{`` / ``}}`` surface as literal text, not substitution."""
        assert validate_template("{{not_a_var}}") is None

    @pytest.mark.parametrize(
        "template", ["{channel!r}", "{sender!s}", "{message_id!a}"]
    )
    def test_accepts_conversion_specifiers(self, template: str) -> None:
        """G6 / K6 — ``!r`` / ``!s`` / ``!a`` are type coercions on the
        trusted-context string. No security concern.
        """
        assert validate_template(template) is None

    @pytest.mark.parametrize(
        "template", ["{channel:>20}", "{sender:<10}", "{timestamp:^30}"]
    )
    def test_accepts_format_specs_without_braces(self, template: str) -> None:
        """K6 — alignment/padding on trusted-context strings is fine."""
        assert validate_template(template) is None

    # Reject paths ─────────────────────────────────────────────

    def test_rejects_unknown_variable(self) -> None:
        with pytest.raises(NotificationTemplateError) as exc:
            validate_template("{body}")
        msg = str(exc.value)
        assert "body" in msg
        # All four allowed names cited (G8).
        for allowed in ("channel", "message_id", "sender", "timestamp"):
            assert allowed in msg

    def test_rejects_empty_positional(self) -> None:
        """G2 — ``{}`` parses with ``field_name=""`` which is not in the
        whitelist.
        """
        with pytest.raises(NotificationTemplateError):
            validate_template("{}")

    def test_rejects_numeric_positional(self) -> None:
        """G2 — ``{0}`` / ``{1}`` parse with ``field_name="0"`` / ``"1"``."""
        with pytest.raises(NotificationTemplateError):
            validate_template("{0}")

    def test_rejects_attribute_access(self) -> None:
        """G3 — ``{channel.upper}`` would silently traverse attribute access
        in ``str.format`` (verified via stdlib probe; see K1).
        """
        with pytest.raises(NotificationTemplateError):
            validate_template("{channel.upper}")

    def test_rejects_attribute_access_to_dunder(self) -> None:
        """G3 — defense-in-depth against
        ``{channel.__class__.__bases__}``-style dunder walk.
        """
        with pytest.raises(NotificationTemplateError):
            validate_template("{channel.__class__}")

    def test_rejects_index_access(self) -> None:
        """G3 — ``{channel[0]}`` would silently slice the trusted string."""
        with pytest.raises(NotificationTemplateError):
            validate_template("{channel[0]}")

    def test_rejects_nested_brace_format_spec(self) -> None:
        """G4 / K6 — ``{channel:>{width}}`` is a covert second-name
        substitution channel that bypasses the outer whitelist check.
        K4's keyword-only signature also blocks supplying ``width``,
        but two independent defenses are cheap.
        """
        with pytest.raises(NotificationTemplateError):
            validate_template("{channel:>{width}}")

    def test_rejects_nested_brace_in_format_spec_via_closing_brace(self) -> None:
        """G4 — defensive: reject ``}`` inside ``format_spec`` too, not just ``{``."""
        with pytest.raises(NotificationTemplateError):
            validate_template("{channel:>{width:0>5}}")

    def test_rejects_mixed_valid_and_invalid(self) -> None:
        """A whitelisted name + a forbidden name → rejected, citing only
        the offender. The valid name MUST NOT appear in the offence text
        (would mislead the user toward fixing the wrong thing).
        """
        with pytest.raises(NotificationTemplateError) as exc:
            validate_template("{channel} and {body}")
        msg = str(exc.value)
        assert "body" in msg
        # ``channel`` will appear in the allowed-set list — that is fine. The
        # offence portion of the message must not name it as the rejected
        # variable. We assert structural shape: "body" appears first in the
        # message (the offence), the allowed-set list follows.
        assert msg.index("body") < msg.index("Allowed")

    def test_validate_rejects_non_str_template(self) -> None:
        """G9 — type-guard before ``string.Formatter().parse()``."""
        with pytest.raises(TypeError):
            validate_template(123)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────
# TestNotificationTemplateError — vector message quality (Framework P3)
# ──────────────────────────────────────────────────────────────


class TestNotificationTemplateError:
    """K5 — bare ``Exception`` subclass (not ``ValueError``) so consumers
    catch the domain, not the broad stdlib type. Vector messages name the
    rejected variable AND the allowed set AND a remediation hint.
    """

    def test_error_is_exception_subclass(self) -> None:
        assert issubclass(NotificationTemplateError, Exception)

    def test_error_is_not_value_error_subclass(self) -> None:
        """K5 — bare-``Exception`` convention; mirrors ``StatePermissionsError``
        (3a) / ``ConfigError`` (1c) / ``MessageTooLarge`` (2a).
        """
        assert not issubclass(NotificationTemplateError, ValueError)

    def test_error_message_names_rejected_variable_and_allowed_set(self) -> None:
        """G8 — Framework P3 vectors-not-walls. Both the rejected name and
        each of the four allowed names appear in ``str(err)``.
        """
        with pytest.raises(NotificationTemplateError) as exc:
            validate_template("{forbidden}")
        msg = str(exc.value)
        assert "forbidden" in msg
        for allowed in ("channel", "message_id", "sender", "timestamp"):
            assert allowed in msg

    def test_error_message_lists_allowed_set_in_sorted_order(self) -> None:
        """G11 — ``frozenset`` iteration order is unspecified; sorting locks
        the message form for stable test assertions and stable user reading.
        Alphabetical: channel, message_id, sender, timestamp.
        """
        with pytest.raises(NotificationTemplateError) as exc:
            validate_template("{forbidden}")
        msg = str(exc.value)
        idx_channel = msg.index("channel")
        idx_message_id = msg.index("message_id")
        idx_sender = msg.index("sender")
        idx_timestamp = msg.index("timestamp")
        assert idx_channel < idx_message_id < idx_sender < idx_timestamp

    def test_error_message_includes_remediation_hint(self) -> None:
        """K5 — point the user at the fix (``letterbox.toml`` /
        ``notification_template``). Vector errors include the next action.
        """
        with pytest.raises(NotificationTemplateError) as exc:
            validate_template("{body}")
        msg = str(exc.value)
        assert "notification_template" in msg

    def test_error_message_for_nested_brace_format_spec_names_the_construct(
        self,
    ) -> None:
        """The structural problem is "nested brace in format spec is not
        allowed", NOT "unknown variable ``width``" (which is technically true
        but misleading).
        """
        with pytest.raises(NotificationTemplateError) as exc:
            validate_template("{channel:>{width}}")
        msg = str(exc.value)
        # The error must reference the format-spec / nested-brace nature,
        # not surface ``width`` as if it were the rejected variable.
        assert "format" in msg.lower() or "brace" in msg.lower()


# ──────────────────────────────────────────────────────────────
# TestPublicSurface — __all__ lock + module-private discipline (K7)
# ──────────────────────────────────────────────────────────────


class TestPublicSurface:
    """Three names export; the whitelist constant stays module-private (K7).

    Mirrors 3d's ``TestPublicSurface`` shape — the ``__all__`` set is the
    declared API contract; ``_ALLOWED_VARS`` is the invariant that
    consumers must NOT catalogue (would invite drift on the next
    whitelist amendment).
    """

    def test_public_exports(self) -> None:
        assert set(notifications_mod.__all__) == {
            "NotificationTemplateError",
            "render_notification",
            "validate_template",
        }

    def test_allowed_vars_is_module_private(self) -> None:
        """K7 — ``_ALLOWED_VARS`` exists at module level but is NOT exported.
        Adding a fifth variable means amending the function signature (K4),
        the frozenset (K7), and the tests in one diff — exporting the
        constant would invite drift in consumer code that catalogues it.
        """
        assert hasattr(notifications_mod, "_ALLOWED_VARS")
        assert "_ALLOWED_VARS" not in notifications_mod.__all__

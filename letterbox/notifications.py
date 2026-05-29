"""Notification template rendering with strict variable whitelist (load-bearing security boundary).

Tier: 1
May import from: stdlib only.
Must NOT import from: any other ``letterbox.*`` module (Tier 1 leaf — see PLANNING_FRAMEWORK P7).

Filled in: Phase 4a per PHASE_INDEX.
"""
from __future__ import annotations

import string


__all__ = [
    "NotificationTemplateError",
    "render_notification",
    "validate_template",
]


# The four trusted-context variables a notification template may reference.
# Sourced exclusively from the watcher's own observations + the launcher's
# resolved channel handle (Vision §6.4 + §13.3); NEVER from a peer-written
# message payload. Module-private (K7) — exporting would invite consumer code
# to catalogue the names and drift on the next amendment. Adding a fifth
# variable means amending this frozenset, the ``render_notification`` keyword
# signature (K4), and the tests in one diff.
_ALLOWED_VARS: frozenset[str] = frozenset(
    {"channel", "sender", "message_id", "timestamp"}
)


# ──────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────


class NotificationTemplateError(Exception):
    """Raised when a template contains a disallowed variable or syntax.

    Bare ``Exception`` subclass (K5) — mirrors ``StatePermissionsError`` (3a),
    ``ConfigError`` (1c), and ``MessageTooLarge`` (2a). Consumers catch the
    domain, not the broad stdlib type; subclassing ``ValueError`` would force
    consumers to inspect message strings to demultiplex error categories.

    The raising sites construct vector messages (Framework P3) that name the
    rejected variable, list the four allowed names in sorted order (G11), and
    point the user at ``notification_template`` in ``letterbox.toml``. The
    launcher (Phase 8a) catches this at startup-validation time and translates
    to a clean stderr line + non-zero exit per Framework P3.
    """


# ──────────────────────────────────────────────────────────────
# Template validation (the parse-time whitelist gate)
# ──────────────────────────────────────────────────────────────


def _format_error(rejected: str, *, reason: str = "unknown variable") -> str:
    """Build the vector error string (Framework P3 / G8 / G11).

    Names the rejected token, lists the four allowed variables in
    alphabetical order (frozenset iteration is unordered — sorting locks the
    message form for stable test assertions and stable user reading), and
    points at the configuration knob the user must edit.
    """
    allowed = ", ".join(sorted(_ALLOWED_VARS))
    return (
        f"Notification template references {reason} {{{rejected}}}. "
        f"Allowed: {allowed}. "
        f"Edit notification_template in letterbox.toml to remove or rename."
    )


def validate_template(template: str) -> None:
    """Raise ``NotificationTemplateError`` if the template is unsafe.

    The whitelist is fixed: ``{channel}``, ``{sender}``, ``{message_id}``,
    ``{timestamp}``. Any other named reference, positional ``{}``, attribute
    access (``{channel.upper}``), index access (``{channel[0]}``), or
    nested-brace format spec (``{channel:>{width}}``) is rejected with a
    vector error message BEFORE any substitution occurs.

    Validation uses ``string.Formatter().parse()`` rather than the seemingly
    simpler ``try: template.format(**known); except KeyError`` pattern
    (K1). The ``KeyError`` approach is actively unsafe, not just less
    ergonomic: ``str.format`` walks attribute access on trusted-context
    values without raising, so ``"{channel.upper}".format(channel="hi")``
    silently returns ``"<built-in method upper of str object at 0x…>"``.
    Parsing the template before substitution lets us inspect every
    ``(literal_text, field_name, format_spec, conversion)`` tuple and reject
    on syntactic shape, not just missing-key.

    Called explicitly by the launcher's startup chain (Phase 8a) — mirrors
    the free-function startup-validation pattern established by
    ``check_state_dir_permissions`` (Phase 3a). Composition is forced UP to
    a Tier-4 orchestrator because ``config.py`` is a Tier-1 leaf forbidden
    from importing ``notifications.py`` (K2, §13.5).

    Args:
        template: The user-configured notification template string.

    Returns:
        ``None`` on accept.

    Raises:
        TypeError: If ``template`` is not a ``str`` (G9).
        NotificationTemplateError: If ``template`` references anything
            outside the whitelist, contains attribute/index access, has a
            nested-brace format spec, or uses positional substitution.
    """
    if not isinstance(template, str):
        raise TypeError(
            f"template must be str, got {type(template).__name__}"
        )
    for _literal, field_name, format_spec, _conversion in string.Formatter().parse(
        template
    ):
        if field_name is None:
            # Pure literal segment (or escaped {{ / }}) — no substitution
            # requested. G1, G5.
            continue
        # G3 — attribute access (``{channel.upper}``) and index access
        # (``{channel[0]}``) surface as dotted/bracketed ``field_name``.
        # The bare ``in _ALLOWED_VARS`` check below would reject them
        # naturally; the explicit check here is defense-in-depth so a
        # future whitelist expansion cannot accidentally re-enable
        # attribute traversal by adding a dotted name to the set.
        if "." in field_name or "[" in field_name or "]" in field_name:
            raise NotificationTemplateError(
                _format_error(field_name, reason="disallowed attribute/index access in")
            )
        # G4 / K6 — nested-brace format spec (``{channel:>{width}}``)
        # references a secondary name that ``vformat`` would resolve at
        # render time. K4's keyword-only signature already prevents the
        # caller from supplying ``width``, but rejecting at parse time
        # closes the covert second-name substitution channel structurally.
        spec = format_spec or ""
        if "{" in spec or "}" in spec:
            raise NotificationTemplateError(
                _format_error(field_name, reason="nested brace in format spec for")
            )
        if field_name not in _ALLOWED_VARS:
            raise NotificationTemplateError(_format_error(field_name))


# ──────────────────────────────────────────────────────────────
# Rendering (the runtime substitution surface)
# ──────────────────────────────────────────────────────────────


def render_notification(
    template: str,
    *,
    channel: str,
    sender: str,
    message_id: str,
    timestamp: str,
) -> str:
    """Substitute the four trusted-context variables into the template.

    The keyword-only required signature IS the §13.3 Join-Key enforcement
    at the notification surface (K4). A caller cannot accidentally splat a
    peer-payload dict (``render_notification(t, **msg.to_dict())``) because
    the four keyword names ARE the exact whitelist and any extra keys
    (``body``, ``id``, ``address``, ``metadata``, ``in_reply_to``, …)
    raise ``TypeError: unexpected keyword argument`` from the function-call
    protocol. The Python language itself enforces the join keys; this
    function does not need a runtime check beyond accepting only these four
    names.

    Re-validates the template per call (K3 / G7 — defense in depth) so any
    caller is structurally safe even if ``validate_template`` was never
    called upstream (e.g., a test that calls this function directly). The
    cost is one ``string.Formatter().parse()`` walk on a ≤200-char string
    — sub-microsecond on the watcher event path, irrelevant against the
    Vision §9.4 watcher latency budget (500 ms P95 write→notify).

    Args:
        template: Pre-validated notification template (re-validated here).
        channel: Trusted source — ``watcher.channel.name``.
        sender: Trusted source — ``channel.recipient_label`` (the local
            agent's recorded label for the peer, NOT the peer's self-asserted
            ``msg.sender`` field).
        message_id: Trusted source — observed filename stem.
        timestamp: Trusted source — watcher's wall-clock UTC ISO string.

    Returns:
        The substituted notification string ready for ``adapter.inject``.

    Raises:
        TypeError: If ``template`` or any of the four variables is not a
            ``str`` (G9 / G10). Silently coercing a non-str via
            ``__format__`` is the wrong cure on a load-bearing security
            boundary — a ``Path`` object would ``__format__`` as an
            absolute filesystem path, leaking layout to the rendered
            output.
        NotificationTemplateError: If ``template`` fails the whitelist
            check (K3 — same path as ``validate_template``).
    """
    if not isinstance(template, str):
        raise TypeError(
            f"template must be str, got {type(template).__name__}"
        )
    # G10 — per-variable type guard. The launcher (8b) sources these from
    # the watcher's trusted context and always passes strings, so this is
    # defensive; the defence is structural and the cost is four
    # ``isinstance`` calls per render.
    for name, value in (
        ("channel", channel),
        ("sender", sender),
        ("message_id", message_id),
        ("timestamp", timestamp),
    ):
        if not isinstance(value, str):
            raise TypeError(
                f"{name} must be str, got {type(value).__name__}"
            )
    # K3 — re-validate even if the launcher already validated at startup.
    # One canonical validation site means one place to amend when the
    # whitelist changes.
    validate_template(template)
    return template.format(
        channel=channel,
        sender=sender,
        message_id=message_id,
        timestamp=timestamp,
    )

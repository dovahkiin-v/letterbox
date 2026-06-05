"""Antigravity CLI adapter — ``agy`` CLI.

Tier: 3
May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,
    ``letterbox.adapters.pty_common``.
Must NOT import from: ``letterbox.adapters.claude``, ``letterbox.adapters.gemini``
    (sibling-Tier-3 isolation), or any Tier 4 module — bulkhead §13.5.

Filled in: Phase 6c per PHASE_INDEX.
"""
from __future__ import annotations

from letterbox.adapters.base import Adapter, register_adapter

__all__ = ["AntigravityAdapter"]


@register_adapter
class AntigravityAdapter(Adapter):
    """The Antigravity CLI reference adapter (Vision §5.3).

    Declarative subclass — five class attrs, zero method overrides. The base
    (Phase 5b/5d) supplies ``spawn``/``inject``/``teardown`` and the four
    no-op lifecycle hooks; ``AntigravityAdapter`` only declares *what harness*.
    This is the third and final v1 adapter — the proof of Kernel L4 a third
    time: adding a harness is config, not code.

    Antigravity is the only v1 harness with empty ``default_args`` — the
    launch argv is just ``["agy", *extra_args]``. The launcher (8a) carries
    ``--mcp-config <temp-path>`` through ``spawn``'s ``extra_args`` (K2);
    ``pre_spawn`` stays the inherited no-op so the adapter never needs to know
    the temp path 5c/8a own. ``line_terminator`` inherits the base default
    ``b"\\r"`` (ADR-018 — the CR that wakes the agent).
    """

    name = "antigravity"
    command = "agy"
    default_args = []
    notification_template = (
        "📬 Peer message on channel {channel}. Use check_messages."
    )
    # line_terminator inherits the base default b"\r" (ADR-018) — no override.
    # The Antigravity CLI (``agy``) has NO ``--mcp-config`` flag either — it
    # manages tools via ``agy plugin`` subcommands (verified against agy
    # 1.0.5). Like Gemini, its letterbox MCP server is configured in the
    # harness's own settings, not injected per launch. ADR-054.
    mcp_config_via_flag = False
    # Antigravity (``agy``) is a TUI in the Gemini family; assume it shares the
    # fast-return submission gate and delay the terminator like Gemini, until a
    # live ``agy`` run says otherwise. ADR-057.
    terminator_delay = 0.1

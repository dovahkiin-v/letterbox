"""Gemini CLI adapter — ``gemini`` CLI, ``--yolo``.

Tier: 3
May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,
    ``letterbox.adapters.pty_common``.
Must NOT import from: ``letterbox.adapters.claude``, ``letterbox.adapters.antigravity``
    (sibling-Tier-3 isolation), or any Tier 4 module — bulkhead §13.5.

Filled in: Phase 6b per PHASE_INDEX.
"""
from __future__ import annotations

from letterbox.adapters.base import Adapter, register_adapter

__all__ = ["GeminiAdapter"]


@register_adapter
class GeminiAdapter(Adapter):
    """The Gemini CLI reference adapter (Vision §5.3).

    Declarative subclass — five class attrs, zero method overrides. The base
    (Phase 5b/5d) supplies ``spawn``/``inject``/``teardown`` and the four
    no-op lifecycle hooks; ``GeminiAdapter`` only declares *what harness*.
    This is the architectural payoff of the adapter substrate and the proof
    of Kernel L4: adding a harness is config, not code.

    The launcher (8a) carries ``--mcp-config <temp-path>`` through ``spawn``'s
    ``extra_args`` (K2); ``pre_spawn`` stays the inherited no-op so the adapter
    never needs to know the temp path 5c/8a own. ``line_terminator`` inherits
    the base default ``b"\\r"`` (ADR-018 — the CR that wakes the agent).
    """

    name = "gemini"
    command = "gemini"
    default_args = ["--yolo"]
    notification_template = (
        "📬 Peer message on channel {channel}. Use check_messages."
    )
    # line_terminator inherits the base default b"\r" (ADR-018) — no override.
    # The Gemini CLI has NO ``--mcp-config`` flag (it aborts with "Unknown
    # arguments: mcp-config" — verified against gemini-cli 0.45.0). Its MCP
    # servers come from settings (``~/.gemini/settings.json`` user-level, or a
    # workspace ``.gemini/settings.json``). So letterbox does NOT inject the
    # flag; the user configures a ``letterbox`` MCP server there once, exactly
    # as the Forge tower orchestrator does. ADR-054.
    mcp_config_via_flag = False
    # Gemini's KeypressContext rewrites an Enter arriving within 30 ms of the
    # previous key into a newline ("FAST_RETURN_TIMEOUT"), so a back-to-back
    # CR lands in the box without submitting (observed live). Delaying the CR
    # past that window makes it a real Enter. 100 ms is a comfortable margin
    # and imperceptible. ADR-057.
    terminator_delay = 0.1

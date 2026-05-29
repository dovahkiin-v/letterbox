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

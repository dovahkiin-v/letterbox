"""Claude Code adapter — ``claude`` CLI, ``--dangerously-skip-permissions``.

Tier: 3
May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,
    ``letterbox.adapters.pty_common``.
Must NOT import from: ``letterbox.adapters.gemini``, ``letterbox.adapters.antigravity``
    (sibling-Tier-3 isolation — each concrete adapter stays self-contained), or any
    Tier 4 module — bulkhead §13.5.

Filled in: Phase 6a per PHASE_INDEX.
"""
from __future__ import annotations

from letterbox.adapters.base import Adapter, register_adapter

__all__ = ["ClaudeAdapter"]


@register_adapter
class ClaudeAdapter(Adapter):
    """The Claude Code reference adapter (Vision §5.3).

    Declarative subclass — five class attrs, zero method overrides. The base
    (Phase 5b/5d) supplies ``spawn``/``inject``/``teardown`` and the four
    no-op lifecycle hooks; ``ClaudeAdapter`` only declares *what harness*.
    This is the architectural payoff of the adapter substrate and the proof
    of Kernel L4: adding a harness is config, not code.

    The launcher (8a) carries ``--mcp-config <temp-path>`` through ``spawn``'s
    ``extra_args`` (K2); ``pre_spawn`` stays the inherited no-op so the adapter
    never needs to know the temp path 5c/8a own. ``line_terminator`` inherits
    the base default ``b"\\r"`` (ADR-018 — the CR that wakes the agent).
    """

    name = "claude"
    command = "claude"
    default_args = ["--dangerously-skip-permissions"]
    notification_template = (
        "📬 Peer message on channel {channel}. Call check_messages to read."
    )
    # line_terminator inherits the base default b"\r" (ADR-018) — no override.
    # Claude Code supports ``--mcp-config <path>``, so letterbox wires its MCP
    # server self-contained per launch (the base default). ADR-054.
    mcp_config_via_flag = True
    # Claude Code *used* to submit on a combined ``b"text\r"`` write, but a
    # 2026-06-10 Claude Code update added a fast-return submission gate (the same
    # class of behaviour as Gemini's ``KeypressContext`` — a ``\r`` arriving in the
    # same burst as the text is treated as a newline-in-box, not a submit). So
    # Claude now needs the SEPARATE, delayed terminator write like Gemini and
    # Antigravity: write the text, wait past the fast-return window, then write the
    # ``\r`` so it registers as a discrete Enter. ADR-057, ADR-063.
    terminator_delay = 0.1

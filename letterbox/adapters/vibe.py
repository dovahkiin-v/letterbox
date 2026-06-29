"""Vibe CLI adapter — ``vibe`` CLI.

Tier: 3
May import from: stdlib; Tier 1 modules; ``letterbox.adapters.base``,
    ``letterbox.adapters.pty_common``.
Must NOT import from: ``letterbox.adapters.claude``, ``letterbox.adapters.gemini``,
    ``letterbox.adapters.antigravity``
    (sibling-Tier-3 isolation), or any Tier 4 module — bulkhead §13.5.

Filled in: vibe adapter per ADR-067.
"""
from __future__ import annotations

from letterbox.adapters.base import Adapter, register_adapter

__all__ = ["VibeAdapter"]


@register_adapter
class VibeAdapter(Adapter):
    """Mistral Vibe CLI adapter (Python Textual TUI).

    Declarative subclass — six class attrs, zero method overrides. The base
    (Phase 5b/5d) supplies ``spawn``/``inject``/``teardown`` and the four
    no-op lifecycle hooks; ``VibeAdapter`` only declares *what harness*.

    STEP 0 finding (ADR-067): Vibe's ``ChatTextArea`` overrides the ``enter``
    key binding to call ``event.prevent_default()`` and post a ``Submitted``
    message — so bare Enter submits, not inserts a newline. The injected
    ``b"\\r"`` therefore submits exactly as for Gemini/Antigravity; the
    standard ``terminator_delay = 0.1`` clears Textual's asyncio event-loop
    timing gap without any ``pre_inject`` hook or custom ``line_terminator``.

    Vibe has NO ``--mcp-config`` flag; its letterbox MCP server is configured
    via ``~/.vibe/config.toml`` (like Gemini via settings.json). Vibe's
    ``acp.transports.spawn_stdio_transport`` passes only a trimmed env to the
    MCP subprocess (``HOME``, ``PATH``, ``SHELL``, ``TERM``, ``USER``,
    ``LOGNAME``), so ``LETTERBOX_CHANNEL`` etc. are not inherited by the MCP
    child directly. The solution is a one-time bridge script
    (``letterbox/data/vibe-mcp-bridge.sh``) that reads the vars from Vibe's
    process env via ``/proc/$PPID/environ`` at spawn time, making the
    config.toml entry channel-agnostic — any ``letterbox vibe --channel X``
    just works, same as Gemini. See ADR-067 for the full finding.
    """

    name = "vibe"
    command = "vibe"
    # --yolo is the auto-approve flag (alias: --auto-approve); matches Gemini's
    # --yolo pattern and lets the agent act without per-tool confirmation prompts.
    default_args = ["--yolo"]
    notification_template = (
        "📬 Peer message on channel {channel}. Use check_messages."
    )
    # line_terminator inherits the base default b"\r" (ADR-018) — no override.
    # Vibe has NO --mcp-config flag (ADR-054 pattern). ADR-067.
    mcp_config_via_flag = False
    # Vibe's Textual asyncio loop needs the text write processed before the CR
    # arrives. 0.1 s matches the TUI-adapter standard (ADR-057). ADR-067.
    terminator_delay = 0.1

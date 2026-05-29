"""Generates temp MCP config files per harness conventions; cleanup hook.

Tier: 2
May import from: stdlib (``json``, ``logging``, ``os``, ``shutil``, ``tempfile``, ``pathlib``).
Must NOT import from: concrete adapters or any Tier 4 module — bulkhead §13.5.

Filled in: Phase 5c per PHASE_INDEX.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

__all__ = ["cleanup_mcp_config", "generate_mcp_config"]

# Stable tool-namespace key for the letterbox MCP server inside the
# ``mcpServers`` envelope. Identical for every v1 harness (K1).
_MCP_SERVER_NAME = "letterbox"
# The console-script name to resolve to an absolute path at generation time
# (K2). Declared in pyproject's ``[project.scripts] letterbox = ...``. Equal in
# value to _MCP_SERVER_NAME today, but conceptually distinct: one is the JSON
# namespace key, the other is the executable the agent spawns.
_LETTERBOX_COMMAND = "letterbox"
_CONFIG_FILE_MODE = 0o600
_TEMP_PREFIX = "letterbox-mcp-"
_LOGGER = logging.getLogger("letterbox.adapters.mcp_config")


def generate_mcp_config(
    harness: str,
    channel: str,
    sender_label: str,
    instance_id: str,
) -> Path:
    """Write a temp MCP config pointing the agent at its `letterbox mcp` child.

    Returns the absolute path to a mode-0600 JSON file. The launcher (8a)
    passes this path to the harness via `--mcp-config <path>`; the launcher
    (8c) deletes it via cleanup_mcp_config at teardown.

    Args:
        harness: Adapter name (e.g. ``"claude"``). Selects the ``--mcp-config``
            flag at 6a-c and the ``_build_payload`` divergence seam; also names
            the temp file for debuggability. Not threaded into the child argv.
        channel: Channel name the MCP child operates on.
        sender_label: Identity label the MCP child sends as.
        instance_id: Ephemeral per-launch instance id for own-write recognition.

    Returns:
        Absolute path to the written mode-0600 JSON config file.

    Raises:
        TypeError: If any argument is not a ``str``.
        ValueError: If any argument is an empty string.
    """
    _require_nonempty_str(harness, "harness")
    _require_nonempty_str(channel, "channel")
    _require_nonempty_str(sender_label, "sender_label")
    _require_nonempty_str(instance_id, "instance_id")

    payload = _build_payload(harness, channel, sender_label, instance_id)

    fd, path_str = tempfile.mkstemp(prefix=f"{_TEMP_PREFIX}{harness}-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
    # mkstemp's 0600 default is masked by umask; restore the exact mode so a
    # pathological umask can't strip owner bits (the 3a chmod discipline).
    os.chmod(path_str, _CONFIG_FILE_MODE)
    return Path(path_str)


def cleanup_mcp_config(path: Path) -> None:
    """Delete a generated MCP config file. Idempotent (missing file is fine)."""
    path.unlink(missing_ok=True)


def _build_payload(
    harness: str,
    channel: str,
    sender_label: str,
    instance_id: str,
) -> dict[str, object]:
    """Build the MCP config payload for the given launch identity.

    One unified ``mcpServers`` shape for every v1 harness (K1). ``harness`` is
    the reserved divergence seam: the day a real harness needs a different file
    shape, it branches here — but no v1 harness diverges, so v1 ignores it.
    """
    args = [
        "mcp",
        "--channel",
        channel,
        "--as",
        sender_label,
        "--instance-id",
        instance_id,
    ]
    return {
        "mcpServers": {
            _MCP_SERVER_NAME: {"command": _resolve_letterbox_command(), "args": args}
        }
    }


def _resolve_letterbox_command() -> str:
    """Resolve the absolute ``letterbox`` path, or fall back to the bare name.

    The MCP child is spawned by the agent under a fully-specified env (5a K6),
    so embedding the absolute path the PTY-Parent resolved decouples the spawn
    from the agent's PATH. When ``which`` can't resolve it, WARN and fall back
    to the bare name — behavior no worse than the literal Vision §6.3 text.
    """
    resolved = shutil.which(_LETTERBOX_COMMAND)
    if resolved is None:
        _LOGGER.warning(
            "%r not found on PATH via shutil.which; falling back to the bare "
            "command name. The agent-spawned MCP child will then depend on its "
            "own PATH to locate the letterbox console script.",
            _LETTERBOX_COMMAND,
        )
        return _LETTERBOX_COMMAND
    return resolved


def _require_nonempty_str(value: object, field: str) -> None:
    """Reject non-``str`` or empty-``str`` input with a vector error."""
    if not isinstance(value, str):
        raise TypeError(
            f"{field} must be a non-empty str, got {type(value).__name__}: {value!r}"
        )
    if not value:
        raise ValueError(f"{field} must be a non-empty str, got empty string")

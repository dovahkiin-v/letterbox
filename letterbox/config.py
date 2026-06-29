"""TOML loading, defaults, resolution order (defaults < user < project < CLI < env), validation.

Tier: 1
May import from: stdlib, ``tomli`` (Python <3.11) or ``tomllib`` (3.11+).
Must NOT import from: any other ``letterbox.*`` module (Tier 1 leaf — see PLANNING_FRAMEWORK P7).

Filled in: Phase 1c per PHASE_INDEX.
"""
from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib  # type: ignore[import-not-found]
else:
    import tomli as tomllib  # type: ignore[import-not-found]


__all__ = [
    "HarnessConfig",
    "ChannelConfig",
    "LetterboxConfig",
    "ConfigError",
    "load_config",
    "resolve_state_dir",
    "DEFAULT_STATE_DIR",
    "DEFAULT_HARNESSES",
    "DEFAULT_CHANNELS",
]


# ──────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────


class ConfigError(Exception):
    """Raised on TOML parse failure, schema violation, or rejected fields.

    Message format: ``<file>:<line> -- <reason>`` when line info is
    discoverable, ``<file> -- <reason>`` when only the file is known,
    or ``<reason>`` for schema-level errors raised without a file context
    (e.g. ``cli_overrides`` shape violations).
    """


# ──────────────────────────────────────────────────────────────
# Dataclasses
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HarnessConfig:
    """A harness adapter declaration loaded from config.

    Attributes:
        command: The CLI command to spawn (e.g. ``"claude"``).
        default_args: CLI args prepended on spawn (Vision §5.3 / §8.2).
        notification_template: Template string consumed by ``notifications.py``;
            placeholder-whitelist validation is deferred to Phase 4a (K6).
    """

    command: str
    default_args: list[str]
    notification_template: str


@dataclass(frozen=True)
class ChannelConfig:
    """A channel declared in ``[[channels]]``.

    Attributes:
        name: Channel name, validated against ``[a-z0-9_-]+``.
        description: Optional one-liner.
    """

    name: str
    description: str | None = None


@dataclass(frozen=True)
class LetterboxConfig:
    """Fully resolved configuration after the §8.1 precedence chain.

    Attributes:
        state_dir: Absolute, tilde-expanded state-directory path.
        harnesses: Built-in harnesses unioned with any user blocks
            (user blocks replace built-ins of the same name).
        channels: Channels declared in ``[[channels]]``. Auto-created
            channels are NOT listed here.
    """

    state_dir: Path
    harnesses: dict[str, HarnessConfig]
    channels: list[ChannelConfig]


# ──────────────────────────────────────────────────────────────
# Defaults (Vision §5.3 / §8.2)
# ──────────────────────────────────────────────────────────────

DEFAULT_STATE_DIR: Path = Path.home() / ".letterbox"

DEFAULT_HARNESSES: dict[str, HarnessConfig] = {
    "claude": HarnessConfig(
        command="claude",
        default_args=["--dangerously-skip-permissions"],
        notification_template=(
            "📬 Peer message on channel {channel}. Call check_messages to read."
        ),
    ),
    "gemini": HarnessConfig(
        command="gemini",
        default_args=["--yolo"],
        notification_template="📬 Peer message on channel {channel}. Use check_messages.",
    ),
    "antigravity": HarnessConfig(
        command="agy",
        default_args=[],
        notification_template="📬 Peer message on channel {channel}. Use check_messages.",
    ),
    "vibe": HarnessConfig(
        command="vibe",
        default_args=["--yolo"],
        notification_template="📬 Peer message on channel {channel}. Use check_messages.",
    ),
}

DEFAULT_CHANNELS: list[ChannelConfig] = []


# ──────────────────────────────────────────────────────────────
# Schema whitelists (K5)
# ──────────────────────────────────────────────────────────────

_TOP_LEVEL_KEYS = frozenset({"letterbox", "harness", "channels"})
_LETTERBOX_KEYS = frozenset({"state_dir"})
_HARNESS_KEYS = frozenset({"command", "default_args", "notification_template"})
_CHANNEL_KEYS = frozenset({"name", "description"})
_REJECTED_CHANNEL_KEYS = frozenset({"default_sender", "default_recipient"})

_CHANNEL_NAME_RE = re.compile(r"^[a-z0-9_-]+$")
_TOML_LINE_RE = re.compile(r"at line (\d+)(?:,?\s+col(?:umn)?\s+(\d+))?")


# ──────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────


def _format_error(path: Path | None, line: int | None, reason: str) -> str:
    """Build the canonical ``<file>:<line> -- <reason>`` string."""
    if path is None:
        return reason
    if line is not None:
        return f"{path}:{line} -- {reason}"
    return f"{path} -- {reason}"


def _expand_user(value: str) -> str:
    """Replace a leading ``~`` (or ``~/``) with ``Path.home()``.

    Embedded tildes (e.g. ``/foo/~/bar``) are preserved verbatim — only the
    *leading* tilde is expanded. Per Gotcha 7.4, we use ``Path.home()`` rather
    than ``os.path.expanduser`` to avoid platform-variant lookup behaviour.

    Args:
        value: Raw string from TOML or env.

    Returns:
        The string with leading-tilde expansion applied.
    """
    if value == "~":
        return str(Path.home())
    if value.startswith("~/"):
        return str(Path.home()) + value[1:]
    return value


def _reject_unknown_keys(
    table: dict[str, Any],
    allowed: frozenset[str],
    context: str,
    path: Path | None,
) -> None:
    """Raise ``ConfigError`` if ``table`` contains any keys outside ``allowed``.

    Args:
        table: Parsed TOML mapping to check.
        allowed: Set of permitted key names.
        context: Human-readable description of the table (for the error).
        path: Optional file path included in the error prefix.

    Raises:
        ConfigError: When unknown keys are found.
    """
    extra = sorted(set(table) - allowed)
    if not extra:
        return
    msg = (
        f"unknown key(s) in {context}: {', '.join(repr(k) for k in extra)}. "
        f"Allowed: {', '.join(repr(k) for k in sorted(allowed))}."
    )
    raise ConfigError(_format_error(path, None, msg))


def _load_toml_file(path: Path) -> dict[str, Any]:
    """Parse a TOML file, raising ``ConfigError`` with line context on failure.

    Args:
        path: Filesystem path of the TOML file to read.

    Returns:
        Parsed TOML as a plain dict.

    Raises:
        ConfigError: On parse failure, IO error, or non-mapping root.
    """
    try:
        with open(path, "rb") as fh:
            data = tomllib.load(fh)
    except FileNotFoundError as exc:
        raise ConfigError(
            _format_error(path, None, f"file not found: {exc}")
        ) from exc
    except OSError as exc:
        raise ConfigError(
            _format_error(path, None, f"I/O error: {exc}")
        ) from exc
    except tomllib.TOMLDecodeError as exc:
        # Gotcha 7.3 / Scout D1: some Python micro-versions expose ``lineno``
        # as an attribute, others embed the line in the message string only.
        line = getattr(exc, "lineno", None)
        if line is None:
            match = _TOML_LINE_RE.search(str(exc))
            if match is not None:
                line = int(match.group(1))
        raise ConfigError(
            _format_error(path, line, f"malformed TOML: {exc}")
        ) from exc

    if not isinstance(data, dict):
        # tomllib.load always returns a dict at the root; defensive guard.
        raise ConfigError(
            _format_error(path, None, "TOML root must be a table.")
        )
    return data


def _parse_letterbox_block(block: Any, path: Path | None) -> dict[str, Any]:
    """Parse and normalise the ``[letterbox]`` block."""
    if block is None:
        return {}
    if not isinstance(block, dict):
        raise ConfigError(
            _format_error(path, None, "[letterbox] must be a table.")
        )
    _reject_unknown_keys(block, _LETTERBOX_KEYS, "[letterbox]", path)
    out: dict[str, Any] = {}
    if "state_dir" in block:
        state_dir = block["state_dir"]
        if not isinstance(state_dir, str) or not state_dir:
            raise ConfigError(
                _format_error(
                    path, None, "[letterbox].state_dir must be a non-empty string."
                )
            )
        out["state_dir"] = state_dir
    return out


def _parse_harnesses(
    harness_table: Any, path: Path | None
) -> dict[str, HarnessConfig]:
    """Parse the ``[harness.<name>]`` cluster into a dict of ``HarnessConfig``."""
    if harness_table is None:
        return {}
    if not isinstance(harness_table, dict):
        raise ConfigError(
            _format_error(
                path,
                None,
                "[harness] must be a table containing [harness.<name>] sub-tables.",
            )
        )
    result: dict[str, HarnessConfig] = {}
    for name, block in harness_table.items():
        if not isinstance(block, dict):
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    f"[harness.{name}] must be a table, got {type(block).__name__}.",
                )
            )
        _reject_unknown_keys(block, _HARNESS_KEYS, f"[harness.{name}]", path)
        if "command" not in block:
            raise ConfigError(
                _format_error(
                    path, None, f"[harness.{name}] missing required field 'command'."
                )
            )
        command = block["command"]
        if not isinstance(command, str) or not command:
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    f"[harness.{name}].command must be a non-empty string.",
                )
            )
        default_args = block.get("default_args", [])
        if not isinstance(default_args, list) or not all(
            isinstance(a, str) for a in default_args
        ):
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    f"[harness.{name}].default_args must be a list of strings.",
                )
            )
        if "notification_template" not in block:
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    f"[harness.{name}] missing required field 'notification_template'.",
                )
            )
        template = block["notification_template"]
        if not isinstance(template, str):
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    f"[harness.{name}].notification_template must be a string.",
                )
            )
        result[name] = HarnessConfig(
            command=command,
            default_args=list(default_args),
            notification_template=template,
        )
    return result


def _parse_channels(
    channels_value: Any, path: Path | None
) -> list[ChannelConfig]:
    """Parse the ``[[channels]]`` array-of-tables (K4 + Gotcha 7.7 / 7.10)."""
    if channels_value is None:
        return []
    if not isinstance(channels_value, list):
        raise ConfigError(
            _format_error(
                path,
                None,
                (
                    "'channels' must be an array of tables — use [[channels]] "
                    "(double brackets), not [channels]."
                ),
            )
        )
    result: list[ChannelConfig] = []
    seen: dict[str, int] = {}
    for index, entry in enumerate(channels_value):
        position = index + 1
        if not isinstance(entry, dict):
            raise ConfigError(
                _format_error(
                    path, None, f"[[channels]] entry #{position} must be a table."
                )
            )
        # K4 — vector rejection of identity fields.
        rejected = sorted(set(entry) & _REJECTED_CHANNEL_KEYS)
        if rejected:
            bad = ", ".join(repr(k) for k in rejected)
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    (
                        f"[[channels]] entry #{position} cannot declare {bad} — "
                        "identity is per-launch (ADR-026). "
                        "Pass --as <label> on the letterbox launch, or set "
                        "LETTERBOX_SENDER, instead."
                    ),
                )
            )
        _reject_unknown_keys(
            entry, _CHANNEL_KEYS, f"[[channels]] entry #{position}", path
        )
        if "name" not in entry:
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    f"[[channels]] entry #{position} missing required field 'name'.",
                )
            )
        name = entry["name"]
        if not isinstance(name, str) or not _CHANNEL_NAME_RE.fullmatch(name):
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    (
                        f"[[channels]] entry #{position} 'name' must match "
                        f"{_CHANNEL_NAME_RE.pattern}, got {name!r}."
                    ),
                )
            )
        description = entry.get("description")
        if description is not None and not isinstance(description, str):
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    (
                        f"[[channels]] entry #{position} 'description' must be a "
                        "string when present."
                    ),
                )
            )
        if name in seen:
            raise ConfigError(
                _format_error(
                    path,
                    None,
                    (
                        f"duplicate [[channels]] name {name!r} "
                        f"(entries #{seen[name] + 1} and #{position}). "
                        "Channel names must be unique."
                    ),
                )
            )
        seen[name] = index
        result.append(ChannelConfig(name=name, description=description))
    return result


def _parse_toml_dict(
    data: dict[str, Any], path: Path | None
) -> dict[str, Any]:
    """Validate the top-level shape and parse each section.

    Returns a normalised intermediate dict with optional keys
    ``state_dir`` (str), ``harnesses`` (dict[str, HarnessConfig]), and
    ``channels`` (list[ChannelConfig]). The presence of the ``channels``
    key (rather than its value) distinguishes "absent in this TOML file"
    from "explicit empty list" so the merge logic in ``load_config`` can
    replace rather than no-op when the user redeclares ``[[channels]]``.
    """
    _reject_unknown_keys(data, _TOP_LEVEL_KEYS, "config root", path)
    result: dict[str, Any] = {}
    letterbox_block = _parse_letterbox_block(data.get("letterbox"), path)
    if "state_dir" in letterbox_block:
        result["state_dir"] = letterbox_block["state_dir"]
    harnesses = _parse_harnesses(data.get("harness"), path)
    if harnesses:
        result["harnesses"] = harnesses
    if "channels" in data:
        result["channels"] = _parse_channels(data["channels"], path)
    return result


def _project_local_config_path() -> Path | None:
    """Resolve the project-local config file (K2 semantics).

    Returns:
        ``$LETTERBOX_CONFIG`` (always honoured when set, missing-file errors
        surface at load time so the user knows their override pointed at
        nothing); else ``./letterbox.toml`` if that file exists; else ``None``.
    """
    env_value = os.environ.get("LETTERBOX_CONFIG")
    if env_value:
        return Path(env_value)
    candidate = Path.cwd() / "letterbox.toml"
    return candidate if candidate.is_file() else None


def _user_global_config_path() -> Path | None:
    """Resolve the user-global config (always ``~/.letterbox/config.toml`` — K1)."""
    candidate = Path.home() / ".letterbox" / "config.toml"
    return candidate if candidate.is_file() else None


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────


def resolve_state_dir(
    cli_override: str | os.PathLike[str] | None = None,
) -> Path:
    """Resolve the effective state directory.

    Precedence (highest wins): ``LETTERBOX_HOME`` env > ``cli_override`` >
    built-in default ``~/.letterbox``. Does NOT consult config files (that is
    ``load_config()``'s job) and does NOT create the directory.

    Args:
        cli_override: Optional explicit path supplied by the caller.

    Returns:
        Absolute ``Path`` to the state directory.
    """
    env_value = os.environ.get("LETTERBOX_HOME")
    if env_value:
        raw = env_value
    elif cli_override is not None:
        raw = os.fspath(cli_override)
    else:
        return (Path.home() / ".letterbox").resolve()
    return Path(_expand_user(raw)).resolve()


def load_config(
    cli_overrides: dict[str, Any] | None = None,
) -> LetterboxConfig:
    """Resolve full config per Vision §8.1 precedence.

    Resolution order (later wins):
      1. Built-in defaults (``DEFAULT_*`` constants).
      2. User-global ``~/.letterbox/config.toml`` (when present).
      3. Project-local ``./letterbox.toml`` — or ``$LETTERBOX_CONFIG``
         when set (K2: env replaces the project-local search).
      4. ``cli_overrides`` mapping.
      5. ``LETTERBOX_HOME`` env override of ``state_dir`` only (K1).

    Harness merge is by name: a user ``[harness.claude]`` block *replaces*
    the built-in ``claude`` entry whole rather than patching it field-by-field.
    User-declared custom harnesses (e.g. ``[harness.codex]``) are added
    alongside the built-ins per Kernel L4.

    Args:
        cli_overrides: Optional flat dict (e.g. ``{"state_dir": "/tmp/foo"}``).
            Wins over file levels but loses to ``LETTERBOX_HOME``.

    Returns:
        Fully resolved ``LetterboxConfig``.

    Raises:
        ConfigError: On malformed TOML, unknown keys, identity fields in
            ``[[channels]]`` (ADR-026), duplicate channel names, or an
            invalid ``cli_overrides`` shape.
    """
    # K3 — env reads happen here at call time, never at module-import time.
    cli_overrides = cli_overrides or {}

    state_dir_raw: str | None = None
    harnesses: dict[str, HarnessConfig] = dict(DEFAULT_HARNESSES)
    channels: list[ChannelConfig] = list(DEFAULT_CHANNELS)

    # Level 2: user-global config.
    user_global = _user_global_config_path()
    if user_global is not None:
        parsed = _parse_toml_dict(_load_toml_file(user_global), user_global)
        if "state_dir" in parsed:
            state_dir_raw = parsed["state_dir"]
        if "harnesses" in parsed:
            harnesses.update(parsed["harnesses"])
        if "channels" in parsed:
            channels = parsed["channels"]

    # Level 3: project-local config (or LETTERBOX_CONFIG override).
    project_local = _project_local_config_path()
    if project_local is not None:
        parsed = _parse_toml_dict(_load_toml_file(project_local), project_local)
        if "state_dir" in parsed:
            state_dir_raw = parsed["state_dir"]
        if "harnesses" in parsed:
            harnesses.update(parsed["harnesses"])
        if "channels" in parsed:
            channels = parsed["channels"]

    # Level 4: cli_overrides (a flat dict; only state_dir honoured today).
    for key in cli_overrides:
        if key != "state_dir":
            raise ConfigError(
                f"cli_overrides contains unsupported key {key!r}. "
                "Supported keys: 'state_dir'."
            )
    if "state_dir" in cli_overrides:
        cli_state_dir = cli_overrides["state_dir"]
        if not isinstance(cli_state_dir, (str, os.PathLike)):
            raise ConfigError(
                "cli_overrides['state_dir'] must be str or os.PathLike."
            )
        state_dir_raw = os.fspath(cli_state_dir)

    # Level 5: LETTERBOX_HOME env override of state_dir only (K1).
    env_home = os.environ.get("LETTERBOX_HOME")
    if env_home:
        state_dir_raw = env_home

    if state_dir_raw is None:
        state_dir = (Path.home() / ".letterbox").resolve()
    else:
        state_dir = Path(_expand_user(state_dir_raw)).resolve()

    return LetterboxConfig(
        state_dir=state_dir,
        harnesses=harnesses,
        channels=channels,
    )

"""Tests for Phase 1c — configuration loading and resolution.

These tests exercise every level of the §8.1 precedence chain, every error
path declared by the plan, custom-harness support (Kernel L4), and the
bundled-sample round-trip (Gotcha 7.6 — the structural lock against the
sample drifting from the parser).

No mocks. Every test writes real TOML files into real temp dirs and reads
them via the real parser. Per the scout brief / plan §9.2.
"""
from __future__ import annotations

import textwrap
from importlib import resources
from pathlib import Path

import pytest

from letterbox.config import (
    DEFAULT_CHANNELS,
    DEFAULT_HARNESSES,
    ChannelConfig,
    ConfigError,
    HarnessConfig,
    LetterboxConfig,
    load_config,
    resolve_state_dir,
)


# ──────────────────────────────────────────────────────────────
# Test isolation helper
# ──────────────────────────────────────────────────────────────


@pytest.fixture
def isolated_config_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Yield an isolated HOME for config-resolution tests.

    Sets ``HOME`` to ``tmp_path`` (which retargets ``Path.home()`` on Linux),
    clears ``LETTERBOX_HOME`` and ``LETTERBOX_CONFIG`` so the env-override
    levels are off by default, and chdir's into ``tmp_path`` so the
    project-local search starts in a known empty dir. Tests opt back into
    any of those by re-setting them in the body.

    Returns the patched home dir (== ``tmp_path``).
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("LETTERBOX_HOME", raising=False)
    monkeypatch.delenv("LETTERBOX_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    home_letterbox = tmp_path / ".letterbox"
    home_letterbox.mkdir(parents=False, exist_ok=False)
    return tmp_path


def _write(path: Path, body: str) -> Path:
    """Write a TOML body (with leading indentation stripped) to ``path``."""
    path.write_text(textwrap.dedent(body).lstrip("\n"), encoding="utf-8")
    return path


# ──────────────────────────────────────────────────────────────
# TestDefaults
# ──────────────────────────────────────────────────────────────


class TestDefaults:
    """No config files anywhere → the DEFAULT_* constants apply verbatim."""

    def test_load_config_no_files_returns_defaults(
        self, isolated_config_env: Path
    ) -> None:
        config = load_config()
        assert config.state_dir == (isolated_config_env / ".letterbox").resolve()
        assert config.harnesses == DEFAULT_HARNESSES
        assert config.channels == DEFAULT_CHANNELS == []

    def test_default_harnesses_have_expected_names(self) -> None:
        assert set(DEFAULT_HARNESSES.keys()) == {"claude", "gemini", "antigravity"}

    def test_antigravity_command_is_agy_not_antigravity(self) -> None:
        # Easy place to drift — the harness *name* and the *command* differ.
        assert DEFAULT_HARNESSES["antigravity"].command == "agy"


# ──────────────────────────────────────────────────────────────
# TestResolutionOrder
# ──────────────────────────────────────────────────────────────


class TestResolutionOrder:
    """Each precedence level wins as documented."""

    def test_user_global_overrides_defaults(
        self, isolated_config_env: Path
    ) -> None:
        user_cfg = isolated_config_env / ".letterbox" / "config.toml"
        _write(
            user_cfg,
            """
            [letterbox]
            state_dir = "/var/lib/lb-user"
            """,
        )
        config = load_config()
        assert config.state_dir == Path("/var/lib/lb-user").resolve()

    def test_project_local_overrides_user_global(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / ".letterbox" / "config.toml",
            """
            [letterbox]
            state_dir = "/var/lib/lb-user"
            """,
        )
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox]
            state_dir = "/var/lib/lb-project"
            """,
        )
        config = load_config()
        assert config.state_dir == Path("/var/lib/lb-project").resolve()

    def test_cli_overrides_beat_file_levels(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox]
            state_dir = "/var/lib/lb-project"
            """,
        )
        config = load_config(cli_overrides={"state_dir": "/var/lib/lb-cli"})
        assert config.state_dir == Path("/var/lib/lb-cli").resolve()

    def test_letterbox_home_env_beats_cli_overrides(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox]
            state_dir = "/var/lib/lb-project"
            """,
        )
        monkeypatch.setenv("LETTERBOX_HOME", "/var/lib/lb-env")
        config = load_config(cli_overrides={"state_dir": "/var/lib/lb-cli"})
        assert config.state_dir == Path("/var/lib/lb-env").resolve()

    def test_harness_user_block_replaces_builtin(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [harness.claude]
            command = "claude-next"
            default_args = ["--experimental"]
            notification_template = "ping {channel}"
            """,
        )
        config = load_config()
        claude = config.harnesses["claude"]
        assert claude.command == "claude-next"
        assert claude.default_args == ["--experimental"]
        assert claude.notification_template == "ping {channel}"
        # Other built-ins are untouched.
        assert config.harnesses["gemini"] == DEFAULT_HARNESSES["gemini"]
        assert config.harnesses["antigravity"] == DEFAULT_HARNESSES["antigravity"]


# ──────────────────────────────────────────────────────────────
# TestEnvVars
# ──────────────────────────────────────────────────────────────


class TestEnvVars:
    """LETTERBOX_HOME retargets state_dir; LETTERBOX_CONFIG replaces project-local search."""

    def test_letterbox_home_env_retargets_state_dir(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        # Defaults everywhere else, only env changes state_dir.
        target = tmp_path / "alt-state"
        monkeypatch.setenv("LETTERBOX_HOME", str(target))
        config = load_config()
        assert config.state_dir == target.resolve()
        assert config.harnesses == DEFAULT_HARNESSES  # other fields unaffected

    def test_letterbox_config_replaces_project_local(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        # A project-local file exists, but LETTERBOX_CONFIG points elsewhere.
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox]
            state_dir = "/var/lib/lb-project"
            """,
        )
        alt = tmp_path / "alt.toml"
        _write(
            alt,
            """
            [letterbox]
            state_dir = "/var/lib/lb-alt"
            """,
        )
        monkeypatch.setenv("LETTERBOX_CONFIG", str(alt))
        config = load_config()
        assert config.state_dir == Path("/var/lib/lb-alt").resolve()

    def test_lazy_env_read(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        # First load: no env, defaults apply.
        first = load_config()
        assert first.state_dir == (isolated_config_env / ".letterbox").resolve()
        # Now set the env *after* import and another load reflects it.
        target = tmp_path / "lazy-target"
        monkeypatch.setenv("LETTERBOX_HOME", str(target))
        second = load_config()
        assert second.state_dir == target.resolve()
        # Clear it again — defaults return.
        monkeypatch.delenv("LETTERBOX_HOME")
        third = load_config()
        assert third.state_dir == first.state_dir


# ──────────────────────────────────────────────────────────────
# TestErrorPaths
# ──────────────────────────────────────────────────────────────


class TestErrorPaths:
    """Every documented error path raises ConfigError with vector remediation."""

    def test_malformed_toml_includes_file_path(
        self, isolated_config_env: Path
    ) -> None:
        bad = _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox
            state_dir = "/x"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert str(bad) in msg
        assert "malformed TOML" in msg

    def test_malformed_toml_extracts_line_number(
        self, isolated_config_env: Path
    ) -> None:
        # Mid-document syntax error so the line number is discoverable.
        bad = _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox]
            state_dir = "/x"

            [harness.claude]
            command = "claude"
            default_args = [unquoted]
            notification_template = "ping"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert str(bad) in msg
        # Line info appears either as "<file>:N --" or "(at line N ...)".
        assert ":" in msg
        assert "line" in msg or any(c.isdigit() for c in msg.split("--")[0])

    def test_unknown_top_level_key_named_in_error(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [harnesses.claude]
            command = "claude"
            notification_template = "x"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "harnesses" in msg
        assert "unknown" in msg.lower()

    def test_single_brackets_channels_shape_hint(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [channels]
            name = "debate-01"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "[[channels]]" in msg
        assert "array of tables" in msg

    def test_default_sender_rejected_with_adr_reference(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [[channels]]
            name = "debate-01"
            default_sender = "claude"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "default_sender" in msg
        assert "ADR-026" in msg
        assert "--as" in msg

    def test_default_recipient_rejected_with_adr_reference(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [[channels]]
            name = "debate-01"
            default_recipient = "gemini"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "default_recipient" in msg
        assert "ADR-026" in msg

    def test_missing_harness_command_named_in_error(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [harness.claude]
            notification_template = "ping"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "command" in msg
        assert "[harness.claude]" in msg

    def test_missing_harness_template_named_in_error(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [harness.claude]
            command = "claude"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "notification_template" in msg

    def test_duplicate_channel_name_rejected(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [[channels]]
            name = "debate-01"

            [[channels]]
            name = "debate-01"
            description = "duplicate by accident"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "duplicate" in msg
        assert "debate-01" in msg

    def test_invalid_channel_name_rejected(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [[channels]]
            name = "Debate 01"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        assert "name" in str(exc_info.value)

    def test_unknown_harness_key_rejected(
        self, isolated_config_env: Path
    ) -> None:
        # Typo: 'commnd' instead of 'command'.
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [harness.claude]
            commnd = "claude"
            notification_template = "ping"
            """,
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config()
        msg = str(exc_info.value)
        assert "commnd" in msg
        assert "[harness.claude]" in msg

    def test_unsupported_cli_override_rejected(
        self, isolated_config_env: Path
    ) -> None:
        with pytest.raises(ConfigError) as exc_info:
            load_config(cli_overrides={"unknown_field": "x"})
        assert "unknown_field" in str(exc_info.value)


# ──────────────────────────────────────────────────────────────
# TestCustomHarness  (Kernel L4)
# ──────────────────────────────────────────────────────────────


class TestCustomHarness:
    """A user-declared harness lands alongside the built-ins."""

    def test_user_harness_added_alongside_builtins(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [harness.codex]
            command = "codex-cli"
            default_args = ["--quiet"]
            notification_template = "codex says hi on {channel}"
            """,
        )
        config = load_config()
        assert set(config.harnesses.keys()) == {
            "claude",
            "gemini",
            "antigravity",
            "codex",
        }
        codex = config.harnesses["codex"]
        assert codex == HarnessConfig(
            command="codex-cli",
            default_args=["--quiet"],
            notification_template="codex says hi on {channel}",
        )
        # Built-ins are intact.
        assert config.harnesses["claude"] == DEFAULT_HARNESSES["claude"]


# ──────────────────────────────────────────────────────────────
# TestSampleFile  (Gotcha 7.6 — the sample/parser structural lock)
# ──────────────────────────────────────────────────────────────


class TestSampleFile:
    """The bundled sample TOML must round-trip through load_config()."""

    def test_bundled_sample_roundtrips_without_error(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        sample = resources.files("letterbox.data") / "sample_letterbox.toml"
        with resources.as_file(sample) as sample_path:
            monkeypatch.setenv("LETTERBOX_CONFIG", str(sample_path))
            config = load_config()
        # Vision §8.2 verbatim — the harness shape exactly matches DEFAULT_*.
        assert config.harnesses == DEFAULT_HARNESSES
        # One declared channel: debate-01 with the documented description.
        assert config.channels == [
            ChannelConfig(name="debate-01", description="Default sample channel"),
        ]

    def test_bundled_sample_contains_load_bearing_identity_comment(self) -> None:
        sample = resources.files("letterbox.data") / "sample_letterbox.toml"
        text = sample.read_text(encoding="utf-8")
        # Gotcha 7.8: the identity-is-per-launch note is load-bearing.
        assert "identity is per-launch" in text
        assert "--as" in text or "LETTERBOX_SENDER" in text


# ──────────────────────────────────────────────────────────────
# TestPathSafety  (Gotcha 7.5)
# ──────────────────────────────────────────────────────────────


class TestPathSafety:
    """Tilde expansion replaces only a leading ``~``."""

    def test_embedded_tilde_preserved(
        self, isolated_config_env: Path
    ) -> None:
        embedded = isolated_config_env / "letterbox.toml"
        _write(
            embedded,
            """
            [letterbox]
            state_dir = "/tmp/has~tilde/in/it"
            """,
        )
        config = load_config()
        assert "~" in str(config.state_dir)
        assert str(config.state_dir).endswith("has~tilde/in/it")

    def test_leading_tilde_expanded(
        self, isolated_config_env: Path
    ) -> None:
        _write(
            isolated_config_env / "letterbox.toml",
            """
            [letterbox]
            state_dir = "~/alt-letterbox"
            """,
        )
        config = load_config()
        # Path.home() is patched to isolated_config_env via HOME.
        assert config.state_dir == (isolated_config_env / "alt-letterbox").resolve()
        assert "~" not in str(config.state_dir)


# ──────────────────────────────────────────────────────────────
# TestStateDirResolver
# ──────────────────────────────────────────────────────────────


class TestStateDirResolver:
    """``resolve_state_dir()`` precedence: env > cli_override > default."""

    def test_resolver_uses_env_when_set(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "env-target"
        monkeypatch.setenv("LETTERBOX_HOME", str(target))
        result = resolve_state_dir(cli_override="/var/should-lose")
        assert result == target.resolve()

    def test_resolver_uses_cli_override_when_no_env(
        self, isolated_config_env: Path
    ) -> None:
        result = resolve_state_dir(cli_override="/tmp/from-cli")
        assert result == Path("/tmp/from-cli").resolve()

    def test_resolver_falls_back_to_default(
        self, isolated_config_env: Path
    ) -> None:
        result = resolve_state_dir()
        assert result == (isolated_config_env / ".letterbox").resolve()

    def test_resolver_does_not_create_dir(
        self, isolated_config_env: Path, tmp_path: Path
    ) -> None:
        target = tmp_path / "uncreated"
        result = resolve_state_dir(cli_override=str(target))
        assert result == target.resolve()
        assert not target.exists()

    def test_resolver_returns_absolute_path(
        self, isolated_config_env: Path, tmp_path: Path
    ) -> None:
        monkeypatch_target = tmp_path / "rel-target"
        result = resolve_state_dir(cli_override=str(monkeypatch_target))
        assert result.is_absolute()

    def test_resolver_expands_leading_tilde(
        self,
        isolated_config_env: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("LETTERBOX_HOME", "~/state")
        result = resolve_state_dir()
        assert result == (isolated_config_env / "state").resolve()


# ──────────────────────────────────────────────────────────────
# Type-level sanity: LetterboxConfig is hashable when frozen.
# ──────────────────────────────────────────────────────────────


def test_dataclasses_are_frozen() -> None:
    h = HarnessConfig("x", [], "y")
    with pytest.raises(Exception):  # FrozenInstanceError subclasses Exception
        h.command = "z"  # type: ignore[misc]
    c = ChannelConfig("debate-01")
    with pytest.raises(Exception):
        c.name = "other"  # type: ignore[misc]
    lc = LetterboxConfig(Path("/tmp"), {}, [])
    with pytest.raises(Exception):
        lc.state_dir = Path("/other")  # type: ignore[misc]

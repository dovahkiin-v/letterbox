"""Tests for ``letterbox.cli`` — Phase 9a (top-level argparse dispatch).

Five concerns: (1) harness subcommands route to ``launcher.run_launcher`` under
``asyncio.run`` with the right kwargs + exit-code passthrough; (2) the ``mcp``
subcommand forwards its raw argv verbatim to ``mcp_server.run`` and names NONE of
the join-key flags (K2 / W13 silent-failure guard); (3) the ``--`` passthrough
split (K1); (4) the utility stubs fail loud without crashing (Gotcha #9); and
(5) the public surface — ``__all__``, the verbatim tier-header lock, and the
no-module-level-sibling-import bulkhead (§13.5).

The two dispatch sinks are mocked — ``run_launcher`` as a coroutine function
(``main`` ``asyncio.run``s it; a plain MagicMock isn't awaitable, Gotcha #5),
``mcp_server.run`` as a sync MagicMock. Patched where the lazy in-handler import
RESOLVES them (the 6a ``base.spawn_pty`` idiom, Gotcha #4). ``main`` is sync and
owns its own ``asyncio.run``, so no ``pytest.mark.asyncio``; no real PTY/state
dir is touched, so no conftest fixtures are required.
"""
from __future__ import annotations

import ast
import inspect
import json
import os
import re
from datetime import datetime, timedelta, timezone
from importlib import resources
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from letterbox import channel, cli, config, protocol

# Deterministic UTC base for built fixtures — never ``now()`` (so filename
# ordering is reproducible across runs / xdist workers). Each message offsets
# by whole seconds so the microsecond-precision filenames sort chronologically.
_BASE_TS = datetime(2026, 5, 28, 14, 30, 0, tzinfo=timezone.utc)


def _write_msg(
    channel_dir: Path,
    *,
    sender: str,
    body: str,
    ts: datetime,
    recipient: str | None = None,
    channel_name: str = "debate",
) -> Path:
    """Write one valid message into ``channel_dir`` via the sealed factories.

    Uses ``make_message_filename(ts)`` → ``new_message(id=stem, timestamp=ts)``
    → ``write_message`` so the on-disk filename embeds ``ts`` (lexical ==
    chronological ordering). Never hardcodes a filename (§8.3).
    """
    stem = protocol.make_message_filename(ts).removesuffix(".json")
    msg = protocol.new_message(
        id=stem,
        channel=channel_name,
        instance_id="inst-1",
        sender=sender,
        body=body,
        recipient=recipient,
        timestamp=ts,
    )
    return protocol.write_message(channel_dir, msg)

# Verbatim copy of cli.py's tier-header (lines 1-10). The
# test_tier_header_preserved_verbatim lock fails if a 9b/9c/9d body fill-in
# disturbs the §13.5 import-discipline record (Gotcha / §13.5). Mirrors the
# convention at test_launcher.py:80 / test_mcp_server.py:88.
_EXPECTED_TIER_HEADER = [
    '"""argparse top-level dispatch — routes subcommands to launcher / mcp_server / utility handlers.',
    "",
    "Tier: 4",
    "May import from: stdlib (including ``argparse``); Tier 1 (``config``, ``notifications``, ``protocol``); Tier 2 (``channel``).",
    "Must NOT import from: ``letterbox.launcher`` or ``letterbox.mcp_server`` at module load time —",
    "    those are imported LAZILY inside their respective subcommand handlers (bulkhead §13.5,",
    "    avoids cross-sibling-Tier-4 module-load dependency).",
    "",
    "Filled in: Phase 9a/9b/9c/9d per PHASE_INDEX.",
    '"""',
]


# ──────────────────────────────────────────────────────────────────────
# Local helpers — async run_launcher stub (records its call), sync mcp.run
# ──────────────────────────────────────────────────────────────────────


def _make_async_launcher(return_code: int = 0):
    """Return a coroutine-function stub for ``run_launcher`` that records its call.

    ``main`` ``asyncio.run``s ``run_launcher``, so the fake MUST return a
    coroutine (Gotcha #5). The recorded ``calls`` list lets tests assert on the
    forwarded args/kwargs.
    """

    calls: list[tuple[tuple, dict]] = []

    async def _fake(*args: object, **kwargs: object) -> int:
        calls.append((args, kwargs))
        return return_code

    _fake.calls = calls  # type: ignore[attr-defined]
    return _fake


@pytest.fixture
def patched_launcher(monkeypatch: pytest.MonkeyPatch):
    """Patch ``launcher.run_launcher`` with a recording coroutine stub (rc=0)."""
    fake = _make_async_launcher(0)
    monkeypatch.setattr("letterbox.launcher.run_launcher", fake)
    return fake


@pytest.fixture
def patched_mcp(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch ``mcp_server.run`` with a sync MagicMock (it blocks → returns None)."""
    mock = MagicMock(return_value=None)
    monkeypatch.setattr("letterbox.mcp_server.run", mock)
    return mock


@pytest.fixture(autouse=True)
def _isolate_config_env():
    """Keep each test's ``LETTERBOX_CONFIG`` mutation from leaking (Gotcha #8).

    The handlers set ``os.environ["LETTERBOX_CONFIG"]`` *directly* (the K5 env
    lever), so isolation MUST guarantee teardown removes it — including the case
    where the var was absent at setup. ``monkeypatch.delenv(raising=False)``
    registers NO teardown for an absent var, so a handler's mid-test write would
    leak to other tests/xdist workers (whose ``load_config`` then chokes on the
    now-stale path). This explicit save/clear/restore closes that hole for every
    test in the module — the only leakers of this var are CLI tests.
    """
    saved = os.environ.pop("LETTERBOX_CONFIG", None)
    try:
        yield
    finally:
        os.environ.pop("LETTERBOX_CONFIG", None)
        if saved is not None:
            os.environ["LETTERBOX_CONFIG"] = saved


# ──────────────────────────────────────────────────────────────────────
# TestHarnessDispatch — claude/gemini/antigravity → run_launcher
# ──────────────────────────────────────────────────────────────────────


class TestHarnessDispatch:
    def test_claude_minimal_dispatch(self, patched_launcher) -> None:
        rc = cli.main(["claude", "--channel", "debate-01"])
        assert rc == 0
        assert len(patched_launcher.calls) == 1
        args, kwargs = patched_launcher.calls[0]
        assert args == ("claude", "debate-01")
        assert kwargs == {
            "as_label": None,
            "cwd": Path.cwd(),
            "extra_args": [],
            "cli_overrides": None,
        }

    def test_gemini_as_and_cwd(self, patched_launcher) -> None:
        cli.main(["gemini", "--channel", "c", "--as", "researcher", "--cwd", "/tmp/x"])
        args, kwargs = patched_launcher.calls[0]
        assert args == ("gemini", "c")
        assert kwargs["as_label"] == "researcher"
        assert kwargs["cwd"] == Path("/tmp/x")

    def test_cwd_tilde_expands(self, patched_launcher) -> None:
        cli.main(["claude", "--channel", "c", "--cwd", "~/projects/myapp"])
        _args, kwargs = patched_launcher.calls[0]
        assert kwargs["cwd"] == Path("~/projects/myapp").expanduser()

    def test_passthrough_after_double_dash(self, patched_launcher) -> None:
        cli.main(["antigravity", "--channel", "c", "--", "--no-permissions-prompt"])
        _args, kwargs = patched_launcher.calls[0]
        assert kwargs["extra_args"] == ["--no-permissions-prompt"]

    def test_agy_alias_dispatches_as_antigravity(self, patched_launcher) -> None:
        # ``letterbox agy`` is an alias for ``antigravity``; it must route to the
        # canonical registry key, not the typed spelling, so the adapter lookup
        # and the [harness.antigravity] config block resolve unchanged.
        cli.main(["agy", "--channel", "c", "--as", "tower"])
        args, kwargs = patched_launcher.calls[0]
        assert args == ("antigravity", "c")
        assert kwargs["as_label"] == "tower"

    def test_passthrough_strips_only_first_double_dash(self, patched_launcher) -> None:
        # The `--` itself is stripped; everything after it (including a second
        # `--`) is verbatim passthrough (Gotcha #2).
        cli.main(["claude", "--channel", "x", "--", "--foo", "--bar"])
        _args, kwargs = patched_launcher.calls[0]
        assert kwargs["extra_args"] == ["--foo", "--bar"]

    @pytest.mark.parametrize("code", [0, 3])
    def test_exit_code_propagates(self, monkeypatch: pytest.MonkeyPatch, code: int) -> None:
        fake = _make_async_launcher(code)
        monkeypatch.setattr("letterbox.launcher.run_launcher", fake)
        assert cli.main(["claude", "--channel", "c"]) == code

    def test_config_flag_sets_env_abspath(self, patched_launcher) -> None:
        cli.main(["--config", "./my.toml", "claude", "--channel", "c"])
        assert os.environ["LETTERBOX_CONFIG"] == os.path.abspath("./my.toml")
        # Set BEFORE run_launcher was awaited (the launcher reads it internally).
        assert len(patched_launcher.calls) == 1

    def test_config_flag_expands_tilde(self, patched_launcher) -> None:
        cli.main(["--config", "~/cfg.toml", "claude", "--channel", "c"])
        assert os.environ["LETTERBOX_CONFIG"] == os.path.abspath(
            os.path.expanduser("~/cfg.toml")
        )

    def test_stray_harness_flag_rejected_with_vector(
        self, patched_launcher, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Harness flag surface is fixed (not transitional): a stray flag before
        # any `--` is a vector error (P3), NOT silently absorbed. run_launcher is
        # never reached.
        with pytest.raises(SystemExit) as exc:
            cli.main(["claude", "--channel", "c", "--bogus"])
        assert exc.value.code == 2
        assert "--bogus" in capsys.readouterr().err
        assert patched_launcher.calls == []


# ──────────────────────────────────────────────────────────────────────
# TestMcpForwarding — raw argv passthrough, NO join-key reparse (K2 / W13)
# ──────────────────────────────────────────────────────────────────────


class TestMcpForwarding:
    def test_mcp_forwards_raw_argv_verbatim(self, patched_mcp: MagicMock) -> None:
        rc = cli.main(["mcp", "--channel", "a", "--as", "b", "--instance-id", "c"])
        assert rc == 0
        patched_mcp.assert_called_once_with(
            ["--channel", "a", "--as", "b", "--instance-id", "c"]
        )

    def test_mcp_does_not_reach_run_launcher(
        self, patched_mcp: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _make_async_launcher(0)
        monkeypatch.setattr("letterbox.launcher.run_launcher", fake)
        cli.main(["mcp", "--channel", "a", "--as", "b", "--instance-id", "c"])
        assert fake.calls == []  # the harness sink is never touched by `mcp`

    def test_mcp_empty_argv_forwards_empty_list(self, patched_mcp: MagicMock) -> None:
        # A bare `letterbox mcp` forwards [] — mcp_server._parse_args then errors
        # on the missing required flags (its contract, not ours).
        assert cli.main(["mcp"]) == 0
        patched_mcp.assert_called_once_with([])

    def test_mcp_via_subparser_path_with_global_config(
        self, patched_mcp: MagicMock
    ) -> None:
        # Non-canonical ordering (global opt before `mcp`) bypasses the intercept
        # and exercises the registered subparser + _handle_mcp; REMAINDER still
        # forwards the join-key argv untouched.
        cli.main(
            ["--config", "/tmp/x.toml", "mcp", "--channel", "a", "--as", "b", "--instance-id", "c"]
        )
        patched_mcp.assert_called_once_with(
            ["--channel", "a", "--as", "b", "--instance-id", "c"]
        )


# ──────────────────────────────────────────────────────────────────────
# TestArgparseErrors — vector errors on bad/absent input (exit 2)
# ──────────────────────────────────────────────────────────────────────


class TestArgparseErrors:
    def test_bare_invocation_errors(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main([])
        assert exc.value.code == 2

    def test_unknown_subcommand_lists_valid_choices(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(["frobnicate"])
        assert exc.value.code == 2
        err = capsys.readouterr().err
        for choice in ("claude", "gemini", "antigravity", "mcp", "list-channels"):
            assert choice in err

    def test_missing_required_channel_errors(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(["claude"])
        assert exc.value.code == 2
        assert "--channel" in capsys.readouterr().err


# ──────────────────────────────────────────────────────────────────────
# TestConfigErrorBoundary — a bad --config surfaces as a vector, not a traceback
# ──────────────────────────────────────────────────────────────────────


class TestConfigErrorBoundary:
    """A malformed ``--config`` must produce a clean one-line stderr vector and
    ``return 1`` — never a raw Python traceback (Framework P3 / r1).

    The single ``main()`` guard covers all three consumer paths that reach
    ``config.load_config``; this exercises a *sync handler* (``list-channels`` →
    ``_resolve_state_dir``) and the *harness path* (``claude`` →
    ``asyncio.run(run_launcher)`` → ``setup_launcher``'s first line). No mocking:
    ``load_config`` raises before any state-dir touch or PTY spawn.
    """

    @staticmethod
    def _write_bad_config(tmp_path: Path) -> Path:
        """Write a malformed TOML file and return its path (mirrors the brief repro)."""
        bad = tmp_path / "bad.toml"
        bad.write_text("this is = = not valid toml [[[\n")
        return bad

    def test_sync_handler_bad_config_is_vector_not_traceback(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        bad = self._write_bad_config(tmp_path)
        rc = cli.main(["--config", str(bad), "list-channels"])
        assert rc == 1
        captured = capsys.readouterr()
        # Stable fragment of the tomllib reason (varies by Python micro-version)
        # plus the file path — the formatted vector, not the wall.
        assert "malformed TOML" in captured.err
        assert str(bad) in captured.err
        # Core regression: no traceback leaks to either stream.
        assert "Traceback" not in captured.err
        assert "Traceback" not in captured.out

    def test_harness_path_bad_config_is_vector_not_traceback(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        bad = self._write_bad_config(tmp_path)
        rc = cli.main(["--config", str(bad), "claude", "--channel", "c"])
        assert rc == 1
        captured = capsys.readouterr()
        assert "malformed TOML" in captured.err
        assert str(bad) in captured.err
        assert "Traceback" not in captured.err
        assert "Traceback" not in captured.out


# ──────────────────────────────────────────────────────────────────────
# TestHarnessStartupErrorBoundary — the four expected pre-spawn launcher
# errors render as one-line stderr vectors, never tracebacks (C2-3 / ADR-053)
# ──────────────────────────────────────────────────────────────────────


class TestHarnessStartupErrorBoundary:
    """Every EXPECTED pre-spawn startup error from ``run_launcher``'s validation
    chain must surface as a clean ``letterbox: <vector>`` line on stderr and
    ``return 1`` — never a raw traceback (Framework P3 / C2-3 / ADR-053).

    The four types: ``FileNotFoundError`` (command not on PATH), ``KeyError``
    (unknown adapter / missing ``[harness.<name>]`` block),
    ``StatePermissionsError`` (existing world-accessible state dir), and
    ``NotificationTemplateError`` (invalid configured template). Each is driven
    through the REAL ``run_launcher`` via ``cli.main([...])`` — NOT asserted at
    the ``setup_launcher`` level — because the gap C2-3 closes is precisely that
    the boundary renders them, not merely that they are raised.

    ``tmp_letterbox_home`` isolates the state dir (created 0700, so the
    permissions check passes for the cases that must reach later checks). The
    config-driven cases write a real ``--config`` TOML; the others force the
    condition at its real raise-site.
    """

    @staticmethod
    def _write_config(tmp_path: Path, *, command: str, template: str) -> Path:
        """Write a valid ``--config`` TOML whose ``[harness.claude]`` block
        replaces the default, and return its path. Both ``command`` and
        ``notification_template`` are required by the config parser."""
        cfg = tmp_path / "lb.toml"
        cfg.write_text(
            "[harness.claude]\n"
            f'command = "{command}"\n'
            f'notification_template = "{template}"\n',
            encoding="utf-8",
        )
        return cfg

    @staticmethod
    def _assert_clean_vector(captured: pytest.CaptureResult, rc: int) -> str:
        """Assert the exit code is 1, no traceback leaked to either stream, and
        the stderr carries the ``letterbox:`` vector prefix. Return stderr."""
        assert rc == 1
        assert "Traceback" not in captured.err
        assert "Traceback" not in captured.out
        assert "letterbox:" in captured.err
        return captured.err

    def test_command_not_on_path_is_vector_not_traceback(
        self,
        tmp_letterbox_home: Path,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # A configured harness command that is not on PATH → FileNotFoundError
        # from the launcher's PATH check (launcher.py §K4 step 5). Deterministic:
        # this binary name cannot exist on PATH.
        cfg = self._write_config(
            tmp_path,
            command="letterbox-no-such-binary-zzz999",
            template="📬 Peer message on channel {channel}.",
        )
        rc = cli.main(["--config", str(cfg), "claude", "--channel", "c"])
        err = self._assert_clean_vector(capsys.readouterr(), rc)
        assert "not on PATH" in err
        assert "letterbox-no-such-binary-zzz999" in err

    def test_bad_notification_template_is_vector_not_traceback(
        self,
        tmp_letterbox_home: Path,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # A configured template referencing the forbidden ``{body}`` variable →
        # NotificationTemplateError (launcher.py §K4 step 4), which fires BEFORE
        # the PATH check, so the command value is irrelevant.
        cfg = self._write_config(
            tmp_path, command="claude", template="📬 {body}"
        )
        rc = cli.main(["--config", str(cfg), "claude", "--channel", "c"])
        err = self._assert_clean_vector(capsys.readouterr(), rc)
        # The NotificationTemplateError message names the offending variable.
        assert "body" in err

    def test_world_accessible_state_dir_is_vector_not_traceback(
        self,
        tmp_letterbox_home: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # An EXISTING world-accessible state dir → StatePermissionsError from the
        # security gate (launcher.py §K4 step 1; the gate ADR-051 left unchanged).
        # 0o777 is fully permissive, so pytest's tmp_path teardown still traverses
        # it — no try/finally restore needed (unlike the 0o000 case, 3a notes).
        os.chmod(tmp_letterbox_home, 0o777)
        rc = cli.main(["claude", "--channel", "c"])
        err = self._assert_clean_vector(capsys.readouterr(), rc)
        # The permissions vector points the user at the fix.
        assert "0700" in err or "world" in err.lower()

    def test_unknown_adapter_keyerror_is_vector_not_traceback(
        self,
        tmp_letterbox_home: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # KeyError is unreachable for the three built-in harnesses via config
        # alone (DEFAULT_HARNESSES always populates them), so force it at the real
        # raise-site (get_adapter) to assert the boundary renders it cleanly —
        # including the quote-unwrap (KeyError stringifies as repr(arg)).
        def _raise_unknown(name: str):
            raise KeyError(
                f"Unknown harness {name!r}. "
                "Registered adapters: ['antigravity', 'claude', 'gemini']."
            )

        monkeypatch.setattr("letterbox.launcher.get_adapter", _raise_unknown)
        rc = cli.main(["claude", "--channel", "c"])
        err = self._assert_clean_vector(capsys.readouterr(), rc)
        assert "Unknown harness" in err
        # Quote-unwrap working: the message follows ``letterbox: `` directly, with
        # no stray leading quote from KeyError's repr-style str().
        vector_line = next(
            line for line in err.splitlines() if line.startswith("letterbox:")
        )
        body = vector_line[len("letterbox:") :].lstrip()
        assert not body.startswith("'")
        assert not body.startswith('"')

    def test_format_startup_error_unwraps_keyerror(self) -> None:
        # Unit-level guard on the rendering helper: KeyError loses its repr quotes,
        # every other type stringifies normally.
        assert cli._format_startup_error(KeyError("no foo")) == "no foo"
        assert cli._format_startup_error(FileNotFoundError("gone")) == "gone"
        assert (
            cli._format_startup_error(channel.StatePermissionsError("bad perms"))
            == "bad perms"
        )


# ──────────────────────────────────────────────────────────────────────
# TestPublicSurface — exports + tier-header lock + bulkhead import discipline
# ──────────────────────────────────────────────────────────────────────


class TestPublicSurface:
    def test_public_exports(self) -> None:
        assert cli.__all__ == ["main"]

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(cli).splitlines()
        assert source_lines[:10] == _EXPECTED_TIER_HEADER

    def test_no_module_level_sibling_imports(self) -> None:
        # §13.5 bulkhead: launcher/mcp_server are imported LAZILY in-handler, never
        # at module load (Tier-4 sibling isolation). Parse cli.py's top-level
        # import statements and assert neither sibling appears.
        tree = ast.parse(inspect.getsource(cli))
        module_level: set[str] = set()
        for node in tree.body:
            if isinstance(node, ast.Import):
                module_level.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                module_level.add(node.module)
        assert "letterbox.launcher" not in module_level
        assert "letterbox.mcp_server" not in module_level


# ──────────────────────────────────────────────────────────────────────
# TestTail — the human's window into a channel (read-only JSONL on stdout)
# ──────────────────────────────────────────────────────────────────────


class TestTail:
    def test_populated_channel_jsonl_chronological(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="researcher", body="first", ts=_BASE_TS)
        _write_msg(
            channel_dir, sender="critic", body="second", ts=_BASE_TS + timedelta(seconds=1)
        )
        _write_msg(
            channel_dir,
            sender="researcher",
            body="third",
            ts=_BASE_TS + timedelta(seconds=2),
        )

        rc = cli.main(["tail", "--channel", "debate"])
        assert rc == 0
        captured = capsys.readouterr()
        lines = captured.out.splitlines()
        assert len(lines) == 3
        # Each line is a complete, parseable JSON object (jq-friendly).
        bodies = [json.loads(line)["body"] for line in lines]
        assert bodies == ["first", "second", "third"]  # chronological
        assert captured.err == ""

    def test_unicode_body_not_ascii_escaped(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # §13.2: to_json_bytes uses ensure_ascii=False, so Lithuanian renders
        # as itself, not \uXXXX. cli.py delegates serialization (K2).
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="r", body="ačiū labai", ts=_BASE_TS)
        cli.main(["tail", "--channel", "debate"])
        out = capsys.readouterr().out
        assert "ačiū labai" in out
        assert "\\u" not in out

    def test_missing_channel_vector_and_no_create(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["tail", "--channel", "ghost"])
        assert rc == 1
        err = capsys.readouterr().err
        assert "no such channel" in err
        assert "ghost" in err
        # Read-only inspection: the channel dir must NOT be auto-created (G3).
        assert not (tmp_letterbox_home / "channels" / "ghost").exists()

    def test_invalid_channel_name_vector(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["tail", "--channel", "Bad Name"])
        assert rc == 1
        assert "invalid channel name" in capsys.readouterr().err

    def test_corrupt_message_skipped_with_stderr_warn(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="researcher", body="valid", ts=_BASE_TS)
        # A file with a VALID message filename but garbage bytes — passes
        # list_messages's filename gate, fails read_message's JSON parse.
        bad_name = protocol.make_message_filename(_BASE_TS + timedelta(seconds=1))
        (channel_dir / bad_name).write_bytes(b"{not valid json")

        rc = cli.main(["tail", "--channel", "debate"])
        assert rc == 0
        captured = capsys.readouterr()
        # stdout stays pure valid JSONL — exactly the one good message.
        lines = captured.out.splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["body"] == "valid"
        # stderr names the corrupt file once.
        assert bad_name in captured.err
        assert "corrupt" in captured.err

    def test_bogus_flag_is_vector_error_exit2(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # tail now has a closed flag surface (G2) — stray flags are rejected.
        with pytest.raises(SystemExit) as exc:
            cli.main(["tail", "--channel", "x", "--bogus"])
        assert exc.value.code == 2
        assert "--bogus" in capsys.readouterr().err

    def test_follow_prints_backlog_then_new_then_clean_exit(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="researcher", body="backlog", ts=_BASE_TS)

        # Fake sleep: 1st tick injects a new message; 2nd tick is the user's
        # Ctrl-C. Exercises exactly one real poll cycle + a clean exit, with no
        # real delay (deterministic by construction — PLANNING_NOTES rule).
        state = {"n": 0}

        def _fake_sleep(_seconds: float) -> None:
            state["n"] += 1
            if state["n"] == 1:
                _write_msg(
                    channel_dir,
                    sender="critic",
                    body="live",
                    ts=_BASE_TS + timedelta(seconds=1),
                )
            else:
                raise KeyboardInterrupt

        monkeypatch.setattr("time.sleep", _fake_sleep)

        rc = cli.main(["tail", "--channel", "debate", "--follow"])
        assert rc == 0  # KeyboardInterrupt → clean exit (G4)
        bodies = [json.loads(line)["body"] for line in capsys.readouterr().out.splitlines()]
        assert bodies == ["backlog", "live"]

    def test_follow_handles_channel_dir_vanishing(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # If the channel dir is rm'd mid-follow, list_messages raises
        # FileNotFoundError — caught as a clean stderr vector + exit 1, not a
        # traceback (no dead ends; the G3 entry pre-check only guards startup).
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="r", body="backlog", ts=_BASE_TS)

        def _fake_sleep(_seconds: float) -> None:
            # Simulate the operator removing the channel between polls.
            for child in channel_dir.iterdir():
                child.unlink()
            channel_dir.rmdir()

        monkeypatch.setattr("time.sleep", _fake_sleep)
        rc = cli.main(["tail", "--channel", "debate", "--follow"])
        assert rc == 1
        captured = capsys.readouterr()
        assert "vanished" in captured.err
        assert json.loads(captured.out.strip())["body"] == "backlog"  # backlog still dumped

    def test_tail_once_skips_prune_race(
        self, tmp_letterbox_home: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A file listed then removed before read_message → FileNotFoundError,
        # skipped silently (gone, not corrupt). _tail_once still returns the
        # cursor so the loop advances past it.
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        path = _write_msg(channel_dir, sender="r", body="vanished", ts=_BASE_TS)

        def _raise_fnf(_p: Path) -> object:
            raise FileNotFoundError(_p)

        monkeypatch.setattr(protocol, "read_message", _raise_fnf)
        out, err = StringIO(), StringIO()
        cursor = cli._tail_once(
            channel_dir, None, fmt="plain", use_color=False, out=out, err=err
        )
        assert out.getvalue() == ""
        assert err.getvalue() == ""
        assert cursor == path.name  # cursor advanced past the vanished file


# ──────────────────────────────────────────────────────────────────────
# TestListChannels — "what's even running?" enumeration
# ──────────────────────────────────────────────────────────────────────


class TestListChannels:
    def test_empty_install_nothing_on_stdout_note_on_stderr(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["list-channels"])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out == ""  # pipe stays clean
        assert "no channels" in captured.err

    def test_plain_lists_name_tab_activity_name_sorted(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channels_root = tmp_letterbox_home / "channels"
        alpha = channels_root / "alpha"
        alpha.mkdir(parents=True)
        _write_msg(alpha, sender="r", body="hi", ts=_BASE_TS, channel_name="alpha")
        (channels_root / "beta").mkdir()  # empty channel → (no messages)

        rc = cli.main(["list-channels"])
        assert rc == 0
        lines = capsys.readouterr().out.splitlines()
        assert len(lines) == 2
        # Name-sorted (list_channels guarantees it).
        assert lines[0].startswith("alpha\t")
        assert lines[1].startswith("beta\t")
        # Tab-separated; empty channel gets the textual sentinel.
        assert "\t" in lines[0]
        assert lines[1] == "beta\t(no messages)"

    def test_rich_format_has_header_and_rows(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        alpha = tmp_letterbox_home / "channels" / "alpha"
        alpha.mkdir(parents=True)
        _write_msg(alpha, sender="r", body="hi", ts=_BASE_TS, channel_name="alpha")

        rc = cli.main(["list-channels", "--format=rich", "--color=never"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "CHANNEL" in out
        assert "LAST ACTIVITY" in out
        assert "alpha" in out
        assert "\x1b[" not in out  # color=never → no escapes

    def test_bogus_flag_is_vector_error_exit2(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(["list-channels", "--bogus"])
        assert exc.value.code == 2
        assert "--bogus" in capsys.readouterr().err

    def test_config_flag_resolves_state_dir_from_toml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Success-Criterion #9. Deliberately NOT using tmp_letterbox_home:
        # LETTERBOX_HOME (level 5) would mask the config-file state_dir (level
        # 3). Use the isolated_config_env pattern instead (scout discrepancy #1).
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("LETTERBOX_HOME", raising=False)
        monkeypatch.delenv("LETTERBOX_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)

        custom_state = tmp_path / "custom_state"
        chan_dir = custom_state / "channels" / "fromconfig"
        chan_dir.mkdir(parents=True)
        _write_msg(chan_dir, sender="r", body="hi", ts=_BASE_TS, channel_name="fromconfig")
        cfg = tmp_path / "x.toml"
        cfg.write_text(f'[letterbox]\nstate_dir = "{custom_state}"\n')

        rc = cli.main(["--config", str(cfg), "list-channels"])
        assert rc == 0
        assert "fromconfig" in capsys.readouterr().out


# ──────────────────────────────────────────────────────────────────────
# TestColorAndFormat — output-discipline helpers (the precedent 10a asserts)
# ──────────────────────────────────────────────────────────────────────


class TestColorAndFormat:
    def test_tail_rich_color_always_has_escapes(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        cli.main(["tail", "--channel", "debate", "--format=rich", "--color=always"])
        out = capsys.readouterr().out
        assert "\x1b[" in out
        assert "researcher" in out

    def test_tail_rich_color_never_no_escapes(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(
            channel_dir, sender="researcher", body="hi", ts=_BASE_TS, recipient="critic"
        )
        cli.main(["tail", "--channel", "debate", "--format=rich", "--color=never"])
        out = capsys.readouterr().out
        assert "\x1b[" not in out
        assert "researcher → critic" in out  # arrow present when recipient set

    def test_tail_plain_is_default_and_colorless(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        # No --format flag → plain (JSONL); no --color → auto, non-TTY → no color.
        cli.main(["tail", "--channel", "debate", "--color=always"])
        out = capsys.readouterr().out.strip()
        assert "\x1b[" not in out  # plain is unconditionally colorless (K4)
        assert json.loads(out)["body"] == "hi"  # JSONL, not rich

    def test_render_message_rich_omits_arrow_without_recipient(self) -> None:
        msg = protocol.new_message(
            id="x", channel="c", instance_id="i", sender="alice", body="yo",
            timestamp=_BASE_TS,
        )
        line = cli._render_message(msg, fmt="rich", use_color=False)
        assert "→" not in line
        assert line.endswith("alice: yo")

    def test_should_use_color_resolves_all_branches(self) -> None:
        class _TTY:
            def isatty(self) -> bool:
                return True

        class _NotTTY:
            def isatty(self) -> bool:
                return False

        assert cli._should_use_color("never", _TTY()) is False
        assert cli._should_use_color("always", _NotTTY()) is True
        assert cli._should_use_color("auto", _TTY()) is True
        assert cli._should_use_color("auto", _NotTTY()) is False
        # A stream with no isatty() attribute → no color under auto.
        assert cli._should_use_color("auto", object()) is False

    def test_colorize_disabled_returns_raw(self) -> None:
        assert cli._colorize("hi", cli._ANSI_BOLD, enabled=False) == "hi"
        assert cli._colorize("hi", cli._ANSI_BOLD, enabled=True) == "\x1b[1mhi\x1b[0m"


# ──────────────────────────────────────────────────────────────────────
# TestInit — the first write command: scaffold letterbox.toml (9c)
# ──────────────────────────────────────────────────────────────────────


def _read_sample() -> str:
    """Return the bundled 1c sample TOML text (init's verbatim source, K5)."""
    return (
        resources.files("letterbox.data")
        .joinpath("sample_letterbox.toml")
        .read_text(encoding="utf-8")
    )


@pytest.fixture
def init_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect both of init's write targets into ``tmp_path``.

    init writes ``./letterbox.toml`` (cwd-derived) and, under ``--global``,
    ``~/.letterbox/config.toml`` (HOME-derived). chdir + ``HOME`` redirect both
    into tmp so a test never touches the repo or the real home (§7 Gotcha). NOT
    ``tmp_letterbox_home`` — that sets ``LETTERBOX_HOME``, leaves ``HOME`` alone
    (1c gotcha); both of init's targets and ``load_config``'s user-global path
    are HOME-derived. ``LETTERBOX_HOME`` is cleared so any ambient value can't
    skew ``load_config().state_dir`` (autouse ``_isolate_config_env`` already
    handles ``LETTERBOX_CONFIG``).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("LETTERBOX_HOME", raising=False)
    return tmp_path


@pytest.fixture
def permissive_umask():
    """Save/restore the process umask so ``mkdir(mode=0o700)`` lands full mode.

    Clone of ``tests/test_channel.py``'s fixture — needed for the ``--global``
    ``0700`` directory-mode assertion (the default ``0o022`` umask would strip
    group/other bits before the assertion can see them).
    """
    old = os.umask(0)
    try:
        yield
    finally:
        os.umask(old)


class TestInit:
    def test_default_writes_project_local_byte_equal_to_sample(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["init"])
        assert rc == 0
        target = init_env / "letterbox.toml"
        assert target.is_file()
        # Criterion #1: byte-equal to the shipped sample (verbatim scaffold).
        assert target.read_text(encoding="utf-8") == _read_sample()
        # Status on stdout, not stderr.
        captured = capsys.readouterr()
        assert str(target) in captured.out
        assert captured.err == ""

    def test_global_writes_user_global_creating_dir_0700(
        self, init_env: Path, permissive_umask, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["init", "--global"])
        assert rc == 0
        target = init_env / ".letterbox" / "config.toml"
        assert target.is_file()
        # Criterion #2: ~/.letterbox/ created at mode 0700 when missing.
        assert oct((init_env / ".letterbox").stat().st_mode & 0o777) == "0o700"
        assert target.read_text(encoding="utf-8") == _read_sample()
        assert str(target) in capsys.readouterr().out

    def test_global_does_not_touch_existing_dir_mode(
        self, init_env: Path, permissive_umask
    ) -> None:
        # A pre-existing ~/.letterbox at a looser mode must NOT be re-tightened
        # (Gotcha: 0700 only when WE create the dir).
        home_lb = init_env / ".letterbox"
        home_lb.mkdir(mode=0o755)
        os.chmod(home_lb, 0o755)
        rc = cli.main(["init", "--global"])
        assert rc == 0
        assert oct(home_lb.stat().st_mode & 0o777) == "0o755"

    def test_refuse_overwrite_project_local(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = init_env / "letterbox.toml"
        target.write_text("pre-existing\n", encoding="utf-8")
        rc = cli.main(["init"])
        # Criterion #3: nothing written, error citing the path on stderr, exit 1.
        assert rc == 1
        assert target.read_text(encoding="utf-8") == "pre-existing\n"
        err = capsys.readouterr().err
        assert str(target) in err
        assert "refusing to overwrite" in err

    def test_refuse_overwrite_global(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        home_lb = init_env / ".letterbox"
        home_lb.mkdir()
        target = home_lb / "config.toml"
        target.write_text("pre-existing\n", encoding="utf-8")
        rc = cli.main(["init", "--global"])
        assert rc == 1
        assert target.read_text(encoding="utf-8") == "pre-existing\n"
        assert str(target) in capsys.readouterr().err

    def test_channel_flag_sets_name_and_drops_placeholder(
        self, init_env: Path
    ) -> None:
        rc = cli.main(["init", "--channel", "myteam"])
        assert rc == 0
        content = (init_env / "letterbox.toml").read_text(encoding="utf-8")
        # Criterion #4: the [[channels]] entry is the requested name, no leftover.
        assert 'name = "myteam"' in content
        assert "debate-01" not in content
        assert content.count("[[channels]]") == 1

    def test_verbatim_roundtrips_through_load_config(self, init_env: Path) -> None:
        cli.main(["init"])
        # Criterion #5: scaffolded file loads, surfacing the sample's channel.
        cfg = config.load_config()
        assert "debate-01" in [c.name for c in cfg.channels]

    def test_channel_roundtrips_through_load_config(self, init_env: Path) -> None:
        cli.main(["init", "--channel", "myteam"])
        cfg = config.load_config()
        assert [c.name for c in cfg.channels] == ["myteam"]

    def test_global_roundtrips_through_load_config(
        self, init_env: Path, permissive_umask
    ) -> None:
        cli.main(["init", "--global"])
        cfg = config.load_config()
        assert "debate-01" in [c.name for c in cfg.channels]

    def test_bogus_flag_exits_2(self, capsys: pytest.CaptureFixture[str]) -> None:
        # Criterion #6: init now rejects unknown flags (it's in _REJECTS_UNKNOWN).
        with pytest.raises(SystemExit) as exc:
            cli.main(["init", "--bogus"])
        assert exc.value.code == 2
        assert "unrecognized arguments" in capsys.readouterr().err

    @pytest.mark.parametrize("bad", ["../etc", "Has Space", "UPPER", "with/slash"])
    def test_invalid_channel_name_exits_1_nothing_written(
        self, init_env: Path, capsys: pytest.CaptureFixture[str], bad: str
    ) -> None:
        rc = cli.main(["init", "--channel", bad])
        # Criterion #7: clear stderr vector, exit 1, nothing written.
        assert rc == 1
        assert not (init_env / "letterbox.toml").exists()
        err = capsys.readouterr().err
        assert "invalid channel name" in err

    def test_emoji_survives_roundtrip(self, init_env: Path) -> None:
        cli.main(["init"])
        content = (init_env / "letterbox.toml").read_text(encoding="utf-8")
        # Criterion #8: the 📬 notification_template emoji survives write→read.
        assert "📬" in content

    def test_no_temp_litter_left_behind(self, init_env: Path) -> None:
        # The atomic write's sibling .tmp must be renamed away, not left littering.
        cli.main(["init"])
        leftovers = list(init_env.glob("letterbox.toml*.tmp"))
        assert leftovers == []

    def test_write_failure_cleans_up_temp_and_propagates(
        self, init_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A failed publish (os.replace raises) must clean up the sibling .tmp so a
        # crashed init leaves no litter beside the target (§14 cleanup-on-error).
        def _boom(*_args: object, **_kwargs: object) -> None:
            raise OSError("simulated replace failure")

        monkeypatch.setattr(cli.os, "replace", _boom)
        with pytest.raises(OSError, match="simulated replace failure"):
            cli.main(["init"])
        assert not (init_env / "letterbox.toml").exists()
        assert list(init_env.glob("letterbox.toml*.tmp")) == []


# ──────────────────────────────────────────────────────────────────────
# Prune fixtures + helpers (9d)
# ──────────────────────────────────────────────────────────────────────


def _seed_channel(
    home: Path, *, count: int, name: str = "debate", base: datetime = _BASE_TS
) -> tuple[Path, list[str]]:
    """Create ``<home>/channels/<name>/`` and seed ``count`` ascending messages.

    Each message's filename timestamp offsets by whole seconds from ``base`` so
    on-disk lexical order == chronological order (§3.2). Never touches mtime —
    age in prune is filename-authoritative (K4).

    Returns:
        The channel dir and the ordered list of message filenames (oldest first).
    """
    channel_dir = home / "channels" / name
    channel_dir.mkdir(parents=True)
    ids = [
        _write_msg(
            channel_dir, sender="alice", body=f"m{i}", ts=base + timedelta(seconds=i)
        ).name
        for i in range(count)
    ]
    return channel_dir, ids


def _write_read_file(channel_dir: Path, label: str, high_water_mark: str) -> Path:
    """Write a ``.read/<label>.json`` endpoint read-state file (ReadState shape)."""
    read_dir = channel_dir / ".read"
    read_dir.mkdir(exist_ok=True)
    path = read_dir / f"{label}.json"
    path.write_text(
        json.dumps(
            {
                "sender_label": label,
                "instance_id": "inst-1",
                "high_water_mark": high_water_mark,
                "updated_at": "2026-05-28T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    return path


def _msg_names(directory: Path) -> list[str]:
    """Sorted ``msg-*.json`` filenames directly under ``directory`` (no recursion)."""
    return sorted(p.name for p in directory.glob("msg-*.json"))


class TestPrune:
    def test_dry_run_keep_last_previews_without_touching(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #1: bare selection = dry-run. The 3 oldest ids on stdout, a
        # "would move 3 to cold/" summary on stderr, exit 0, all 8 still present,
        # no cold/ created.
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        rc = cli.main(["prune", "--channel", "debate", "--keep-last", "5"])
        assert rc == 0
        captured = capsys.readouterr()
        out_lines = captured.out.split()
        assert out_lines == ids[:3]  # the 3 oldest, on stdout
        assert "would move 3" in captured.err and "cold/" in captured.err
        assert _msg_names(channel_dir) == sorted(ids)  # all 8 untouched
        assert not (channel_dir / "cold").exists()

    def test_move_to_cold_on_consent(
        self,
        tmp_letterbox_home: Path,
        permissive_umask,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Criterion #2: --yes-i-am-sure moves the 3 oldest into cold/ (mode 0700),
        # 5 newest stay in root, exit 0, ZERO files deleted (8 still on disk).
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        rc = cli.main(
            ["prune", "--channel", "debate", "--keep-last", "5", "--yes-i-am-sure"]
        )
        assert rc == 0
        cold = channel_dir / "cold"
        assert _msg_names(channel_dir) == sorted(ids[3:])  # 5 newest remain in root
        assert _msg_names(cold) == sorted(ids[:3])  # 3 oldest moved to cold/
        assert oct(cold.stat().st_mode & 0o777) == "0o700"
        # Nothing deleted: 8 files still exist (5 root + 3 cold).
        assert len(_msg_names(channel_dir)) + len(_msg_names(cold)) == 8
        assert "moved 3" in capsys.readouterr().err

    def test_delete_on_double_gate(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #3: --delete --yes-i-am-sure removes the 3 oldest entirely
        # (not in cold/, not in root); 5 remain; exit 0.
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        rc = cli.main(
            [
                "prune",
                "--channel",
                "debate",
                "--keep-last",
                "5",
                "--delete",
                "--yes-i-am-sure",
            ]
        )
        assert rc == 0
        assert _msg_names(channel_dir) == sorted(ids[3:])  # 5 remain
        assert not (channel_dir / "cold").exists()  # nothing moved
        assert "deleted 3" in capsys.readouterr().err

    def test_delete_without_consent_refuses(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #4: --delete without --yes-i-am-sure refuses (exit 1), names
        # the consent flag, leaves all 8 untouched, no cold/.
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        rc = cli.main(
            ["prune", "--channel", "debate", "--keep-last", "5", "--delete"]
        )
        assert rc == 1
        err = capsys.readouterr().err
        assert "--yes-i-am-sure" in err
        assert _msg_names(channel_dir) == sorted(ids)  # all 8 untouched
        assert not (channel_dir / "cold").exists()

    def test_older_than_ages_by_filename_not_mtime(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #5: --older-than ages by the FILENAME timestamp (K4). Build an
        # old message (filename ts = now - 10d) and a new one (now - 1h); never
        # touch mtime. --older-than 7d matches only the old one.
        now = datetime.now(timezone.utc)
        channel_dir = tmp_letterbox_home / "channels" / "debate"
        channel_dir.mkdir(parents=True)
        old = _write_msg(
            channel_dir, sender="alice", body="old", ts=now - timedelta(days=10)
        ).name
        new = _write_msg(
            channel_dir, sender="alice", body="new", ts=now - timedelta(hours=1)
        ).name
        rc = cli.main(["prune", "--channel", "debate", "--older-than", "7d"])
        assert rc == 0
        out = capsys.readouterr().out
        assert old in out
        assert new not in out

    def test_acknowledged_by_all_matches_min_high_water_mark(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #6 (main case): two endpoints with differing hwm → match only
        # ids <= min(hwm) (inclusive).
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        _write_read_file(channel_dir, "alice", ids[4].removesuffix(".json"))
        _write_read_file(channel_dir, "bob", ids[2].removesuffix(".json"))  # the min
        rc = cli.main(["prune", "--channel", "debate", "--acknowledged-by-all"])
        assert rc == 0
        out_lines = capsys.readouterr().out.split()
        assert out_lines == ids[:3]  # ids[0..2] <= ids[2] (inclusive)

    def test_acknowledged_by_all_fails_safe_on_empty_mark(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #6 (fresh endpoint): one endpoint at hwm="" drives min to "" →
        # match nothing, exit 0.
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        _write_read_file(channel_dir, "alice", ids[4].removesuffix(".json"))
        _write_read_file(channel_dir, "bob", "")  # fresh — never acknowledged
        rc = cli.main(["prune", "--channel", "debate", "--acknowledged-by-all"])
        assert rc == 0
        assert capsys.readouterr().out == ""  # nothing matched
        assert _msg_names(channel_dir) == sorted(ids)  # untouched

    def test_acknowledged_by_all_fails_safe_on_corrupt_read_file(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #6 (corrupt .read): an unparseable read-state → that endpoint
        # is treated as fresh → match nothing. AND the corrupt file is left
        # BYTE-IDENTICAL (prune never renames/rewrites read-state — K1/L8).
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        _write_read_file(channel_dir, "alice", ids[4].removesuffix(".json"))
        corrupt = channel_dir / ".read" / "bob.json"
        corrupt.parent.mkdir(exist_ok=True)
        corrupt.write_bytes(b"{ this is not valid json ")
        before = corrupt.read_bytes()
        rc = cli.main(["prune", "--channel", "debate", "--acknowledged-by-all"])
        assert rc == 0
        assert capsys.readouterr().out == ""  # nothing matched
        assert corrupt.read_bytes() == before  # byte-identical — no rename/rewrite
        assert not (channel_dir / ".read").joinpath("bob.json.broken").exists()

    def test_acknowledged_by_all_no_endpoints_notes_and_matches_nothing(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #6 (no .read/*.json): match nothing + an informational note,
        # exit 0.
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=4)
        rc = cli.main(["prune", "--channel", "debate", "--acknowledged-by-all"])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "no endpoints have acknowledged" in captured.err
        assert _msg_names(channel_dir) == sorted(ids)

    def test_keep_last_zero_matches_all(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The slice footgun: msgs[:-0] is empty, NOT "all". --keep-last 0 must
        # match every message (§7 gotcha).
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=4)
        rc = cli.main(["prune", "--channel", "debate", "--keep-last", "0"])
        assert rc == 0
        assert capsys.readouterr().out.split() == ids  # all 4 matched

    def test_keep_last_ge_count_matches_none(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=3)
        rc = cli.main(["prune", "--channel", "debate", "--keep-last", "5"])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "nothing to prune" in captured.err

    def test_no_selection_rule_is_argparse_error(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Criterion #7: a bare ``prune --channel x`` (no rule) → argparse exit 2.
        # There is no "prune everything by accident".
        with pytest.raises(SystemExit) as exc:
            cli.main(["prune", "--channel", "debate"])
        assert exc.value.code == 2

    def test_mutually_exclusive_rules_rejected(
        self, tmp_letterbox_home: Path
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(
                ["prune", "--channel", "debate", "--keep-last", "1", "--older-than", "7d"]
            )
        assert exc.value.code == 2

    @pytest.mark.parametrize("bad", ["7x", "abc", "", "7d3h", "7"])
    def test_bad_duration_is_argparse_error(
        self, tmp_letterbox_home: Path, bad: str
    ) -> None:
        # Criterion #8: a malformed --older-than → argparse exit 2 vector.
        with pytest.raises(SystemExit) as exc:
            cli.main(["prune", "--channel", "debate", "--older-than", bad])
        assert exc.value.code == 2

    def test_negative_keep_last_is_argparse_error(
        self, tmp_letterbox_home: Path
    ) -> None:
        # Criterion #8: --keep-last -1 → argparse exit 2 vector.
        with pytest.raises(SystemExit) as exc:
            cli.main(["prune", "--channel", "debate", "--keep-last", "-1"])
        assert exc.value.code == 2

    @pytest.mark.parametrize("bad", ["abc", "1.5", "5x"])
    def test_non_integer_keep_last_is_argparse_error(
        self, tmp_letterbox_home: Path, bad: str
    ) -> None:
        # Criterion #8: a non-integer --keep-last → argparse exit 2 vector.
        with pytest.raises(SystemExit) as exc:
            cli.main(["prune", "--channel", "debate", "--keep-last", bad])
        assert exc.value.code == 2

    def test_nonexistent_channel_exits_1_no_dir_created(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Criterion #9: a missing channel → exit 1, "no such channel" vector, and
        # NO directory created (read-only inspection; never Channel.get_or_create).
        rc = cli.main(["prune", "--channel", "nonesuch", "--keep-last", "1"])
        assert rc == 1
        assert "no such channel" in capsys.readouterr().err
        assert not (tmp_letterbox_home / "channels" / "nonesuch").exists()

    def test_invalid_channel_name_exits_1(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["prune", "--channel", "UPPER", "--keep-last", "1"])
        assert rc == 1
        assert "invalid channel name" in capsys.readouterr().err

    def test_unknown_flag_rejected_exit_2(self, tmp_letterbox_home: Path) -> None:
        # Criterion #10: prune is in _REJECTS_UNKNOWN — a stray flag is exit 2.
        with pytest.raises(SystemExit) as exc:
            cli.main(["prune", "--channel", "debate", "--keep-last", "1", "--bogus"])
        assert exc.value.code == 2

    def test_move_leaves_read_state_untouched(
        self,
        tmp_letterbox_home: Path,
        permissive_umask,
    ) -> None:
        # Criterion #12: the cold/ move never writes/renames/deletes .read state.
        channel_dir, ids = _seed_channel(tmp_letterbox_home, count=8)
        read_path = _write_read_file(channel_dir, "alice", ids[2].removesuffix(".json"))
        before = read_path.read_bytes()
        rc = cli.main(
            ["prune", "--channel", "debate", "--keep-last", "5", "--yes-i-am-sure"]
        )
        assert rc == 0
        assert read_path.read_bytes() == before  # read-state byte-identical


# ──────────────────────────────────────────────────────────────────────
# TestIroncladInvariant — prune is the ONLY user-message deletion path (L8)
# ──────────────────────────────────────────────────────────────────────


# The deletion-primitive scan (Kernel L8). os.rename/os.replace are MOVES, not
# deletions, so they are deliberately absent (the cold/ move + the .broken
# corruption-rename are L8-safe by construction).
_DELETION_RE = re.compile(r"os\.unlink|os\.remove|\.unlink\(|shutil\.rmtree|os\.rmdir")

# Each curated site is bound to (module relative path, actual call-form
# substring, justification) — NOT a bare occurrence count (a count silently
# passes a swapped site; PLANNING_NOTES versioned-assertion lesson). The
# call-form binds to the ACTUAL primitive at the site (e.g.
# ``path.unlink(missing_ok=True)``, not ``os.unlink``) so a future swap to a
# different primitive at the same line fails the test loudly.
_DELETION_ALLOWLIST = [
    (
        "adapters/mcp_config.py",
        "path.unlink(missing_ok=True)",
        "a generated temp MCP config file (tempfile.mkstemp) — not a channel message",
    ),
    (
        "protocol.py",
        "os.unlink(entry.path)",
        "orphaned msg-*.json.tmp files >1h old — a .tmp, never a published message",
    ),
    (
        "cli.py",
        "tmp_path.unlink()",
        "init's own temp on the error path — a .tmp, never a published message",
    ),
    (
        "cli.py",
        "os.unlink(path)",
        "THE user-message deletion path — only behind --delete --yes-i-am-sure (L8)",
    ),
    (
        "launcher.py",
        "lock_path.unlink()",
        "pid lock file in state_dir/locks/ — not a channel message, released on session exit",
    ),
]


class TestIroncladInvariant:
    def _scan(self) -> list[tuple[str, int, str]]:
        """Return every (module-rel-path, lineno, stripped-line) deletion-primitive hit."""
        pkg_root = Path(cli.__file__).parent  # letterbox/
        found: list[tuple[str, int, str]] = []
        for py in sorted(pkg_root.rglob("*.py")):
            for lineno, line in enumerate(
                py.read_text(encoding="utf-8").splitlines(), start=1
            ):
                if _DELETION_RE.search(line):
                    found.append(
                        (py.relative_to(pkg_root).as_posix(), lineno, line.strip())
                    )
        return found

    def test_every_deletion_site_is_on_the_curated_allowlist(self) -> None:
        # Criterion #11: scanning letterbox/ sources, every deletion-primitive hit
        # must match exactly one curated allowlist entry by (module, call-form).
        # A new, unlisted site means: if it does NOT delete a user message file
        # (msg-*.json), add it to the allowlist with a one-line justification. If
        # it DOES, you are breaking Kernel L8 — stop and escalate.
        unmatched: list[tuple[str, int, str]] = []
        for rel, lineno, text in self._scan():
            hits = [
                entry
                for entry in _DELETION_ALLOWLIST
                if entry[0] == rel and entry[1] in text
            ]
            if len(hits) != 1:
                unmatched.append((rel, lineno, text))
        assert not unmatched, (
            "Un-allowlisted deletion primitive(s) found in letterbox/:\n"
            + "\n".join(f"  {rel}:{lineno}: {text}" for rel, lineno, text in unmatched)
            + "\n\nYou added a deletion primitive. If it does NOT delete a user "
            "message file (msg-*.json), add it to _DELETION_ALLOWLIST with a "
            "one-line justification. If it DOES, you are breaking Kernel L8 — "
            "stop and escalate."
        )

    def test_prune_deletion_path_is_present(self) -> None:
        # Guard against silent removal: the one sanctioned message-deletion site
        # must exist (a deleted-by-accident prune deletion would otherwise pass
        # the allowlist test trivially).
        prune_sites = [
            (rel, lineno, text)
            for rel, lineno, text in self._scan()
            if rel == "cli.py" and "os.unlink(path)" in text
        ]
        assert len(prune_sites) == 1, (
            "Expected exactly one prune deletion site (cli.py os.unlink(path)), "
            f"found {len(prune_sites)}: {prune_sites}"
        )

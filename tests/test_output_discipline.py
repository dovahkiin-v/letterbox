"""The Vision §11.1 accessibility contract, made executable (Phase 10a).

This is the single auditable home for letterbox's output-discipline promises
(Framework P9 — Universal Access). ``test_cli.py`` proves each command behaves
*per command*; this file proves the §11.1 *invariant* holds *across the whole
CLI-rendered surface* — so a reviewer can point at one file and verify the door
is open to everyone, and any future regression (a banner, a ``\\uXXXX``-escaped
body, a color-only state signal) fails a test here.

Organized by **commitment** (C1–C7 of §11.1), not by command (Decision K1).
Overlap with ``test_cli.py::TestColorAndFormat`` is intentional and acceptable
— that file is command-scoped; this one is the contract. Neither owns the other;
do NOT delete or rewrite the command-scoped tests.

**Scope (Decision K2):** the four CLI-*rendered* utility commands — ``tail``,
``list-channels``, ``init``, ``prune`` — plus the argparse/dispatch error
surface. The harness subcommands (``claude``/``gemini``/``antigravity``) and
``mcp`` delegate to ``launcher``/``mcp_server`` (they block / spawn); their
output discipline belongs to those modules' e2e tests (``test_launcher_e2e.py``)
and the 13c smoke checklist, NOT to this CLI-render audit.

**Isolation discipline:** these tests NEVER pass ``--config`` — the CLI writes
``os.environ["LETTERBOX_CONFIG"]`` only under ``if args.config:`` (cli.py), so a
no-``--config`` test mutates no env and needs no autouse teardown (the
``_isolate_config_env`` autouse fixture lives in ``test_cli.py`` and does NOT
reach this module — by design, we sidestep the leak rather than clone the guard).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from letterbox import cli, protocol

if sys.version_info >= (3, 11):
    import tomllib  # type: ignore[import-not-found]
else:  # pragma: no cover - exercised only on the 3.10 floor
    import tomli as tomllib  # type: ignore[import-not-found]

# Deterministic UTC base — never ``now()`` so filename ordering is reproducible
# across runs / xdist workers (mirrors test_cli.py:38). Whole-second offsets keep
# the microsecond-precision filenames sorting chronologically.
_BASE_TS = datetime(2026, 5, 28, 14, 30, 0, tzinfo=timezone.utc)

# The ANSI escape-introducer (CSI). Its ABSENCE is the colorless assertion; its
# PRESENCE is the rich-color assertion. One substring, both directions.
_ANSI_CSI = "\x1b["

# C5 — a body that exercises all three non-ASCII families the §11.1 UTF-8 promise
# must carry intact: Lithuanian diacritics, CJK, and an emoji (the same emoji the
# shipped sample's notification_template uses).
_UTF8_BODY = "ąčęėįšųūž 你好 📬"
# The JSON unicode-escape prefix. If ``ensure_ascii`` ever flips to True, a
# non-ASCII glyph serializes as ``\uXXXX`` and this substring appears — so its
# ABSENCE is the real proof, stronger than merely finding the glyph (G4).
_UNICODE_ESCAPE_PREFIX = "\\u"

# C6 — TUI / telemetry / progress libraries letterbox must never depend on. A new
# dependency crossing this line trips the manifest guard. Superset of the plan's
# K3 list is fine (§15 latitude); these are the common offenders.
_FORBIDDEN_DEPS = frozenset(
    {
        "rich",
        "click",
        "colorama",
        "blessed",
        "halo",
        "yaspin",
        "tqdm",
        "progressbar",
        "progressbar2",
        "alive-progress",
        "alive_progress",
        "tqdm-loggable",
        "enlighten",
        "sentry-sdk",
        "posthog",
        "analytics-python",
        "segment-analytics-python",
        "mixpanel",
    }
)


# ──────────────────────────────────────────────────────────────────────
# Local helpers — never import a sibling test module (permanent coupling for
# a ~12-line helper, §13.2). If a THIRD file ever needs this, promote to
# tests/helpers.py then, not before.
# ──────────────────────────────────────────────────────────────────────


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

    Mirrors ``test_cli.py:41`` — ``make_message_filename(ts)`` →
    ``new_message(id=stem, timestamp=ts)`` → ``write_message`` so the on-disk
    filename embeds ``ts`` (lexical == chronological). Never hardcodes a filename
    (§8.3 / ADR-028).
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


def _make_channel(state_home: Path, name: str = "debate") -> Path:
    """Create ``channels/<name>/`` under a ``tmp_letterbox_home`` and return it."""
    channel_dir = state_home / "channels" / name
    channel_dir.mkdir(parents=True)
    return channel_dir


@pytest.fixture
def init_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect both of ``init``'s write targets into ``tmp_path`` (clone of
    test_cli.py:661).

    ``init`` writes ``./letterbox.toml`` (cwd-derived) and, under ``--global``,
    ``~/.letterbox/config.toml`` (HOME-derived). chdir + ``HOME`` redirect both
    into tmp so a test never touches the repo or the real home. NOT
    ``tmp_letterbox_home`` — that sets ``LETTERBOX_HOME`` but leaves ``HOME``
    alone, and both of init's targets are HOME/cwd-derived. ``LETTERBOX_HOME`` is
    cleared so an ambient value can't skew ``load_config().state_dir``.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("LETTERBOX_HOME", raising=False)
    return tmp_path


# ──────────────────────────────────────────────────────────────────────
# C1 — Plain text by default; rich is opt-in only.
# ──────────────────────────────────────────────────────────────────────


class TestC1PlainByDefault:
    """``--format=plain`` is the default; ``--format=rich`` requires the flag."""

    def test_tail_default_is_jsonl_not_rich(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import json

        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        # No --format flag → plain (JSONL).
        rc = cli.main(["tail", "--channel", "debate"])
        assert rc == 0
        out = capsys.readouterr().out.strip()
        # Parses as JSON (the JSONL contract) and is NOT the rich "[ts] sender:"
        # human line.
        assert json.loads(out)["body"] == "hi"
        assert not out.startswith("[")

    def test_list_channels_default_is_tab_separated_not_table(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _make_channel(tmp_letterbox_home, "debate")
        # No --format flag → plain (name\tlast_activity), no aligned "CHANNEL" header.
        rc = cli.main(["list-channels"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "\t" in out
        assert "CHANNEL" not in out  # the rich header is opt-in only

    def test_rich_requires_the_explicit_flag(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _make_channel(tmp_letterbox_home, "debate")
        cli.main(["list-channels", "--format=rich"])
        out = capsys.readouterr().out
        assert "CHANNEL" in out  # the rich table header appears only with the flag


# ──────────────────────────────────────────────────────────────────────
# C3 — --color is independent of structure; plain is unconditionally colorless.
# ──────────────────────────────────────────────────────────────────────


class TestC3ColorIndependentOfStructure:
    """``never`` → no escapes anywhere; ``always`` → escapes in **rich only**;
    plain stays colorless **even under** ``--color=always`` (K4 / G5).

    The fourth branch — ``auto`` + a *real* TTY → color — is already locked by
    ``test_cli.py::TestColorAndFormat::test_should_use_color_resolves_all_branches``
    (a fake-TTY object). Standing up a real PTY here to re-prove it would add
    platform-fragile plumbing for a branch already unit-locked, so we don't (K4).
    """

    def test_color_never_no_escapes_in_rich_or_plain(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        # Rich + never → no escapes.
        cli.main(["tail", "--channel", "debate", "--format=rich", "--color=never"])
        assert _ANSI_CSI not in capsys.readouterr().out
        # Plain + never → no escapes (and plain never colorizes anyway).
        cli.main(["tail", "--channel", "debate", "--format=plain", "--color=never"])
        assert _ANSI_CSI not in capsys.readouterr().out
        # list-channels (rich + never) → no escapes either.
        cli.main(["list-channels", "--format=rich", "--color=never"])
        assert _ANSI_CSI not in capsys.readouterr().out

    def test_color_always_emits_escapes_in_rich(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        cli.main(["tail", "--channel", "debate", "--format=rich", "--color=always"])
        out = capsys.readouterr().out
        assert _ANSI_CSI in out  # rich + always → escapes present

    def test_plain_is_colorless_even_under_color_always(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The natural-but-wrong expectation (G5): that --color=always forces
        # escapes regardless of format. It does NOT — plain delegates to the
        # JSONL serializer, which never calls _colorize. Structure ≠ color (K4).
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        cli.main(["tail", "--channel", "debate", "--format=plain", "--color=always"])
        out = capsys.readouterr().out
        assert _ANSI_CSI not in out

    def test_list_channels_plain_colorless_under_color_always(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _make_channel(tmp_letterbox_home, "debate")
        cli.main(["list-channels", "--format=plain", "--color=always"])
        assert _ANSI_CSI not in capsys.readouterr().out


# ──────────────────────────────────────────────────────────────────────
# C4 — stdout vs stderr discipline; the pipe stays clean.
# ──────────────────────────────────────────────────────────────────────


class TestC4StreamDiscipline:
    """Data → stdout; logs/errors/summaries → stderr. On an error exit, stdout
    is empty (the pipe stays clean).

    Per-command, NOT uniform (Gotcha G3): ``init``'s status line goes to
    **stdout** (it emits no machine data, only a status), while ``prune``'s
    summary goes to **stderr** (because prune *also* emits ids as data on stdout,
    so the summary must not pollute the pipe). Asserted command-by-command.
    """

    def test_tail_data_on_stdout(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        cli.main(["tail", "--channel", "debate"])
        captured = capsys.readouterr()
        assert "hi" in captured.out
        assert captured.err == ""

    def test_init_status_on_stdout(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # G3: init's status line is on STDOUT (it emits only a status, no data).
        rc = cli.main(["init"])
        assert rc == 0
        captured = capsys.readouterr()
        assert "wrote" in captured.out
        assert captured.err == ""

    def test_prune_ids_on_stdout_summary_on_stderr(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # G3: prune's matched ids are DATA → stdout (pipeable); its summary is a
        # diagnostic → stderr (so it never pollutes the id pipe).
        channel_dir = _make_channel(tmp_letterbox_home)
        path = _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        rc = cli.main(["prune", "--channel", "debate", "--keep-last", "0"])
        assert rc == 0
        captured = capsys.readouterr()
        # The id is on stdout, alone (one filename per line, nothing else).
        assert captured.out.strip() == path.name
        # The "would move" summary is on stderr, NOT stdout.
        assert "would move" in captured.err
        assert "would move" not in captured.out

    @pytest.mark.parametrize(
        "argv",
        [
            pytest.param(["tail", "--channel", "nonexistent"], id="tail-missing"),
            pytest.param(["prune", "--channel", "nope", "--keep-last", "0"], id="prune-missing"),
            pytest.param(["tail", "--channel", "BAD NAME"], id="tail-invalid-name"),
            pytest.param(["prune", "--channel", "BAD NAME", "--keep-last", "0"], id="prune-invalid-name"),
        ],
    )
    def test_error_exit_keeps_stdout_empty(
        self,
        tmp_letterbox_home: Path,
        capsys: pytest.CaptureFixture[str],
        argv: list[str],
    ) -> None:
        # On every handler error path, the pipe stays clean: nonzero exit, a
        # vector on stderr, and ABSOLUTELY nothing on stdout.
        rc = cli.main(argv)
        assert rc == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err != ""

    def test_init_refuse_overwrite_keeps_stdout_empty(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        cli.main(["init"])  # first write succeeds
        capsys.readouterr()  # drain the success status
        rc = cli.main(["init"])  # second refuses (file exists)
        assert rc == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "already exists" in captured.err


# ──────────────────────────────────────────────────────────────────────
# C5 — UTF-8 throughout; non-ASCII survives, no \uXXXX escaping.
# ──────────────────────────────────────────────────────────────────────


class TestC5Utf8Throughout:
    """A Lithuanian + CJK + emoji body round-trips through ``tail`` (plain JSONL
    and rich) as literal glyphs — and the ``\\u`` escape prefix is ABSENT.

    Asserting the escape prefix is absent (not merely that glyphs are present) is
    the real proof (G4): a ``\\uXXXX`` stream could still happen to contain a glyph
    elsewhere, but it could never lack the ``\\u`` prefix.
    """

    def _capture_tail(
        self,
        argv: list[str],
        capsys: pytest.CaptureFixture[str],
    ) -> str:
        """Drive tail and return decoded stdout, with a capsysbinary-free path.

        ``capsys`` returns decoded ``str`` and under pytest preserves the UTF-8
        glyphs from ``to_json_bytes(...).decode("utf-8")``. If a runner ever
        mangles capture, the fallback would be ``capsysbinary`` + ``.decode`` —
        not needed today, noted for the maintainer.
        """
        cli.main(argv)
        return capsys.readouterr().out

    def test_plain_jsonl_carries_literal_glyphs(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body=_UTF8_BODY, ts=_BASE_TS)
        out = self._capture_tail(["tail", "--channel", "debate"], capsys)
        assert _UTF8_BODY in out  # literal glyphs present
        assert _UNICODE_ESCAPE_PREFIX not in out  # no \uXXXX escaping (the real proof)

    def test_rich_carries_literal_glyphs(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body=_UTF8_BODY, ts=_BASE_TS)
        out = self._capture_tail(
            ["tail", "--channel", "debate", "--format=rich"], capsys
        )
        assert _UTF8_BODY in out
        assert _UNICODE_ESCAPE_PREFIX not in out


# ──────────────────────────────────────────────────────────────────────
# C2 — No color-only signaling; every state difference carried by words.
# ──────────────────────────────────────────────────────────────────────


class TestC2NoColorOnlySignaling:
    """Every state difference is conveyed with a textual label, never color
    alone — so a monochrome terminal / screen reader loses nothing."""

    def test_empty_channel_row_has_textual_sentinel(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # A channel with no messages must read as a word, never a blank cell.
        _make_channel(tmp_letterbox_home, "debate")
        cli.main(["list-channels"])
        out = capsys.readouterr().out
        assert "(no messages)" in out

    def test_empty_install_note_is_textual(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # No channels at all → a textual note on stderr, clean stdout.
        cli.main(["list-channels"])
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "no channels found" in captured.err

    def test_prune_dry_run_vs_execute_differ_by_words(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Dry-run says "would move"; execute says "moved" — the distinction is
        # carried by WORDS, never by color.
        channel_dir = _make_channel(tmp_letterbox_home)
        _write_msg(channel_dir, sender="researcher", body="hi", ts=_BASE_TS)
        cli.main(["prune", "--channel", "debate", "--keep-last", "0"])
        dry_err = capsys.readouterr().err
        # Dry-run is hedged with "would move" + "dry-run"; it never claims "moved".
        assert "would move" in dry_err
        assert "dry-run" in dry_err
        assert "moved" not in dry_err

        # Re-create a message and actually execute.
        _write_msg(channel_dir, sender="researcher", body="hi2", ts=_BASE_TS)
        cli.main(
            ["prune", "--channel", "debate", "--keep-last", "0", "--yes-i-am-sure"]
        )
        exec_err = capsys.readouterr().err
        # Execute asserts "moved" and drops the "would" hedge — words distinguish
        # the two states, never color.
        assert "moved" in exec_err
        assert "would move" not in exec_err

    def test_init_refuse_names_the_path(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The refuse-overwrite vector names the offending path AND the remedy
        # (Framework P3 — errors are vectors, not walls).
        cli.main(["init"])
        capsys.readouterr()
        cli.main(["init"])
        err = capsys.readouterr().err
        assert "letterbox.toml" in err
        assert "remove it to re-scaffold" in err


# ──────────────────────────────────────────────────────────────────────
# C6 — Calm surface: no spinners, no telemetry banners, no upgrade nags.
# ──────────────────────────────────────────────────────────────────────


class TestC6CalmSurface:
    """Proven two ways (K3): a dependency-manifest guard (no TUI/telemetry libs
    can sneak in) AND exact-output asserts on clean runs (no banner/nag/spinner
    line can be added without breaking the count). Assert the shape, not the
    absence of arbitrary code."""

    def test_no_forbidden_tui_or_telemetry_dependency(self) -> None:
        # Parse the runtime dependency NAMES from pyproject.toml and assert the
        # set is disjoint from the forbidden TUI/telemetry set. Reads
        # [project].dependencies ONLY — dev extras (pytest etc.) are not runtime
        # deps and are correctly excluded.
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        raw_deps = data["project"]["dependencies"]
        names = {_dep_name(spec) for spec in raw_deps}
        offenders = names & _FORBIDDEN_DEPS
        assert offenders == set(), f"forbidden TUI/telemetry dep(s) in manifest: {offenders}"

    def test_init_clean_run_emits_exactly_its_status_line(
        self, init_env: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # A clean init emits EXACTLY one status line on stdout and nothing on
        # stderr — any future banner/nag/spinner added to the happy path breaks
        # this line count.
        rc = cli.main(["init"])
        assert rc == 0
        captured = capsys.readouterr()
        out_lines = captured.out.splitlines()
        assert len(out_lines) == 1
        assert out_lines[0].startswith("letterbox init: wrote ")
        assert captured.err == ""

    def test_empty_list_channels_emits_exact_shape(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # An empty install: stdout is EXACTLY empty, stderr is EXACTLY the one
        # informational note — no marketing, no first-run banner, no nag.
        rc = cli.main(["list-channels"])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err.splitlines() == [
            "letterbox list-channels: no channels found"
        ]


# ──────────────────────────────────────────────────────────────────────
# C7 — Errors are vectors (Framework P3): bad input cites the offending thing.
# ──────────────────────────────────────────────────────────────────────


class TestC7ErrorsAreVectors:
    """Bad input fails loud with exit 2 (argparse) and a stderr message that
    names what was wrong / what's valid — never a silent wall, never a stack
    trace, never anything on stdout."""

    def test_unknown_subcommand_lists_valid_choices(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(["bogus-subcommand"])
        assert exc.value.code == 2
        captured = capsys.readouterr()
        assert captured.out == ""
        # argparse's invalid-choice vector names the offending token and the
        # valid choices (it includes the subcommand list).
        assert "bogus-subcommand" in captured.err
        assert "choose from" in captured.err

    def test_unknown_flag_on_real_command_is_a_vector(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            cli.main(["list-channels", "--bogus-flag"])
        assert exc.value.code == 2
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "unrecognized arguments" in captured.err

    def test_prune_without_selection_rule_is_a_vector(
        self, tmp_letterbox_home: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The required mutually-exclusive selection group means a bare
        # ``prune --channel x`` can't silently prune everything — it's a vector.
        with pytest.raises(SystemExit) as exc:
            cli.main(["prune", "--channel", "debate"])
        assert exc.value.code == 2
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "one of the arguments" in captured.err


def _dep_name(spec: str) -> str:
    """Strip a PEP 508 requirement spec down to its bare distribution name.

    ``"watchdog>=4.0,<5.0"`` → ``"watchdog"``; ``"tomli>=2.0; python_version <
    '3.11'"`` → ``"tomli"``. Splits off the environment marker (``;``) and any
    version/extras delimiter, lower-cased for case-insensitive comparison.

    Args:
        spec: A single PEP 508 dependency specifier.

    Returns:
        The normalized (lower-cased) distribution name.
    """
    # Drop the environment marker first, then peel version/extras delimiters.
    head = spec.split(";", 1)[0].strip()
    for delim in ("<", ">", "=", "!", "~", "[", " ", "("):
        head = head.split(delim, 1)[0]
    return head.strip().lower()

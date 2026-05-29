"""Meta-tests for the Phase 1b test infrastructure.

These prove that the fixtures and helpers themselves behave as documented
before any other phase depends on them. Per the scout brief: every test
here exercises the real fixture / real script / real subprocess — no
mocking. The point of the Migration Test Kit is fidelity to production.
"""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from tests.conftest import FakeHarness
from tests.helpers import wait_for

# Path to the lint script — resolved once so tests are independent of CWD.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_LINT_SCRIPT = _REPO_ROOT / "scripts" / "lint_ensure_ascii.sh"


# ──────────────────────────────────────────────────────────────
# tmp_letterbox_home
# ──────────────────────────────────────────────────────────────


def test_tmp_letterbox_home_creates_dir_with_mode_0700(tmp_letterbox_home: Path) -> None:
    """Vision §6.4: state dir must be mode 0700, owner-only."""
    assert tmp_letterbox_home.is_dir()
    mode = stat.S_IMODE(os.stat(tmp_letterbox_home).st_mode)
    assert mode == 0o700, f"expected mode 0o700, got {oct(mode)}"


def test_tmp_letterbox_home_sets_env(tmp_letterbox_home: Path) -> None:
    """LETTERBOX_HOME must point at the fixture's dir for the duration of the test."""
    assert os.environ["LETTERBOX_HOME"] == str(tmp_letterbox_home)


def test_tmp_letterbox_home_isolated_per_test_first(tmp_letterbox_home: Path) -> None:
    """First half of the isolation pair: drop a marker in this test's dir."""
    (tmp_letterbox_home / "marker.txt").write_text("first", encoding="utf-8")
    assert (tmp_letterbox_home / "marker.txt").exists()


def test_tmp_letterbox_home_isolated_per_test_second(tmp_letterbox_home: Path) -> None:
    """Second half: a fresh test must not see the previous test's marker."""
    assert not (tmp_letterbox_home / "marker.txt").exists()


# ──────────────────────────────────────────────────────────────
# fake_harness
# ──────────────────────────────────────────────────────────────


def test_fake_harness_echoes_stdin_to_file(fake_harness: FakeHarness) -> None:
    """Spawn the harness, write to its stdin, assert echo file contains the bytes."""
    proc = subprocess.Popen(
        [sys.executable, str(fake_harness.script_path), "--echo-to", str(fake_harness.echo_file)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = proc.communicate(input=b"hello\r", timeout=10.0)
    assert proc.returncode == 0, f"harness exit {proc.returncode}; stderr={stderr!r}"
    assert fake_harness.read_echo() == b"hello\r"


def test_fake_harness_clean_exit_on_eof(fake_harness: FakeHarness) -> None:
    """EOF on stdin → harness exits 0."""
    proc = subprocess.Popen(
        [sys.executable, str(fake_harness.script_path), "--echo-to", str(fake_harness.echo_file)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # communicate() drains the PIPE buffers before reap so no ResourceWarning
    # fires on garbage-collection (filterwarnings=error would promote that
    # warning to a failure).
    proc.communicate(input=b"", timeout=10.0)
    assert proc.returncode == 0


def test_fake_harness_parses_mcp_config_flag(
    fake_harness: FakeHarness, tmp_path: Path
) -> None:
    """--mcp-config: harness reads the config and spawns the configured child.

    The dummy "MCP server" writes a marker file (proving it was spawned)
    then sleeps so it survives long enough to be observed before the
    harness sends SIGTERM during cleanup. We poll for the marker BEFORE
    closing stdin — closing stdin triggers the harness's reap path, and
    if we raced past the child's startup the marker wouldn't be written
    in time.
    """
    marker = tmp_path / "mcp_child_ran.marker"
    payload = (
        f"open({str(marker)!r}, 'w').write('ok'); "
        "import time; time.sleep(30)"
    )
    mcp_config = tmp_path / "mcp.json"
    mcp_config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "test": {
                        "command": sys.executable,
                        "args": ["-c", payload],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    proc = subprocess.Popen(
        [
            sys.executable,
            str(fake_harness.script_path),
            "--echo-to",
            str(fake_harness.echo_file),
            "--mcp-config",
            str(mcp_config),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the marker to appear — proves the child started executing
    # the configured command. Loose deadline accommodates cold python startup.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and not marker.exists():
        time.sleep(0.05)

    if not marker.exists():
        proc.kill()
        proc.communicate(timeout=5.0)
        pytest.fail("MCP child marker never appeared within 10s")

    # Close stdin → harness exits → terminates the still-sleeping child.
    _stdout, stderr = proc.communicate(input=b"", timeout=10.0)
    assert proc.returncode == 0, f"harness exit {proc.returncode}; stderr={stderr!r}"
    assert marker.read_text(encoding="utf-8") == "ok"
    assert b"spawned MCP child pid=" in stderr


# ──────────────────────────────────────────────────────────────
# wait_for
# ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_wait_for_returns_when_predicate_true() -> None:
    """Always-true predicate returns immediately without raising."""
    await wait_for(lambda: True, timeout=1.0)


@pytest.mark.asyncio
async def test_wait_for_raises_timeout_on_false_predicate() -> None:
    """Always-false predicate raises TimeoutError after the deadline."""
    with pytest.raises(TimeoutError):
        await wait_for(lambda: False, timeout=0.1, interval=0.02)


@pytest.mark.asyncio
async def test_wait_for_polls_at_interval() -> None:
    """A predicate that flips after a delay is caught when timeout > delay,
    and times out when timeout < delay."""
    flip_at = time.monotonic() + 0.2

    def predicate() -> bool:
        return time.monotonic() >= flip_at

    # Generous timeout — should catch the flip.
    start = time.monotonic()
    await wait_for(predicate, timeout=1.0, interval=0.02)
    elapsed = time.monotonic() - start
    # Loose upper bound to avoid CI flake; we only care that it returned.
    assert elapsed < 1.5

    # Tight timeout — should time out before the flip.
    flip_at = time.monotonic() + 0.2
    with pytest.raises(TimeoutError):
        await wait_for(predicate, timeout=0.05, interval=0.02)


# ──────────────────────────────────────────────────────────────
# ensure_ascii lint
# ──────────────────────────────────────────────────────────────


def test_ensure_ascii_lint_passes_clean_tree() -> None:
    """The current letterbox/ tree has zero json.dump(s) calls — trivially clean."""
    result = subprocess.run(
        [str(_LINT_SCRIPT)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=15.0,
    )
    assert result.returncode == 0, (
        f"lint failed unexpectedly: stdout={result.stdout!r} stderr={result.stderr!r}"
    )


def test_ensure_ascii_lint_fails_on_planted_violation(tmp_path: Path) -> None:
    """A file with json.dumps({}) (no flag) trips the lint with file:line in stderr."""
    bad_dir = tmp_path / "planted"
    bad_dir.mkdir()
    bad_file = bad_dir / "bad.py"
    bad_file.write_text(
        textwrap.dedent(
            """\
            import json
            data = json.dumps({"x": 1})
            """
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [str(_LINT_SCRIPT), str(bad_dir)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=15.0,
    )
    assert result.returncode == 1, (
        f"expected exit 1, got {result.returncode}; "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    # File path + line number must appear in stderr so CI logs are actionable.
    assert str(bad_file) in result.stderr
    assert ":2:" in result.stderr


def test_ensure_ascii_lint_accepts_compliant_call(tmp_path: Path) -> None:
    """A file that passes ensure_ascii=False on the same line is accepted."""
    ok_dir = tmp_path / "compliant"
    ok_dir.mkdir()
    (ok_dir / "ok.py").write_text(
        textwrap.dedent(
            """\
            import json
            data = json.dumps({"x": 1}, ensure_ascii=False)
            """
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [str(_LINT_SCRIPT), str(ok_dir)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=15.0,
    )
    assert result.returncode == 0, (
        f"lint flagged a compliant call: stdout={result.stdout!r} stderr={result.stderr!r}"
    )

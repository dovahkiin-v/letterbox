"""Shared pytest fixtures for the letterbox test suite.

Two load-bearing fixtures land here per Cross-Cutting §13.1 — the
"Migration Test Kit First" mandate:

* ``tmp_letterbox_home`` — isolated ``~/.letterbox/`` per test. Sets
  ``LETTERBOX_HOME`` via monkeypatch and creates the directory with mode
  ``0700`` (Vision §6.4). Used by every test that touches the protocol,
  channel, or config layer.
* ``fake_harness`` — handle pointing at the bundled ``fake_harness.py``
  script. Used by every adapter / launcher / e2e test that needs to
  spawn a real subprocess representing a CLI harness.

The fixtures are deliberately minimal. Tests that need a populated
channel directory build it on top of ``tmp_letterbox_home``; tests that
need the harness to misbehave wrap or substitute it explicitly. See
``IMPLEMENTATION_NOTES`` for the resist-anticipation rule.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest

_THIS_DIR = Path(__file__).resolve().parent
_FAKE_HARNESS_SCRIPT = _THIS_DIR / "fake_harness.py"


@dataclass
class FakeHarness:
    """Handle for the bundled fake harness CLI.

    Attributes:
        script_path: Absolute path to ``tests/fake_harness.py``.
        echo_file: Path the harness has been told to append stdin bytes to.
            Tests pass this on the CLI as ``--echo-to <echo_file>`` when
            spawning the subprocess.
    """

    script_path: Path
    echo_file: Path

    def read_echo(self) -> bytes:
        """Return everything the harness has echoed so far, or b"" if untouched."""
        if not self.echo_file.exists():
            return b""
        return self.echo_file.read_bytes()


@pytest.fixture
def tmp_letterbox_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Yield an isolated ``~/.letterbox/`` rooted under pytest's ``tmp_path``.

    Sets ``LETTERBOX_HOME`` for the duration of the test and creates the
    directory eagerly with mode ``0o700`` so Vision §6.4's permissions
    check has something to find.

    IMPORTANT — module-scope env reads (G2): tests that import a module
    which reads ``LETTERBOX_HOME`` at *import* time will get the wrong
    value, because monkeypatch fires when the fixture is *invoked*, not
    when the test module loads. Use the fixture only with code paths
    that read the env at call time (per the planned ``config.resolve_state_dir``
    contract). If a future module reads the env at import, it needs a
    reload helper, not this fixture.

    Worker safety (G5): ``tmp_path`` is per-test-per-worker isolated under
    ``pytest-xdist``, so ``-n auto`` is safe. No shared global state is
    introduced by this fixture.
    """
    home = tmp_path / ".letterbox"
    home.mkdir(mode=0o700, parents=False, exist_ok=False)
    # mkdir's mode is masked by the process umask — re-chmod to be safe.
    os.chmod(home, 0o700)
    monkeypatch.setenv("LETTERBOX_HOME", str(home))
    return home


@pytest.fixture
def fake_harness(tmp_path: Path) -> FakeHarness:
    """Yield a ``FakeHarness`` handle with a per-test scratch echo file.

    The harness script itself is the bundled ``tests/fake_harness.py``;
    each test gets its own ``echo_file`` path so concurrent xdist workers
    don't collide.
    """
    return FakeHarness(
        script_path=_FAKE_HARNESS_SCRIPT,
        echo_file=tmp_path / "fake_harness_echo.bin",
    )

"""Trivial smoke test so pytest collects > 0 tests and exits 0.

This file is the smallest possible answer to Phase 1a's deferred deviation
(pytest exited 5 — "no tests collected"). The first test in the repo lives
here on purpose: it has no fixtures, no imports beyond pytest itself, and
no environment assumptions. If it fails, something is wrong with pytest's
collection or with the venv, not with letterbox.
"""


def test_smoke_pytest_runs() -> None:
    """Pytest collected this file and ran the test. That is the assertion."""
    assert True

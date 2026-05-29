"""Static packaging-metadata guard (Phase 13a).

Locks the publishable-metadata invariants the 13a release audit established so a
future edit cannot silently regress them: the release version is real and stays
in lockstep with ``letterbox.__version__`` (K1), the project URLs point at the
personal ``dovahkiin-v`` repo and never at ``skyforge`` (K2), ``keywords`` is
non-empty (K4), the console-script entry point is intact (W16), and the Python
floor matches ADR-007.

This is a *static* guard: it only parses ``pyproject.toml`` and reads the
already-imported ``__version__``. It runs no build, touches no network, and
asserts no coverage numbers — those belong to the manual 13a smoke and to 13b.
``filterwarnings = ["error"]`` is active for the suite, so this module is also
careful to emit no warnings of its own.
"""

import sys
from pathlib import Path

import letterbox

if sys.version_info >= (3, 11):
    import tomllib  # type: ignore[import-not-found]
else:  # pragma: no cover - exercised only on the 3.10 floor
    import tomli as tomllib  # type: ignore[import-not-found]

# Same locator idiom as test_output_discipline.py:469 — repo root is the
# parent of the tests/ directory.
_PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def _load() -> dict:
    return tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))


def test_version_is_real_and_synced_with_dunder() -> None:
    # K1: pyproject version is a real release (not the 0.0.0 placeholder) and
    # equals letterbox.__version__ exactly — the two literals must never drift.
    version = _load()["project"]["version"]
    assert version != "0.0.0"
    assert version == letterbox.__version__


def test_project_urls_point_at_personal_repo() -> None:
    # K2: URLs are the personal dovahkiin-v GitHub, never skyforge-sh.
    urls = _load()["project"]["urls"]
    assert urls, "expected a [project.urls] block"
    joined = " ".join(urls.values()).lower()
    assert "dovahkiin-v" in joined
    assert "skyforge" not in joined


def test_keywords_present() -> None:
    # K4: keywords aid discovery even unpushed; must be non-empty.
    keywords = _load()["project"].get("keywords", [])
    assert keywords, "expected a non-empty keywords list"


def test_console_script_entry_point_intact() -> None:
    # W16: the entry point the agent self-spawns by name post-install.
    scripts = _load()["project"]["scripts"]
    assert scripts.get("letterbox") == "letterbox.cli:main"


def test_requires_python_floor() -> None:
    # ADR-007: the 3.10 floor.
    assert _load()["project"]["requires-python"] == ">=3.10"


def test_license_is_spdx_expression() -> None:
    # K3: PEP 639 SPDX string form (chosen after the live build deprecated the
    # license-table form); the deprecated `License ::` classifier is gone.
    project = _load()["project"]
    assert project["license"] == "MIT"
    classifiers = project.get("classifiers", [])
    assert not any(c.startswith("License ::") for c in classifiers)

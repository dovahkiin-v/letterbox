"""Static MCP-Registry metadata guard (ADR-071).

``server.json`` introduces a third home for the release version, alongside
``letterbox.__version__`` (the source of truth, K1) and the dynamic pyproject
read. It also introduces a cross-file join key: the registry proves PyPI
ownership by finding ``mcp-name: <server-name>`` in the package README, and
that name **must** byte-match ``name`` in ``server.json``. Both are exactly the
silent-drift shapes this suite locks elsewhere (K1, and the join-key discipline
of ADR-035), so they get the same treatment here.

The failure this prevents is quiet and remote: a version bump that misses
``server.json`` publishes a registry entry pointing at a PyPI version that does
not exist, and a renamed marker makes the registry reject the publish for
"ownership verification failed" — neither shows up in any runtime test.

Static only: parses two files and reads the already-imported ``__version__``.
No build, no network. ``filterwarnings = ["error"]`` is active, so this module
emits no warnings of its own.
"""

import json
import re
from pathlib import Path

import letterbox

_ROOT = Path(__file__).resolve().parent.parent
_SERVER_JSON = _ROOT / "server.json"
_README = _ROOT / "README.md"

# The registry's documented marker form: `mcp-name: <name>`, which may sit
# inside an HTML comment. Kept deliberately loose about surrounding syntax
# (the registry scans the rendered description as text) and strict about the
# name, which is the part that must match.
_MARKER = re.compile(r"mcp-name:\s*(?P<name>[A-Za-z0-9._-]+/[A-Za-z0-9._-]+)")


def _server() -> dict:
    return json.loads(_SERVER_JSON.read_text(encoding="utf-8"))


def test_server_json_version_tracks_dunder() -> None:
    # Both version fields — the server version and the package version — must
    # equal letterbox.__version__. server.json is metadata *about* a published
    # PyPI release; a mismatch points the registry at a version that isn't there.
    server = _server()
    assert server["version"] == letterbox.__version__
    assert server["packages"][0]["version"] == letterbox.__version__


def test_readme_marker_matches_server_name() -> None:
    # The PyPI ownership join key. The registry reads this out of the published
    # long_description, so it must be present in README.md (which pyproject
    # declares as `readme`) and must match server.json's name exactly.
    found = _MARKER.search(_README.read_text(encoding="utf-8"))
    assert found is not None, (
        "README.md is missing the `mcp-name:` marker — the MCP Registry proves "
        "PyPI ownership by finding it in the package description"
    )
    assert found.group("name") == _server()["name"]


def test_server_name_is_in_the_github_namespace() -> None:
    # GitHub-authenticated publishing only permits names under
    # io.github.<username>/. A name outside it is rejected at publish time with
    # a permissions error, long after the release is cut.
    assert _server()["name"].startswith("io.github.dovahkiin-v/")


def test_package_identifier_is_the_pypi_dist_name() -> None:
    # The registry resolves this against pypi.org; it must be the distribution
    # name, not the import package or the repo name (they coincide here, which
    # is exactly why a typo would be easy to miss).
    package = _server()["packages"][0]
    assert package["registryType"] == "pypi"
    assert package["identifier"] == "letterbox"

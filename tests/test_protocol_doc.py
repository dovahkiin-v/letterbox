"""Guard tests for ``docs/PROTOCOL.md`` (Phase 12b).

These are cheap, additive regression guards against documentation drift —
not behavioral tests of the protocol itself (that is ``test_protocol.py``).
They assert that the protocol reference exists, that every ADR it cross-links
actually resolves in ``DECISIONS.md`` (catching a future ADR renumbering or a
typo'd reference), and that the README's links to the doc point at a real file.

The doc's literal fidelity to the code (field names, regexes, parse_error
strings) is verified by human review against source at authoring time; these
tests guard the structural cross-references that rot silently.
"""
import re
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_PROTOCOL_DOC = _PROJECT_ROOT / "docs" / "PROTOCOL.md"
_DECISIONS = _PROJECT_ROOT / "DECISIONS.md"
_README = _PROJECT_ROOT / "README.md"

# ADR headings in DECISIONS.md are flat ``## ADR-0NN — <title>`` lines.
_ADR_HEADING_RE = re.compile(r"^## (ADR-\d+)\b", re.MULTILINE)
# ADR cross-references anywhere in the protocol doc.
_ADR_CITE_RE = re.compile(r"ADR-\d+")


def test_protocol_doc_exists() -> None:
    """The protocol reference file exists (creating it created ``docs/``)."""
    assert _PROTOCOL_DOC.is_file()


def test_every_cited_adr_resolves() -> None:
    """Every ``ADR-0NN`` cited in the doc has a heading in ``DECISIONS.md``."""
    decisions_text = _DECISIONS.read_text(encoding="utf-8")
    known_adrs = set(_ADR_HEADING_RE.findall(decisions_text))
    assert known_adrs, "no ADR headings parsed from DECISIONS.md"

    doc_text = _PROTOCOL_DOC.read_text(encoding="utf-8")
    cited_adrs = set(_ADR_CITE_RE.findall(doc_text))
    assert cited_adrs, "no ADR citations found in docs/PROTOCOL.md"

    dangling = sorted(cited_adrs - known_adrs)
    assert not dangling, f"dangling ADR citations in PROTOCOL.md: {dangling}"


def test_no_channel_archive_directory_documented() -> None:
    """The doc never documents a live ``channel/archive/`` directory (G3).

    The literal ``channel/archive/`` may appear only inside the historical
    "earlier drafts had…" / "does not exist in v1" framing — never as a
    described current-state path. We assert the doc explicitly states the
    directory does not exist, which is the load-bearing correction.
    """
    doc_text = _PROTOCOL_DOC.read_text(encoding="utf-8")
    assert "There is no shared archive directory." in doc_text
    assert "There is no `channel/archive/` directory." in doc_text


def test_readme_links_to_existing_protocol_doc() -> None:
    """The README's ``docs/PROTOCOL.md`` link targets resolve to a real file."""
    readme_text = _README.read_text(encoding="utf-8")
    link_targets = re.findall(r"\]\((docs/PROTOCOL\.md)\)", readme_text)
    assert link_targets, "expected at least one [..](docs/PROTOCOL.md) link in README"
    for target in link_targets:
        assert (_PROJECT_ROOT / target).is_file()

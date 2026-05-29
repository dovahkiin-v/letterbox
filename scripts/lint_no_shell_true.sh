#!/usr/bin/env bash
# Lint: no `shell=True` anywhere in letterbox/ (Vision §6.4 — no execution path).
#
# Letterbox never `exec`s, `eval`s, or `subprocess`-shells a message body
# or metadata field. The function-signature defense lives in
# `letterbox/adapters/pty_common.py` (``spawn_pty`` accepts only
# ``list[str]``); this lint is the second layer — any ``shell=True``
# appearing anywhere under ``letterbox/`` is a violation, with no
# exemption mechanism (P1 anti-pattern: do not speculatively wire
# ``# noqa`` exemptions before a phase legitimately needs one). See
# DECISIONS.md ADR-032 for the committed invariant.
#
# Usage:   scripts/lint_no_shell_true.sh [<dir>]
# Default: letterbox/
# Exits 0 if clean, 1 if any `shell=True` (with optional whitespace
# around ``=``) appears in a ``letterbox/**.py`` file, 2 if the target
# directory does not exist.
# On failure, prints "file:line: shell=True forbidden" for each hit.
#
# Heuristic: match-and-fail variant of PATTERNS.md P1's lint-script
# shape — clones ``lint_ensure_ascii.sh`` and replaces the
# match-with-required-context loop with a simpler "every hit is a
# violation" loop. The regex ``shell\s*=\s*True`` catches
# ``shell=True``, ``shell = True``, ``shell =True``, ``shell= True``.
set -euo pipefail

# ── Lint configuration ────────────────────────────────────────────────
IDIOM_REGEX='shell\s*=\s*True'
DEFAULT_DIR='letterbox'
EXCLUDE_DIRS=(tests venv .venv build dist .git __pycache__ .pytest_cache htmlcov)
# ──────────────────────────────────────────────────────────────────────

target_dir="${1:-$DEFAULT_DIR}"

if [[ ! -d "$target_dir" ]]; then
    echo "lint_no_shell_true: target directory not found: $target_dir" >&2
    exit 2
fi

# Build the find exclusion expression: -not -path '*/<dir>/*' for each.
find_excludes=()
for excl in "${EXCLUDE_DIRS[@]}"; do
    find_excludes+=(-not -path "*/${excl}/*")
done

violations=0
while IFS= read -r -d '' py_file; do
    # grep -n prints "lineno:contents" for each match. Every line is a
    # violation (no required-context check — that's the K7 variant).
    matches=$(grep -nE "$IDIOM_REGEX" "$py_file" 2>/dev/null || true)
    [[ -z "$matches" ]] && continue

    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        lineno="${line%%:*}"
        echo "${py_file}:${lineno}: shell=True forbidden" >&2
        violations=$((violations + 1))
    done <<< "$matches"
done < <(find "$target_dir" "${find_excludes[@]}" -type f -name '*.py' -print0)

if [[ "$violations" -gt 0 ]]; then
    echo "lint_no_shell_true: ${violations} violation(s) in ${target_dir}" >&2
    exit 1
fi

exit 0

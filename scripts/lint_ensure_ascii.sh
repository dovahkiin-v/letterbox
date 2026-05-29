#!/usr/bin/env bash
# Lint: every json.dump( / json.dumps( call must pass ensure_ascii=False.
#
# Python's default json encoder mangles non-ASCII into \uXXXX escapes,
# silently corrupting the multi-language sovereignty letterbox promises
# (Lithuanian ąčęėįšųūž, CJK, emoji). This lint catches the omission at
# CI-write-time across every phase that touches JSON.
#
# Usage:   scripts/lint_ensure_ascii.sh [<dir>]
# Default: letterbox/
# Exits 0 if clean, 1 if any json.dump(s) call lacks ensure_ascii=False.
# On failure, prints "file:line: missing ensure_ascii=False" for each hit.
#
# Heuristic: greps for json.dump( or json.dumps( and three following
# lines; flags any call whose 4-line window does not contain
# "ensure_ascii=False". Compact one-liners and short multi-line calls
# are covered. If a future site legitimately needs a 5+ line call, add
# a `# noqa: ensure-ascii` mechanism then — do not speculatively build it.
#
# Structured for easy cloning: future lints (e.g., shell=True) can copy
# this file and swap the IDIOM_REGEX + REQUIRED_ARG values at the top.
set -euo pipefail

# ── Lint configuration ────────────────────────────────────────────────
IDIOM_REGEX='json\.dumps?\('
REQUIRED_ARG='ensure_ascii=False'
DEFAULT_DIR='letterbox'
EXCLUDE_DIRS=(tests venv .venv build dist .git __pycache__ .pytest_cache htmlcov)
# ──────────────────────────────────────────────────────────────────────

target_dir="${1:-$DEFAULT_DIR}"

if [[ ! -d "$target_dir" ]]; then
    echo "lint_ensure_ascii: target directory not found: $target_dir" >&2
    exit 2
fi

# Returns 0 if the stanza (a grep -A 3 match block) contains the required
# argument; returns 1 otherwise and prints a "file:line: ..." diagnostic.
check_stanza() {
    local file="$1"
    local block="$2"
    local first_line
    first_line=$(printf '%s\n' "$block" | head -n 1)
    local lineno="${first_line%%:*}"
    [[ "$lineno" =~ ^[0-9]+$ ]] || return 0
    if printf '%s' "$block" | grep -qF "$REQUIRED_ARG"; then
        return 0
    fi
    echo "${file}:${lineno}: missing ensure_ascii=False" >&2
    return 1
}

# Build the find exclusion expression: -not -path '*/<dir>/*' for each.
find_excludes=()
for excl in "${EXCLUDE_DIRS[@]}"; do
    find_excludes+=(-not -path "*/${excl}/*")
done

violations=0
while IFS= read -r -d '' py_file; do
    # grep -n -A 3 finds the idiom and prints 3 trailing context lines.
    # Stanzas are separated by lines containing only "--".
    matches=$(grep -nE -A 3 "$IDIOM_REGEX" "$py_file" 2>/dev/null || true)
    [[ -z "$matches" ]] && continue

    stanza=""
    while IFS= read -r line; do
        if [[ "$line" == "--" ]]; then
            if ! check_stanza "$py_file" "$stanza"; then
                violations=$((violations + 1))
            fi
            stanza=""
        else
            stanza+="${line}"$'\n'
        fi
    done <<< "$matches"
    if [[ -n "$stanza" ]]; then
        if ! check_stanza "$py_file" "$stanza"; then
            violations=$((violations + 1))
        fi
    fi
done < <(find "$target_dir" "${find_excludes[@]}" -type f -name '*.py' -print0)

if [[ "$violations" -gt 0 ]]; then
    echo "lint_ensure_ascii: ${violations} violation(s) in ${target_dir}" >&2
    exit 1
fi

exit 0

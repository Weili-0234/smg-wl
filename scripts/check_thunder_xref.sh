#!/usr/bin/env bash
# Sanity-check thunder spec cross-references.
#
# Catches:
# 1. Markdown links to thunder/*.md files that don't exist
# 2. Surviving §X.Y in-document references (warn — may need conversion to file-link)
# 3. References to legacy single-file spec (THUNDER_POLICY_DESIGN.md) that no longer exists
# 4. Worklog D-N references that don't have a corresponding ## D-N: heading in worklog.md
#
# Run: bash scripts/check_thunder_xref.sh
# Exit 0 = no errors. Exit 1 = at least one broken reference.

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/docs/thunder"
ERRORS=0

if [ ! -d "$DIR" ]; then
    echo "[FATAL] $DIR does not exist; thunder docs not yet split" >&2
    exit 1
fi

cd "$DIR"

# Check 1: markdown links to *.md within docs/thunder
broken=$(grep -rEho '\[[^]]*\]\(([0-9]+-[a-z-]+\.md|worklog\.md|slurm-cluster\.md|legacy/[a-z-]+\.md|00-INDEX\.md)\)' *.md legacy/*.md 2>/dev/null \
    | sed -E 's/.*\(([^)]+)\).*/\1/' \
    | sort -u \
    | while read -r target; do
        if [ ! -f "$DIR/$target" ]; then
            echo "$target"
        fi
      done)

if [ -n "$broken" ]; then
    echo "[ERROR] broken markdown links to non-existent files:"
    echo "$broken" | sed 's/^/  /'
    ERRORS=$((ERRORS+1))
fi

# Check 2: surviving §X.Y in-document references — warn, not error
strays=$(grep -rEn '§[0-9]+\.[0-9]+[a-z]?' *.md 2>/dev/null | head -20)
if [ -n "$strays" ]; then
    echo "[WARN] §X.Y references found (consider replacing with file-relative cross-link):"
    echo "$strays" | sed 's/^/  /'
fi

# Check 3: references to legacy single-file path
legacy_refs=$(grep -rEn 'THUNDER_POLICY_DESIGN\.md' "$ROOT" \
    --include='*.md' --include='*.rs' --include='*.toml' --include='*.sh' --include='*.py' 2>/dev/null \
    | grep -v '^Binary file' \
    | grep -v '/legacy/' || true)
if [ -n "$legacy_refs" ]; then
    echo "[WARN] references to legacy THUNDER_POLICY_DESIGN.md:"
    echo "$legacy_refs" | sed 's/^/  /'
fi

# Check 4: worklog D-N consistency (warn, not error — D-1..D-8 are legacy inlined in
# 02-decisions.md and don't have separate worklog entries; D-15+ are post-split entries
# that should appear in worklog.md)
if [ -f "$DIR/worklog.md" ]; then
    declared_in_worklog=$(grep -E '^## D-[0-9]+:' "$DIR/worklog.md" | sed -E 's/^## (D-[0-9]+):.*/\1/' | sort -u)
    declared_in_decisions=$(grep -E 'D-[0-9]+' "$DIR/02-decisions.md" 2>/dev/null | grep -oE 'D-[0-9]+' | sort -u)
    declared=$(echo -e "$declared_in_worklog\n$declared_in_decisions" | sort -u | grep -v '^$')
    referenced=$(grep -rEho 'D-[0-9]+' *.md 2>/dev/null | sort -u)
    missing=$(comm -23 <(echo "$referenced") <(echo "$declared"))
    if [ -n "$missing" ]; then
        echo "[WARN] D-N references not found in worklog.md or 02-decisions.md (verify these are valid legacy / typo):"
        echo "$missing" | sed 's/^/  /'
    fi
fi

if [ "$ERRORS" -eq 0 ]; then
    echo "[OK] thunder cross-references look clean ($(ls $DIR/*.md | wc -l | tr -d ' ') topic files + companions)"
    exit 0
else
    echo "[FAIL] $ERRORS error(s)"
    exit 1
fi

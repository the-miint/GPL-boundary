#!/usr/bin/env bash
# Check that a PR description carries the "Reviewer loop" block the
# gpl-boundary-review skill prints (CLAUDE.md, "Reviewing a branch"). Environment:
#   PR_BODY   the PR description
#   LABELS    comma-joined PR labels
#   HEAD_SHA  the PR's head commit (default: HEAD)
#   BASE_SHA  the PR's base branch tip, e.g. origin/main (required: a stacked PR's
#             base is another PR's branch, so there is no default)
# Run from a checkout whose history holds both.
#
# Passes when the 'no-agent-review' label is present, or when the description has a
# "## Reviewer loop" section whose "Reviewed at:" line names one of the PR's own
# commits, whose Fixed, Declined, Deferred and Not probed lines are present, and
# whose Reviewed at and Fixed lines hold no template placeholder. It
# checks that the block is there and points at this branch; it cannot check that the
# loop was run.
set -euo pipefail

fail() {
    {
        echo "ERROR: $1"
        echo "Run the gpl-boundary-review skill (.claude/skills/gpl-boundary-review/SKILL.md)"
        echo "on the branch and replace the '## Reviewer loop' section of the PR description"
        echo "with the block it prints. If the loop cannot be run for this PR, add the"
        echo "'no-agent-review' label and say why under that heading."
    } >&2
    exit 1
}

case ",${LABELS:-}," in *,no-agent-review,*)
    echo "Skipped: 'no-agent-review' label present."; exit 0;;
esac

# The section runs from its heading to the next level-2 heading or the end. CRs are
# dropped because GitHub stores descriptions with CRLF line endings.
# Here-strings, not pipes: under pipefail a `grep -q` that exits on its first match
# can leave the writer killed by SIGPIPE, which would read as "no match".
body=${PR_BODY:-}
body=${body//$'\r'/}
grep -q '^## Reviewer loop[[:space:]]*$' <<<"$body" \
    || fail "the PR description has no '## Reviewer loop' section."
section=$(awk '/^## Reviewer loop[[:space:]]*$/ {on=1; next} /^## / {on=0} on' <<<"$body")

# shellcheck disable=SC2016 # the backticks are literal: the sha may be wrapped in them
sha=$(sed -n 's/^- Reviewed at:[[:space:]]*`\{0,1\}\([0-9a-f]\{7,40\}\)`\{0,1\}.*/\1/p' \
    <<<"$section")
sha=${sha%%$'\n'*}
[ -n "$sha" ] || fail "'- Reviewed at:' does not name a commit sha."

git cat-file -e "${sha}^{commit}" 2>/dev/null \
    || fail "'Reviewed at: ${sha}' is not a commit in this repository."
head="${HEAD_SHA:-HEAD}"
base="${BASE_SHA:-}"
[ -n "$base" ] || { echo "ERROR: BASE_SHA is not set; name the PR's base branch tip." >&2; exit 2; }
# `merge-base --is-ancestor` exits 128 on a ref it cannot resolve, which an `if`
# reads the same as "not an ancestor". Resolve both first.
for ref in "$head" "$base"; do
    git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null \
        || { echo "ERROR: cannot resolve '${ref}' in this checkout." >&2; exit 2; }
done
git merge-base --is-ancestor "$sha" "$head" \
    || fail "'Reviewed at: ${sha}' is not on this branch."
if git merge-base --is-ancestor "$sha" "$base"; then
    fail "'Reviewed at: ${sha}' is a commit of the base branch, not one of this PR's."
fi

for heading in "Fixed:" "Declined:" "Deferred:" "Not probed:"; do
    grep -q "^- ${heading}" <<<"$section" \
        || fail "the Reviewer loop section has no '- ${heading}' line."
done
# The template's placeholders are angle-bracketed. The Reviewed at and Fixed lines
# carry no code, so a '<...>' left on either means the block was not filled in; the
# entry lines are not checked, since they can name a type such as Vec<u8>.
if grep -qE '^- (Reviewed at|Fixed):.*<[^>]*>' <<<"$section"; then
    fail "the Reviewed at or Fixed line still holds a template placeholder ('<...>')."
fi

echo "Reviewer loop block present; reviewed at ${sha}."

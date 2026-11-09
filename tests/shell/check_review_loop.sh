#!/bin/bash
# Scenario tests for scripts/check-review-loop.sh, the PR-description check behind
# the review-loop-check workflow: which descriptions it accepts, which it refuses,
# and with what message.
#
# Each scenario runs the script inside a throwaway repository holding a base branch,
# a PR branch two commits ahead of the point it branched from (the base has moved on
# since), and a third branch with a commit on neither, so "is this sha one of the PR's
# own commits" is decided by real git ancestry. The repository lives under the
# cargo target directory's scratch/ (target/scratch/ unless CARGO_TARGET_DIR says
# otherwise), is removed when all scenarios pass, and is kept otherwise.
#
# Usage: bash tests/shell/check_review_loop.sh [script-under-test]

set -e

# A git hook or wrapper that exports these would point every git call below at the
# developer's repository instead of the throwaway one.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SCRIPT="${1:-$ROOT/scripts/check-review-loop.sh}"
TEMPLATE="$ROOT/.github/pull_request_template.md"
SKILL="$ROOT/.claude/skills/gpl-boundary-review/SKILL.md"
FAILED=0
ALL_PASSED=0

SCRATCH="${CARGO_TARGET_DIR:-$ROOT/target}/scratch"
mkdir -p "$SCRATCH"
# Absolute, since invoke() redirects into it after a cd.
SCRATCH="$(cd "$SCRATCH" && pwd -P)"
TMP_ROOT="$(mktemp -d "$SCRATCH/check_review_loop.XXXXXX")"
trap 'if [ "$ALL_PASSED" = 1 ]; then rm -rf "$TMP_ROOT"; fi' EXIT
mkdir -p "$TMP_ROOT/repo"
REPO="$(cd "$TMP_ROOT/repo" && pwd -P)"

# The developer's global hooks and signing settings must not reach the throwaway repo.
g() { git -C "$REPO" -c commit.gpgsign=false -c core.hooksPath=/dev/null "$@"; }

g init -q -b base
# The throwaway repository sits inside this work tree. If it is not its own
# repository, the commits below would land in the enclosing one.
if [ "$(git -C "$REPO" rev-parse --show-toplevel 2> /dev/null)" != "$REPO" ]; then
    echo "ERROR: $REPO is not its own git repository; refusing to commit in it." >&2
    exit 2
fi
g config user.email t@example.org
g config user.name t
commit() {
    printf '%s\n' "$1" > "$REPO/f"
    g add f
    g commit -q -m "$1"
    g rev-parse --short HEAD
}
SHA_base="$(commit base)"
g checkout -q -b pr
SHA_first="$(commit first)"
SHA_second="$(commit second)"
# The base branch moves on after the PR branches, so its tip is not the branch point.
g checkout -q base
commit base_tip > /dev/null
g checkout -q -b elsewhere base
SHA_elsewhere="$(commit elsewhere)"
g checkout -q pr

BLOCK='## Summary

Something.

## Reviewer loop

- Reviewed at: @SHA@ · rounds: 2 · stopped: nothing new
- Fixed: 3
- Declined:
  - none
- Deferred:
  - none
- Not probed:
  - none

## Notes
'
block() { printf '%s' "${BLOCK//@SHA@/$1}"; }

# fill TEXT: TEXT with the block's Reviewed at and Fixed lines filled in, as an
# author fills in the template or the skill's example.
fill() {
    # shellcheck disable=SC2001 # a line-anchored edit, which ${var//} cannot express
    sed -e "s/^- Reviewed at:.*/- Reviewed at: $SHA_second · rounds: 1 · stopped: nothing new/" \
        -e 's/^- Fixed:.*/- Fixed: 0/' <<< "$1"
}

# invoke BODY [LABELS] [BASE]: run the script as the workflow does, with the PR head
# at the `pr` branch. BASE defaults to `base`; an empty BASE leaves BASE_SHA unset.
invoke() {
    local body="$1" labels="${2:-}" base="${3-base}"
    EXIT=0
    OUT="$(cd "$REPO" && env -u BASE_SHA ${base:+"BASE_SHA=$base"} \
        PR_BODY="$body" LABELS="$labels" HEAD_SHA=pr \
        bash "$SCRIPT" 2> "$TMP_ROOT/stderr")" || EXIT=$?
    ERR="$(cat "$TMP_ROOT/stderr")"
}

report() {
    local name="$1" why="$2" ok="$3"
    echo "Running: $name"
    if [ "$ok" = 1 ]; then
        echo "  PASS"
    else
        echo "  FAIL"
        echo "  Why it matters: $why"
        echo "  Exit: $EXIT"
        echo "  Stdout: $OUT"
        echo "  Stderr: $ERR"
        FAILED=1
    fi
}

# --- accepted ---

invoke "$(block "$SHA_first")"
ok=0
[[ $EXIT -eq 0 && "$OUT" == *"$SHA_first"* ]] && ok=1
report "a block naming one of the PR's commits passes" \
    "the check exists to accept exactly this, and its output names the commit the loop read" "$ok"

body="$(block "\`$SHA_second\`")"
invoke "${body//$'\n'/$'\r\n'}"
ok=0
[[ $EXIT -eq 0 ]] && ok=1
report "CRLF line endings and a backticked sha pass" \
    "GitHub stores descriptions with CRLF, and the skill's block may wrap the sha in backticks" "$ok"

invoke "" "documentation,no-agent-review"
ok=0
[[ $EXIT -eq 0 && "$OUT" == *"no-agent-review"* ]] && ok=1
report "the no-agent-review label passes without a block" \
    "the label is the only opt-out for a PR whose author cannot run the loop" "$ok"

body="$(block "$SHA_first")"
invoke "${body/$'- Declined:\n  - none'/$'- Declined:\n  - a copy into a Vec<u8>, \x60src/x.rs\x60 — test-only path'}"
ok=0
[[ $EXIT -eq 0 ]] && ok=1
report "an entry line naming a type such as Vec<u8> passes" \
    "only the Reviewed at and Fixed lines are checked for placeholders, since an entry can quote code" "$ok"

# 100 KB is over a pipe buffer and under Linux's 128 KiB cap on one environment
# string. Against a piped `grep -q` this fails on most Linux runs but not every one,
# because whether the writer is killed is a race.
body="$(block "$SHA_first")"
long="$(printf '%999s\n' '' | tr ' ' x)"
for _ in $(seq 100); do body+=$'\n'"$long"; done
invoke "$body"
ok=0
[[ $EXIT -eq 0 ]] && ok=1
report "a 100 KB description does not hide the section" \
    "a grep -q that exits early can kill its writer with SIGPIPE, which pipefail reads as no match" "$ok"

# --- refused ---

invoke $'## Summary\n\nNo section here.\n'
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"no '## Reviewer loop' section"* ]] && ok=1
report "a description without the section is refused" \
    "a PR opened without running the loop has to fail the check" "$ok"

invoke "$(block '<sha>')"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"does not name a commit sha"* ]] && ok=1
report "the template's unfilled placeholder is refused" \
    "the template carries the block's shape, so an untouched template must not pass" "$ok"

invoke "$(block 0123456789abcdef)"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"is not a commit in this repository"* ]] && ok=1
report "a sha that names no commit is refused" \
    "a made-up sha cannot record which commit the loop read" "$ok"

invoke "$(block "$SHA_base")"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"a commit of the base branch"* ]] && ok=1
report "a base-branch commit is refused" \
    "the branch point is an ancestor of the PR head, so ancestry alone would accept a review of none of the PR's code" "$ok"

invoke "$(block "$SHA_elsewhere")"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"is not on this branch"* ]] && ok=1
report "a commit on another branch is refused" \
    "a block copied from another PR names a commit this branch does not contain" "$ok"

for heading in "Fixed:" "Declined:" "Deferred:" "Not probed:"; do
    body="$(block "$SHA_first")"
    invoke "${body/"- $heading"/- Other:}"
    ok=0
    [[ $EXIT -eq 1 && "$ERR" == *"no '- $heading' line"* ]] && ok=1
    report "a block without the '- $heading' line is refused" \
        "the Fixed, Declined, Deferred and Not probed lines hand the open findings to the human reviewer" "$ok"
done

body="$(block "$SHA_first")"
heading='## Reviewer loop'
other='## Something else'
invoke "${body/"$heading"/$other}"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"no '## Reviewer loop' section"* ]] && ok=1
report "the block's lines under another heading do not count" \
    "only the Reviewer loop section is read, so a stray copy elsewhere must not satisfy it" "$ok"

invoke $'## Summary\n\n## Reviewer loop\n\n\n## Notes\n'
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"does not name a commit sha"* ]] && ok=1
report "an empty section is reported as unfilled, not missing" \
    "the author added the heading and needs to be told what to put under it" "$ok"

body="$(block "$SHA_first")"
invoke $'## Reviewer loop\n\n'"${body/"$heading"/$other}"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"does not name a commit sha"* ]] && ok=1
report "a filled block under another heading does not fill an empty section" \
    "the block is read from the Reviewer loop section only, not from anywhere in the description" "$ok"

# Three of the four lines in the section, the fourth only under a later heading.
np=$'- Not probed:\n  - none\n'
body="$(block "$SHA_first")"
invoke "${body/"$np"/}"$'\n'"$np"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"no '- Not probed:' line"* ]] && ok=1
report "a line found only under another heading does not count" \
    "each of the four lines is read from the Reviewer loop section, not from anywhere in the description" "$ok"

body="$(block "$SHA_first")"
invoke "${body/"rounds: 2"/rounds: <N>}"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"template placeholder"* ]] && ok=1
report "a placeholder left on the Reviewed at line is refused" \
    "a sha pasted into an otherwise untouched template line records no rounds and no stop rule" "$ok"

body="$(block "$SHA_first")"
invoke "${body/"- Fixed: 3"/- Fixed: <count>}"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"template placeholder"* ]] && ok=1
report "a placeholder left on the Fixed line is refused" \
    "an unfilled count records nothing about what the loop changed" "$ok"

invoke "" "no-agent-review-requested"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"no '## Reviewer loop' section"* ]] && ok=1
report "a label that only starts with no-agent-review does not skip the check" \
    "the opt-out is one exact label; a near-miss label is not a decision to skip the loop" "$ok"

for fixed in "- Fixed:" "- Fixed: TBD"; do
    body="$(block "$SHA_first")"
    invoke "${body/"- Fixed: 3"/$fixed}"
    ok=0
    [[ $EXIT -eq 1 && "$ERR" == *"does not give a count"* ]] && ok=1
    report "a Fixed line reading '$fixed' is refused" \
        "the count is the record of what the loop changed; a line without one records nothing" "$ok"
done

body="$(block "$SHA_first")"
invoke "${body/" · rounds: 2 · stopped: nothing new"/}"
ok=0
[[ $EXIT -eq 1 && "$ERR" == *"does not give 'rounds:"* ]] && ok=1
report "a Reviewed at line holding only the sha is refused" \
    "the rounds and the stop rule say how the loop ended; a sha alone does not" "$ok"

EXIT=0 OUT="" ERR=""
if tpl="$(cat "$TEMPLATE")"; then
    invoke "${tpl/<sha>/$SHA_second}"
fi
ok=0
[[ -n "${tpl:-}" && $EXIT -eq 1 && "$ERR" == *"template placeholder"* ]] && ok=1
report "the PR template with only its sha filled in is refused" \
    "pasting a sha into the untouched template is the shortcut the placeholder check exists to stop" "$ok"

invoke "$(block "$SHA_base")" "" no-such-ref
ok=0
[[ $EXIT -eq 2 && "$ERR" == *"cannot resolve 'no-such-ref'"* ]] && ok=1
report "an unresolvable base is an error, not a pass" \
    "merge-base exits 128 on an unknown ref, which an if reads as not-an-ancestor and would let a base commit through" "$ok"

invoke "$(block "$SHA_base")" "" ""
ok=0
[[ $EXIT -eq 2 && "$ERR" == *"BASE_SHA"* ]] && ok=1
report "an unset BASE_SHA is an error, not a default" \
    "a stacked PR's base is another PR's branch, so defaulting to main would accept that PR's commits as this one's" "$ok"

# --- the published block shape ---

EXIT=0 OUT="" ERR=""
if tpl="$(cat "$TEMPLATE")"; then
    invoke "$(fill "$tpl")"
fi
ok=0
[[ -n "${tpl:-}" && $EXIT -eq 0 ]] && ok=1
report "the PR template passes once its sha and count are filled in" \
    "the template carries a copy of the block, and it has to be the shape the script accepts" "$ok"

# The fenced example under the skill's PR-body heading.
EXIT=0 OUT="" ERR=""
example="$(awk '/^## Then write the PR-body block/ {s = 1} s && /^```markdown$/ {on = 1; next} on && /^```$/ {exit} on' \
    "$SKILL" 2> /dev/null)" || true
if [ -n "$example" ]; then
    invoke "$(fill "$example")"
fi
ok=0
[[ -n "$example" && $EXIT -eq 0 ]] && ok=1
report "the skill's example block passes once its sha and count are filled in" \
    "the skill prints the block authors paste, and it has to be the shape the script accepts" "$ok"

echo ""
if [ $FAILED -eq 0 ]; then
    ALL_PASSED=1
    echo "All tests passed!"
    exit 0
else
    echo "Some tests failed! Scenario repository kept in $TMP_ROOT"
    exit 1
fi

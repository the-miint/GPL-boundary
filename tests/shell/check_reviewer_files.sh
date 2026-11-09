#!/bin/bash
# Structural checks on the review tooling's prose: the reviewer agent
# (.claude/agents/gpl-boundary-reviewer.md) and the skill that runs its loop
# (.claude/skills/gpl-boundary-review/SKILL.md). The checks cover what a reader
# cannot see by skimming one file:
#   - every rule the families table or the Trigger map names has a "### R<n>"
#     heading, every heading has a family (or is coordinator-only) and a
#     Trigger-map row, and no rule has two families;
#   - the skill and the agent name each other's paths, and their frontmatter
#     names are the ones callers spawn them by;
#   - CLAUDE.md has a "Reviewing a branch" section naming both files and the
#     opt-out label;
#   - the agent, the skill, the PR template and CLAUDE.md's "Reviewing a branch"
#     section carry no planning handles.
#
# Usage: bash tests/shell/check_reviewer_files.sh

set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
AGENT_REL=".claude/agents/gpl-boundary-reviewer.md"
SKILL_REL=".claude/skills/gpl-boundary-review/SKILL.md"
AGENT="$ROOT/$AGENT_REL"
SKILL="$ROOT/$SKILL_REL"
TEMPLATE="$ROOT/.github/pull_request_template.md"
CLAUDE_MD="$ROOT/CLAUDE.md"
FAILED=0

report() {
    local name="$1" why="$2" ok="$3" detail="${4:-}"
    echo "Running: $name"
    if [ "$ok" = 1 ]; then
        echo "  PASS"
    else
        echo "  FAIL"
        echo "  Why it matters: $why"
        [ -n "$detail" ] && printf '  %s\n' "$detail"
        FAILED=1
    fi
}

# section FILE HEADING: the lines after a heading that matches HEADING (an ERE
# anchored at the line start), up to the next heading of the same or a higher
# level.
section() {
    awk -v h="$2" '
        on && /^#+ / { if (length($1) <= lvl) exit }
        on { print }
        !on && $0 ~ "^" h { on = 1; lvl = length($1) }
    ' "$1" 2> /dev/null
}

# The R<n> tokens in stdin, one per line, sorted and unique.
rules_in() { grep -oE '(^|[^A-Za-z0-9_])R[0-9]+' | grep -oE 'R[0-9]+' | sort -u; }

# A newline-separated set on one line, for messages.
oneline() { tr '\n' ' ' <<< "$1"; }

# Words of $1 not in $2 (both newline-separated sets).
missing_from() { comm -23 <(printf '%s\n' "$1" | sed '/^$/d' | sort -u) <(printf '%s\n' "$2" | sed '/^$/d' | sort -u); }

# --- rule numbering ---

families="$(section "$AGENT" '### Rule families')"
family_rows="$(grep '^|' <<< "$families")"
family_rules="$(rules_in <<< "$family_rows")"
coordinator_rules="$(grep -v '^|' <<< "$families" | grep -i 'coordinator' | rules_in)"
trigger_rules="$(section "$AGENT" '## Trigger map' | grep '^|' | rules_in)"
headings="$(grep -oE '^### R[0-9]+' "$AGENT" 2> /dev/null | rules_in)"

ok=0
[[ -n "$headings" && -n "$family_rules" && -n "$trigger_rules" ]] && ok=1
report "the agent has rule headings, a families table and a Trigger map" \
    "every check below compares these three sets; an empty one would pass them vacuously" "$ok" \
    "headings: $(oneline "$headings") | families: $(oneline "$family_rules") | triggers: $(oneline "$trigger_rules")"

gap="$(missing_from "$family_rules"$'\n'"$trigger_rules" "$headings")"
ok=0
[[ -n "$headings" && -z "$gap" ]] && ok=1
report "every rule the families table or Trigger map names has a heading" \
    "a family or trigger pointing at a missing rule sends the reviewer to text that does not exist" "$ok" \
    "no heading for: $(oneline "$gap")"

gap="$(missing_from "$headings" "$family_rules"$'\n'"$coordinator_rules")"
ok=0
[[ -n "$headings" && -z "$gap" ]] && ok=1
report "every rule heading has a family or is coordinator-only" \
    "a fanned-out review applies only the rules its family names, so a rule with no family is never applied" "$ok" \
    "no family for: $(oneline "$gap")"

gap="$(missing_from "$headings" "$trigger_rules")"
ok=0
[[ -n "$headings" && -z "$gap" ]] && ok=1
report "every rule heading has a Trigger-map row" \
    "only rules whose trigger appears in the diff can fire, so a rule with no row never fires" "$ok" \
    "no trigger for: $(oneline "$gap")"

dup="$(while IFS= read -r row; do rules_in <<< "$row"; done <<< "$family_rows" | sort | uniq -d)"
ok=0
[[ -n "$family_rows" && -z "$dup" ]] && ok=1
report "no rule belongs to two families" \
    "every rule has one home; a rule in two families is reviewed twice and reported twice" "$ok" \
    "in more than one family: $(oneline "$dup")"

# --- the skill and the agent find each other ---

ok=0
[[ -f "$AGENT" && -f "$SKILL" ]] && grep -qF "$AGENT_REL" "$SKILL" && grep -qF "$SKILL_REL" "$AGENT" && ok=1
report "the skill and the agent exist and name each other's paths" \
    "the skill defers the rules to the agent and the agent defers the loop to the skill; a stale path breaks the handoff" "$ok"

frontmatter_name() { awk 'NR == 1 && /^---$/ {on = 1; next} on && /^---$/ {exit} on && /^name:/ {sub(/^name:[[:space:]]*/, ""); print}' "$1" 2> /dev/null; }
ok=0
[[ "$(frontmatter_name "$SKILL")" == gpl-boundary-review && "$(frontmatter_name "$AGENT")" == gpl-boundary-reviewer ]] && ok=1
report "the frontmatter names are gpl-boundary-review and gpl-boundary-reviewer" \
    "the skill spawns the agent by name, and the check script's message names the skill" "$ok" \
    "skill: '$(frontmatter_name "$SKILL")' agent: '$(frontmatter_name "$AGENT")'"

# --- CLAUDE.md points at the loop ---

review_section="$(section "$CLAUDE_MD" '## Reviewing a branch')"
ok=0
grep -qF "$SKILL_REL" <<< "$review_section" && grep -qF "$AGENT_REL" <<< "$review_section" \
    && grep -qF 'no-agent-review' <<< "$review_section" && ok=1
report "CLAUDE.md's 'Reviewing a branch' section names the skill, the agent and the label" \
    "CLAUDE.md is what a session reads first; without the section, nothing tells it a PR needs the loop or how to opt out" "$ok"

# --- planning handles ---

# The handle pattern of the agent's PR-level checks. The agent's own statement of
# that check quotes the pattern, so its lines are blanked (line numbers kept).
HANDLES='(^|[^A-Za-z0-9_])M[0-9]+([^A-Za-z0-9_]|$)|[Pp]hase [0-9]|[Cc]ycle [0-9]|localdocs|[Mm]ilestone'
hits=""
scan() {  # scan LABEL: stdin is the text
    local found
    found="$(grep -nE "$HANDLES" | sed "s|^|$1:|")" || true
    [ -n "$found" ] && hits+="$found"$'\n'
}
if [[ -f "$AGENT" && -f "$SKILL" ]]; then
    scan "$AGENT_REL" < <(awk '/^5\. \*\*Planning marks/ {skip = 1} /^6\. / {skip = 0} {print (skip ? "" : $0)}' "$AGENT")
    while IFS= read -r f; do
        [ "$f" = "$AGENT" ] && continue
        scan "${f#"$ROOT"/}" < "$f"
    done < <(find "$ROOT/.claude" -name '*.md' | sort)
    scan ".github/pull_request_template.md" < "$TEMPLATE"
    scan "CLAUDE.md (Reviewing a branch, line numbers relative)" < <(section "$CLAUDE_MD" '## Reviewing a branch')
fi
ok=0
[[ -f "$AGENT" && -f "$SKILL" && -z "$hits" ]] && ok=1
report "the review tooling carries no planning handles" \
    "these files are committed and read by every future reviewer; a handle points into notes they do not have" "$ok" \
    "${hits:-agent or skill missing}"

echo ""
if [ $FAILED -eq 0 ]; then
    echo "All tests passed!"
    exit 0
else
    echo "Some tests failed!"
    exit 1
fi

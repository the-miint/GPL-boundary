## Summary

<!-- What changes and why. -->

## Test plan

<!-- What you ran, and what you did not. CI runs Linux and macOS, but name anything
it cannot reach: a test needing an environment the runner lacks, an #[ignore]d test,
or a tool fork's own CI lanes when a submodule pin moves. -->

## Downstream (miint)

<!-- A change to the wire protocol, an Arrow schema, a --describe surface, a config
knob, the CLI or the install path needs a matching change in duckdb-miint. Name it,
say whether miint's gpl-boundary version pin needs a bump, or write "none". -->

## Reviewer loop

<!--
Run the `gpl-boundary-review` skill (`.claude/skills/gpl-boundary-review/SKILL.md`, in
Claude Code) on the branch and replace this section, heading included, with the block
it prints. `review-loop-check` reads this section. A PR that cannot run the loop
carries the `no-agent-review` label and says why in this section.
Open the issue for each deferred finding before opening the PR, and name it here as #N.
-->

- Reviewed at: <sha> · rounds: <N> · code-review: <level> · stopped: <stop rule>
- Fixed: <count>
- Declined:
  - none
- Deferred:
  - none
- Not probed:
  - none

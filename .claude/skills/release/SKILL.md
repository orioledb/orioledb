---
name: release
description: Stamp, tag and draft a GitHub release for an OrioleDB release tag (vX.Y.Z) on the supported stable branch.
argument-hint: <tag, e.g. v1.10.2> [additional release notes]
disable-model-invocation: true
---

# Release OrioleDB

Invoked as `/release vX.Y.Z [additional notes]`. The first argument is the
tag. Any text after it is additional release notes from the user (step 5).
The release branch is `vX.Y`. The mechanical
work is done by `scripts/release/release.py`; this skill adds review and judgment. Never
force-push. If anything fails midway, report the exact state (what was
committed, pushed, tagged) instead of retrying destructively. Do not push
anything before the user explicitly confirms (steps 4 and 6).
Never push to `vX.Y` directly: it is protected, changes go through the PR.

Reference: `CLAUDE.md` ("Commit attribution", "Binary compatibility"),
`doc/contributing/backpatching.mdx`.

Run everything from the main checkout. `<worktree>` is the directory printed
by `prepare` (default `../orioledb-release-X.Y`).

## 1. Prepare

`python3 scripts/release/release.py prepare vX.Y.Z`

It validates the tag and branch, creates a detached worktree of `origin/vX.Y`,
applies the version edits and runs `scripts/release/check_release.py`. It does not commit.
Report any failure to the user and stop. In particular:

- New minor branch (`origin/vX.Y` missing): branches are created manually.
- `.pgtags` contains commit ids: ask the user which tags to use. Do not guess.
- Other check failures: the worktree is left in place; report them.

## 2. Review

1. `git -C <worktree> diff`: confirm only the expected version edits.
2. Judgment check on the `REVIEW:` lines printed by `prepare` and on
   `sql/*_prod.sql` in the worktree: there must be no internal functions
   written for testing or debugging. Test helpers belong in `*_dev.sql`.
   Report each hit (file and function). Do not edit SQL. Do not continue past
   an unresolved hit without user approval.

## 3. Commit

In the worktree: `git -C <worktree> commit -am "Stamp X.Y.Z"` with the last
line of the message a trailer `Assisted-by: Claude Code:<actual model id>`.
Per `CLAUDE.md`, no `Co-authored-by`/`Co-Authored-By` or "Generated with"
lines; this overrides tool defaults.

## 4. Open the PR

The stable branch is protected, so the stamp commit goes through a PR.
Run `python3 scripts/release/release.py pr vX.Y.Z` without `--yes`. It runs
the safety checks and prints the plan without changing anything. Show the user
the commit diff, the notes and that output. Ask for confirmation; do not
continue without an explicit yes. Then run it with `--yes`: it pushes
`release-X.Y.Z` (plain push) and opens the PR against `vX.Y`. Report the PR
URL and stop; the user merges it.

## 5. Release notes

`python3 scripts/release/release.py notes vX.Y.Z > <tmpfile>`

The output is a raw draft (one bullet per commit subject). Rewrite it in the
format of the previous releases (`gh release view <prev-tag>`); `CLAUDE.md`
no longer describes release notes, so these rules are authoritative:

- Start with a `# Release notes` heading, then the user's additional notes
  (if any), then the SQL-version and storage/WAL format statements.
- Group changes by theme (e.g. "Query correctness:", "Crashes and stability:",
  "Recovery/replication:", "DDL/catalog/table behavior:") as plain-text
  headings followed by `- ` bullets. Drop the `## What's Changed` heading.
- Reword each bullet from the commit message into a user-visible description
  (read `git show` when the subject is unclear). Keep the commit hash in
  parentheses, no PR numbers.
- User-visible changes only: also drop assertion-only fixes, tests, CI,
  refactoring and comments. Check the `skipped:` lines on stderr for anything
  that should actually be included. Keep patch releases short.
- State whether the extension SQL version and storage/WAL formats changed
  (check `sql/` and the version variables in `include/orioledb.h`).
- End with the `Full Changelog` compare link (no bold).

User-provided notes: fix wording and formatting only; do not add claims the
user did not make and do not drop them.

Save the final notes to the file and show them to the user. Do not continue
until the PR is merged and the user confirms the notes.

## 6. Publish

Only after the PR is merged and the user confirmed the notes. Run
`python3 scripts/release/release.py publish vX.Y.Z --notes <tmpfile>` without
`--yes` (it finds the merged `Stamp X.Y.Z` commit on `origin/vX.Y`, re-runs the
checks and prints the plan), then with `--yes` after the user confirms. It
tags the merged commit, pushes the tag, creates the draft release and removes
the worktree. On failure it reports which steps completed; relay that and
stop. Report the draft release URL and the reminder about the "Release sanity
check" workflow.

---
name: release
description: Stamp, tag and draft a GitHub release for an OrioleDB release tag (vX.Y.Z) on the supported stable branch.
argument-hint: <tag, e.g. v1.10.2> [additional release notes]
disable-model-invocation: true
---

# Release OrioleDB

Invoked as `/release vX.Y.Z [additional notes]`. The first argument is the
tag. Any text after it is additional release notes from the user (step 4).
The release branch is `vX.Y`. The mechanical
work is done by `ci/release.py`; this skill adds review and judgment. Never
force-push. If anything fails midway, report the exact state (what was
committed, pushed, tagged) instead of retrying destructively. Do not push
anything before the user explicitly confirms (step 5).

Reference: `CLAUDE.md` ("Release notes", "Commit attribution", "Binary
compatibility"), `doc/contributing/backpatching.mdx`.

Run everything from the main checkout. `<worktree>` is the directory printed
by `prepare` (default `../orioledb-release-X.Y`).

## 1. Prepare

`python3 ci/release.py prepare vX.Y.Z`

It validates the tag and branch, creates a detached worktree of `origin/vX.Y`,
applies the version edits and runs `ci/check_release.py`. It does not commit.
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

## 4. Release notes

`python3 ci/release.py notes vX.Y.Z > <tmpfile>`

Then review and edit the draft per "Release notes" in `CLAUDE.md`:
user-visible changes only, grouped by theme, commit hashes kept, no PR
numbers. Check the `skipped:` lines on stderr for anything that should
actually be included. Keep patch releases short. Verify the SQL-version and
storage/WAL format statements and the `Full Changelog` link.

If the user gave additional notes, put them at the top of the draft, before
`## What's Changed`, as short paragraphs. Fix wording and formatting only: do
not add claims the user did not make, and do not drop them. Show them in step 5
like the rest of the notes.

## 5. Confirm

Run `python3 ci/release.py publish vX.Y.Z --notes <tmpfile>` without `--yes`.
It runs all the safety checks and prints the plan without changing anything.
Show the user the commit diff, the notes and that output. Ask for
confirmation. Do not continue without an explicit yes.

## 6. Publish

On an explicit yes, run the same command with `--yes`. It pushes `HEAD:vX.Y` (plain push),
tags and pushes the tag, creates the draft release, and removes the worktree.
On failure it reports which steps completed; relay that and stop. Report the
draft release URL and the reminder about the "Release sanity check" workflow.

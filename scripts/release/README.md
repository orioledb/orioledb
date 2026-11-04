# Release scripts

`release.py` prepares and publishes a release of an existing stable branch
(`vX.Y`). Creating a new minor branch is done manually. `check_release.py` is
the sanity check that `release.py` and the `release_check.yml` workflow run.

Requirements: run from the main checkout, with `git` and an authenticated `gh`.
The work happens in a separate worktree of `origin/vX.Y` (default
`../orioledb-release-X.Y`, override with `--worktree`), so no local branch is
created or changed. Nothing is force-pushed.

## Steps

```sh
# 1. Create the worktree, bump the version in the files, run the checks.
scripts/release/release.py prepare vX.Y.Z

# 2. Review the changes in the worktree and commit them as "Stamp X.Y.Z".
#    Fix any FAIL or REVIEW lines printed by prepare first.

# 3. Print draft release notes (commits since the previous tag), edit them
#    and save to a file.
scripts/release/release.py notes vX.Y.Z > notes.md

# 4. Dry run: re-runs the checks and prints what would be pushed.
scripts/release/release.py publish vX.Y.Z --notes notes.md

# 5. Push the branch and the tag, and create a draft GitHub release.
scripts/release/release.py publish vX.Y.Z --notes notes.md --yes
```

After the push, check the "Release sanity check" workflow
(`gh run list --workflow release_check.yml`), then review and publish the
draft release on GitHub. Remove the worktree when done:
`git worktree remove <worktree>`.

To run only the sanity check on a tagged commit:
`scripts/release/check_release.py vX.Y.Z [--repo DIR]`.

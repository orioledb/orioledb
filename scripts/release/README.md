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

# 3. Dry run: re-runs the checks and prints what would be pushed.
scripts/release/release.py pr vX.Y.Z

# 4. Push release-X.Y.Z and open a PR against vX.Y (the branch is protected).
scripts/release/release.py pr vX.Y.Z --yes

# 5. Print draft release notes (commits since the previous tag), edit them
#    and save to a file.
scripts/release/release.py notes vX.Y.Z > notes.md

# 6. After the PR is merged: find the merged "Stamp X.Y.Z" commit, tag it,
#    push the tag and create a draft GitHub release.
scripts/release/release.py publish vX.Y.Z --notes notes.md
scripts/release/release.py publish vX.Y.Z --notes notes.md --yes
```

After the tag push, check the "Release sanity check" workflow
(`gh run list --workflow release_check.yml`), then review and publish the
draft release on GitHub. `publish --yes` removes the worktree.

To run only the sanity check on a tagged commit:
`scripts/release/check_release.py vX.Y.Z [--repo DIR]`.

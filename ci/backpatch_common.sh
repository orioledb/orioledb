#!/usr/bin/env bash
#
# Helpers shared by backpatch.sh and backpatch_check.sh.
#
# Release branches are named v1, v2, v3, ...  A commit says how far back it
# goes with one of these lines in its message:
#
#   Backpatch-through: vX     backpatch to vX and every later release branch
#   No-backpatch: <reason>    the commit is not backpatched, and why
#
# Like a git trailer, a declaration starts at the beginning of its line.  An
# indented one, such as the two above, only mentions the convention.

# Is the branch name a release branch?
is_release_branch() {
	[[ "$1" =~ ^v[0-9]+$ ]]
}

# Run a command, retrying what GitHub fails now and then (5xx, timeouts).
gh_retry() {
	local i
	for i in 1 2 3 4; do
		if "$@"; then
			return 0
		fi
		[ "$i" -lt 4 ] && sleep $((i * ${GH_RETRY_DELAY:-5}))
	done
	return 1
}

# Release branches of the repository, oldest first, one per line.
release_branches() {
	local names
	names=$(gh_retry gh api --paginate "repos/$GH_REPO/branches" --jq '.[].name')
	grep -E '^v[0-9]+$' <<< "$names" | sort -t v -k 2 -n || true
}

# "Backpatch-through:" lines of a commit message read from stdin.
backpatch_through_lines() {
	grep -E '^Backpatch-through:' || true
}

# "No-backpatch:" lines of a commit message read from stdin.
no_backpatch_lines() {
	grep -E '^No-backpatch:' || true
}

# The oldest release named by "Backpatch-through: vX" lines of a commit
# message read from stdin, as the number X, or nothing.  A squash merge can
# gather several such lines; the oldest one wins.
backpatch_through_version() {
	backpatch_through_lines |
		sed -nE 's/^Backpatch-through:[[:space:]]*v([0-9]+)[[:space:]]*$/\1/p' |
		sort -n | head -n 1
}

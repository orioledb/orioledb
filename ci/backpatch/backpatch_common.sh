#!/usr/bin/env bash
#
# Helpers shared by backpatch.sh and backpatch_check.sh.
#
# Release branches are named v<major>.<minor>: v1.10, v1.11, v2.0, ...
# At any given time, only the most recent release branch is supported.
# A commit says whether to backpatch it with one of these lines in its
# message:
#
#   Backpatch: stable           backpatch to the supported release branch
#   Backpatch: no <reason>      the commit is not backpatched, and why
#
# Like a git trailer, a declaration starts at the beginning of its line.  An
# indented one, such as the two above, only mentions the convention.

# Is the branch name a release branch?
is_release_branch() {
	[[ "$1" =~ ^v[0-9]+\.[0-9]+$ ]]
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

# The supported release branch, read from .backpatch-target at the repo root.
stable_branch() {
	local f=.backpatch-target
	if [ -f "$f" ]; then
		head -n 1 "$f" | tr -d '[:space:]'
	fi
}

# "Backpatch:" lines of a commit message read from stdin.
backpatch_lines() {
	grep -E '^Backpatch:' || true
}

# Whether the commit message read from stdin says "Backpatch: stable".
# Prints "stable" if so, nothing otherwise.
has_backpatch() {
	backpatch_lines |
		sed -nE 's/^Backpatch:[[:space:]]*stable[[:space:]]*$/stable/p' |
		head -n 1
}

# Whether the commit message read from stdin says "Backpatch: no ...".
# Prints the reason if so, nothing otherwise.
no_backpatch_reason() {
	backpatch_lines |
		sed -nE 's/^Backpatch:[[:space:]]*no[[:space:]]+//p' |
		head -n 1
}

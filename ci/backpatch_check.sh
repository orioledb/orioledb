#!/usr/bin/env bash
#
# Backpatch sanity check of a pull request.
#
# A pull request passes when either
#   - every commit says whether to backpatch it: "Backpatch-through: vX" or
#     "No-backpatch: <reason>", or
#   - no commit says so, and the pull request has no "bug" label.
# That is, a fix declares the backpatch decision for each of its commits, and
# anything else declares nothing.
#
# Environment: GH_TOKEN, GH_REPO (owner/name), PR_NUMBER, BASE_REF.
# BUG_LABEL overrides the name of the label that marks a fix.
#
set -euo pipefail

# shellcheck source=ci/backpatch_common.sh
. "$(dirname "$0")/backpatch_common.sh"

BUG_LABEL=${BUG_LABEL:-bug}

if is_release_branch "$BASE_REF"; then
	echo "The pull request targets release branch $BASE_REF: nothing to check."
	exit 0
fi

summary() {
	echo "$*"
	if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
		echo "$*" >> "$GITHUB_STEP_SUMMARY"
	fi
}

fail() {
	echo "::error::$*"
	summary "**Error:** $*"
}

releases=$(release_branches)
labels=$(gh_retry gh api "repos/$GH_REPO/issues/$PR_NUMBER/labels" --jq '.[].name')
is_bug=false
if grep -qxF "$BUG_LABEL" <<< "$labels"; then
	is_bug=true
fi

errors=0
n_through_commits=0
n_skip_commits=0
declared=()
undeclared=()

summary "### Backpatch declarations"
summary ""
summary "| commit | declaration |"
summary "|---|---|"

# Merge commits only bring in other branches, and are not backpatched
# themselves.
while read -r sha; do
	message=$(gh_retry gh api "repos/$GH_REPO/commits/$sha" --jq '.commit.message')
	subject=$(head -n 1 <<< "$message")
	short=${sha:0:9}
	through=$(backpatch_through_lines <<< "$message")
	none=$(no_backpatch_lines <<< "$message")
	n_through=$(grep -c . <<< "$through" || true)
	n_none=$(grep -c . <<< "$none" || true)

	if [ "$n_through" -gt 0 ] && [ "$n_none" -gt 0 ]; then
		fail "$short ($subject) has both Backpatch-through and No-backpatch."
		errors=$((errors + 1))
		declared+=("$short")
		summary "| $short $subject | both, invalid |"
		continue
	fi
	if [ "$n_through" -gt 1 ] || [ "$n_none" -gt 1 ]; then
		fail "$short ($subject) has more than one backpatch declaration."
		errors=$((errors + 1))
		declared+=("$short")
		summary "| $short $subject | several, invalid |"
		continue
	fi

	if [ "$n_through" -eq 1 ]; then
		declared+=("$short")
		version=$(backpatch_through_version <<< "$message")
		if [ -z "$version" ]; then
			fail "$short ($subject): malformed \"$(xargs <<< "$through")\", expected \"Backpatch-through: vX\"."
			errors=$((errors + 1))
			summary "| $short $subject | malformed Backpatch-through |"
		elif ! grep -qxF "v$version" <<< "$releases"; then
			fail "$short ($subject): Backpatch-through: v$version names no release branch (have: $(xargs <<< "$releases"))."
			errors=$((errors + 1))
			summary "| $short $subject | Backpatch-through: v$version, no such branch |"
		else
			n_through_commits=$((n_through_commits + 1))
			summary "| $short $subject | Backpatch-through: v$version |"
		fi
	elif [ "$n_none" -eq 1 ]; then
		declared+=("$short")
		reason=$(sed -nE 's/^No-backpatch:[[:space:]]*//p' <<< "$none" | xargs)
		if [ -z "$reason" ]; then
			fail "$short ($subject): No-backpatch needs a reason."
			errors=$((errors + 1))
			summary "| $short $subject | No-backpatch without a reason |"
		else
			n_skip_commits=$((n_skip_commits + 1))
			summary "| $short $subject | No-backpatch: $reason |"
		fi
	else
		undeclared+=("$short")
		summary "| $short $subject | none |"
	fi
done < <(gh_retry gh api --paginate "repos/$GH_REPO/pulls/$PR_NUMBER/commits" \
			--jq '.[] | select((.parents | length) == 1) | .sha')

summary ""

if [ ${#declared[@]} -gt 0 ] && [ ${#undeclared[@]} -gt 0 ]; then
	fail "Some commits declare the backpatch decision and some don't: add \"Backpatch-through: vX\" or \"No-backpatch: <reason>\" to ${undeclared[*]}."
	errors=$((errors + 1))
fi

if [ ${#declared[@]} -eq 0 ] && $is_bug; then
	fail "The pull request is labeled \"$BUG_LABEL\", so every commit must say \"Backpatch-through: vX\" or \"No-backpatch: <reason>\"."
	errors=$((errors + 1))
fi

if [ "$errors" -eq 0 ] && [ "$n_through_commits" -gt 0 ] && [ "$n_skip_commits" -gt 0 ]; then
	echo "::warning::Some commits are backpatched and some are not: merge with \"Rebase and merge\".  A squash merge backpatches the changes of the No-backpatch commits too."
	summary "**Warning:** some commits are backpatched and some are not: merge with \"Rebase and merge\".  A squash merge backpatches the changes of the No-backpatch commits too."
fi

if [ "$errors" -gt 0 ]; then
	exit 1
fi

if [ ${#declared[@]} -gt 0 ]; then
	summary "Every commit declares the backpatch decision."
else
	summary "No commit declares a backpatch decision, and the pull request is not labeled \"$BUG_LABEL\"."
fi

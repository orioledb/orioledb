#!/usr/bin/env bash
#
# Backpatch sanity check of a pull request.
#
# A pull request passes when either
#   - every commit says whether to backpatch it: "Backpatch: stable" or
#     "Backpatch: no <reason>", or
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

stable=$(stable_branch)
if [ -n "$stable" ]; then
	if ! gh_retry gh api "repos/$GH_REPO/branches/$stable" --jq '.name' > /dev/null 2>&1; then
		stable_exists=false
	else
		stable_exists=true
	fi
else
	stable_exists=false
fi
labels=$(gh_retry gh api "repos/$GH_REPO/issues/$PR_NUMBER/labels" --jq '.[].name')
is_bug=false
if grep -qxF "$BUG_LABEL" <<< "$labels"; then
	is_bug=true
fi

errors=0
n_bp_commits=0
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
	bp_line=$(backpatch_lines <<< "$message")
	n_bp=$(grep -c . <<< "$bp_line" || true)

	if [ "$n_bp" -gt 1 ]; then
		fail "$short ($subject) has more than one Backpatch declaration."
		errors=$((errors + 1))
		declared+=("$short")
		summary "| $short $subject | several, invalid |"
		continue
	fi

	if [ "$n_bp" -eq 1 ]; then
		declared+=("$short")
		bp=$(has_backpatch <<< "$message")
		if [ -n "$bp" ]; then
			if ! $stable_exists; then
				fail "$short ($subject): Backpatch: stable, but release branch ${stable:-'(none)'} does not exist."
				errors=$((errors + 1))
				summary "| $short $subject | Backpatch: stable, no release branch |"
			else
				n_bp_commits=$((n_bp_commits + 1))
				summary "| $short $subject | Backpatch: stable |"
			fi
		else
			reason=$(no_backpatch_reason <<< "$message")
			if [ -n "$reason" ]; then
				n_skip_commits=$((n_skip_commits + 1))
				summary "| $short $subject | Backpatch: no $reason |"
			else
				fail "$short ($subject): malformed \"$(xargs <<< "$bp_line")\", expected \"Backpatch: stable\" or \"Backpatch: no <reason>\"."
				errors=$((errors + 1))
				summary "| $short $subject | malformed Backpatch |"
			fi
		fi
	else
		undeclared+=("$short")
		summary "| $short $subject | none |"
	fi
done < <(gh_retry gh api --paginate "repos/$GH_REPO/pulls/$PR_NUMBER/commits" \
			--jq '.[] | select((.parents | length) == 1) | .sha')

summary ""

if [ ${#declared[@]} -gt 0 ] && [ ${#undeclared[@]} -gt 0 ]; then
	fail "Some commits declare the backpatch decision and some don't: add \"Backpatch: stable\" or \"Backpatch: no <reason>\" to ${undeclared[*]}."
	errors=$((errors + 1))
fi

if [ ${#declared[@]} -eq 0 ] && $is_bug; then
	fail "The pull request is labeled \"$BUG_LABEL\", so every commit must say \"Backpatch: stable\" or \"Backpatch: no <reason>\"."
	errors=$((errors + 1))
fi

if [ "$n_bp_commits" -gt 0 ] && [ "$n_skip_commits" -gt 0 ]; then
	fail "Contradicting declarations: some commits say \"Backpatch: stable\" and some say \"Backpatch: no\".  All commits in a pull request must agree."
	errors=$((errors + 1))
fi

if [ "$errors" -gt 0 ]; then
	exit 1
fi

if [ ${#declared[@]} -gt 0 ]; then
	summary "Every commit declares the backpatch decision."
else
	summary "No commit declares a backpatch decision, and the pull request is not labeled \"$BUG_LABEL\"."
fi

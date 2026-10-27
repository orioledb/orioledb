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
# When all commits say "Backpatch: stable", the script adds that line to the
# pull request description so squash-merge commits carry it.  Removes it when
# commits no longer declare backpatch.
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

# Ensure the PR description carries "Backpatch: stable" for squash-merge
# commit messages when all commits declare it; remove it when they don't.
# Skip on "edited" events to avoid a loop: our own body edit triggers "edited".
if [ "${EVENT_ACTION:-}" != "edited" ]; then
	pr_body=$(gh_retry gh api "repos/$GH_REPO/pulls/$PR_NUMBER" --jq '.body // ""')
	body_has_bp=false
	if printf '%s\n' "$pr_body" | grep -qE '^Backpatch:[[:space:]]*stable[[:space:]]*$'; then
		body_has_bp=true
	fi

	if [ "$n_bp_commits" -gt 0 ] && ! $body_has_bp; then
		if [ -z "$pr_body" ]; then
			new_body="Backpatch: stable"
		else
			new_body="${pr_body}"$'\n\n'"Backpatch: stable"
		fi
		bp_tmpfile=$(mktemp)
		printf '%s' "$new_body" > "$bp_tmpfile"
		if gh_retry gh pr edit "$PR_NUMBER" --repo "$GH_REPO" --body-file "$bp_tmpfile"; then
			summary "Added \"Backpatch: stable\" to the pull request description for squash-merge."
		else
			echo "::warning::Could not update the pull request description (insufficient permissions?)."
		fi
		rm -f "$bp_tmpfile"
	elif [ "$n_bp_commits" -eq 0 ] && $body_has_bp; then
		new_body=$(printf '%s\n' "$pr_body" | sed '/^Backpatch:[[:space:]]*stable[[:space:]]*$/d')
		bp_tmpfile=$(mktemp)
		printf '%s' "$new_body" > "$bp_tmpfile"
		if gh_retry gh pr edit "$PR_NUMBER" --repo "$GH_REPO" --body-file "$bp_tmpfile"; then
			summary "Removed \"Backpatch: stable\" from the pull request description."
		else
			echo "::warning::Could not update the pull request description (insufficient permissions?)."
		fi
		rm -f "$bp_tmpfile"
	fi
fi

if [ ${#declared[@]} -gt 0 ]; then
	summary "Every commit declares the backpatch decision."
else
	summary "No commit declares a backpatch decision, and the pull request is not labeled \"$BUG_LABEL\"."
fi

#!/usr/bin/env bash
#
# Backpatch a merged pull request.
#
# Every commit the pull request brought into its base branch that says
# "Backpatch-through: vX" is cherry-picked into vX and every later release
# branch.  For each release branch that gets commits, a pull request is opened
# from the branch backpatch/pr-<number>-<release>.
#
# A commit that does not apply cleanly still gets its pull request, so the
# backpatch is not forgotten: the commit is made with the conflict markers in
# it, the commits after it are left out, and the pull request is a draft
# labeled backpatch-conflict that says what is left to do.  The run then
# fails, and the results are commented on the original pull request.
#
# Run from a clone with full history and the release branches fetched.
# Environment: GH_TOKEN, GH_REPO (owner/name), PR_NUMBER.
#
set -euo pipefail

# shellcheck source=ci/backpatch_common.sh
. "$(dirname "$0")/backpatch_common.sh"

NOTE="This commit was automatically created by backpatch workflow."

summary() {
	echo "$*"
	if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
		echo "$*" >> "$GITHUB_STEP_SUMMARY"
	fi
}

pr_json=$(gh api "repos/$GH_REPO/pulls/$PR_NUMBER")
merged=$(jq -r '.merged' <<< "$pr_json")
base_ref=$(jq -r '.base.ref' <<< "$pr_json")
merge_sha=$(jq -r '.merge_commit_sha' <<< "$pr_json")
title=$(jq -r '.title' <<< "$pr_json")
pr_url=$(jq -r '.html_url' <<< "$pr_json")

if [ "$merged" != "true" ]; then
	echo "Pull request #$PR_NUMBER is not merged: nothing to backpatch."
	exit 0
fi
if is_release_branch "$base_ref"; then
	echo "Pull request #$PR_NUMBER was merged into release branch $base_ref: not backpatched further."
	exit 0
fi

git fetch -q origin "$merge_sha"

# Commits of the pull request as they landed in the base branch, oldest first.
#   merge commit: the commits of the merged side;
#   rebase merge: as many first-parent commits as the pull request had,
#                 if their subjects match the pull request's commits;
#   squash merge: the single squashed commit.
pr_subjects=$(gh api --paginate "repos/$GH_REPO/pulls/$PR_NUMBER/commits" \
				--jq '.[] | select((.parents | length) == 1) | .commit.message | split("\n")[0]')
n_pr_commits=$(grep -c . <<< "$pr_subjects" || true)
n_parents=$(($(git rev-list --parents -n 1 "$merge_sha" | wc -w) - 1))

if [ "$n_parents" -gt 1 ]; then
	landed=$(git rev-list --reverse --no-merges "$merge_sha^1..$merge_sha^2")
else
	candidates=$(git rev-list --reverse --first-parent -n "$n_pr_commits" "$merge_sha")
	candidate_subjects=$(for c in $candidates; do git log -1 --format=%s "$c"; done)
	if [ "$n_pr_commits" -gt 1 ] && [ "$candidate_subjects" = "$pr_subjects" ]; then
		landed=$candidates
	else
		landed=$merge_sha
	fi
fi

# Release branch number each landed commit goes back to.
declare -A through
any=false
for c in $landed; do
	version=$(git log -1 --format=%B "$c" | backpatch_through_version)
	if [ -n "$version" ]; then
		through[$c]=$version
		any=true
	fi
done

if ! $any; then
	echo "No commit of #$PR_NUMBER says Backpatch-through: nothing to backpatch."
	exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

created=()
conflicted=()

for release in $(release_branches); do
	n=${release#v}
	picks=()
	for c in $landed; do
		if [ -n "${through[$c]:-}" ] && [ "${through[$c]}" -le "$n" ]; then
			picks+=("$c")
		fi
	done
	[ ${#picks[@]} -eq 0 ] && continue

	branch="backpatch/pr-$PR_NUMBER-$release"
	git fetch -q origin "$release"
	git checkout -q -B "$branch" FETCH_HEAD

	applied=0
	conflict=""			# the commit that did not apply
	conflict_files=""
	left=()				# commits after it, not applied
	for c in "${picks[@]}"; do
		if [ -n "$conflict" ]; then
			left+=("$c")
			continue
		fi
		if git cherry-pick -x "$c" > /dev/null 2>&1; then
			git commit -q --amend -m "$(git log -1 --format=%B HEAD)" -m "$NOTE"
			applied=$((applied + 1))
			continue
		fi
		if [ -z "$(git ls-files -u)" ] && git diff --cached --quiet; then
			# Nothing left to apply: the release already has the change.
			git cherry-pick --skip
			continue
		fi

		# Keep the conflict for whoever resolves it.
		conflict=$c
		conflict_files=$(git diff --name-only --diff-filter=U)
		git add -A
		git -c core.editor=true commit -q --no-verify \
			-m "$(git log -1 --format=%B "$c")" \
			-m "(cherry picked from commit $c)" \
			-m "This commit has unresolved conflicts in: $(xargs <<< "$conflict_files")" \
			-m "$NOTE"
		git cherry-pick --quit 2> /dev/null || true
		applied=$((applied + 1))
	done
	if [ "$applied" -eq 0 ]; then
		summary "$release already has every commit to backpatch."
		continue
	fi

	git push -q --force origin "$branch"

	commits_line="Commits: $(for c in "${picks[@]}"; do printf '%s ' "${c:0:9}"; done)"
	if [ -n "$conflict" ]; then
		# shellcheck disable=SC2016
		body=$(printf '%s\n\n%s\n\n%s\n\n%s\n%s\n\n%s\n\n%s\n' \
			"Backpatch of $pr_url to \`$release\`." \
			"$commits_line" \
			"**${conflict:0:9} $(git log -1 --format=%s "$conflict") did not apply cleanly.** It is committed with the conflict markers in:" \
			"$(sed 's/^/- `/; s/$/`/' <<< "$conflict_files")" \
			"$(if [ ${#left[@]} -gt 0 ]; then printf 'Not applied yet, after it: %s' "$(for c in "${left[@]}"; do printf '%s ' "${c:0:9}"; done)"; fi)" \
			"Resolve the conflicts in branch \`$branch\`, cherry-pick the rest with \`git cherry-pick -x\`, and mark the pull request ready for review." \
			"$NOTE")
		labels=backpatch,backpatch-conflict
		draft=--draft
	else
		body=$(printf '%s\n\n%s\n\n%s\n' \
			"Backpatch of $pr_url to \`$release\`." "$commits_line" "$NOTE")
		labels=backpatch
		draft=
	fi

	url=$(gh pr list --repo "$GH_REPO" --head "$branch" --base "$release" \
			--state open --json url --jq '.[0].url')
	if [ -n "$url" ]; then
		gh pr edit "$url" --repo "$GH_REPO" --body "$body" > /dev/null
		status="$url (updated)"
	else
		# shellcheck disable=SC2086
		url=$(gh pr create --repo "$GH_REPO" --base "$release" --head "$branch" \
				--title "[$release] $title" --body "$body" $draft)
		status=$url
	fi
	gh pr edit "$url" --repo "$GH_REPO" --add-label "$labels" > /dev/null 2>&1 || true

	if [ -n "$conflict" ]; then
		conflicted+=("$release: $status, draft: ${conflict:0:9} $(git log -1 --format=%s "$conflict") conflicts in $(xargs <<< "$conflict_files")")
	else
		created+=("$release: $status")
	fi
done

report="Backpatch workflow results for this pull request:"
for line in "${created[@]}"; do
	report+=$'\n'"- $line"
done
for line in "${conflicted[@]}"; do
	report+=$'\n'"- **$line**: resolve them in that pull request"
done
summary "$report"
gh pr comment "$PR_NUMBER" --repo "$GH_REPO" --body "$report"

[ ${#conflicted[@]} -eq 0 ]

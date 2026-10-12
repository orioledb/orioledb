#!/usr/bin/env bash
#
# Backpatch a merged pull request.
#
# Every commit the pull request brought into its base branch that says
# "Backpatch-through: vX" is cherry-picked into vX and every later release
# branch.  For each release branch that gets commits, a pull request is opened
# from the branch backpatch/pr-<number>-<release>.  Commits that don't apply
# cleanly are reported on the original pull request instead.
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
failed=()

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

	ok=true
	applied=0
	for c in "${picks[@]}"; do
		if ! git cherry-pick -x "$c" > /dev/null 2>&1; then
			if [ -z "$(git ls-files -u)" ] && git diff --cached --quiet; then
				# Nothing left to apply: the release already has the change.
				git cherry-pick --skip
				continue
			fi
			git cherry-pick --abort
			failed+=("$release: ${c:0:9} $(git log -1 --format=%s "$c") does not apply cleanly")
			ok=false
			break
		fi
		git commit -q --amend -m "$(git log -1 --format=%B HEAD)" -m "$NOTE"
		applied=$((applied + 1))
	done
	$ok || continue
	if [ "$applied" -eq 0 ]; then
		summary "$release already has every commit to backpatch."
		continue
	fi

	git push -q --force origin "$branch"
	existing=$(gh pr list --repo "$GH_REPO" --head "$branch" --base "$release" \
				--state open --json url --jq '.[0].url')
	if [ -n "$existing" ]; then
		created+=("$release: $existing (updated)")
		continue
	fi
	body=$(printf '%s\n\n%s\n\n%s\n' \
		"Backpatch of $pr_url to \`$release\`." \
		"Commits: $(for c in "${picks[@]}"; do printf '%s ' "${c:0:9}"; done)" \
		"$NOTE")
	url=$(gh pr create --repo "$GH_REPO" --base "$release" --head "$branch" \
			--title "[$release] $title" --body "$body")
	gh pr edit "$url" --repo "$GH_REPO" --add-label backpatch > /dev/null 2>&1 || true
	created+=("$release: $url")
done

report="Backpatch workflow results for this pull request:"
for line in "${created[@]}"; do
	report+=$'\n'"- $line"
done
for line in "${failed[@]}"; do
	report+=$'\n'"- **$line**: backpatch it by hand with \`git cherry-pick -x\`"
done
summary "$report"
gh pr comment "$PR_NUMBER" --repo "$GH_REPO" --body "$report"

[ ${#failed[@]} -eq 0 ]

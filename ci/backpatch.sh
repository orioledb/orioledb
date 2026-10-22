#!/usr/bin/env bash
#
# Backpatch a merged pull request.
#
# Every commit the pull request brought into its base branch that says
# "Backpatch: stable" is cherry-picked into the supported release branch,
# and a pull request is opened from the branch
# backpatch/pr-<number>.
#
# A squash commit takes the declarations of the pull request's commits, not
# its own message, which is the pull request description here.
#
# A commit that does not apply cleanly still gets its pull request, so the
# backpatch is not forgotten: the commit is made with the conflict markers in
# it and with [skip ci], the commits after it are left out, and the pull
# request is a draft labeled backpatch-conflict that says what is left to do.  The run then
# fails, and the results are commented on the original pull request.
#
# Run from a clone with full history.
# Environment: GH_TOKEN, GH_REPO (owner/name), and either PR_NUMBER, or
# PUSH_SHA: the head of a push to a development branch, to backpatch the pull
# request whose merge it is.
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

# Open pull request from head into base, or find the one already there: a
# create that failed on the client may still have been done on the server.
# Prints the URL.
open_pr() {
	local base=$1 head=$2 title=$3 body=$4 draft=$5 url i
	for i in 1 2 3 4; do
		url=$(gh pr list --repo "$GH_REPO" --head "$head" --base "$base" \
				--state open --json url --jq '.[0].url // empty' 2> /dev/null) || url=
		if [ -n "$url" ]; then
			echo "$url"
			return 0
		fi
		# shellcheck disable=SC2086
		if url=$(gh pr create --repo "$GH_REPO" --base "$base" --head "$head" \
					--title "$title" --body "$body" $draft); then
			echo "$url"
			return 0
		fi
		[ "$i" -lt 4 ] && sleep $((i * ${GH_RETRY_DELAY:-5}))
	done
	return 1
}

if [ -z "${PR_NUMBER:-}" ]; then
	# The pull request merged by the push.  The API may take a moment to
	# associate the merge commit with it.
	for _ in 1 2 3 4 5 6; do
		PR_NUMBER=$(gh_retry gh api "repos/$GH_REPO/commits/$PUSH_SHA/pulls" \
						--jq "[.[] | select(.merged_at != null and .merge_commit_sha == \"$PUSH_SHA\")][0].number // empty")
		[ -n "$PR_NUMBER" ] && break
		sleep "${PR_LOOKUP_DELAY:-10}"
	done
	if [ -z "$PR_NUMBER" ]; then
		echo "Push of ${PUSH_SHA:0:9} is not the merge of a pull request: nothing to backpatch."
		exit 0
	fi
	echo "Push of ${PUSH_SHA:0:9} merged pull request #$PR_NUMBER."
fi

pr_json=$(gh_retry gh api "repos/$GH_REPO/pulls/$PR_NUMBER")
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
pr_commits=$(gh_retry gh api --paginate "repos/$GH_REPO/pulls/$PR_NUMBER/commits" \
				--jq '.[] | select((.parents | length) == 1) | .commit.message | @base64')
pr_subjects=$(for m in $pr_commits; do base64 -d <<< "$m" | head -n 1; done)
n_pr_commits=$(grep -c . <<< "$pr_subjects" || true)
n_parents=$(($(git rev-list --parents -n 1 "$merge_sha" | wc -w) - 1))

squashed=false
if [ "$n_parents" -gt 1 ]; then
	landed=$(git rev-list --reverse --no-merges "$merge_sha^1..$merge_sha^2")
else
	candidates=$(git rev-list --reverse --first-parent -n "$n_pr_commits" "$merge_sha")
	candidate_subjects=$(for c in $candidates; do git log -1 --format=%s "$c"; done)
	if [ "$n_pr_commits" -gt 1 ] && [ "$candidate_subjects" = "$pr_subjects" ]; then
		landed=$candidates
	else
		landed=$merge_sha
		squashed=true
	fi
fi

# Whether each landed commit should be backpatched.
#
# A squash commit's message is whatever the merge made of it; here the
# pull request description; so its declarations are those of the pull
# request's commits, which the sanity check verified.  Since the sanity
# check rejects mixed declarations, all commits agree.
declare -A should_backpatch
any=false
if $squashed; then
	bp=
	for m in $pr_commits; do
		bp=$(base64 -d <<< "$m" | has_backpatch)
		[ -n "$bp" ] && break
	done
	if [ -n "$bp" ]; then
		should_backpatch[$merge_sha]=true
		any=true
	fi
else
	for c in $landed; do
		bp=$(git log -1 --format=%B "$c" | has_backpatch)
		if [ -n "$bp" ]; then
			should_backpatch[$c]=true
			any=true
		fi
	done
fi

if ! $any; then
	echo "No commit of #$PR_NUMBER says Backpatch: nothing to backpatch."
	exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

created=()
conflicted=()
errors=()

for release in $(stable_branch); do
	picks=()
	for c in $landed; do
		if [ "${should_backpatch[$c]:-}" = true ]; then
			picks+=("$c")
		fi
	done
	[ ${#picks[@]} -eq 0 ] && continue

	branch="backpatch/pr-$PR_NUMBER"
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
		# It is the last commit of the branch, so [skip ci] keeps CI off the
		# pull request until the conflicts are resolved: the markers would
		# only fail it.
		git -c core.editor=true commit -q --no-verify \
			-m "$(git log -1 --format=%B "$c")" \
			-m "(cherry picked from commit $c)" \
			-m "This commit has unresolved conflicts in: $(xargs <<< "$conflict_files")" \
			-m "[skip ci]" \
			-m "$NOTE"
		git cherry-pick --quit 2> /dev/null || true
		applied=$((applied + 1))
	done
	if [ "$applied" -eq 0 ]; then
		summary "$release already has every commit to backpatch."
		continue
	fi

	if ! gh_retry git push -q --force origin "$branch"; then
		errors+=("$release: could not push \`$branch\`")
		continue
	fi

	commits_line="Commits: $(for c in "${picks[@]}"; do printf '%s ' "${c:0:9}"; done)"
	if [ -n "$conflict" ]; then
		# The commits the conflict stopped: the developer must not lose them.
		if [ ${#left[@]} -gt 0 ]; then
			left_text="**Not applied yet**, because they come after the conflicting commit:"$'\n'
			for c in "${left[@]}"; do
				left_text+=$'\n'"- \`${c:0:9}\` $(git log -1 --format=%s "$c")"
			done
			left_text+=$'\n\n'"After resolving the conflict, apply them with:"$'\n\n'
			left_text+='```'$'\n'"git cherry-pick -x ${left[*]}"$'\n''```'
			left_short=$(for c in "${left[@]}"; do printf '%s ' "${c:0:9}"; done | xargs)
		else
			left_text="**Not applied yet:** none, the conflicting commit is the last one to backpatch to \`$release\`."
			left_short=none
		fi
		# shellcheck disable=SC2016
		body=$(printf '%s\n\n%s\n\n%s\n\n%s\n\n%s\n\n%s\n\n%s\n' \
			"Backpatch of $pr_url to \`$release\`." \
			"$commits_line" \
			"**${conflict:0:9} $(git log -1 --format=%s "$conflict") did not apply cleanly.** It is committed with the conflict markers in:" \
			"$(sed 's/^/- `/; s/$/`/' <<< "$conflict_files")" \
			"$left_text" \
			"Resolve the conflicts in branch \`$branch\`, and remove the \`[skip ci]\` line and the line about the conflicts from the commit message: CI does not run on this pull request until then.  Apply the commits not applied yet, if any, and mark the pull request ready for review." \
			"$NOTE")
		labels=backpatch,backpatch-conflict
		draft=--draft
	else
		body=$(printf '%s\n\n%s\n\n%s\n' \
			"Backpatch of $pr_url to \`$release\`." "$commits_line" "$NOTE")
		labels=backpatch
		draft=
	fi

	existing=$(gh_retry gh pr list --repo "$GH_REPO" --head "$branch" --base "$release" \
				--state open --json url --jq '.[0].url // empty') || existing=
	if [ -n "$existing" ]; then
		url=$existing
		gh_retry gh pr edit "$url" --repo "$GH_REPO" --body "$body" > /dev/null ||
			echo "::warning::could not update the description of $url"
		status="$url (updated)"
	elif url=$(open_pr "$release" "$branch" "[$release] $title" "$body" "$draft"); then
		status=$url
	else
		errors+=("$release: branch \`$branch\` is pushed, but its pull request could not be opened")
		continue
	fi
	gh_retry gh pr edit "$url" --repo "$GH_REPO" --add-label "$labels" > /dev/null ||
		echo "::warning::could not label $url"
	if [ -n "$draft" ] && [ -n "$existing" ]; then
		gh_retry gh pr ready "$url" --repo "$GH_REPO" --undo > /dev/null 2>&1 || true
	fi

	if [ -n "$conflict" ]; then
		conflicted+=("$release: $status, draft: ${conflict:0:9} $(git log -1 --format=%s "$conflict") conflicts in $(xargs <<< "$conflict_files"); not applied yet: $left_short")
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
for line in "${errors[@]}"; do
	report+=$'\n'"- **Error, $line**: open it by hand"
done
summary "$report"
gh_retry gh pr comment "$PR_NUMBER" --repo "$GH_REPO" --body "$report" ||
	echo "::error::could not comment on #$PR_NUMBER"

[ ${#conflicted[@]} -eq 0 ] && [ ${#errors[@]} -eq 0 ]

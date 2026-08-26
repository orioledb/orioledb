#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: launch-jepsen.sh [OPTIONS] [BRANCH] [DURATION_MINUTES]

Build, validate, push, and launch the Jepsen RR and RC Antithesis tests.

Arguments:
  BRANCH             Remote OrioleDB branch to test (default: main)
  DURATION_MINUTES   Antithesis duration in minutes (default: 480)

Options:
  --skip-service-build  Service images were already built and loaded by CI
  --resolved-ref SHA    Use a SHA already resolved after a fresh CI fetch
  -h, --help            Show this help
EOF
}

skip_service_build=false
resolved_ref=""
positionals=()
while (($# > 0)); do
	case "$1" in
	--skip-service-build)
		skip_service_build=true
		shift
		;;
	--resolved-ref)
		if (($# < 2)); then
			echo "Error: --resolved-ref requires a SHA" >&2
			exit 2
		fi
		resolved_ref="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	--*)
		echo "Error: unknown option: $1" >&2
		usage >&2
		exit 2
		;;
	*)
		positionals+=("$1")
		shift
		;;
	esac
done

if ((${#positionals[@]} > 2)); then
	usage >&2
	exit 2
fi

target_branch="${positionals[0]:-main}"
duration_minutes="${positionals[1]:-480}"

if ! git check-ref-format --branch "$target_branch" >/dev/null; then
	echo "Error: invalid branch name: $target_branch" >&2
	exit 2
fi
if [[ ! "$duration_minutes" =~ ^[1-9][0-9]*$ ]]; then
	echo "Error: duration must be a positive number of minutes: $duration_minutes" >&2
	exit 2
fi

for command in docker git jq make snouty; do
	if ! command -v "$command" >/dev/null; then
		echo "Error: required command is not installed: $command" >&2
		exit 1
	fi
done
if [[ -z "${ANTITHESIS_API_KEY:-}" ]]; then
	echo "Error: ANTITHESIS_API_KEY is not set" >&2
	exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

if [[ -n "$resolved_ref" ]]; then
	orioledb_ref="$(git rev-parse --verify "${resolved_ref}^{commit}")"
else
	git fetch --force --no-tags origin \
		"+refs/heads/${target_branch}:refs/remotes/origin/${target_branch}"
	orioledb_ref="$(
		git rev-parse --verify "refs/remotes/origin/${target_branch}^{commit}"
	)"
fi

short_ref="$(git rev-parse --short=12 "$orioledb_ref")"
branch_slug="${target_branch//\//-}"
branch_slug="$(printf '%s' "$branch_slug" | tr -c 'A-Za-z0-9_-' '-')"

# NB parallel arrays
modes=(RR RC)
configs=(
	"workload/jepsen workload/jepsen-RR"
	"workload/jepsen workload/jepsen-RC"
)
config_images=()
active_cfg=""

cleanup() {
	status=$?
	trap - EXIT INT TERM
	if [[ -n "$active_cfg" ]]; then
		make down ORIOLEDB_REF="$orioledb_ref" CFG="$active_cfg" \
			>/dev/null 2>&1 || true
	fi
	exit "$status"
}
trap cleanup EXIT INT TERM

printf 'Harness ref:  %s\n' "$(git rev-parse HEAD)"
printf 'Target branch: %s\n' "$target_branch"
printf 'OrioleDB ref:  %s\n' "$orioledb_ref"
printf 'Duration:      %sm\n' "$duration_minutes"

if [[ "$skip_service_build" == false ]]; then
	active_cfg="${configs[0]}"
	make build-services ORIOLEDB_REF="$orioledb_ref" CFG="$active_cfg"
fi

for index in "${!modes[@]}"; do
	mode="${modes[index]}"
	active_cfg="${configs[index]}"

	make build-config ORIOLEDB_REF="$orioledb_ref" CFG="$active_cfg"
	config_images[index]="$(
		make config-image ORIOLEDB_REF="$orioledb_ref" CFG="$active_cfg"
	)"

	echo "Validating Jepsen $mode configuration"
	snouty validate target/
done

# RR and RC use the same service images; push those images only once, then push
# the two small config images that contain their different runtime settings.
active_cfg="${configs[0]}"
make push-services ORIOLEDB_REF="$orioledb_ref" CFG="$active_cfg"
for image in "${config_images[@]}"; do
	docker push "$image"
done

for index in "${!modes[@]}"; do
	mode="${modes[index]}"
	source_name="${branch_slug}_jepsen-${mode}_${duration_minutes}m"
	description="branch=${target_branch},sha=${short_ref},workload=jepsen-${mode},time=${duration_minutes}m"

	echo "Launching Jepsen $mode: ${config_images[index]}"
	launch_output="$(
		snouty launch --json \
			--config-image "${config_images[index]}" \
			--test-name 'orioledb_jepsen' \
			--description "$description" \
			--source "$source_name" \
			--duration "${duration_minutes}m" \
			--recipients 'paul.bauer@supabase.io' \
			--webhook basic_test
	)"
	printf '%s\n' "$launch_output"

	run_id="$(
		jq -r '[.. | objects | .run_id? // empty][0] // empty' \
			<<<"$launch_output"
	)"
	if [[ -z "$run_id" ]]; then
		run_id="unavailable (see launch JSON above)"
	fi
	printf 'Jepsen %s run_id: %s\n' "$mode" "$run_id"

	if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
		printf -- "- Jepsen %s: branch \`%s\`, commit \`%s\`, duration \`%sm\`, run_id \`%s\`\n" \
			"$mode" "$target_branch" "$orioledb_ref" "$duration_minutes" "$run_id" \
			>>"$GITHUB_STEP_SUMMARY"
	fi
done

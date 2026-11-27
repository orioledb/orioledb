#!/usr/bin/env bash
# Run the whole harness locally against the published OrioleDB image.
#
#   ci/ext/docker-run.sh                 # everything in extensions.json
#   ci/ext/docker-run.sh --only hstore   # one suite
#   ci/ext/docker-run.sh --kind contrib  # contrib only
#
# Steps: build a derived image with python3/make/gcc (Dockerfile.ext), fetch the
# suites (fetch.sh), then for every group of suites that share start-time
# settings start a fresh server with exactly those settings and run run.py
# inside the container.  ci/ext/work is bind-mounted at /work so fetched
# sources, results and the report survive container restarts.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
base=${BASE_IMAGE:-orioledb/orioledb:latest-pg17}
image=${IMAGE:-orioledb-ext:pg17}
cname=${CONTAINER:-orioledb-ext-run}
pg_major=${PG_MAJOR:-17}
keep=0
rebuild=0
run_args=()
fetch_only=""
while [ $# -gt 0 ]; do
	case "$1" in
		--keep) keep=1; shift ;;
		--rebuild) rebuild=1; shift ;;
		--only) run_args+=(--only "$2"); fetch_only="$2"; shift 2 ;;
		--kind) run_args+=(--kind "$2"); shift 2 ;;
		--timeout) run_args+=(--timeout "$2"); shift 2 ;;
		*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

echo "== build $image (from $base)"
docker build -q -t "$image" --build-arg "BASE=$base" -f "$here/Dockerfile.ext" "$here" >/dev/null

echo "== fetch suites"
"$here/fetch.sh" --pg-major "$pg_major" ${fetch_only:+--only "$fetch_only"}

work="$here/work"
mkdir -p "$work/ext"
rm -rf "$work/results"
mkdir -p "$work/results"
for f in run.py report.py extensions.json getkey.sh load-orioledb.sql; do cp "$here/$f" "$work/ext/$f"; done
rm -rf "$work/ext/smoke" && cp -R "$here/smoke" "$work/ext/smoke"
chmod +x "$work/ext/getkey.sh"
cp "$here/../filter_regression_diff.py" "$work/ext/filter_regression_diff.py"

# Groups and their settings are computed inside the image, so Makefiles are
# evaluated with make against the real pgxs and fixture paths are container paths.
in_image() { docker run --rm -v "$work:/work" "$image" python3 /work/ext/run.py --src /work/src "$@"; }
group_ids=$(in_image --list-groups "${run_args[@]}" \
	| python3 -c 'import json,sys; print(" ".join(str(g["id"]) for g in json.load(sys.stdin)))')

start_server() {
	docker rm -f "$cname" >/dev/null 2>&1 || true
	docker run -d --name "$cname" -e POSTGRES_PASSWORD=orioledb -e POSTGRES_INITDB_ARGS="--encoding=UTF-8 --locale=C" \
		-v "$work:/work" "$image" postgres "$@" >/dev/null
	for i in $(seq 1 60); do
		docker exec "$cname" pg_isready -U postgres >/dev/null 2>&1 && return 0
		sleep 1
	done
	docker logs "$cname" | tail -20
	return 1
}

# Build and install every external once, then commit that container as the
# image the group servers start from: the .so and .sql files have to be in the
# server's filesystem, and a suite's server may need to preload them.  Build
# logs and done-markers live in work/build; the committed image is reused while
# it is newer than the toolchain image (pass --rebuild to force, e.g. after
# fetch.sh moved a `latest` tag).
built_image="$image-built"
newer() { [ "$(docker inspect -f '{{.Created}}' "$1" 2>/dev/null)" \> "$(docker inspect -f '{{.Created}}' "$2")" ]; }
if [ $rebuild = 1 ] || ! newer "$built_image" "$image"; then
	echo "== build external extensions"
	rm -rf "$work/build"
	docker rm -f "$cname-build" >/dev/null 2>&1 || true
	docker run --name "$cname-build" -v "$work:/work" "$image" \
		python3 /work/ext/run.py --src /work/src --build-dir /work/build --build-only "${run_args[@]}" \
		|| echo "   (some builds failed; their suites will report build-failed)"
	docker commit "$cname-build" "$built_image" >/dev/null
	docker rm -f "$cname-build" >/dev/null
else
	echo "== reusing built image $built_image (--rebuild to force)"
fi
image="$built_image"

for gid in $group_ids; do
	echo "== group $gid"
	pg_args=()
	while IFS= read -r s; do
		[ -n "$s" ] || continue
		echo "   $s"
		pg_args+=(-c "$s")
	done < <(in_image --config-only --group "$gid" "${run_args[@]}" | grep -v '^#')
	if ! start_server "${pg_args[@]}"; then
		echo "   server for group $gid did not start; see server-group$gid.log"
		docker logs "$cname" > "$work/results/server-group$gid.log" 2>&1 || true
		continue
	fi
	docker exec -e PGUSER=postgres "$cname" python3 /work/ext/run.py --src /work/src --out /work/results \
		--build-dir /work/build --group "$gid" "${run_args[@]}" || true
	# Keep this server's log: a crash inside a suite is only visible here.
	docker logs "$cname" > "$work/results/server-group$gid.log" 2>&1 || true
done

echo "== report"
in_image_report() { docker run --rm -v "$work:/work" "$image" python3 /work/ext/report.py --results /work/results \
	--filter-script /work/ext/filter_regression_diff.py --manifest /work/ext/extensions.json; }
in_image_report
echo "== results: $work/results/report.md"
[ $keep = 1 ] || docker rm -f "$cname" >/dev/null

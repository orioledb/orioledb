#!/usr/bin/env bash
# Fetch the test suites listed in extensions.json without modifying them.
#
#   contrib  -> contrib/<module>/ from the patched PostgreSQL tree at the tag
#               .pgtags pins for the requested major (same codeload tarball
#               technique ci/post_build_prerequisites.sh uses for pgvector)
#   external -> shallow clone of the upstream repo at the pinned tag
#
# Usage: fetch.sh [--pg-major 17] [--pg-tag patches17_22] [--only a,b] [--dest DIR]
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$here/../.." && pwd)
manifest="$here/extensions.json"
dest="$here/work/src"
pg_major=17
pg_tag=""
only=""

while [ $# -gt 0 ]; do
	case "$1" in
		--pg-major) pg_major=$2; shift 2 ;;
		--pg-tag) pg_tag=$2; shift 2 ;;
		--only) only=$2; shift 2 ;;
		--dest) dest=$2; shift 2 ;;
		--manifest) manifest=$2; shift 2 ;;
		*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

if [ -z "$pg_tag" ]; then
	pg_tag=$(awk -F': *' -v m="$pg_major" '$1 == m {print $2}' "$repo_root/.pgtags")
	[ -n "$pg_tag" ] || { echo ".pgtags has no entry for PostgreSQL $pg_major" >&2; exit 1; }
fi
pg_repo=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pg_source"]["repo"])' "$manifest")

mkdir -p "$dest/contrib" "$dest/external" "$here/work/cache"
echo "pg source: $pg_repo @ $pg_tag"
echo "$pg_tag" > "$dest/PG_TAG"

want() {
	[ -z "$only" ] && return 0
	case ",$only," in *",$1,"*) return 0 ;; esac
	return 1
}

# --- contrib ---------------------------------------------------------------
tarball="$here/work/cache/pg-$pg_tag.tar.gz"
if [ ! -s "$tarball" ]; then
	# codeload accepts both tag names and commit hashes after tar.gz/
	url="https://codeload.github.com/$pg_repo/tar.gz/$pg_tag"
	case "$pg_tag" in patches*) url="https://codeload.github.com/$pg_repo/tar.gz/refs/tags/$pg_tag" ;; esac
	echo "downloading $url"
	curl -fsSL -o "$tarball.tmp" "$url" && mv "$tarball.tmp" "$tarball"
fi
top=$(tar -tzf "$tarball" | head -1 | cut -d/ -f1)

contrib_mods=$(python3 -c '
import json,sys
for e in json.load(open(sys.argv[1]))["extensions"]:
    if e["kind"] == "contrib": print(e["name"])' "$manifest")
for m in $contrib_mods; do
	want "$m" || continue
	if [ -d "$dest/contrib/$m" ]; then continue; fi
	tar -xzf "$tarball" -C "$dest/contrib" --strip-components=2 "$top/contrib/$m" 2>/dev/null \
		|| { echo "warning: contrib/$m not in $pg_tag" >&2; continue; }
	echo "contrib/$m"
done

# --- external --------------------------------------------------------------
python3 -c '
import json,sys
for e in json.load(open(sys.argv[1]))["extensions"]:
    if e["kind"] == "external":
        print(e["name"], e["repo"], e.get("tag", "latest"), e.get("tag_match", "-"))' "$manifest" |
while read -r name repo tag tag_match; do
	want "$name" || continue
	if [ "$tag" = latest ]; then
		# Default: newest release tag, i.e. the highest plain version tag (v1.2.3 or
		# 1.2.3; pre-releases and unrelated tags such as debian/*, wasm_*_v* drop
		# out).  tag_match replaces that filter when upstream names tags
		# differently (wal2json_2_6) or per PostgreSQL major (pgaudit 17.x).
		pattern='^v?[0-9]+(\.[0-9]+)+$'
		[ "$tag_match" = "-" ] || pattern=$(printf '%s' "$tag_match" | sed "s/\${PG_MAJOR}/$pg_major/g")
		tag=$(git ls-remote --tags --sort=-v:refname "$repo" | sed -n 's,.*refs/tags/,,p' | grep -v '\^{}' \
			| grep -E "$pattern" | head -1)
		[ -n "$tag" ] || { echo "warning: could not resolve latest tag for $name" >&2; continue; }
		if [ -d "$dest/external/$name" ] && [ "$(cat "$dest/external/$name/.resolved_tag" 2>/dev/null)" != "$tag" ]; then
			echo "external/$name: latest moved to $tag, refetching"
			rm -rf "$dest/external/$name"
		fi
		resolved_latest=1
	else
		resolved_latest=0
	fi
	if [ -d "$dest/external/$name" ]; then continue; fi
	echo "external/$name <- $repo @ $tag"
	if command -v git >/dev/null; then
		case "$tag" in
			# a 40-hex pin (upstream without release tags): fetch that commit alone
			[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f])
				git init --quiet "$dest/external/$name"
				git -C "$dest/external/$name" fetch --quiet --depth 1 "$repo" "$tag"
				git -C "$dest/external/$name" checkout --quiet FETCH_HEAD ;;
			*)
				git clone --quiet --depth 1 --branch "$tag" "$repo" "$dest/external/$name" ;;
		esac
		rm -rf "$dest/external/$name/.git"
	else
		curl -fsSL "${repo%.git}/archive/refs/tags/$tag.tar.gz" | tar -xz -C "$dest/external" \
			&& mv "$dest/external/$(basename "$repo" .git)-"* "$dest/external/$name"
	fi
	[ $resolved_latest = 1 ] && echo "$tag" > "$dest/external/$name/.resolved_tag"
done

echo "done: $dest"

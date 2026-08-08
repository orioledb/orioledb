#!/bin/bash
set -eo pipefail

dir="$(dirname "$(readlink -f "$BASH_SOURCE")")"

# Get the image from the first argument
image="$1"

serverImage="librarytest/orioledb-check:$(echo "$image" | sed 's![:/]!-!g')"

docker build --progress=plain --network=host \
   --build-arg BASE="$image" \
   -f "$dir/Dockerfile.orioledb-check" \
   -t "$serverImage" \
   .  >&2

docker images $serverImage >&2

echo "orioledb-check completed." >&2

#!/usr/bin/env bash
#shellcheck disable=SC2034,SC2154
set -euo pipefail  # Fail on errors and undefined variables

# if running in the docker-nightly.yml workflow,
#   use the $DOCKER_REGISTRY tag for fork friendlyness
#   ( running all test in forked repo  )
# if not set, use localhost:5000/orioledb as a default
readonly DOCKER_REGISTRY="${DOCKER_REGISTRY:-localhost:5000/orioledb}"

# Base image names
readonly BASE_ALIASES=(
    "orioledb"
    "orioledb-nightly"
    "orioletest"
    "orioledbtest"
	"orioledb-amd64"
    "orioletest-amd64"
	"orioledbtest-amd64"
	"orioledb-nightly-amd64"
)

# Registry prefixes
readonly REGISTRIES=(
    ""  # no prefix
    "$DOCKER_REGISTRY/"
    "orioledb/"
    "docker.io/orioledb/"
    "ghcr.io/orioledb/"
)

# Main configuration loop
for base_alias in "${BASE_ALIASES[@]}"; do
    for registry in "${REGISTRIES[@]}"; do
		alias="${registry}${base_alias}"
		testAlias["$alias"]='postgres'
		imageTests["$alias"]='
			orioledb-basics
		'
		echo "- Configured: testAlias[\"$alias\"]='postgres'  imageTests[\"$alias\"]='orioledb-basics'"
    done
done

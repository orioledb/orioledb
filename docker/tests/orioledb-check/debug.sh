#!/bin/bash
set -eo pipefail



# run from the repo root.
# ./docker/tests/orioledb-check/debug.sh <orioletest-image>
#
# example:
#  bash -x ./docker/tests/orioledb-check/debug.sh orioletest:17-clang-fedora-43
#
#  in the docker shell - run the steps (find the Dockerfile.orioledb-check )
#  1.:  bash -x /local_workspace/ci/check_docker.sh --running init
#  2.:  bash -x /local_workspace/ci/check_docker.sh --running test regresscheck

echo "start debugging ..."

# Get the image from the first argument
image="$1"

# start Docker container with the specified image
docker run --network=host  -it \
  --volume "$(pwd)":/local_workspace \
  "$image" bash


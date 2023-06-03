#!/usr/bin/env bash
set -Eeo pipefail

# Testing all possible docker builds on a local machine
# run from project root: ./ci/local_docker_matrix.sh
# and check the logs in ./log_docker_build/*.log

# set and prepare $logpath for build logs
logpath=./log_docker_build
mkdir -p $logpath
rm -f    ${logpath}/*.log

# simple local build matxix - for build all images
for alpine in edge 3.18 3.17 3.16 3.15 3.14 3.13 ; do
  # Refresh alpine images!
  docker pull alpine:$alpine
  for pg_major in 16 15 14 13; do
    for compiler in clang gcc; do

      # LLVM 16 hack for setting the correct DOCKER_PG_LLVM_DEPS
      case "$alpine" in 3.18 | edge )  pg_llvm_deps='llvm15-dev clang15' ;; \
                                  * )  pg_llvm_deps='llvm-dev   clang'   ;; \
      esac ; \

      docker_tag="${pg_major}-${compiler}-alpine${alpine}"
      echo "------------ $docker_tag ------------------"
      echo "params: ALPINE_VERSION=$alpine"
      echo "params: BUILD_CC_COMPILER=$compiler"
      echo "params: PG_MAJOR=$pg_major"
      echo "params: DOCKER_PG_LLVM_DEPS=$pg_llvm_deps"
      echo "----------------------------------------"

      rm -f ${logpath}/${docker_tag}.log
      time docker build --network=host --progress=plain \
          --build-arg ALPINE_VERSION="$alpine" \
          --build-arg BUILD_CC_COMPILER="$compiler" \
          --build-arg PG_MAJOR="$pg_major" \
          --build-arg DOCKER_PG_LLVM_DEPS="$pg_llvm_deps" \
          -t orioletest:${docker_tag} . 2>&1 | tee ${logpath}/${docker_tag}.log

    done
  done
done

docker images orioletest:* | sort

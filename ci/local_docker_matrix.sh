#!/usr/bin/env bash
set -Eeo pipefail

# Testing all possible docker builds on a local machine
# run from project root: ./ci/local_docker_matrix.sh
# and check the logs in ./log_docker_build/*.*.log

# Full matrix of test builds 2x2x13 = 52 builds
pg_major_list=( 16 17)
compiler_list=( clang gcc )
base_list=(
   # alpine versions
   alpine:3.21
   alpine:3.20
   alpine:3.19
   alpine:3.18
   alpine:3.17
   alpine:3.16
   alpine:3.15
   alpine:3.14

   # debian versions
   debian:bullseye
   debian:bookworm

   # ubuntu versions
   ubuntu:24.10
   ubuntu:24.04
   ubuntu:22.04
   ubuntu:20.04

   # developer versions
   alpine:edge
   debian:testing
   ubuntu:devel
  )

# Function test targets
# if OS(python3.10+)
#    then set test targets to "regresscheck isolationcheck testgrescheck"
#    else                     "regresscheck isolationcheck"
#
get_test_targets() {
    local base="$1"
    local base_test_targets="installcheck"

    # Define minimal test targets for Python < 3.10
    local minimal_versions=(
        "alpine:3.15"     # python3.9
        "alpine:3.14"     # python3.9
        "debian:bullseye" # python3.9
        "ubuntu:20.04"    # python3.8
    )

    # Check if current base is in minimal versions list
    for ver in "${minimal_versions[@]}"; do
        if [ "$base" = "$ver" ]; then
            echo "regresscheck isolationcheck"
            return
        fi
    done

    # Default to full test targets for other versions
    echo "regresscheck isolationcheck testgrescheck"
}

# set and prepare $logpath for build logs
mkdir -p ./log_docker_build
logpath=./log_docker_build/"$(date +%Y-%m-%d-%H%M%S)-pid-$$"
mkdir -p $logpath
rm -f    ${logpath}/*.log


# Using official postgres docker test code
# from https://github.com/docker-library/postgres/blob/master/test
OFFIMG_LOCAL_CLONE=./log_docker_build/official-images
OFFIMG_REPO_URL=https://github.com/docker-library/official-images.git
# Check if the directory exists and contains a git repository
mkdir -p "$OFFIMG_LOCAL_CLONE"
if [ -d "$OFFIMG_LOCAL_CLONE/.git" ]; then
    echo "::Updating official-images : $OFFIMG_LOCAL_CLONE"
    pushd "$OFFIMG_LOCAL_CLONE" && git pull origin master && popd
else
    echo "::Cloning official-images into $OFFIMG_LOCAL_CLONE"
    git clone "$OFFIMG_REPO_URL" "$OFFIMG_LOCAL_CLONE"
fi

for pg_major in "${pg_major_list[@]}" ; do
  for compiler in "${compiler_list[@]}" ; do
    for base in "${base_list[@]}" ; do

      base_os="${base%%:*}"
      base_tag="${base##*:}"
      base_os_upper="${base_os^^}"

      # Determine the Dockerfile based on base OS
      if [ "$base_os" = "alpine" ]; then
        dockerfile="docker/Dockerfile"
      elif [ "$base_os" = "ubuntu" ]; then
        dockerfile="docker/Dockerfile.ubuntu"
      fi

      # Get appropriate test targets for this base image
      test_targets=$(get_test_targets "$base")

      docker_tag="${pg_major}-${compiler}-${base_os}-${base_tag}"
      echo "------------ $docker_tag : $test_targets ------------------"

      rm -f ${logpath}/"${docker_tag}".*.log

      time docker build --pull --network=host --progress=plain \
          -f $dockerfile \
          --build-arg "${base_os_upper}_VERSION=$base_tag" \
          --build-arg BUILD_CC_COMPILER="$compiler" \
          --build-arg PG_MAJOR="$pg_major" \
          -t orioletest:"${docker_tag}" . 2>&1 | tee ${logpath}/"${docker_tag}".build.log

      # Run docker test : oriole + postgres official test scripts
      "${OFFIMG_LOCAL_CLONE}/test/run.sh" \
          -c "${OFFIMG_LOCAL_CLONE}/test/config.sh" \
          -c "docker/orioledb-config.sh" \
          "orioletest:${docker_tag}" 2>&1 | tee ${logpath}/"${docker_tag}".test.log

      # run the oriole test suite
      time docker run --rm \
          --volume $(pwd):/local_workspace \
          "orioletest:${docker_tag}" \
          bash -c "bash +x /local_workspace/ci/check_docker.sh --running all \"${test_targets}\"" 2>&1 \
           | tee ${logpath}/"${docker_tag}".check.log

    done
  done
done

docker images orioletest:* | sort

# You can check the build logs with:
#    grep -i  -C 1 warning: ./log_docker_build/*/*.build.log

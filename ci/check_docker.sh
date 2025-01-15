#!/bin/bash
set -Eeo pipefail

show_help() {
    cat << EOF

Usage: ./ci/check_docker.sh [TEST_TARGETS]

TEST_TARGETS (optional): Space-separated list of test targets to run, enclosed in quotes.
  Valid targets include:
    - installcheck
    - regresscheck
    - isolationcheck
    - testgrescheck
    - testgrescheck_part_1
    - testgrescheck_part_2

Examples: ( expects to be run inside a Docker container! )
  /github/workspace/orioledb/ci/check_docker.sh 'installcheck'
  /github/workspace/orioledb/ci/check_docker.sh 'regresscheck isolationcheck'

Usage/Note:
  '-j\$(nproc)' is hard coded for parallel builds!

  # create docker test image
  time ./ci/docker_matrix.sh --base alpine:edge --pg-major 17 --compiler clang
  # run the tests
  time docker run -it --rm --volume $(pwd):/github/workspace/orioledb orioletest:17-clang-alpine-edge-debug-false bash -c "bash +x /github/workspace/orioledb/ci/check_docker.sh 'installcheck' "

  # or start a docker test environments.
  time docker run -it --rm --volume $(pwd):/github/workspace/orioledb orioletest:17-clang-alpine-edge-debug-false bash
  and run inside the docker: "bash +x /github/workspace/orioledb/ci/check_docker.sh 'installcheck' "

EOF
    exit 1
}


# Show help if -h, --help, no parameters, or more than 1 parameter is passed
if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]] || [[ $# -eq 0 ]] || [[ $# -gt 1 ]] ; then
    show_help
fi


if [ ! -f /.dockerenv ] || [ -z "$PG_MAJOR" ] || [ -z "$DOCKER_PG_LLVM_DEPS" ]; then
    echo " --------------------------------------------"
    echo "This code should run in a Docker environment."
    echo "---------------------------------------------"
    exit 1
else
    echo "Running in Docker environment."
fi

# expected docker volume mounts:
cd /github/workspace/orioledb

# install some stuff for testing
cat /etc/os-release
if grep -Eq '^ID=(ubuntu|debian)' /etc/os-release; then
    echo "Running on Ubuntu/Debian - install some stuff for testing"
    export DEBIAN_FRONTEND=noninteractive
    apt update
    apt-get -y install --no-install-recommends \
        python3 python3-pip python3-dev python3-virtualenv \
        build-essential ${DOCKER_PG_LLVM_DEPS} make sudo wget
elif grep -q '^ID=alpine' /etc/os-release; then
    echo "Running on Alpine - install some stuff for testing"
    apk add --no-cache \
        python3 python3-dev py3-pip py3-virtualenv \
        linux-headers libffi-dev \
        build-base ${DOCKER_PG_LLVM_DEPS} libc-dev make sudo wget
else
    echo "Unsupported OS. Exiting. (please add support!)"
    exit 1
fi

cd /tmp
virtualenv /tmp/env
. /tmp/env/bin/activate
pip3 install --no-cache-dir --upgrade pip

# create a new temp dir for testing - and copy all necessary files
# the "postgres" user will run the tests,
# and it needs to have access to the files
rm -Rf /temp_orioledb
mkdir -p /temp_orioledb
cd /github/workspace/orioledb
cp -r ./test ./src ./sql ./include ./ci /temp_orioledb/
find . -maxdepth 1 -type f -exec cp {} /temp_orioledb/ \;
chown -R postgres:postgres /temp_orioledb
chown -R postgres:postgres /tmp/env

# install test dependencies, python packages and wal2json
cd /temp_orioledb
export GITHUB_JOB="dockertest"
./ci/post_build_prerequisites.sh pgxs

# Construct make command with provided targets or empty for setup-only
MAKE_TARGETS=""
if [ ! -z "$1" ]; then
    MAKE_TARGETS="$1"
fi

# run the tests
cd /temp_orioledb
echo "Running with targets: ${MAKE_TARGETS:-setup only}"
time su postgres -c "\
    set -x; \
    cd /temp_orioledb && \
    source /tmp/env/bin/activate && \
    make \
        NO_INSTALL=1 \
        USE_PGXS=1 \
        ${MAKE_TARGETS} \
        -j$(nproc) \
        LANG=C \
        PGUSER=postgres"

echo "--- end of the tests ---"

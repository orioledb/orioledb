#!/bin/bash
set -Eeo pipefail

show_help() {
    cat << EOF

OrioleDB Docker Test Runner
==========================

This script is designed to run OrioleDB test targets defined in the Makefile
within a specialized OrioleDB Docker environment.
Supports multiple Linux distributions : (Alpine, Debian, Ubuntu)
and provides a consistent testing platform for the OrioleDB PostgreSQL extension.

Usage: ./ci/check_docker.sh [OPTIONS] [TEST_TARGETS]

OPTIONS:
  --running MODE    Execution mode (required)
        Valid modes:
            init    - Run only initialization
            test    - Run only tests
            all     - Run both initialization and tests (default)

TEST_TARGETS (optional): Space-separated list of test targets to run, enclosed in quotes.
  Valid targets include:
    - installcheck
    - regresscheck
    - isolationcheck
    - testgrescheck
    - testgrescheck_part_1
    - testgrescheck_part_2

Examples: ( expects to be run inside a Docker container! - mounted in /local_workspace/. )
  /local_workspace/ci/check_docker.sh --running init
  /local_workspace/ci/check_docker.sh --running test 'installcheck'
  /local_workspace/ci/check_docker.sh --running all  'regresscheck isolationcheck'

EOF
    exit 1
}


# Parse script arguments
RUNNING_MODE="all"
while [[ $# -gt 0 ]]; do
    case $1 in
        --running)
            RUNNING_MODE="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            TEST_TARGETS="$1"
            shift
            ;;
    esac
done

# Validate the execution mode
if [[ ! "$RUNNING_MODE" =~ ^(init|test|all)$ ]]; then
    echo "Error: Invalid running mode '$RUNNING_MODE'"
    show_help
fi

# Check if running inside a Docker container
if [ ! -f /.dockerenv ] || [ -z "$PG_MAJOR" ] || [ -z "$DOCKER_PG_LLVM_DEPS" ]; then
    echo " --------------------------------------------"
    echo "This code should run in a Docker environment."
    echo "---------------------------------------------"
    exit 1
else
    echo "Running in Docker environment."
fi

# Define workspace paths
export LOCAL_WORKSPACE="/local_workspace"
export USE_PGXS=1
export GITHUB_JOB="dockertest"
export GITHUB_WORKSPACE="/github_workspace"

# Run initialization steps if mode is 'init' or 'all'
if [[ "$RUNNING_MODE" == "init" || "$RUNNING_MODE" == "all" ]]; then
    # Create necessary directories for testing
    mkdir -p $GITHUB_WORKSPACE
    mkdir -p $GITHUB_WORKSPACE/pgsql/bin

    # Switch to the local workspace where the repository is mounted
    cd ${LOCAL_WORKSPACE}

    # Display OS details and install required packages
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
            python3 python3-dev py3-pip  \
            linux-headers libffi-dev \
            build-base ${DOCKER_PG_LLVM_DEPS} libc-dev make sudo wget
    else
        echo "Unsupported OS. Exiting. (please add support!)"
        exit 1
    fi

    # Prepare test environment
    cd ${GITHUB_WORKSPACE}
    # Create a temporary directory for test execution
    rm -Rf ${GITHUB_WORKSPACE}/temp_orioledb
    mkdir -p ${GITHUB_WORKSPACE}/temp_orioledb

    # Copy necessary files to the test workspace
    cd ${LOCAL_WORKSPACE}
    cp -r ./test ./src ./sql ./include ./ci ${GITHUB_WORKSPACE}/temp_orioledb/
    find . -maxdepth 1 -type f -exec cp {}  ${GITHUB_WORKSPACE}/temp_orioledb/ \;

    # Install additional test dependencies
    cd ${GITHUB_WORKSPACE}/temp_orioledb
    ./ci/post_build_prerequisites.sh
    # Grant permissions to the 'postgres' user
    chown -R postgres:postgres ${GITHUB_WORKSPACE}
fi



# Run tests if mode is 'test' or 'all'
if [[ "$RUNNING_MODE" == "test" || "$RUNNING_MODE" == "all" ]]; then
    # Determine the test targets to run
    MAKE_TARGETS=""
    if [ ! -z "$TEST_TARGETS" ]; then
        MAKE_TARGETS="$TEST_TARGETS"
    fi

    # Check if the test environment exists
    if [ ! -f "$GITHUB_WORKSPACE/python3-venv/bin/activate" ]; then
        echo "Error: test environment not exists; stop"
        exit 1
    fi

    # Activate the Python virtual environment
    cd ${GITHUB_WORKSPACE}/temp_orioledb
    source $GITHUB_WORKSPACE/python3-venv/bin/activate
    echo "Running with targets: ${MAKE_TARGETS:-setup only}"

    # Execute the tests as the 'postgres' user
    time su postgres -c "\
        set -x; \
        make \
            SKIP_INSTALL=1 \
            USE_PGXS=1 \
            ${MAKE_TARGETS} \
            -j$(nproc) \
            LANG=C \
            PGUSER=postgres"

    echo "--- end of the tests ---"
fi
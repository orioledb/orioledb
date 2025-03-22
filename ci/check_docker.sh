#!/bin/bash
set -Eeo pipefail

show_help() {
    cat << EOF

OrioleDB Docker Test Runner
==========================

This script runs OrioleDB test targets (from the Makefile)
inside a specialized Docker container.
It supports multiple Linux distributions (Alpine, Debian, Ubuntu)
and ensures a consistent testing environment for the OrioleDB PostgreSQL extension.

Usage:
  ./ci/check_docker.sh [OPTIONS] [TEST_TARGETS]

Options:
  --running MODE
    The execution mode (required). Possible values:
      init  - Perform only environment initialization.
      test  - Run tests only.
      all   - Perform both initialization and tests (default).

Test Targets (optional):
  A space-separated list of test targets. Examples:
    installcheck
    regresscheck
    isolationcheck
    testgrescheck
    testgrescheck_part_1
    testgrescheck_part_2

  If no targets are specified, the script automatically chooses:
    - "installcheck" if Python 3.10 or higher is available,
    - "regresscheck isolationcheck" otherwise.

Examples (run inside a Docker container, with /local_workspace/ mounted):
  /local_workspace/ci/check_docker.sh --running init
  /local_workspace/ci/check_docker.sh --running test 'installcheck'
  /local_workspace/ci/check_docker.sh --running all  'regresscheck isolationcheck'

EOF
    exit 1
}


# Parse script arguments
RUNNING_MODE="all"
TEST_TARGETS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
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
    echo "Error: Invalid running mode '$RUNNING_MODE'."
    show_help
fi

# Ensure we are inside a Docker container with the required environment variables
if [ ! -f /.dockerenv ] || [ -z "$PG_MAJOR" ] || [ -z "$DOCKER_PG_LLVM_DEPS" ]; then
    echo "------------------------------------------------"
    echo "Error: This script must be run inside Docker with"
    echo "       the required environment variables set."
    echo "------------------------------------------------"
    exit 1
else
    echo "Running in Docker environment."
fi

# Define workspace paths
export LOCAL_WORKSPACE="/local_workspace"
export TEST_WORKSPACE="/test_workspace"
export GITHUB_JOB="check-docker"

# Run initialization steps if mode is 'init' or 'all'
if [[ "$RUNNING_MODE" == "init" || "$RUNNING_MODE" == "all" ]]; then
    # Create necessary directories for testing
    mkdir -p $TEST_WORKSPACE

    # Switch to the local workspace (where the repository is mounted)
    cd ${LOCAL_WORKSPACE}

    # Display OS details and install required packages
    cat /etc/os-release
    if grep -Eq '^ID=(ubuntu|debian)' /etc/os-release; then
        echo "Detected Ubuntu/Debian. Installing test dependencies..."
        export DEBIAN_FRONTEND=noninteractive
        apt update
        apt-get -y install --no-install-recommends \
            make \
            python3-venv
        python3 -m venv "$TEST_WORKSPACE/python3-venv"
    elif grep -q '^ID=alpine' /etc/os-release; then
        echo "Detected Alpine. Installing test dependencies..."
        apk add --no-cache \
            make \
            py3-cffi \
            py3-psutil \
            py3-virtualenv
        python3 -m venv --system-site-packages "$TEST_WORKSPACE/python3-venv"
    else
        echo "Unsupported OS. Exiting. (Please extend support if needed!)"
        exit 1
    fi

    # Prepare test environment for the  'postgres' user
    cd ${TEST_WORKSPACE}
    rm -Rf ${TEST_WORKSPACE}/temp_orioledb
    mkdir -p ${TEST_WORKSPACE}/temp_orioledb

    # Copy orioledb test files to the test workspace
    cd ${LOCAL_WORKSPACE}
    cp -r ./test ./src ./sql ./include ./ci ${TEST_WORKSPACE}/temp_orioledb/
    find . -maxdepth 1 -type f -exec cp {}  ${TEST_WORKSPACE}/temp_orioledb/ \;

    # activate a Python virtual environment for additional test dependencies
    # (This mimics the 'ci/post_build_prerequisites.sh' script.)
    # shellcheck disable=SC1091
    source "$TEST_WORKSPACE/python3-venv/bin/activate"
    pip3 install --upgrade psycopg2-binary six testgres moto[s3] flask flask_cors boto3 pyOpenSSL
    pip3 freeze

    # Grant ownership to the 'postgres' user
    chown -R postgres:postgres "${TEST_WORKSPACE}"
fi



# Run tests if mode is 'test' or 'all'
if [[ "$RUNNING_MODE" == "test" || "$RUNNING_MODE" == "all" ]]; then
    # Determine which Makefile targets to run
    MAKE_TARGETS=""
    if [ -n "$TEST_TARGETS" ]; then
        MAKE_TARGETS="$TEST_TARGETS"
    else
        # Default targets depend on Python version
        if python3 -c "import sys; sys.exit(0) if sys.version_info >= (3, 10) else sys.exit(1)"; then
            MAKE_TARGETS="installcheck"
        else
            MAKE_TARGETS="regresscheck isolationcheck"
        fi
    fi

    # Check if the test environment is properly set up
    if [ ! -f "$TEST_WORKSPACE/python3-venv/bin/activate" ]; then
        echo "Error: Test environment not found. Please run with '--running init' first."
        exit 1
    fi

    echo "Running with targets: ${MAKE_TARGETS}"

    # Execute tests as the 'postgres' user - without installing the extension
    trap 'echo "--- check_docker: FAILED : ${MAKE_TARGETS}"' ERR
    time su postgres -c "\
        set -x \
        && cd ${TEST_WORKSPACE}/temp_orioledb \
        && source $TEST_WORKSPACE/python3-venv/bin/activate \
        && make \
            SKIP_INSTALL=1 \
            USE_PGXS=1 \
            ${MAKE_TARGETS} \
            -j$(nproc) \
            LANG=C \
            PGUSER=postgres \
        && echo \'--- check_docker: OK : ${MAKE_TARGETS}\' \
    "
fi
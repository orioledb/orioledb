#!/bin/bash
set -Eeo pipefail

# Generate test status markers for tracking test execution
generate_test_marker() {
    local test_name="$1"
    local marker_type="$2"  # "start" or "success"

    # Convert test name to marker format (each char separated by superscript minus)
    local marker_suffix=""
    for (( i=0; i<${#test_name}; i++ )); do
        local char="${test_name:$i:1}"
        marker_suffix="${marker_suffix}${char}⁻"
    done

    # Choose marker symbol based on type
    case "$marker_type" in
        "start")
            echo "▶${marker_suffix}"
            ;;
        "success")
            echo "✓${marker_suffix}"
            ;;
        *)
            echo "ERROR: Invalid marker type: $marker_type" >&2
            return 1
            ;;
    esac
}

# Print test status marker to stdout (will be captured in test logs)
print_test_marker() {
    local test_name="$1"
    local marker_type="$2"
    local marker=$(generate_test_marker "$test_name" "$marker_type")
    echo "$marker"
}

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
# [ ! -f /.dockerenv ] ||
if [ -z "$PG_MAJOR" ] || [ -z "$DOCKER_PG_LLVM_DEPS" ]; then
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
    bash --version
    if grep -Eq '^ID=(ubuntu|debian)' /etc/os-release; then
        echo "Detected Ubuntu/Debian. Installing test dependencies..."
        export DEBIAN_FRONTEND=noninteractive
        apt update
        apt-get -y install --no-install-recommends \
            make \
            python3-venv
        python3 -m venv --system-site-packages "$TEST_WORKSPACE/python3-venv"
    elif grep -q '^ID=alpine' /etc/os-release; then
        echo "Detected Alpine. Installing test dependencies..."
        apk add --no-cache \
            diffutils \
            make \
            python3 \
            py3-cffi \
            py3-psutil \
            py3-virtualenv \
            py3-pip
        python3 -m venv --system-site-packages "$TEST_WORKSPACE/python3-venv"
    elif grep -Eq '^ID=(arch|cachyos)' /etc/os-release; then
        echo "Detected Arch/CachyOS. Installing test dependencies..."
        pacman -Sy --noconfirm --noprogressbar || true
        pacman -S --noconfirm --needed --noprogressbar \
            make \
            python \
            python-pip \
            diffutils \
            which || true
        # On Arch, 'python' provides venv; use 'python' explicitly.
        python -m venv --system-site-packages "$TEST_WORKSPACE/python3-venv"
    elif grep -q '^ID=fedora' /etc/os-release; then
        echo "Detected Fedora. Installing test dependencies..."
        dnf -y update || true
        # TODO: not yet perfect.
        dnf -y install \
            make \
            diffutils \
            python3 \
            python3-pip \
            python3-devel \
            gcc \
            libffi-devel \
            bison \
            flex \
            pkgconf-pkg-config \
            readline-devel \
            gdb \
            perl-IPC-Run \
            libicu-devel \
            libcurl-devel \
            openssl-devel \
        || true
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
    # Upgrade core Python packaging tools. On older distros (e.g., Alpine 3.12)
    # the system 'packaging' is too old for latest setuptools, so upgrade it too.
    python -m pip install --upgrade "pip>=23.1" "setuptools>=68" "wheel>=0.41" "packaging>=24"
    # Avoid building cryptography/cffi from source on Alpine 3.12 (py38, musl):
    # preinstall a wheel-only cryptography version known to provide musllinux wheels for cp38.
    python -m pip install --only-binary=:all: "cryptography<42" || true
    # Then install the rest of test deps, preferring wheels and using system-site cffi where possible.
    python -m pip install --upgrade --prefer-binary psycopg2-binary six testgres==1.11.0 moto[s3] flask flask_cors boto3 pyOpenSSL
    python -m pip freeze

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
        if python3 -c "import sys; sys.exit(0) if sys.version_info >= (3, 12) else sys.exit(1)"; then
            MAKE_TARGETS="regresscheck isolationcheck testgrescheck"
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

    # Parse individual test targets and print start markers
    echo "Printing test status markers..."
    for target in $MAKE_TARGETS; do
        print_test_marker "$target" "start"
    done

    # Execute tests as the 'postgres' user - without installing the extension
    trap 'echo "--- check_docker: FAILED : ${MAKE_TARGETS}"' ERR
    CHECKCMD="  set -Eeo pipefail \
        && cd ${TEST_WORKSPACE}/temp_orioledb \
        && source $TEST_WORKSPACE/python3-venv/bin/activate \
        && export PYTHONPATH=${TEST_WORKSPACE}/temp_orioledb:\$PYTHONPATH \
        && make \
            IS_DEV=1 \
            NO_INSTALL=1 \
            USE_PGXS=1 \
            ${MAKE_TARGETS} \
            -j$(nproc) \
            LANG=C \
            PGUSER=postgres \
        && echo \'--- check_docker: OK : ${MAKE_TARGETS}\' \
    "

    echo "....Running tests as the 'postgres' user...."
    if time su postgres -c "bash -c \"$CHECKCMD\""; then
        # Tests completed successfully - print success markers
        echo "Tests completed successfully. Printing success markers..."
        for target in $MAKE_TARGETS; do
            print_test_marker "$target" "success"
        done
    else
        echo "Tests failed or were interrupted."
        exit 1
    fi

fi

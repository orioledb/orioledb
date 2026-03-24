#!/usr/bin/env bash
# Local OrioleDB Docker matrix tool for local AI-agentic development/testing
# and human use. It is intended for exploratory compatibility checking across
# base images, PostgreSQL versions, compilers, debug/LLVM variants, and
# related Docker image tests. It is coupled to ./docker/Dockerfile and
# ./docker/Dockerfile.ubuntu, so changes there should be reviewed for possible
# impact on ci/docker_matrix.sh arguments, help text, examples, and behavior.
# Prefer reading the detailed built-in help before use:
# run ./ci/docker_matrix.sh --help
set -Eeo pipefail

# Default values
BASE_IMAGE="ubuntu"
BASE_VERSION="24.04"
PG_MAJOR="17"
COMPILER="clang"
DEBUG="false"
PG_LLVM_ENABLED="true"
DRY_RUN="false"
LLVM_VERSION=""  # Default is empty (no LLVM version specified)
BASE_MATRIX="ubuntu:24.04"  # Default base matrix
TEST_MODE="all"  # Default test mode: all, no, none
FAIL_FAST="true"  # Default fail-fast behavior
CACHE_MODE="on"  # Default cache mode: on, off

# Generate git tag line for Docker labels
git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
git_desc=$(git describe --tags --long 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo "unknown")
git_date=$(git show -s --format='%cd' --date=short HEAD 2>/dev/null || echo "unknown")
git_dirty=""
if git diff --quiet --ignore-submodules --exit-code 2>/dev/null; then
    git_dirty=""
else
    git_dirty="-dirty"
fi
git_tag_line="${git_desc}-${git_date}${git_dirty}"

# Define base lists
declare -A base_lists
base_lists[all-oldest]="ubuntu:20.04 alpine:3.12 debian:bullseye"
base_lists[all-latest]="ubuntu:latest alpine:latest debian:latest"
base_lists[all-dev]="ubuntu:devel alpine:edge debian:testing"
base_lists[all-alpine]="alpine:edge alpine:3.23 alpine:3.22 alpine:3.21 alpine:3.20 alpine:3.19 alpine:3.18 alpine:3.17 alpine:3.16 alpine:3.15 alpine:3.14 alpine:3.13 alpine:3.12"
base_lists[all-ubuntu]="ubuntu:devel ubuntu:26.04 ubuntu:25.10 ubuntu:24.04 ubuntu:22.04 ubuntu:20.04"
base_lists[all-debian]="debian:experimental debian:testing debian:trixie debian:bookworm debian:bullseye"
base_lists[all]="${base_lists[all-alpine]} ${base_lists[all-ubuntu]} ${base_lists[all-debian]}"

# Valid Alpine, Ubuntu, PG and Compiler versions
VALID_ALPINE_VERSIONS="edge 3.23 3.22 3.21 3.20 3.19 3.18 3.17 3.16 3.15 3.14 3.13 3.12 latest"
VALID_UBUNTU_VERSIONS="devel 26.04 25.10 24.04 22.04 20.04 resolute questing noble jammy focal latest rolling"
VALID_DEBIAN_VERSIONS="unstable testing experimental bookworm bookworm-backports bookworm-slim bullseye bullseye-backports bullseye-slim trixie trixie-backports trixie-slim sid 11 12 13 stable oldstable latest"
VALID_PG_MAJOR_VERSIONS="17 16"
VALID_COMPILERS="clang gcc"
VALID_LLVM_VERSIONS="10 11 12 13 14 15 16 17 18 19 20 21 22"


# Function to display help message
show_help() {
    cat << EOF
Usage: ./ci/docker_matrix.sh [options]
This script should be run from the project root directory!

Experimental OrioleDB Docker matrix build command.
Builds a Cartesian product across the selected dimensions:
  base x pg-major x compiler x debug x pg-llvm
Optionally runs post-build docker-library/official-images tests.

Behavior summary:
  1. Expand the selected matrix dimensions ("all" expands to every listed value).
  2. Execute the expanded combinations serially, one after another.
  3. Build each Docker image and write per-image build logs/marker files.
  4. Optionally run official-images tests for each built image.
  5. Write per-image test logs/marker files in the per-run subdirectory:
     ./log_docker_build/<iso_date_time>-pid-<process_id>
     (the parent ./log_docker_build directory may also contain
      ./official-images used by --test all)
  6. Print the resulting local image list at the end.
Large matrices can therefore take a long time to finish.

Requirements and side effects:
  - Run from the project root directory.
  - Docker must be installed and available in PATH.
  - Note: Docker is currently checked before argument parsing,
          so --help also requires docker in PATH.
  - This tool builds development/test images, not minimal production images.
    The OrioleDB extension is built with IS_DEV=1 inside the image.
  - Development distro tags such as: alpine:edge, ubuntu:devel,
    debian:unstable/testing/experimental are less stable than released tags.
    Failures there do not always need fixing; prefer fixing only documented
    upstream changes or repo-relevant breakages.
  - The build is network/upstream-dependent: it pulls base images, upgrades
    distro packages inside the Docker build, fetches the patched PostgreSQL
    source from GitHub, and with --test all also clones/pulls official-images.
  - --test all clones or updates:
      ./log_docker_build/official-images

Options:
  --base MATRIX|IMAGE Specify the base/matrix option or individual image
                      Matrix options:
                        all-alpine # [ ${base_lists[all-alpine]} ],
                        all-ubuntu # [ ${base_lists[all-ubuntu]} ],
                        all-debian # [ ${base_lists[all-debian]} ],
                        all-oldest # [ ${base_lists[all-oldest]} ],
                        all-latest # [ ${base_lists[all-latest]} ],
                        all-dev # [ ${base_lists[all-dev]} ],
                        all,
                      Individual image examples:
                        alpine:* [ $VALID_ALPINE_VERSIONS ],
                        ubuntu:* [ $VALID_UBUNTU_VERSIONS ],
                        debian:* [ $VALID_DEBIAN_VERSIONS ],
                      Default: $BASE_IMAGE:$BASE_VERSION
  --pg-major VERSION  Specify PostgreSQL major version
                      Accepted values: [ all $VALID_PG_MAJOR_VERSIONS ]
                      Default: $PG_MAJOR
  --compiler TYPE     Specify compiler type
                      Accepted values: [ all $VALID_COMPILERS ]
                      Default: $COMPILER
  --llvm VERSION      Specify LLVM/Clang package version for Alpine images
                      Affects only Alpine package selection
                      On Ubuntu and Debian this value is currently ignored
                      When used with --compiler clang on Alpine, the effective
                      build compiler may become clang-N in the build log
                      Valid options: [ $VALID_LLVM_VERSIONS ]
                      Default: none (no specific LLVM version - uses the system default)
  --debug BOOL        Enable debug mode and preserve the build environments.
                      Adds PostgreSQL configure flags:
                        --enable-debug --enable-cassert
                      Skips final image cleanup; debug images typically exceed +1GB
                      Accepted values: [ all true false ]
                      Default: $DEBUG
  --pg-llvm BOOL      Enable PostgreSQL LLVM/JIT support at configure time
                      true  -> PostgreSQL configure uses --with-llvm
                      false -> PostgreSQL configure uses --without-llvm
                      Note: this does not remove LLVM/clang toolchain packages
                            from the image build; it only changes PostgreSQL's
                            configure-time LLVM/JIT setting
                      Valid options: [ all true false ]
                      Default: $PG_LLVM_ENABLED
  --without-llvm      Alias for: --pg-llvm false
  --test MODE         Control post-build official-images test execution
                      all = clone/update docker-library/official-images
                            and run tests from docker/orioledb-config.sh
                            This also creates temporary helper images such as:
                              librarytest/postgres-initdb:orioletest-17-gcc-ubuntu-25.10
                      no/none = skip those tests
                      Valid options: [ all no none ] ("no" and "none" are equal)
                      Default: $TEST_MODE
  --skip-test         Alias for: --test no
  --fail-fast BOOL    Stop on first failure (GitHub Actions-like)
                      true  = exit immediately on first build/test failure
                      false = continue with the remaining matrix entries
                      Note: with false, failures are recorded in marker files,
                            but the script may still finish with exit code 0
                      Valid options: [ true false ]
                      Default: $FAIL_FAST
  --continue-on-failure  Alias for: --fail-fast false
  --cache MODE        Control build cache usage
                      on  = normal docker build cache behavior
                      off = pass --no-cache to docker build
                      Valid options: [ on off ]
                      Default: $CACHE_MODE (cache enabled by default)
  --no-cache          Alias for: --cache off
  --dry-run           Only print commands without executing
                      Prints the fully expanded build/test commands
                      Default: $([ "$DRY_RUN" = true ] && echo "enabled" || echo "disabled")
  --help              Display this help message

For the details: check the "--dry-run" output and the Dockerfiles:
  - alpine Dockerfile        : ./docker/Dockerfile
  - ubuntu+debian Dockerfile : ./docker/Dockerfile.ubuntu

Image tag format:
  Base format:
    orioletest:<pg_major>-<compiler>-<base_os>-<base_tag>
  Inspect/runtime note:
    The image env var BASE_OS currently stores <image-name>:<image-tag>
    (for example: alpine:edge), not only the bare OS family name.
  Optional suffixes:
    -llvmN         only for Alpine when --llvm N is used
    -without-llvm  when --pg-llvm false is used
    -debug         when --debug true is used
  Examples:
    orioletest:17-clang-ubuntu-24.04
    orioletest:17-gcc-alpine-3.22-llvm20
    orioletest:16-gcc-debian-bookworm-without-llvm-debug

Output artifacts:
The Docker build logs are generated in the ./log_docker_build directory,
with one timestamped subdirectory per script run.
Per-image log files are named from the Docker tag:
  <docker_tag>.build.log
  <docker_tag>.test.log
Per-image marker files:
  *.build.progress *.build.ok *.build.failed
  *.test.progress  *.test.ok  *.test.failed *.test.skipped
Typical success indicators for one matrix entry:
  - <docker_tag>.build.ok exists
  - <docker_tag>.test.ok exists (or <docker_tag>.test.skipped if tests were disabled)
  - docker image orioletest:<docker_tag> exists
Useful log inspection commands:
  grep -i -C 1 warning: ./log_docker_build/*/*.build.log
  grep -i -C 1 error:   ./log_docker_build/*/*.build.log
Machine-readable image checks:
  docker images --format '{{.Repository}}:{{.Tag}}' 'orioletest*'
  docker images --format '{{.Repository}}:{{.Tag}}' 'librarytest/*:*'

Clean orioletest test images:
  docker images "orioletest*"
  docker images "orioletest*" -q | xargs -r docker rmi -f
  docker images --format '{{.Repository}}:{{.Tag}}' 'orioletest*'
Clean official-images helper test images:
  docker images "librarytest/*:*"
  docker images "librarytest/*:*" -q | xargs -r docker rmi -f
  docker images --format '{{.Repository}}:{{.Tag}}' 'librarytest/*:*'
Cleanup note:
  - Due to Docker layer/cache retention, removing test images may not always
    reclaim as much disk space as expected.
  - If disk usage remains high, consult the Docker documentation before doing
    stronger cleanup of caches, layers, build data, or other Docker artifacts.
  - These tests are best run on a dedicated non-production Docker environment;
    in shared or production-like Docker environments they can consume
    substantial disk space and clutter the Docker workspace.

Examples:
  Build one image with defaults for unspecified dimensions:
    ./ci/docker_matrix.sh --base debian:bookworm --pg-major 17
  Build one debug image:
    ./ci/docker_matrix.sh --base alpine:3.22 --compiler gcc --debug true
  Build a family of bases:
    ./ci/docker_matrix.sh --base all-latest
  Build without PostgreSQL LLVM/JIT (useful if the newest LLVM causes
  problems and is not yet fully supported by PostgreSQL):
    ./ci/docker_matrix.sh --base ubuntu:24.04 --pg-llvm false
  Build Alpine with an explicit LLVM package version:
    ./ci/docker_matrix.sh --base alpine:3.22 --llvm 20
  Continue after failures and disable cache:
    ./ci/docker_matrix.sh --base all-dev --compiler gcc --continue-on-failure --no-cache --without-llvm
  Print the expanded commands only:
    ./ci/docker_matrix.sh --base all-oldest --dry-run
  Run a large matrix (can easily take about >10 hours, depending on host/network)/etc.:
    ./ci/docker_matrix.sh --base all --pg-major all --compiler all --debug all --pg-llvm all --continue-on-failure --no-cache

Default behavior:
  ./ci/docker_matrix.sh --base $BASE_IMAGE:$BASE_VERSION --pg-major $PG_MAJOR --compiler $COMPILER --debug $DEBUG --pg-llvm $PG_LLVM_ENABLED --test $TEST_MODE --fail-fast $FAIL_FAST --cache $CACHE_MODE

EOF
}

# Function to check if the script is run from the correct directory
check_directory() {
    if [[ "$(basename "$(pwd)")" == "ci" ]]; then
        echo "Error: This script should be run from the project root directory, not the 'ci' directory."
        echo "Please change to the project root directory and run: ./ci/docker_matrix.sh"
        exit 1
    fi

    if [[ ! -f "./ci/docker_matrix.sh" ]]; then
        echo "Error: This script should be run from the project root directory."
        echo "Please change to the project root directory and run: ./ci/docker_matrix.sh"
        exit 1
    fi
}

# Function to check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH. Please install Docker and try again."
        exit 1
    fi
}

# Function to format a command for readable output
format_command() {
    local cmd="$1"
    # Formatting: insert line breaks before flags (-x), pipes (|),
    # and keywords (tee, 2>&1, bash, "orioletest:) for readability
    local formatted_cmd
    formatted_cmd=$(echo "$cmd" | sed -E '
        s/(\s)(\||tee|2>&1|bash|"orioletest:)/ \\\n  \2/g;
        s/ (\-[^ ]+)/ \\\n  \1/g
    ')

    # Process each line to replace multiple spaces before backslash at end of lines
    formatted_cmd=$(echo "$formatted_cmd" | sed -E 's/[[:space:]]+\\$/ \\/')

    echo "$formatted_cmd"
}

print_command() {
    local cmd="$1"
    echo
    echo "# -----------"
    format_command "$cmd"
}

execute_command() {
    local cmd="$*"
    print_command "$cmd"

    if [ "$DRY_RUN" = false ]; then
        eval "$cmd"
    fi
}

run_command_with_status() {
    local cmd="$1"
    local exit_code=0

    print_command "$cmd"

    if [ "$DRY_RUN" = false ]; then
        eval "$cmd" || exit_code=$?
    fi

    return "$exit_code"
}



# Function to validate and process the base parameter
process_base_parameter() {
    local base="$1"
    if [[ "${base_lists[$base]}" ]]; then
        echo "${base_lists[$base]}"
    elif [[ $base == alpine:* ]]; then
        local version="${base#alpine:}"
        if [[ " $VALID_ALPINE_VERSIONS " == *" $version "* ]]; then
            echo "$base"
        else
            echo "Invalid Alpine version: $version" >&2
            exit 1
        fi
    elif [[ $base == ubuntu:* ]]; then
        local version="${base#ubuntu:}"
        if [[ " $VALID_UBUNTU_VERSIONS " == *" $version "* ]]; then
            echo "$base"
        else
            echo "Invalid Ubuntu version: $version" >&2
            exit 1
        fi
    elif [[ $base == debian:* ]]; then
        local version="${base#debian:}"
        if [[ " $VALID_DEBIAN_VERSIONS " == *" $version "* ]]; then
            echo "$base"
        else
            echo "Invalid Debian version: $version" >&2
            exit 1
        fi
    else
        echo "Invalid base parameter: $base" >&2
        exit 1
    fi
}

# Function to get LLVM dependencies based on OS and version
get_llvm_deps() {
    local base_os="$1"
    local llvm_ver="$2"

    if [[ -z "$llvm_ver" ]]; then
        # No LLVM version specified, return empty
        echo ""
    elif [[ "$base_os" == "alpine" ]]; then
        # For Alpine, format is llvm20-dev clang20
        echo "--build-arg DOCKER_PG_LLVM_DEPS=\"llvm${llvm_ver}-dev clang${llvm_ver}\""
    elif [[ "$base_os" == "ubuntu" || "$base_os" == "debian" ]]; then
        # Ubuntu/Debian do not support explicit LLVM selection here
        echo ""
    else
        # Unknown OS, return empty
        echo ""
    fi
}

value_in_list() {
    local value="$1"
    shift
    local candidate

    for candidate in "$@"; do
        if [[ "$value" == "$candidate" ]]; then
            return 0
        fi
    done

    return 1
}

validate_space_separated_list() {
    local option_name="$1"
    local value="$2"
    local valid_values="$3"
    local all_values

    # shellcheck disable=SC2206
    all_values=( $valid_values )
    if ! value_in_list "$value" "${all_values[@]}"; then
        echo "Invalid ${option_name}: ${value}. Valid options are: ${valid_values}" >&2
        exit 1
    fi
}

# Main build logic
main() {
    local pg_major_list compiler_list base_list pg_llvm_list

    # Set up lists based on input parameters
    # shellcheck disable=SC2206
    [[ $PG_MAJOR == "all" ]] && pg_major_list=( $VALID_PG_MAJOR_VERSIONS ) || pg_major_list=( "$PG_MAJOR" )
    # shellcheck disable=SC2206
    [[ $COMPILER == "all" ]] && compiler_list=( $VALID_COMPILERS ) || compiler_list=( "$COMPILER" )
    # shellcheck disable=SC2206
    [[ $PG_LLVM_ENABLED == "all" ]] && pg_llvm_list=( true false ) || pg_llvm_list=( "$PG_LLVM_ENABLED" )
    # shellcheck disable=SC2207
    base_list=($(process_base_parameter "$BASE_MATRIX"))

    # Prepare log directory
    logpath="./log_docker_build/$(date +%Y-%m-%d-%H%M%S)-pid-$$"
    execute_command "mkdir -p $logpath"

    # Clone or update docker-library/official-images repository - only needed for testing
    local OFFIMG_LOCAL_CLONE="./log_docker_build/official-images"
    local OFFIMG_REPO_URL="https://github.com/docker-library/official-images.git"
    if [[ "$TEST_MODE" == "all" ]]; then
        execute_command "mkdir -p $OFFIMG_LOCAL_CLONE"
        if [ -d "$OFFIMG_LOCAL_CLONE/.git" ]; then
            execute_command "git -C $OFFIMG_LOCAL_CLONE pull origin master"
        else
            execute_command "git clone --branch=master --single-branch --depth=1 $OFFIMG_REPO_URL $OFFIMG_LOCAL_CLONE"
        fi
    fi

    # Build and test loop
    for pg_major in "${pg_major_list[@]}"; do
        for compiler in "${compiler_list[@]}"; do
            for base in "${base_list[@]}"; do
                for debug in $([[ $DEBUG == "all" ]] && echo "true false" || echo "$DEBUG"); do
                    for pg_llvm_enabled in "${pg_llvm_list[@]}"; do

                        local base_os="${base%%:*}"
                        local base_tag="${base##*:}"

                        local dockerfile="./docker/Dockerfile"
                        [[ $base_os == "ubuntu" || $base_os == "debian" ]] && dockerfile="./docker/Dockerfile.ubuntu"
                        # Get LLVM dependency parameter if LLVM version is specified
                        local llvm_deps
                        llvm_deps=$(get_llvm_deps "$base_os" "$LLVM_VERSION")

                        # Add LLVM version to tag if specified
                        local llvm_tag=""
                        [[ $base_os == "alpine" && -n "$LLVM_VERSION" ]] && llvm_tag="-llvm${LLVM_VERSION}"

                        local llvm_config_tag=""
                        [[ "$pg_llvm_enabled" == "false" ]] && llvm_config_tag="-without-llvm"

                        local docker_tag="${pg_major}-${compiler}-${base_os}-${base_tag}${llvm_tag}${llvm_config_tag}"
                        if [[ "$debug" == "true" ]]; then
                            docker_tag+="-debug"
                        fi

                        echo "#"
                        echo "#------------ $docker_tag ------------------"
                        echo "#"

                        # Build Docker image
                        execute_command "touch ${logpath}/${docker_tag}.build.progress"

                        # Add --no-cache flag if CACHE_MODE is off
                        local cache_flag=""
                        [[ "$CACHE_MODE" == "off" ]] && cache_flag="--no-cache"

                        local build_cmd="docker build --pull --network=host --progress=plain ${cache_flag} \
                            --build-arg BASE_IMAGE=\"$base_os\" \
                            --build-arg BASE_VERSION=\"$base_tag\" \
                            --build-arg BUILD_CC_COMPILER=\"$compiler\" \
                            --build-arg PG_MAJOR=\"$pg_major\" \
                            --build-arg DEBUG_MODE=\"$debug\" \
                            --build-arg PG_LLVM_ENABLED=\"$pg_llvm_enabled\" \
                            --label \"org.opencontainers.image.title=OrioleDB ${docker_tag}\" \
                            --label \"org.opencontainers.image.description=OrioleDB: next-generation storage engine for PostgreSQL\" \
                            --label \"org.opencontainers.image.url=https://www.orioledb.com/\" \
                            --label \"org.opencontainers.image.licenses=Apache-2.0 OR PostgreSQL\" \
                            --label \"org.opencontainers.image.revision=$git_tag_line\" \
                            --label \"org.opencontainers.image.source=https://github.com/orioledb/orioledb\" \
                            ${llvm_deps} \
                            -f \"$dockerfile\" \
                            -t \"orioletest:${docker_tag}\" . \
                            2>&1 | \
                            tee \"${logpath}/${docker_tag}.build.log\""

                        # Use || to prevent set -e from exiting before we can check
                        # the exit code and apply fail-fast / continue-on-failure logic.
                        local build_exit_code=0
                        run_command_with_status "$build_cmd" || build_exit_code=$?

                        # Check if build succeeded for fail-fast behavior
                        if [[ $build_exit_code -ne 0 ]]; then
                            execute_command "rm ${logpath}/${docker_tag}.build.progress"
                            execute_command "touch ${logpath}/${docker_tag}.build.failed"
                            if [[ "$FAIL_FAST" == "true" ]]; then
                                echo "Error: Docker build failed for ${docker_tag}. Stopping due to fail-fast mode."
                                exit 1
                            else
                                echo "Warning: Docker build failed for ${docker_tag}. Continuing due to --continue-on-failure."
                                continue
                            fi
                        fi

                        # On success, replace the in-progress marker with an OK marker.
                        execute_command "rm ${logpath}/${docker_tag}.build.progress"
                        execute_command "touch ${logpath}/${docker_tag}.build.ok"

                        # Run Docker tests based on TEST_MODE
                        if [[ "$TEST_MODE" == "all" ]]; then
                            execute_command "touch ${logpath}/${docker_tag}.test.progress"

                            local test_cmd="\"${OFFIMG_LOCAL_CLONE}/test/run.sh\" \
                                -c \"${OFFIMG_LOCAL_CLONE}/test/config.sh\" \
                                -c \"docker/orioledb-config.sh\" \"orioletest:${docker_tag}\" \
                                2>&1 | \
                                tee \"${logpath}/${docker_tag}.test.log\""

                            # Use || to prevent set -e from exiting before we can check
                            # the exit code and apply fail-fast / continue-on-failure logic.
                            local test_exit_code=0
                            run_command_with_status "$test_cmd" || test_exit_code=$?

                            # Check if test succeeded for fail-fast behavior
                            if [[ $test_exit_code -ne 0 ]]; then
                                execute_command "rm ${logpath}/${docker_tag}.test.progress"
                                execute_command "touch ${logpath}/${docker_tag}.test.failed"
                                if [[ "$FAIL_FAST" == "true" ]]; then
                                    echo "Error: Docker tests failed for ${docker_tag}. Stopping due to fail-fast mode."
                                    exit 1
                                else
                                    echo "Warning: Docker tests failed for ${docker_tag}. Continuing due to --continue-on-failure."
                                    continue
                                fi
                            fi

                            # On success, replace the in-progress marker with an OK marker.
                            execute_command "rm ${logpath}/${docker_tag}.test.progress"
                            execute_command "touch ${logpath}/${docker_tag}.test.ok"
                        else
                            echo "Skipping tests for ${docker_tag} (TEST_MODE=$TEST_MODE)"
                            execute_command "touch ${logpath}/${docker_tag}.test.skipped"
                        fi

                    done
                done
            done
        done
    done

    execute_command "docker images orioletest:* | sort"
}

# Run the main function if not sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Check if the script is run from the correct directory
    check_directory

    # Check if Docker is installed
    check_docker

    # Parse command line arguments
    if [ $# -eq 0 ]; then
        show_help
        exit 0
    fi

    while [[ $# -gt 0 ]]; do
        case $1 in
            --base)
                BASE_MATRIX="$2"
                shift 2
                ;;
            --pg-major)
                PG_MAJOR="$2"
                validate_space_separated_list "PostgreSQL major version" "$PG_MAJOR" "all $VALID_PG_MAJOR_VERSIONS"
                shift 2
                ;;
            --compiler)
                COMPILER="$2"
                validate_space_separated_list "compiler" "$COMPILER" "all $VALID_COMPILERS"
                shift 2
                ;;
            --llvm)
                LLVM_VERSION="$2"
                validate_space_separated_list "LLVM version" "$LLVM_VERSION" "$VALID_LLVM_VERSIONS"
                shift 2
                ;;
            --debug)
                DEBUG="$2"
                validate_space_separated_list "debug mode" "$DEBUG" "all true false"
                shift 2
                ;;
            --pg-llvm)
                PG_LLVM_ENABLED="$2"
                if [[ ! "$PG_LLVM_ENABLED" =~ ^(all|true|false)$ ]]; then
                    echo "Invalid pg-llvm option: $PG_LLVM_ENABLED. Valid options are: all, true, false"
                    exit 1
                fi
                shift 2
                ;;
            --without-llvm)
                PG_LLVM_ENABLED="false"
                shift
                ;;
            --test)
                TEST_MODE="$2"
                # Validate test mode
                if [[ ! "$TEST_MODE" =~ ^(all|no|none)$ ]]; then
                    echo "Invalid test mode: $TEST_MODE. Valid options are: all, no, none"
                    exit 1
                fi
                shift 2
                ;;
            --skip-test)
                TEST_MODE="no"
                shift
                ;;
            --fail-fast)
                FAIL_FAST="$2"
                # Validate fail-fast option
                if [[ ! "$FAIL_FAST" =~ ^(true|false)$ ]]; then
                    echo "Invalid fail-fast option: $FAIL_FAST. Valid options are: true, false"
                    exit 1
                fi
                shift 2
                ;;
            --continue-on-failure)
                FAIL_FAST="false"
                shift
                ;;
            --cache)
                CACHE_MODE="$2"
                # Validate cache mode
                if [[ ! "$CACHE_MODE" =~ ^(on|off)$ ]]; then
                    echo "Invalid cache mode: $CACHE_MODE. Valid options are: on, off"
                    exit 1
                fi
                shift 2
                ;;
            --no-cache)
                CACHE_MODE="off"
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # Run the main function
    main

    # Only show environment info and summary if not in dry-run mode
    if [ "$DRY_RUN" = false ]; then
        echo

        # Collect environment info (best-effort)
        host_os=$(uname -s 2>/dev/null || echo unknown)
        host_kernel=$(uname -r 2>/dev/null || echo unknown)
        host_machine=$(uname -m 2>/dev/null || echo unknown)
        case "$host_machine" in
            x86_64|amd64) host_arch_name="x86-64" ;;
            aarch64|arm64) host_arch_name="armv8" ;;
            armv7l) host_arch_name="armv7" ;;
            riscv64) host_arch_name="riscv64" ;;
            ppc64le) host_arch_name="ppc64le" ;;
            s390x) host_arch_name="s390x" ;;
            *) host_arch_name="$host_machine" ;;
        esac

        cli_version=$(docker --version 2>/dev/null | head -n1 || true)

        echo "Environment:"
        echo "  Host: ${host_os} ${host_kernel} ${host_machine} [arch: ${host_arch_name}]"
        echo "  Container CLI: ${cli_version:-unknown} "
        echo "  Git: ${git_tag_line}"
        echo "  Branch: ${git_branch}"
        echo "#----------------"
        echo "# Build process completed. You can check the build logs with:"
        echo "#  grep -i -C 1 warning: ${logpath}/*.build.log"
        echo "#  grep -i -C 1 error:   ${logpath}/*.build.log"
        echo
        echo "# To remove test images, run:"
        echo '#  docker images "orioletest*" -q | xargs -r docker rmi -f '
        echo "# ----------------------------------"
    fi

fi

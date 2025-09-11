#!/usr/bin/env bash
set -Eeo pipefail

# Default values
BASE_IMAGE="ubuntu"
BASE_VERSION="24.04"
PG_MAJOR="17"
COMPILER="clang"
DEBUG="false"
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
base_lists[all-alpine]="alpine:edge alpine:3.22 alpine:3.21 alpine:3.20 alpine:3.19 alpine:3.18 alpine:3.17 alpine:3.16 alpine:3.15 alpine:3.14 alpine:3.13 alpine:3.12"
base_lists[all-ubuntu]="ubuntu:devel ubuntu:25.10 ubuntu:25.04 ubuntu:24.04 ubuntu:22.04 ubuntu:20.04"
base_lists[all-debian]="debian:experimental debian:testing debian:trixie debian:bookworm debian:bullseye"
base_lists[all]="${base_lists[all-alpine]} ${base_lists[all-ubuntu]} ${base_lists[all-debian]}"

# Valid Alpine, Ubuntu, PG and Compiler versions
VALID_ALPINE_VERSIONS="edge 3.22 3.21 3.20 3.19 3.18 3.17 3.16 3.15 3.14 3.13 latest"
VALID_UBUNTU_VERSIONS="devel 25.10 25.04 24.10 24.04 22.04 20.04 questing plucky oracular noble jammy focal latest rolling"
VALID_DEBIAN_VERSIONS="unstable testing experimental bookworm bookworm-backports bookworm-slim bullseye bullseye-backports bullseye-slim trixie trixie-backports trixie-slim sid 11 12 13 stable oldstable latest"
VALID_PG_MAJOR_VERSIONS="17 16"
VALID_COMPILERS="clang gcc"
VALID_LLVM_VERSIONS="10 11 12 13 14 15 16 17 18 19 20 21"


# Function to display help message
show_help() {
    cat << EOF
Usage: ./ci/docker_matrix.sh [options]
This script should be run from the project root directory!

Experimental OrioleDB Docker matrix build command
  for testing multiple: PostgreSQL versions, compilers, and base images.

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
                      Valid options: [ all $VALID_PG_MAJOR_VERSIONS ]
                      Default: $PG_MAJOR
  --compiler TYPE     Specify compiler type
                      Valid options: [ all $VALID_COMPILERS ]
                      Default: $COMPILER
  --llvm VERSION      Specify LLVM/Clang version to Alpine images
                      Ubuntu and Debian images are not supported!
                      Valid options: [ $VALID_LLVM_VERSIONS ]
                      Default: none (no specific LLVM version - uses the system default)
  --debug BOOL        Enable debug mode and preserve the build environments.
                      In this case, each image size exceeds +1GB
                      Valid options: [ all true false ]
                      Default: $DEBUG
  --test MODE         Control tests execution
                      Valid options: [ all no none ] ("no" and "none" are equal)
                      Default: $TEST_MODE
  --skip-test         Alias for: --test no
  --fail-fast BOOL    Stop on first failure (GitHub Actions-like)
                      Valid options: [ true false ]
                      Default: $FAIL_FAST
  --continue-on-failure  Alias for: --fail-fast false
  --cache MODE        Control build cache usage
                      Valid options: [ on off ]
                      Default: $CACHE_MODE (cache enabled by default)
  --no-cache          Alias for: --cache off
  --dry-run           Only print commands without executing
                      Default: $([ "$DRY_RUN" = true ] && echo "enabled" || echo "disabled")
  --help              Display this help message

For the details: check the "--dry-run" and the Dockerfiles in the root directory
  - alpine Dockerfile        : ./docker/Dockerfile
  - ubuntu+debian Dockerfile : ./docker/Dockerfile.ubuntu

The Docker build logs are generated in the ./log_docker_build directory,
and you can check the build logs with:
  grep -i -C 1 warning: ./log_docker_build/*/*.build.log
And you can clean the orioletest test images with:
  docker images | grep orioletest | awk '{print \$3}' | sort -u | xargs docker rmi -f

Examples:
  ./ci/docker_matrix.sh --base all-dev --pg-major all --compiler clang
  ./ci/docker_matrix.sh --base alpine:3.22 --compiler gcc --debug true
  ./ci/docker_matrix.sh --base ubuntu:oracular --pg-major 16 --compiler all
  ./ci/docker_matrix.sh --base debian:bookworm --pg-major 17
  ./ci/docker_matrix.sh --base alpine:edge --compiler gcc
  ./ci/docker_matrix.sh --base alpine:3.22 --llvm 20          # --llvm is alpine only
  ./ci/docker_matrix.sh --base alpine:3.22 --llvm 19          # --llvm is alpine only
  ./ci/docker_matrix.sh --base all-dev --compiler gcc --continue-on-failure --no-cache
  ./ci/docker_matrix.sh --base all-latest
  ./ci/docker_matrix.sh --base all-oldest
  ./ci/docker_matrix.sh --base debian:trixie
  ./ci/docker_matrix.sh --base ubuntu:20.04
Full test:
  ./ci/docker_matrix.sh --base all --pg-major all --compiler all --continue-on-failure --no-cache

Default behavior:
  ./ci/docker_matrix.sh --base $BASE_IMAGE:$BASE_VERSION --pg-major $PG_MAJOR --compiler $COMPILER --debug $DEBUG --test $TEST_MODE --fail-fast $FAIL_FAST --cache $CACHE_MODE

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

## Function to format and optionally execute a command
execute_command() {
    echo
    echo "# -----------"
    local cmd="$*"

    # Formatting: insert line breaks before certain options
    # and at '|' and 'tee' for readability
    local formatted_cmd
    formatted_cmd=$(echo "$cmd" | sed -E '
        s/(\s)(\||tee|2>&1|bash|"orioletest:)/ \\\n  \2/g;
        s/ (\-[^ ]+)/ \\\n  \1/g
    ')

    # Process each line to replace multiple spaces before backslash at end of lines
    formatted_cmd=$(echo "$formatted_cmd" | sed -E 's/[[:space:]]+\\$/ \\/')

    echo "$formatted_cmd"

    if [ "$DRY_RUN" = false ]; then
        eval "$cmd"
    fi
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

# Main build logic
main() {
    local pg_major_list compiler_list base_list

    # Set up lists based on input parameters
    # shellcheck disable=SC2206
    [[ $PG_MAJOR == "all" ]] && pg_major_list=( $VALID_PG_MAJOR_VERSIONS ) || pg_major_list=( "$PG_MAJOR" )
    # shellcheck disable=SC2206
    [[ $COMPILER == "all" ]] && compiler_list=( $VALID_COMPILERS ) || compiler_list=( "$COMPILER" )
    # shellcheck disable=SC2207
    base_list=($(process_base_parameter "$BASE_MATRIX"))

    # Prepare log directory
    logpath="./log_docker_build/$(date +%Y-%m-%d-%H%M%S)-pid-$$"
    execute_command "mkdir -p $logpath"
    execute_command "rm -f ${logpath}/*.log"

    # Clone or update docker-library/official-images repository - for testing
    local OFFIMG_LOCAL_CLONE="./log_docker_build/official-images"
    local OFFIMG_REPO_URL="https://github.com/docker-library/official-images.git"
    execute_command "mkdir -p $OFFIMG_LOCAL_CLONE"
    if [ -d "$OFFIMG_LOCAL_CLONE/.git" ]; then
        execute_command "pushd $OFFIMG_LOCAL_CLONE && git pull origin master && popd"
    else
        execute_command "git clone $OFFIMG_REPO_URL $OFFIMG_LOCAL_CLONE"
    fi

    # Build and test loop
    for pg_major in "${pg_major_list[@]}"; do
        for compiler in "${compiler_list[@]}"; do
            for base in "${base_list[@]}"; do
                for debug in $([[ $DEBUG == "all" ]] && echo "true false" || echo "$DEBUG"); do

                        local base_os="${base%%:*}"
                        local base_tag="${base##*:}"

                        local dockerfile="./docker/Dockerfile"
                        [[ $base_os == "ubuntu" || $base_os == "debian" ]] && dockerfile="./docker/Dockerfile.ubuntu"
                        # Get LLVM dependency parameter if LLVM version is specified
                        local llvm_deps
                        llvm_deps=$(get_llvm_deps "$base_os" "$LLVM_VERSION")

                        # Add LLVM version to tag if specified
                        local llvm_tag=""
                        [[ -n "$LLVM_VERSION" ]] && llvm_tag="-llvm${LLVM_VERSION}"

                        local docker_tag="${pg_major}-${compiler}-${base_os}-${base_tag}${llvm_tag}"
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
                            --label \"org.opencontainers.image.title=OrioleDB ${docker_tag}\" \
                            --label \"org.opencontainers.image.description=OrioleDB: next-generation storage engine for PostgreSQL\" \
                            --label \"org.opencontainers.image.url=https://www.orioledb.com/\" \
                            --label \"org.opencontainers.image.licenses=PostgreSQL\" \
                            --label \"org.opencontainers.image.revision=$git_tag_line\" \
                            --label \"org.opencontainers.image.source=https://github.com/orioledb/orioledb\" \
                            ${llvm_deps} \
                            -f \"$dockerfile\" \
                            -t \"orioletest:${docker_tag}\" . \
                            2>&1 | \
                            tee \"${logpath}/${docker_tag}.build.log\""

                        if [ "$DRY_RUN" = false ]; then
                            eval "$build_cmd"
                            local build_exit_code=$?

                            # Check if build succeeded for fail-fast behavior
                            if [[ "$FAIL_FAST" == "true" && $build_exit_code -ne 0 ]]; then
                                execute_command "rm ${logpath}/${docker_tag}.build.progress"
                                echo "Error: Docker build failed for ${docker_tag}. Stopping due to fail-fast mode."
                                exit 1
                            fi
                        else
                            # For dry-run, just show the formatted command
                            echo
                            echo "# -----------"
                            local formatted_cmd
                            formatted_cmd=$(echo "$build_cmd" | sed -E '
                                s/(\s)(\||tee|2>&1|bash|"orioletest:)/ \\\n  \2/g;
                                s/ (\-[^ ]+)/ \\\n  \1/g
                            ')
                            formatted_cmd=$(echo "$formatted_cmd" | sed -E 's/[[:space:]]+\\$/ \\/')
                            echo "$formatted_cmd"
                        fi

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

                            if [ "$DRY_RUN" = false ]; then
                                eval "$test_cmd"
                                local test_exit_code=$?

                                # Check if test succeeded for fail-fast behavior
                                if [[ "$FAIL_FAST" == "true" && $test_exit_code -ne 0 ]]; then
                                    execute_command "rm ${logpath}/${docker_tag}.test.progress"
                                    echo "Error: Docker tests failed for ${docker_tag}. Stopping due to fail-fast mode."
                                    exit 1
                                fi
                            else
                                # For dry-run, just show the formatted command
                                echo
                                echo "# -----------"
                                local formatted_cmd
                                formatted_cmd=$(echo "$test_cmd" | sed -E '
                                    s/(\s)(\||tee|2>&1|bash|"orioletest:)/ \\\n  \2/g;
                                    s/ (\-[^ ]+)/ \\\n  \1/g
                                ')
                                formatted_cmd=$(echo "$formatted_cmd" | sed -E 's/[[:space:]]+\\$/ \\/')
                                echo "$formatted_cmd"
                            fi

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
                shift 2
                ;;
            --compiler)
                COMPILER="$2"
                shift 2
                ;;
            --llvm)
                LLVM_VERSION="$2"
                # Validate LLVM version
                if [[ -n "$LLVM_VERSION" && ! " $VALID_LLVM_VERSIONS " =~  $LLVM_VERSION  ]]; then
                    echo "Invalid LLVM version: $LLVM_VERSION. Valid versions are: $VALID_LLVM_VERSIONS"
                    exit 1
                fi
                shift 2
                ;;
            --debug)
                DEBUG="$2"
                shift 2
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
        echo "#  docker images | grep orioletest | awk '{print \$3}' | sort -u | xargs docker rmi -f"
        echo "# ----------------------------------"
    fi

fi

#!/usr/bin/env bash
set -Eeo pipefail

# Default values
BASE_MATRIX="ubuntu:24.04"
PG_MAJOR="16"
COMPILER="clang"
DEBUG="false"
DRY_RUN="false"

# Define base lists
declare -A base_lists
base_lists[all-oldest]="ubuntu:22.04 alpine:3.14"
base_lists[all-latest]="ubuntu:24.10 alpine:3.20"
base_lists[all-dev]="ubuntu:devel alpine:edge"
base_lists[all-alpine]="alpine:edge alpine:3.20 alpine:3.19 alpine:3.18 alpine:3.17 alpine:3.16 alpine:3.15 alpine:3.14"
base_lists[all-debian]="ubuntu:devel ubuntu:24.10 ubuntu:24.04 ubuntu:22.04 ubuntu:20.04 "
base_lists[all]="${base_lists[all-alpine]} ${base_lists[all-debian]}"

# Valid Alpine, Ubuntu, PG and Compiler versions
VALID_ALPINE_VERSIONS="edge 3.20 3.19 3.18 3.17 3.16 3.15 3.14"
VALID_UBUNTU_VERSIONS="devel 24.10 24.04 22.04 20.04 oracular noble jammy focal"
VALID_PG_MAJOR_VERSIONS="17 16"
VALID_COMPILERS="clang gcc"


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
                        all-debian # [ ${base_lists[all-debian]} ],
                        all-oldest # [ ${base_lists[all-oldest]} ],
                        all-latest # [ ${base_lists[all-latest]} ],
                        all-dev # [ ${base_lists[all-dev]} ],
                        all,
                      Individual image examples:
                        alpine:* [ $VALID_ALPINE_VERSIONS ],
                        ubuntu:* [ $VALID_UBUNTU_VERSIONS ],
                      Default: $BASE_MATRIX
  --pg-major VERSION  Specify PostgreSQL major version
                      Valid options: [ all $VALID_PG_MAJOR_VERSIONS ]
                      Default: $PG_MAJOR
  --compiler TYPE     Specify compiler type
                      Valid options: [ all $VALID_COMPILERS ]
                      Default: $COMPILER
  --debug BOOL        Enable debug mode and preserve the build environments.
                      In this case, each image size exceeds +1GB
                      Valid options: [ all true false ]
                      Default: $DEBUG
  --dry-run           Only print commands without executing
                      Default: $([ "$DRY_RUN" = true ] && echo "enabled" || echo "disabled")
  --clean             Clean Docker images ( remove orioletest:* )
                      [ --dry-run mode is not working with this option! ]
  --help              Display this help message

For the details: check the "--dry-run" and the Dockerfiles in the root directory
  - alpine Dockerfile : ./Dockerfile
  - ubuntu Dockerfile : ./Dockerfile.ubuntu

The Docker build logs generated in the ./log_docker_build directory,
and you can check the build logs with:
  grep -i -C 1 warning: ./log_docker_build/*/*.build.log

Examples:
  ./ci/docker_matrix.sh --base all-dev --pg-major all --compiler clang
  ./ci/docker_matrix.sh --base alpine:3.20 --pg-major 16 --compiler gcc --debug true
  ./ci/docker_matrix.sh --base ubuntu:oracular --pg-major 16 --compiler all --debug false

Default behavior:
  ./ci/docker_matrix.sh --base $BASE_MATRIX --pg-major $PG_MAJOR --compiler $COMPILER --debug $DEBUG

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
        echo "Warning: Docker is not installed or not in PATH. Please install Docker and try again."
        exit 1
    fi
}

# Function to clean Docker images
clean_docker_images() {
    echo "Cleaning Docker images..."
    docker images | grep 'orioletest' | awk '{print $3}' | sort -u | xargs -r docker rmi -f
}

## Function to format and optionally execute a command
execute_command() {
    echo
    echo "# -----------"
    local cmd="$*"

    # Formatting: insert line breaks before certain options
    # and at '|' and 'tee' for readability
    local formatted_cmd=$(echo "$cmd" | sed -E '
        s/(\s)(\||tee|2>&1)/ \\\n  \2/g;
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
    else
        echo "Invalid base parameter: $base" >&2
        exit 1
    fi
}

# Main build logic
main() {
    local pg_major_list compiler_list base_list

    # Set up lists based on input parameters
    [[ $PG_MAJOR == "all" ]] && pg_major_list=( $VALID_PG_MAJOR_VERSIONS ) || pg_major_list=( $PG_MAJOR )
    [[ $COMPILER == "all" ]] && compiler_list=( $VALID_COMPILERS ) || compiler_list=( $COMPILER )
    base_list=($(process_base_parameter "$BASE_MATRIX"))

    # Prepare log directory
    local logpath="./log_docker_build/$(date +%Y-%m-%d-%H%M%S)-pid-$$"
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
                        local base_os_upper="${base_os^^}"
                        local dockerfile="Dockerfile"
                        [[ $base_os == "ubuntu" ]] && dockerfile="Dockerfile.ubuntu"
                        local docker_tag="${pg_major}-${compiler}-${base_os}-${base_tag}-debug-${debug}"

                        echo "#------------ $docker_tag ------------------"

                        # Build Docker image
                        execute_command "docker build --pull --network=host --progress=plain \
                            --build-arg ${base_os_upper}_VERSION=\"$base_tag\" \
                            --build-arg BUILD_CC_COMPILER=\"$compiler\" \
                            --build-arg PG_MAJOR=\"$pg_major\" \
                            --build-arg DEBUG_MODE=\"$debug\" \
                            -f \"$dockerfile\" \
                            -t \"orioletest:${docker_tag}\" . \
                            2>&1 | \
                            tee \"${logpath}/${docker_tag}.build.log\""

                        # Run Docker tests
                        execute_command "\"${OFFIMG_LOCAL_CLONE}/test/run.sh\" \
                            -c \"${OFFIMG_LOCAL_CLONE}/test/config.sh\" \
                            -c \"test/orioledb-config.sh\" \"orioletest:${docker_tag}\" \
                            2>&1 | \
                            tee \"${logpath}/${docker_tag}.test.log\""

                        #TODO - add regression test - running inside in the docker container

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
            --debug)
                DEBUG="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --clean)
                clean_docker_images
                exit 0
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

    echo
    echo "#----------------"
    echo "# Build process completed. You can check the build logs with:"
    echo "#  grep -i -C 1 warning: ./log_docker_build/*/*.build.log"
    echo
    echo "# To remove test images, run:"
    echo "#  docker images | grep orioletest | awk '{print \$3}' | sort -u | xargs docker rmi -f"
    echo "# ----------------------------------"

fi
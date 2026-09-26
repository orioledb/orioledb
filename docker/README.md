# OrioleDB Docker images

This directory contains Dockerfiles, init scripts, and test runners for building and validating OrioleDB images. It supports local development and CI via the Docker Official Images test suite and project-specific checks. The sections below outline the layout and primary entry points.

## Directory structure:

```
docker\
  ├── config.guess
  ├── config.sub
  ├── Dockerfile             # alpine Dockerfile
  ├── Dockerfile.ubuntu      # ubuntu/debian Dockerfile
  ├── Dockerfile.archlinux   # experimental - archlinux Dockerfile
  ├── Dockerfile.fedora      # experimental - fedora Dockerfile
  ├── init                       # docker image - init files
  │   ├── default-orioledb.sh     # initialisation
  │   ├── docker-entrypoint.sh    # docker-entrypoint
  │   └── postgresql.docker.conf  # postgresql.conf for the orioledb docker image
  ├── orioledb-config.sh     # docker test config
  ├── README.md              # this file
  └── tests
      ├── orioledb-basics
      │   └── run.sh         # minimal orioledb test
      └── orioledb-check
          ├── debug.sh       # test file for debugging.
          ├── Dockerfile.orioledb-check # run the checks.
          └── run.sh         # run the "check"

Related files:
./ci/docker_matrix.sh        # run local matrix builds/tests (alpine/ubuntu)
                             #  note: fedora/archlinux Dockerfiles exist, but not in this matrix yet.
./ci/check_docker.sh         # create a test-python environment in a docker image
                             # and run the checks.

./.github/workflows/dockertest.yml # build + test images in GitHub CI
./.github/workflows/docker.yml     # publish images (no tests)

```

## Testing OrioleDB Docker images

Running the Docker Official Image tests against orioledb images,
see: "Docker Official Images Test Suite":
* https://github.com/docker-library/official-images/tree/master/test

Used by:
* `./ci/docker_matrix.sh`
* `./.github/workflows/dockertest.yml`

### Running docker test suite

```bash
# clone official-images test suite
OFFIMG_LOCAL_CLONE=./log_docker_build/official-images
OFFIMG_REPO_URL=https://github.com/docker-library/official-images.git
mkdir -p "$OFFIMG_LOCAL_CLONE"
git clone "$OFFIMG_REPO_URL" "$OFFIMG_LOCAL_CLONE"

"${OFFIMG_LOCAL_CLONE}/test/run.sh" \
    -c "${OFFIMG_LOCAL_CLONE}/test/config.sh" \
    -c "docker/orioledb-config.sh" \
    "orioletest:17-gcc-ubuntu-24.04"
```

If the test is ok, you can see:

```bash
testing orioletest:17-clang-ubuntu-noble-debug-false
        'utc' [1/7]...passed
        'no-hard-coded-passwords' [2/7]...passed
        'override-cmd' [3/7]...passed
        'postgres-basics' [4/7]....passed
        'postgres-initdb' [5/7]....passed
        'orioledb-basics' [6/7]...NOTICE:  extension "orioledb" already exists, skipping
               passed
        'orioledb-check' [7/7]...#0 building with "default" instance using docker driver
    < ..orioledb-check log....>

```

### test: postgres-basics

https://github.com/docker-library/official-images/blob/master/test/tests/postgres-basics/run.sh

### test: postgres-initdb

https://github.com/docker-library/official-images/blob/master/test/tests/postgres-initdb/run.sh
https://github.com/docker-library/official-images/blob/master/test/tests/postgres-initdb/initdb.sql

### test: orioledb-basics

* `./tests/orioledb-basics/run.sh`

### test: orioledb-check

* `./tests/orioledb-check/run.sh`
  Runs in the `./tests/orioledb-check/Dockerfile.orioledb-check` environment:
  * Sets up the Python3 test environment.
  * Runs `regresscheck`.
  * Runs `isolationcheck`.
  * If Python >= 3.12: runs `testgrescheck_part_1`.
  * If Python >= 3.12: runs `testgrescheck_part_2`.

Note: wal2json-related OrioleDB tests are skipped.

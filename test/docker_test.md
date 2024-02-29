
# Official Postgres Docker Image test

Running the Docker Official Image tests against orioledb images,
see: "Docker Official Images Test Suite":
* https://github.com/docker-library/official-images/tree/master/test


used by:
* `./ci/local_docker_matrix.sh`
* todo: add to Github CI


## Running docker test suite

```bash
# clone official-images test suite
OFFIMG_LOCAL_CLONE=./log_docker_build/official-images
OFFIMG_REPO_URL=https://github.com/docker-library/official-images.git
mkdir -p "$OFFIMG_LOCAL_CLONE"
git clone "$OFFIMG_REPO_URL" "$OFFIMG_LOCAL_CLONE"

"${OFFIMG_LOCAL_CLONE}/test/run.sh" \
    -c "${OFFIMG_LOCAL_CLONE}/test/config.sh" \
    -c "test/orioledb-config.sh" \
    "orioletest:16-gcc-ubuntu-22.04" 
```

If the test is ok, you can see:

```bash
testing orioletest:16-gcc-ubuntu-22.04
	'utc' [1/6]...passed
	'no-hard-coded-passwords' [2/6]...passed
	'override-cmd' [3/6]...passed
	'postgres-basics' [4/6]....passed
	'postgres-initdb' [5/6]....passed
	'orioledb-basics' [6/6]...passed
```

## test: postgres-basics

https://github.com/docker-library/official-images/blob/master/test/tests/postgres-basics/run.sh

## test: postgres-initdb

https://github.com/docker-library/official-images/blob/master/test/tests/postgres-initdb/run.sh
https://github.com/docker-library/official-images/blob/master/test/tests/postgres-initdb/initdb.sql

## test: orioledb-basics

* `./tests/orioledb-basics/run.sh`

#!/bin/bash

set -eu

python3 -m venv $GITHUB_WORKSPACE/python3-venv

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$GITHUB_WORKSPACE/python3-venv/bin:$PATH"

# install required packages

# psycopg2 depends on existing postgres installation
if [ $GITHUB_JOB = "run-benchmark" ]; then
	pip_packages="psycopg2-binary six testgres==1.11.0 unidiff python-telegram-bot matplotlib"
	sudo env "PATH=$PATH" pip3 install --upgrade $pip_packages
elif [ $GITHUB_JOB = "pgindent" ]; then
	sudo env "PATH=$PATH" pip3 install --upgrade yapf
else
	sudo env "PATH=$PATH" pip3 install --upgrade -r orioledb/requirements.txt
fi

if [ $GITHUB_JOB != "run-benchmark" ] && [ $GITHUB_JOB != "pgindent" ] && [ $GITHUB_JOB != "pg_upgrade" ]; then
    # pgvector is built against stock PostgreSQL upstream, so it leaves its
    # insert callback on aminsert rather than on the aminsertextended entry
    # point this fork added.  Every access method shipped with the fork has been
    # converted, so pgvector is the only thing that covers index bridging for
    # the legacy signature; test/sql/pgvector.sql runs its body only when this
    # install has happened, and matches its pgvector_1 expected output when it
    # has not.
    #
    # Its default build is -march=native and dispatches to hand-written AVX-512
    # kernels, neither of which valgrind can execute: the first INSERT of a
    # vector value dies with SIGILL.  Build it for the baseline architecture
    # there instead, which costs nothing but the speed of an access method we
    # only test for correctness.
    if [ "${CHECK_TYPE:-}" = "valgrind_1" ] || [ "${CHECK_TYPE:-}" = "valgrind_2" ]; then
        pgvector_make_args="OPTFLAGS= PG_CPPFLAGS=-DDISABLE_DISPATCH"
    else
        pgvector_make_args=""
    fi
    wget https://codeload.github.com/pgvector/pgvector/tar.gz/refs/tags/v0.8.1
    tar -zxf v0.8.1
    rm v0.8.1
    (cd pgvector-0.8.1 && make $pgvector_make_args && make install)

    wget https://codeload.github.com/eulerto/wal2json/tar.gz/refs/tags/wal2json_2_6
    tar -zxf wal2json_2_6
    rm wal2json_2_6
    cd wal2json-wal2json_2_6
    make
    make install
fi

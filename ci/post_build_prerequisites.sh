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
    # the legacy signature; the test/sql/pgvector.sql regression test is added
    # to the suite only when this install has happened.
    wget https://codeload.github.com/pgvector/pgvector/tar.gz/refs/tags/v0.8.1
    tar -zxf v0.8.1
    rm v0.8.1
    (cd pgvector-0.8.1 && make && make install)

    wget https://codeload.github.com/eulerto/wal2json/tar.gz/refs/tags/wal2json_2_6
    tar -zxf wal2json_2_6
    rm wal2json_2_6
    cd wal2json-wal2json_2_6
    make
    make install
fi

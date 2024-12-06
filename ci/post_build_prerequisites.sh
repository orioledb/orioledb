#!/bin/bash

set -eu

# Build type parameter: 'pgxs' for USE_PGXS=1, empty or anything else for regular build
BUILD_TYPE="${1:-}"

# Set up environment variables
if [ "$BUILD_TYPE" = "pgxs" ]; then
    # For PGXS builds, use system PostgreSQL
    echo "Using system PostgreSQL - setting up wal2json with USE_PGXS=1"
else
    # For regular builds, use workspace PostgreSQL
    export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"
fi

# psycopg2 depends on existing postgres installation
if [ $GITHUB_JOB = "run-benchmark" ]; then
	pip_packages="psycopg2-binary six testgres python-telegram-bot matplotlib"
elif [ $GITHUB_JOB = "pgindent" ]; then
	pip_packages="psycopg2 six testgres moto[s3] flask flask_cors boto3 pyOpenSSL yapf"
else
	pip_packages="psycopg2 six testgres moto[s3] flask flask_cors boto3 pyOpenSSL"
fi

# install required packages
sudo env "PATH=$PATH" pip3 install --upgrade $pip_packages

if [ $GITHUB_JOB != "run-benchmark" ] && [ $GITHUB_JOB != "pgindent" ]; then
    wget https://codeload.github.com/eulerto/wal2json/tar.gz/refs/tags/wal2json_2_6
    tar -zxf wal2json_2_6
    rm wal2json_2_6
    cd wal2json-wal2json_2_6

    if [ "$BUILD_TYPE" = "pgxs" ]; then
        make USE_PGXS=1
        make USE_PGXS=1 install
    else
        make
        make install
    fi
fi

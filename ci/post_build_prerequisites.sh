#!/bin/bash

set -eu

python3 -m venv $GITHUB_WORKSPACE/python3-venv

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$GITHUB_WORKSPACE/python3-venv/bin:$PATH"

# psycopg2 depends on existing postgres installation
if [ $GITHUB_JOB = "run-benchmark" ]; then
	pip_packages="psycopg2-binary six testgres==1.10.5 python-telegram-bot matplotlib"
elif [ $GITHUB_JOB = "pgindent" ]; then
	pip_packages="psycopg2 six testgres==1.10.5 moto[s3] flask flask_cors boto3 pyOpenSSL yapf"
else
	pip_packages="psycopg2 six testgres==1.10.5 moto[s3] flask flask_cors boto3 pyOpenSSL"
fi

# install required packages
sudo env "PATH=$PATH" pip3 install --upgrade $pip_packages

if [ $GITHUB_JOB != "run-benchmark" ] && [ $GITHUB_JOB != "pgindent" ]; then
    wget https://codeload.github.com/eulerto/wal2json/tar.gz/refs/tags/wal2json_2_6
    tar -zxf wal2json_2_6
    rm wal2json_2_6
    cd wal2json-wal2json_2_6
    make
    make install
fi

#!/bin/bash

set -eu

cd postgresql/src/tools/pg_bsd_indent
make -sj4 install
cd ../../../..

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$GITHUB_WORKSPACE/postgresql/src/tools/pgindent:$PATH"

cd orioledb
make USE_PGXS=1 -s pgindent
git diff > pgindent.diff
cd ..

if [ -s orioledb/pgindent.diff ]; then
	echo "========= Contents of pgindent.diff" 
	cat orioledb/pgindent.diff
	exit 1
else
	exit 0
fi

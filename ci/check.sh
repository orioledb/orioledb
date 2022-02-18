#!/bin/bash

set -eu
export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

# unsets limit for coredumps size
ulimit -c unlimited -S
# sets a coredump file pattern
mkdir -p /tmp/cores-$GITHUB_SHA-$TIMESTAMP
sudo sh -c "echo \"/tmp/cores-$GITHUB_SHA-$TIMESTAMP/%t_%p.core\" > /proc/sys/kernel/core_pattern"


status=0
THREADS=4

cd orioledb
if [ $CHECK_TYPE = "valgrind_1" ]; then
	make USE_PGXS=1 VALGRIND=1 regresscheck isolationcheck testgrescheck_part_1 -j$THREADS || status=$?
elif [ $CHECK_TYPE = "valgrind_2" ]; then
	make USE_PGXS=1 VALGRIND=1 testgrescheck_part_2 -j$THREADS || status=$?
else
	make USE_PGXS=1 installcheck -j$THREADS || status=$?
fi
cd ..

exit $status

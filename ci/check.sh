#!/bin/bash

set -eu
export PATH="$GITHUB_WORKSPACE/pgsql/bin:$GITHUB_WORKSPACE/python3-venv/bin:$PATH"

# unsets limit for coredumps size
ulimit -c unlimited -S
# sets a coredump file pattern
mkdir -p /tmp/cores-$GITHUB_SHA-$TIMESTAMP
sudo sh -c "echo \"/tmp/cores-$GITHUB_SHA-$TIMESTAMP/%t_%p.core\" > /proc/sys/kernel/core_pattern"

# remember number of oom-killer visits in syslog before test
[ -f /var/log/system.log ] && syslogfile=/var/log/system.log || syslogfile=/var/log/syslog
[ -f $syslogfile ] && cat $syslogfile | grep oom-kill | wc -l > ./ooms.tmp \
					|| { echo "Syslog file not found"; status=1; }


status=0

cd orioledb
if [ $CHECK_TYPE = "valgrind_1" ]; then
	make USE_PGXS=1 VALGRIND=1 regresscheck isolationcheck testgrescheck_part_1 -j $(nproc) || status=$?
elif [ $CHECK_TYPE = "valgrind_2" ]; then
	make USE_PGXS=1 VALGRIND=1 testgrescheck_part_2 -j $(nproc) || status=$?
else
	make USE_PGXS=1 installcheck -j $(nproc) || status=$?
fi
cd ..

exit $status

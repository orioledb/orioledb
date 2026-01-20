#!/bin/bash

set -eu

status=0

[ -f /var/log/system.log ] && syslogfile=/var/log/system.log || syslogfile=/var/log/syslog
[ -f $syslogfile ] || { echo "Syslog file not found"; status=1; }
oomcount=$(cat $syslogfile | grep oom-kill | wc -l)
[ -f ./ooms.tmp ] && { oomsbefore=$(cat ./ooms.tmp); rm ./ooms.tmp; } || \
	{ oomsbefore=0; echo "File ooms.tmp not found. check.sh should be run before check-output.sh"; status=1;}
if [ $oomcount != $oomsbefore ]; then
    echo "======== OOM-killer came during the tests"
    status=1
fi

# show diff if it exists
for f in ` find ./orioledb/test ./postgresql/src/test -type f \( -name 'regression.diffs' -o -name 'regress_filtered.diffs' -o -name 'isolation_filtered.diffs' \) ` ; do
	echo "========= Contents of $f (first 500 lines)"
	head -n 500 $f
	line_count=$(wc -l < $f)
	if [ $line_count -gt 500 ]; then
		echo "... (truncated $(($line_count - 500)) lines)"
		echo "Full log available as artifact"
	fi
	status=1
done

# show valgrind logs if needed
if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	for f in ` find . -name pid-*.log ` ; do
		if grep -q 'Command: [^ ]*/postgres' $f && grep -E -q '(Process terminating|ERROR SUMMARY: [1-9])' $f; then
			echo "========= Contents of $f"
			cat $f
			status=1
		fi
	done
fi

# check core dumps if any
if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	cores=$(find orioledb/ -name '*.core.*' 2>/dev/null)
else
	cores=$(find /tmp/cores-$GITHUB_SHA-$TIMESTAMP/ -name '*.core' 2>/dev/null)
fi

if [ -n "$cores" ]; then
	for corefile in $cores ; do
		if [[ $corefile != *_3.core ]]; then
			if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
				# Valgring core dumps have not auxiliary vector. We can't detect a binary file dynamically
				# but this value is valid for most cases.
				binary=tmp_install/usr/local/pgsql/bin/postgres
			else
				binary=$(gdb -quiet -core $corefile -batch -ex 'info auxv' | grep AT_EXECFN | perl -pe "s/^.*\"(.*)\"\$/\$1/g")
			fi
			echo dumping $corefile for $binary
			gdb --batch --quiet -x ./orioledb/ci/cmds.gdb $binary $corefile
			status=1
		fi
	done
	tar -czf /tmp/cores-$GITHUB_SHA-$TIMESTAMP.tar.gz . $cores
fi

rm -rf /tmp/cores-$GITHUB_SHA-$TIMESTAMP

for f in ` find . -name 'ubsan.log.*' ` ; do
	echo "========= Contents of $f"
	cat $f
	status=1
done

for f in ` find . -name 'asan.log.*' ` ; do
	echo "========= Contents of $f"
	cat $f
	status=1
done

exit $status

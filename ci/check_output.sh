#!/bin/bash

set -eu

status=0

# show diff if it exists
for f in ` find . -name regression.diffs ` ; do
	echo "========= Contents of $f" 
	cat $f
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
			gdb --batch --quiet -ex "thread apply all bt full" -ex "quit" $binary $corefile
			status=1
		fi
	done
	tar -czf /tmp/cores-$GITHUB_SHA-$TIMESTAMP.tar.gz . $cores
fi

rm -rf /tmp/cores-$GITHUB_SHA-$TIMESTAMP

exit $status

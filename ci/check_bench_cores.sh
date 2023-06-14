#!/bin/bash

set -eu

status=0

# check core dumps if any
cores=$(find /mnt/ -name '*.core' 2>/dev/null || true)

if [ -n "$cores" ]; then
	for corefile in $cores ; do
		if [[ $corefile != *_3.core ]]; then
			binary=$(gdb -quiet -core $corefile -batch -ex 'info auxv' | grep AT_EXECFN | perl -pe "s/^.*\"(.*)\"\$/\$1/g")
			echo dumping $corefile for $binary
			gdb --batch --quiet -ex "thread apply all bt full" -ex "quit" $binary $corefile
			status=1
		fi
	done
fi

exit $status

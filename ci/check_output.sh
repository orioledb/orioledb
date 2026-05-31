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

# Surface filter residuals first -- these are the *actual* reasons a
# streaming/installcheck-oriole run failed, since the full
# regression.diffs is intentionally huge and most of it is expected
# orioledb-vs-heap drift that gets filtered.  Run filter_*.py against
# the source diffs the same way the test scripts do, so we always have
# fresh residuals to print even if the test harness only kept the
# unfiltered output around.
declare -A residual_sources=(
	["postgresql/src/test/recovery/tmp_check/filtered.out"]="postgresql/src/test/recovery/tmp_check/regression.diffs"
	["postgresql/src/test/regress_filtered.diffs"]="postgresql/src/test/regress/regression.diffs"
	["postgresql/src/test/isolation_filtered.diffs"]="postgresql/src/test/isolation/output_iso/regression.diffs"
)
for residual in "${!residual_sources[@]}"; do
	source_diff="${residual_sources[$residual]}"
	# Regenerate the residual if it's missing or stale relative to source
	if [ -f "$source_diff" ] && [ ! -s "$residual" -o "$source_diff" -nt "$residual" ]; then
		case "$residual" in
			*isolation_filtered.diffs)
				python3 ./orioledb/ci/filter_isolation_diff.py --diff "$source_diff" > "$residual" 2>/dev/null || true
				;;
			*)
				python3 ./orioledb/ci/filter_regression_diff.py --diff "$source_diff" > "$residual" 2>/dev/null || true
				;;
		esac
	fi
	if [ -s "$residual" ]; then
		echo "========= FILTER RESIDUAL: $residual (the lines below are what made the run fail; full diff is shown afterwards)"
		# Summarize hunks per test for orientation
		tests=$(grep -oE '/expected/[^/]+\.out' "$residual" | sort -u | tr '\n' ' ')
		hunk_count=$(grep -c '^@@' "$residual" || true)
		line_count=$(wc -l < "$residual")
		echo "Tests with residual diffs: ${tests:-<none-parsed>}"
		echo "Total residual hunks: ${hunk_count:-0}, lines: $line_count"
		echo "----- residual contents -----"
		head -n 500 "$residual"
		if [ "$line_count" -gt 500 ]; then
			echo "... (truncated $(($line_count - 500)) lines)"
		fi
		status=1
	fi
done

# Then the full (unfiltered) diffs as a fallback, in case the residual
# is empty but the source diff still exists (which means the filter
# decided everything was OK -- shouldn't get here, but useful when
# debugging the filter itself).
for f in ` find ./orioledb/test ./postgresql/src/test -type f \( -name 'regression.diffs' -o -name 'regress_filtered.diffs' -o -name 'isolation_filtered.diffs' -o -name 'filtered.out' \) ` ; do
	# Skip files we already printed as residuals above
	case "$f" in
		./postgresql/src/test/recovery/tmp_check/filtered.out) continue ;;
		./postgresql/src/test/regress_filtered.diffs) continue ;;
		./postgresql/src/test/isolation_filtered.diffs) continue ;;
	esac
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

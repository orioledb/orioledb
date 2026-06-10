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
		if [[ $corefile == *_3.core ]]; then
			continue
		fi

		# Filter out cores that don't belong to a postgres process.  The
		# kernel can drop a core into the same directory for any process
		# the tests happen to spawn -- shell wrappers, `cp` invoked from
		# perl recovery tests, etc.  Those are unrelated to orioledb /
		# PostgreSQL and trying to read PG symbols out of them just makes
		# `cmds.gdb` error out and tank the whole job.
		#
		# `file` parses the ELF psinfo / NT_AUXV notes and prints
		#   "from '<argv0> <argv1>...', ..., execfn: '<path>'"
		# We accept the core if either:
		#   - argv0 starts with "postgres" (followed by space, ':',
		#     end-of-quote -- this matches the postmaster as well as
		#     backends renamed via set_ps_display, e.g. "postgres: io
		#     worker 0"), or
		#   - execfn ends in "/postgres" (canonical for the temp-install
		#     postgres binary).
		# That works for both plain cores and the valgrind-produced ones
		# (the existing code couldn't use `info auxv` for the latter,
		# but `file` extracts argv0 from NT_PRPSINFO).
		file_out=$(file -b "$corefile" 2>/dev/null || true)
		if ! { echo "$file_out" | grep -Eq "from 'postgres[ :']|execfn: '[^']*/postgres'"; }; then
			echo "skipping non-postgres core $corefile: $file_out"
			continue
		fi

		if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
			# Valgrind core dumps do not have an auxiliary vector.  Fall
			# back to the well-known postgres path used by the temp
			# install used in CI.
			binary=tmp_install/usr/local/pgsql/bin/postgres
		else
			binary=$(gdb -quiet -core $corefile -batch -ex 'info auxv' 2>/dev/null \
				| grep AT_EXECFN | perl -pe "s/^.*\"(.*)\"\$/\$1/g")
			if [ -z "$binary" ]; then
				# Fall back to the temp-install postgres if we have
				# already verified above that the core belongs to a
				# postgres process.
				binary=tmp_install/usr/local/pgsql/bin/postgres
			fi
		fi
		echo dumping $corefile for $binary
		gdb --batch --quiet -x ./orioledb/ci/cmds.gdb $binary $corefile
		status=1
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

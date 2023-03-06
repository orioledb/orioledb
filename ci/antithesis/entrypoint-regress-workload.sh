#!/bin/bash

set -e

if [ "${1}" == "workload" ]; then
	until pg_isready; do
		echo "Waiting for server to connect...";
		sleep 1s;
	done;

	cd /usr/src/postgresql/contrib/orioledb/
	MAJORVERSION=$( pg_config --version | sed 's/.* \([[:digit:]]\+\).*/\1/' )
	REGRESSION_TESTS=( $(ls -1 sql) )
	ISOLATION_TESTS=( $(ls -1 specs) )
	STATUS=0

	pg_config --version

	# Filter out btree_sys_check because it depends on test order and will always fail
	# Filter out composite_pk_quals because it is not a test
	for index in "${!REGRESSION_TESTS[@]}" ; do
		case ${REGRESSION_TESTS[index]} in
		"btree_sys_check.sql" | \
		"composite_pk_quals.sql")
			unset -v 'REGRESSION_TESTS[$index]'
			;;
		esac
	done
	REGRESSION_TESTS=("${REGRESSION_TESTS[@]}")

	# Filter out version specific tests
	if [ $MAJORVERSION -lt 14 ]; then
		for index in "${!REGRESSION_TESTS[@]}" ; do
			if [ ${REGRESSION_TESTS[index]} == "toast_column_compress.sql" ]; then
				unset -v 'REGRESSION_TESTS[$index]'
			fi
		done
		REGRESSION_TESTS=("${REGRESSION_TESTS[@]}")
	fi

	if [ $MAJORVERSION -lt 15 ]; then
		for index in "${!ISOLATION_TESTS[@]}" ; do
			if [ ${ISOLATION_TESTS[index]} == "isol_merge.spec" ]; then
				unset -v 'ISOLATION_TESTS[$index]'
			fi
		done
		ISOLATION_TESTS=("${ISOLATION_TESTS[@]}")
	fi

	function run_regression_test()
	{
		REGRESSION_TEST=$1
		echo "Regression test '$REGRESSION_TEST' started"
			../../src/test/regress/pg_regress \
				--inputdir=$(pwd) \
				--bindir='$(pg_config --bindir)' \
				$REGRESSION_TEST || STATUS=$?
			if [ $STATUS -ne 0 ]; then
				echo "regression.diffs contents start"
				cat regression.diffs
				echo "regression.diffs contents end"
			fi
		echo "Regression test '$REGRESSION_TEST' ended"
	}

	while true; do
		RANDOM_NUMBER=$(od -vAn -N1 -tu1 < /dev/urandom)
		REGRESSION_TEST_NUM=$(( $RANDOM_NUMBER % ${#REGRESSION_TESTS[@]} ))
		ISOLATION_TEST_NUM=$(( $RANDOM_NUMBER % ${#ISOLATION_TESTS[@]} ))
		REGRESSION_TEST=${REGRESSION_TESTS[$REGRESSION_TEST_NUM]%.*}
		ISOLATION_TEST=${ISOLATION_TESTS[$ISOLATION_TEST_NUM]%.*}

		run_regression_test $REGRESSION_TEST

		echo "Isolation test '$ISOLATION_TEST' started"
		../../src/test/isolation/pg_isolation_regress \
			--inputdir=$(pwd) --outputdir=output_iso \
			--bindir='$(pg_config --bindir)' \
			$ISOLATION_TEST || STATUS=$?
		if [ $STATUS -ne 0 ]; then
			echo "output_iso/regression.diffs contents start"
			cat output_iso/regression.diffs
			echo "output_iso/regression.diffs contents end"
		fi
		echo "Isolation test '$ISOLATION_TEST' ended"
	done
else
    # An unknown command (debugging the container?): Forward as is
    exec ${@}
fi
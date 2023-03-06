#!/bin/bash

set -e

if [ "${1}" == "workload" ]; then
	RANDOM_NUMBER=$(od -vAn -N1 -tu1 < /dev/urandom)
	cd /usr/src/postgresql/contrib/orioledb
	MAJORVERSION=$( pg_config --version | sed 's/.* \([[:digit:]]\+\).*/\1/' )
	TESTGRES_TESTS=( $(ls -1 t) )

	# Filter out base_test because it not contains test
	for index in "${!TESTGRES_TESTS[@]}" ; do
		case ${TESTGRES_TESTS[index]} in
		*base_test.py)
			unset -v 'TESTGRES_TESTS[$index]'
			;;
		esac
	done
	TESTGRES_TESTS=("${TESTGRES_TESTS[@]}")

	# Filter out version specific tests
	if [ $MAJORVERSION -lt 15 ]; then
		for index in "${!TESTGRES_TESTS[@]}" ; do
			if [ ${TESTGRES_TESTS[index]} == "merge_into_test.py" ]; then
				unset -v 'TESTGRES_TESTS[$index]'
			fi
		done
		TESTGRES_TESTS=("${TESTGRES_TESTS[@]}")
	fi

	while true; do
		RANDOM_NUMBER=$(od -vAn -N1 -tu1 < /dev/urandom)
		TESTGRES_TEST_FILE_NUM=$(( $RANDOM_NUMBER % ${#TESTGRES_TESTS[@]} ))
		TESTGRES_TEST_FILE=${TESTGRES_TESTS[$TESTGRES_TEST_FILE_NUM]}
		TEST_CLASS=$(cat t/$TESTGRES_TEST_FILE| sed -n 's/class \(.*\)(.*BaseTest):.*/\1/p')
		TESTS=( $(cat t/$TESTGRES_TEST_FILE | sed -n '/def test_/p' | sed 's/.*def \(test_.*\)(.*/\1/') )
		TEST_NUM=$(( $RANDOM_NUMBER % ${#TESTS[@]} ))
		TEST="t.${TESTGRES_TEST_FILE%.*}.$TEST_CLASS.${TESTS[$TEST_NUM]}"
		echo "Testgres test '$TEST' started"
		gosu postgres make testgrescheck_part_1 TESTGRESCHECKS_PART_1="$TEST"
		echo "Testgres test '$TEST' ended"
		STATUS=$?
		if [ $STATUS -ne 0 ]; then
			echo "output_iso/regression.diffs contents start"
			cat output_iso/regression.diffs
			echo "output_iso/regression.diffs contents end"
		fi
	done
else
    # An unknown command (debugging the container?): Forward as is
    exec ${@}
fi
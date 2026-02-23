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
	make USE_PGXS=1 IS_DEV=1 VALGRIND=1 regresscheck isolationcheck testgrescheck_part_1 -j $(nproc) || status=$?
elif [ $CHECK_TYPE = "valgrind_2" ]; then
	make USE_PGXS=1 IS_DEV=1 VALGRIND=1 testgrescheck_part_2 -j $(nproc) || status=$?
elif [ $CHECK_TYPE = "sanitize" ]; then
	if [ $COMPILER = "clang" ]; then
		FAKE_STACK=1
	else
		FAKE_STACK=0 # it is really slow for gcc
	fi

	UBSAN_OPTIONS="log_path=$PWD/ubsan.log" \
	ASAN_OPTIONS=$(cat <<-END
		verify_asan_link_order=0:
		detect_stack_use_after_return=$FAKE_STACK:
		detect_leaks=0:
		abort_on_error=1:
		disable_coredump=0:
		strict_string_checks=1:
		check_initialization_order=1:
		strict_init_order=1:
		detect_odr_violation=0:
		log_path=$PWD/asan.log:
		max_uar_stack_size_log=25:
	END
	) \
		make USE_PGXS=1 IS_DEV=1 installcheck -j $(nproc) || status=$?
elif [ $CHECK_TYPE = "pg_tests" ]; then
    cd ../postgresql
    # Backport float tests patch
    wget -O float-patch.patch "https://git.postgresql.org/gitweb/?p=postgresql.git;a=patch;h=da83b1ea10c2b7937d4c9e922465321749c6785b"
    git apply float-patch.patch
    # Initialize data directory and set OrioleDB as default AM
    initdb -N --encoding=UTF-8 --locale=C -D $GITHUB_WORKSPACE/pgsql/pgdata

    echo "shared_preload_libraries = 'orioledb'" >> $GITHUB_WORKSPACE/pgsql/pgdata/postgresql.conf
    pg_ctl -D $GITHUB_WORKSPACE/pgsql/pgdata -l pg.log start
    make -C src/test/regress installcheck -j $(nproc) || status=$?
    make -C src/test/isolation installcheck -j $(nproc) || status=$?
    make -C src/test/subscription installcheck -j $(nproc) || status=$?

    if [ $status -eq 0 ]; then
        echo "default_table_access_method = 'orioledb'" >> $GITHUB_WORKSPACE/pgsql/pgdata/postgresql.conf
        if [ $PG_VERSION = "17" ]; then
            git apply patches/subscription_enable_oriole.diff
        fi
        pg_ctl -D $GITHUB_WORKSPACE/pgsql/pgdata -l pg.log restart
        # Run Postgress regression tests
        make -C src/test/regress EXTRA_REGRESS_OPTS="--load-extension=orioledb" installcheck-oriole -j $(nproc) || true
        if [ -f src/test/regress/regression.diffs ]; then
          python3 ../orioledb/ci/filter_regression_diff.py --diff src/test/regress/regression.diffs > src/test/regress_filtered.diffs
          rm src/test/regress/regression.diffs
          [ -s src/test/regress_filtered.diffs ] || rm -f src/test/regress_filtered.diffs src/test/regress/regression.diffs
        fi

        echo "orioledb.strict_mode = true" >> $GITHUB_WORKSPACE/pgsql/pgdata/postgresql.conf
        pg_ctl -D $GITHUB_WORKSPACE/pgsql/pgdata -l pg.log restart
        make -C src/test/isolation EXTRA_REGRESS_OPTS="--load-extension=orioledb" installcheck -j $(nproc) || true
        if [ -f src/test/isolation/output_iso/regression.diffs ]; then
          python3 ../orioledb/ci/filter_isolation_diff.py --diff src/test/isolation/output_iso/regression.diffs > src/test/isolation_filtered.diffs
          [ -s src/test/isolation_filtered.diffs ] || rm src/test/isolation_filtered.diffs src/test/isolation/output_iso/regression.diffs
        fi

        if [ $PG_VERSION = "17" ]; then
            make -C src/test/subscription installcheck-oriole -j $(nproc) || status=$?
        fi
    fi
    pg_ctl -D $GITHUB_WORKSPACE/pgsql/pgdata -l pg.log stop
else
	make USE_PGXS=1 IS_DEV=1 installcheck -j $(nproc) || status=$?
fi
cd ..

exit $status

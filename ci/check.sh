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
    cat src/test/regress/parallel_schedule | sed "s/indirect_toast//" >$GITHUB_WORKSPACE/parallel_schedule_no_segfaults
    # Backport float tests patch
    wget -O float-patch.patch "https://git.postgresql.org/gitweb/?p=postgresql.git;a=patch;h=da83b1ea10c2b7937d4c9e922465321749c6785b"
    git apply float-patch.patch
    # Initialize data directory and set OrioleDB as default AM
    initdb -N --encoding=UTF-8 --locale=C -D $GITHUB_WORKSPACE/pgsql/pgdata

    echo "wal_level = logical" >> $GITHUB_WORKSPACE/pgsql/pgdata/postgresql.conf
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

        pg_basebackup -D $GITHUB_WORKSPACE/pgsql/rep_pgdata -Fp -Xs -P
        touch $GITHUB_WORKSPACE/pgsql/rep_pgdata/standby.signal
        echo "port = 5433" >> $GITHUB_WORKSPACE/pgsql/rep_pgdata/postgresql.conf
        echo "primary_conninfo = 'host=/tmp port=5432'" >> $GITHUB_WORKSPACE/pgsql/rep_pgdata/postgresql.conf
        echo "allow_in_place_tablespaces = true" >> $GITHUB_WORKSPACE/pgsql/rep_pgdata/postgresql.conf
        pg_ctl -D $GITHUB_WORKSPACE/pgsql/rep_pgdata -l rep_pg.log start

        cd src/test/regress
        make installcheck-tests EXTRA_REGRESS_OPTS="--load-extension=orioledb --schedule=$GITHUB_WORKSPACE/parallel_schedule_no_segfaults" TESTS="" -j $(nproc) || true
        cd ../../..

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

        psql postgres -p 5432 -c 'CREATE EXTENSION orioledb;' || true

        # Wait for replica to synchronize with primary after tests
        replica_synced=0
        for i in $(seq 1 60); do
            primary_lsn=$(psql postgres -p 5432 -tA -c "SELECT pg_current_wal_lsn();" 2>/dev/null || echo "N/A")
            replica_lsn=$(psql postgres -p 5433 -tA -c "SELECT pg_last_wal_replay_lsn();" 2>/dev/null || echo "N/A")
            if [ "$primary_lsn" != "N/A" ] && [ "$replica_lsn" != "N/A" ] && \
               [ "$primary_lsn" = "$replica_lsn" ]; then
                echo "Replica synchronized (primary: $primary_lsn, replica: $replica_lsn)"
                replica_synced=1
                break
            fi
            echo "Waiting for replica to synchronize... ($i/60): primary=$primary_lsn replica=$replica_lsn"
            sleep 1
        done
        if [ $replica_synced -eq 0 ]; then
            echo "ERROR: Replica failed to synchronize within 30 seconds"
            exit 1
        fi

        echo "=== Replica xid_meta ==="
        psql postgres -p 5433 -x -c "SELECT * FROM orioledb_get_xid_meta();" || true
        echo "=== Replica undo_meta ==="
        psql postgres -p 5433 -x -c "SELECT * FROM orioledb_get_undo_meta();" || true
        echo "=== Replica proc retain undo locations ==="
        psql postgres -p 5433 -c "SELECT * FROM orioledb_get_proc_retain_undo_locations();" || true

        echo "=== Checking xid_meta: nextXid == runXmin + 1 ==="
        xid_check=$(psql postgres -p 5433 -tA -c "SELECT nextxid = runxmin + 1 FROM orioledb_get_xid_meta();" 2>/dev/null || echo "error")
        if [ "$xid_check" != "t" ]; then
            echo "ERROR: nextXid != runXmin + 1"
            psql postgres -p 5433 -x -c "SELECT * FROM orioledb_get_xid_meta();" || true
            status=1
        fi

        echo "=== Checking undo_meta: lastUsedLocation == minProcRetainLocation ==="
        undo_check=$(psql postgres -p 5433 -tA -c "SELECT bool_and(lastusedlocation = minprocretainlocation) FROM orioledb_get_undo_meta();" 2>/dev/null || echo "error")
        if [ "$undo_check" != "t" ]; then
            echo "ERROR: lastUsedLocation != minProcRetainLocation for some undo type"
            psql postgres -p 5433 -x -c "SELECT * FROM orioledb_get_undo_meta();" || true
            status=1
        fi

        echo "=== Comparing primary and replica data ==="
        for db in regression isolation_regression; do
            if ! psql -p 5432 -tA -c "SELECT 1" "$db" >/dev/null 2>&1; then
                echo "Skipping $db: database does not exist"
                continue
            fi

            echo "--- Comparing $db schema ---"
            pg_dump -s -p 5432 "$db" | grep -v '^\\\(un\)\{0,1\}restrict' > "/tmp/primary_${db}_schema.sql"
            pg_dump -s -p 5433 "$db" | grep -v '^\\\(un\)\{0,1\}restrict' > "/tmp/replica_${db}_schema.sql"
            if ! diff -u "/tmp/primary_${db}_schema.sql" "/tmp/replica_${db}_schema.sql" > "dump_diff_${db}_schema.txt"; then
                echo "ERROR: $db schema differs between primary and replica"
                head -200 "dump_diff_${db}_schema.txt"
                status=1
            else
                echo "$db schema matches"
                rm -f "dump_diff_${db}_schema.txt"
            fi

            echo "--- Comparing $db data ---"
            # Build --exclude-table flags for unlogged tables
            exclude_flags=""
            for tbl in $(psql -p 5432 -tA -c "SELECT schemaname || '.' || tablename FROM pg_tables WHERE tableowner = current_user AND tablename IN (SELECT relname FROM pg_class WHERE relpersistence = 'u')" "$db" 2>/dev/null); do
                exclude_flags="$exclude_flags --exclude-table=$tbl"
            done
            pg_dump -a -Fd --compress=0 -p 5432 $exclude_flags -f "/tmp/primary_${db}_data" "$db"
            pg_dump -a -Fd --compress=0 -p 5433 $exclude_flags -f "/tmp/replica_${db}_data" "$db"
            python3 ../orioledb/ci/sort_dump.py "/tmp/primary_${db}_data" "/tmp/primary_${db}_sorted"
            python3 ../orioledb/ci/sort_dump.py "/tmp/replica_${db}_data" "/tmp/replica_${db}_sorted"
            if ! diff -ru "/tmp/primary_${db}_sorted" "/tmp/replica_${db}_sorted" > "dump_diff_${db}_data.txt"; then
                echo "ERROR: $db data differs between primary and replica"
                head -200 "dump_diff_${db}_data.txt"
                status=1
            else
                echo "$db data matches"
                rm -f "dump_diff_${db}_data.txt"
            fi
        done

        pg_ctl -D $GITHUB_WORKSPACE/pgsql/rep_pgdata -l rep_pg.log stop
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

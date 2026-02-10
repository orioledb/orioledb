#!/bin/bash

set -ex

cleanup() {
	pkill -KILL postgres || true
	pkill -KILL memcheck-amd64- || true
	echo "Killed postgres"
}

trap cleanup EXIT

source ~/scripts/vim.sh
source ~/python3-venv/bin/activate

############################################ Regular running of regress tests

TEST=ddl
TEST=tableam
PG_OUT=test/results/pg_$TEST.out
CUR_OUT=test/results/o_$TEST.out

# cat test/sql/$TEST.sql | \
# 	sed 's/USING orioledb/USING heap/' | \
# 	sed 's/WITH (orioledb_index=false)//' | \
# 	sed 's/orioledb_index=false,//' | \
# 	sed 's/orioledb_index = off,//' | \
# 	sed 's/ WITH (orioledb_index = off)//' | \
# 	sed 's/WITH (orioledb_index=true)//' | \
# 	sed 's/ WITH (index_bridging)//' | \
# 	sed 's/ALTER INDEX .* RESET (orioledb_index);//' | \
# 	sed 's/ALTER INDEX .* SET (orioledb_index = off);//' | \
# 	sed 's/ALTER TABLE .* SET (index_bridging);//' | \
# 	sed 's/SELECT orioledb_tbl_structure.*//' | \
# 	sed 's/SELECT orioledb_tbl_indices.*//' | \
# 	sed 's/SELECT orioledb_table_description.*//' | \
# 	cat >test/sql/pg_$TEST.sql
# touch test/expected/pg_$TEST.out
# 
# grep "test/sql/pg_$TEST.sql" .git/info/exclude || echo "test/sql/pg_$TEST.sql" >>.git/info/exclude || true
# grep "test/expected/pg_$TEST.out" .git/info/exclude || echo "test/expected/pg_$TEST.out" >>.git/info/exclude || true
# 
# cat test/sql/$TEST.sql | \
# 	# sed 's/WITH (orioledb_index=false)/WITH (orioledb_index=true)/' | \
# 	cat >test/sql/o_$TEST.sql
# touch test/expected/o_$TEST.out
# 
# grep "test/sql/o_$TEST.sql" .git/info/exclude || echo "test/sql/o_$TEST.sql" >>.git/info/exclude || true
# grep "test/expected/o_$TEST.out" .git/info/exclude || echo "test/expected/o_$TEST.out" >>.git/info/exclude || true

PATH="$HOME/pg17/bin:$PATH"
# PATH="$HOME/pg17_temp/bin:$PATH" # now main uses patches17_exclude commit
# PATH="$HOME/pg16/bin:$PATH"

# PG_SRC_PATH="../postgres-patches17_temp"
# pushd $PG_SRC_PATH

# configure with libunwind
# LDFLAGS="-lunwind" ./configure --enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix=/home/bdebribuh/pg17

# make -j $(nproc) install
# popd

pg_config --version
grep "PATCHSET" $(dirname $(pg_config --pgxs))/../Makefile.global
# exit

# make -j $(nproc) IS_DEV=1 USE_PGXS=1 clean
# make USE_PGXS=1 IS_DEV=1 -j$(nproc) install

############## valgrind
# make -j $(nproc) USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage -fprofile-update=atomic -flto"

############# check_page
make -j $(nproc) USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -DCHECK_PAGE_STRUCT -DCHECK_PAGE_STATS" install

# make USE_PGXS=1 IS_DEV=1 -j$(nproc) pgindent yapf
# make USE_PGXS=1 IS_DEV=1 -j$(nproc) yapf
# yapf -i ci/filter_regression_diff.py
# exit

# static
# sed -i.bak "s/ -Werror=unguarded-availability-new//g" $(dirname $(pg_config --pgxs))/../Makefile.global
# scan-build-18 --status-bugs \
# 		-disable-checker deadcode.DeadStores \
# 		make USE_PGXS=1 IS_DEV=1 USE_ASSERT_CHECKING=1 || status=$?

# make USE_PGXS=1 IS_DEV=1 regresscheck REGRESSCHECKS="pg_$TEST" || true
# make USE_PGXS=1 IS_DEV=1 regresscheck REGRESSCHECKS="o_$TEST" || true

# make USE_PGXS=1 IS_DEV=1 regresscheck REGRESSCHECKS="$TEST" || true

# make USE_PGXS=1 IS_DEV=1 regresscheck REGRESSCHECKS="btree_sys_check \
# 				alter_type \
# 				alter_storage \
# 				bitmap_scan \
# 				btree_compression \
# 				btree_print \
# 				createas \
# 				database \
# 				ddl \
# 				exclude \
# 				explain \
# 				fillfactor \
# 				foreign_keys \
# 				generated \
# 				getsomeattrs \
# 				index_bridging \
# 				indices \
# 				indices_build \
# 				inherits \
# 				ioc \
# 				joins \
# 				nulls \
# 				opclass \
# 				parallel_scan \
# 				partial \
# 				partition \
# 				primary_key \
# 				row_level_locks \
# 				row_security \
# 				sanitizers \
# 				stats \
# 				subquery \
# 				subtransactions \
# 				tableam" || true

# make USE_PGXS=1 IS_DEV=1 regresscheck || true

# make USE_PGXS=1 IS_DEV=1 testgrescheck_part_1 TESTGRESCHECKS_PART_1="test.t.ddl_test.DDLTest.test_1" || true

# rm -f pid-*.log
# make USE_PGXS=1 IS_DEV=1 regresscheck VALGRIND=1 REGRESSCHECKS="$TEST" || true

rm -rf test/tmp_check/data
rm -rf test/log/postmaster.log
initdb -N --encoding=UTF-8 --locale=C -D test/tmp_check/data
echo "shared_preload_libraries = 'orioledb'" >> test/tmp_check/data/postgresql.conf
echo "default_table_access_method = 'orioledb'" >> test/tmp_check/data/postgresql.conf
pg_ctl -D test/tmp_check/data -l test/log/postmaster.log start

# date +"%H:%M:%S"
# i=0
# while true; do 
# 	if [ $i -ge 50 ]; then
# 		break
# 	fi
# 	((++i))
# 	echo $i\)
# 	make USE_PGXS=1 IS_DEV=1 VALGRIND=1 testgrescheck_part_1 TESTGRESCHECKS_PART_1="test.t.replication_test.ReplicationTest.test_replication_root_eviction" && status=$?
# 	if [ $status -ne 0 ]; then
# 		break
# 	fi
# done

psql -d postgres >test/log/out.log 2>&1 <<EOF
CREATE EXTENSION IF NOT EXISTS orioledb;

DO \$$
DECLARE
    i integer;
BEGIN
    -- FOR i IN 1..100000 LOOP
    FOR i IN 1..1 LOOP

        -- Simple table with PK
        RAISE NOTICE 'Step: %', i;
        DROP TABLE IF EXISTS t1 CASCADE;
        -- IF (i > 128 AND i < 130) OR i > 570 THEN
        --     SET LOCAL log_error_verbosity = 'terse';
        -- END IF;
        CREATE TABLE t1 (id int PRIMARY KEY, val text) USING orioledb;
        INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
        INSERT INTO t1 SELECT v, v || 'WOAH' FROM generate_series(4, 100) v;
        RAISE NOTICE '%', orioledb_tbl_structure('t1'::regclass, 'bUCKSivo');
        RAISE NOTICE '%', orioledb_tbl_bin_structure('t1'::regclass, true);
        DROP TABLE t1;

    END LOOP;
END \$$;
EOF
pg_ctl -D test/tmp_check/data -l test/log/postmaster.log stop

# for f in $(find . -name 'pid-*.log') ; do
# 	if grep -q 'Command: [^ ]*/postgres' $f && grep -E -q '(Process terminating|ERROR SUMMARY: [1-9])' $f; then
# 		vimr $f
# 	fi
# done

# vimr $CUR_OUT
# vimr $PG_OUT

# vimdiffr $PG_OUT $CUR_OUT

# for file in $(find test/expected -name "$TEST.out" -o -name "${TEST}_[0-9].out"); do
# 	vimdiffr $file test/results/$TEST.out
# done

# vimdiffr test/expected/$TEST.out test/expected/${TEST}_1.out

############################################## Fix running asan for postgres

# TEST=toast
# REGRESS_TEST="REGRESSCHECKS='$TEST'"
# CUR_OUT=test/results/$TEST.out
# 
# # vimr $CUR_OUT
# 
# export ASAN_OPTIONS="detect_leaks=0" 
# UBSAN_OPTIONS="log_path=$PWD/ubsan.log:abort_on_error=1" ASAN_OPTIONS=$(cat <<-END
# verify_asan_link_order=0:
# detect_stack_use_after_return=0:
# detect_leaks=0:
# abort_on_error=1:
# disable_coredump=0:
# strict_string_checks=1:
# check_initialization_order=1:
# strict_init_order=1:
# detect_odr_violation=0:
# log_path=$PWD/asan.log:
# max_uar_stack_size_log=25:
# END
# ) make USE_PGXS=1 IS_DEV=1 -j $(nproc) regresscheck $REGRESS_TEST CFLAGS_SL="$(pg_config --cflags_sl) -Werror -fsanitize=undefined" LDFLAGS_SL="-fsanitize=undefined"
# exit

############################################ Running of regress tests from pg17_temp

# pushd $PG_SRC_PATH
# cd src/test/subscription
# make installcheck TEMP_CONFIG="./orioledb.conf" PG_TEST_INITDB_EXTRA_OPTS="--locale=C" PROVE_TESTS="t/013_partition.pl"

# rm -rf test/tmp_check/data
# initdb -N --encoding=UTF-8 --locale=C -D test/tmp_check/data
# echo "shared_preload_libraries = 'orioledb'" >> test/tmp_check/data/postgresql.conf
# echo "default_table_access_method = 'orioledb'" >> test/tmp_check/data/postgresql.conf
# echo "orioledb.strict_mode = true" >> test/tmp_check/data/postgresql.conf
# echo "orioledb.strict_mode = false" >> test/tmp_check/data/postgresql.conf
# pg_ctl -D test/tmp_check/data -l test/log/postmaster.log start
# 
# pushd $PG_SRC_PATH
# make -C src/test/regress installcheck-oriole -j $(nproc) || true

# cd src/test/regress
# make installcheck-tests EXTRA_TESTS="--load-extension=orioledb" TESTS="test_setup create_index alter_table" -j $(nproc) || status=$?

# popd
# pg_ctl -D test/tmp_check/data -l test/log/postmaster.log stop

# python3 ci/filter_regression_diff.py --diff $PG_SRC_PATH/src/test/regress/regression.diffs

# vimdiffr $PG_SRC_PATH/src/test/regress/expected/create_index.out $PG_SRC_PATH/src/test/regress/results/create_index.out 



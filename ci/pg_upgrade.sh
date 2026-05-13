#!/bin/bash
#
# pg_upgrade smoke test: populate a PG16 cluster with orioledb data,
# upgrade in-place to PG17, then verify that
#
#   1. pg_upgrade itself runs (with --check and the real run),
#   2. a pg_dump of the upgraded cluster matches the pre-upgrade dump,
#   3. the orioledb regression suite still runs on the upgraded cluster.

set -eux

OLD_PREFIX="$GITHUB_WORKSPACE/pgsql16"
NEW_PREFIX="$GITHUB_WORKSPACE/pgsql17"
OLD_DATA="$OLD_PREFIX/pgdata"
NEW_DATA="$NEW_PREFIX/pgdata"
PORT_OLD=5432
PORT_NEW=5433
TEST_DB=upgrade_test

write_pg_conf() {
	local data=$1
	cat >> "$data/postgresql.conf" <<EOF
wal_level = logical
shared_preload_libraries = 'orioledb'
default_table_access_method = 'orioledb'
EOF
}

# ----------------------------------------------------------------------
# 1. Initialize and start the PG16 cluster with orioledb preloaded.
# ----------------------------------------------------------------------
"$OLD_PREFIX/bin/initdb" -N --encoding=UTF-8 --locale=C -D "$OLD_DATA"
write_pg_conf "$OLD_DATA"
"$OLD_PREFIX/bin/pg_ctl" -D "$OLD_DATA" \
	-o "-p $PORT_OLD" -l "$GITHUB_WORKSPACE/pg16.log" -w start

# ----------------------------------------------------------------------
# 2. Populate the cluster.
#
#    First create a deterministic database with a few orioledb tables
#    so the pre/post dump diff is meaningful even if the heavier
#    regression suite is noisy.  Then run the orioledb-aware
#    regression schedule from PG16's tree to add the "real" workload.
#    Individual regression diffs are not fatal here -- we only care
#    that the server stays alive and ends with the populated state.
# ----------------------------------------------------------------------
"$OLD_PREFIX/bin/psql" -p $PORT_OLD -d postgres -v ON_ERROR_STOP=1 <<EOF
CREATE DATABASE $TEST_DB;
EOF

"$OLD_PREFIX/bin/psql" -p $PORT_OLD -d $TEST_DB -v ON_ERROR_STOP=1 <<'EOF'
CREATE EXTENSION orioledb;

CREATE TABLE numbers (id int PRIMARY KEY, val text) USING orioledb;
INSERT INTO numbers SELECT i, 'v_' || i FROM generate_series(1, 10000) i;
CREATE INDEX numbers_val_idx ON numbers (val);

CREATE TABLE composite (
	a int,
	b int,
	c text,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO composite SELECT i / 100, i % 100, md5(i::text)
                      FROM generate_series(1, 10000) i;

CHECKPOINT;
EOF

(
	cd "$GITHUB_WORKSPACE/postgresql16/src/test/regress"
	PATH="$OLD_PREFIX/bin:$PATH" PGPORT=$PORT_OLD \
		make installcheck-oriole \
		EXTRA_REGRESS_OPTS="--load-extension=orioledb" \
		-j "$(nproc)" || \
		echo "[WARN] some installcheck-oriole tests failed in PG16; continuing"
)

# ----------------------------------------------------------------------
# 2b. Repair tablespace directory orphans before pg_upgrade --check.
#
#     The regression suite creates tablespaces both via pg_regress's
#     temp-dir substitution (LOCATION '@testtablespace@') and via
#     allow_in_place_tablespaces (LOCATION '').  When tests fail
#     mid-way -- which we explicitly tolerate above -- pg_regress's
#     temp directory may be torn down while the pg_tablespace row and
#     its pg_tblspc/<oid> symlink remain; for the in-place case the
#     reverse can happen.  Either way pg_upgrade --check refuses to
#     proceed with "tablespace directory does not exist".
#
#     Recreate an empty backing directory for each such orphan.  The
#     regression DB itself stays intact so pg_upgrade actually has
#     non-trivial workload to upgrade.  Any tables left in the
#     half-broken tablespaces are already unusable -- their data files
#     are gone with the directory -- so an empty stub is no regression.
# ----------------------------------------------------------------------
ts_oids=$("$OLD_PREFIX/bin/psql" -p $PORT_OLD -d postgres -tA -c \
	"SELECT oid FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')")
for oid in $ts_oids; do
	[ -n "$oid" ] || continue
	dir="$OLD_DATA/pg_tblspc/$oid"
	if [ ! -e "$dir" ]; then
		# Handles missing entry AND dangling symlink (-e is false for both).
		rm -f "$dir"
		mkdir -p "$dir"
		echo "Recreated missing tablespace dir: pg_tblspc/$oid"
	fi
done

# ----------------------------------------------------------------------
# 3. Dump the pre-upgrade state.  Use PG17's pg_dump for both pre and
#    post so the dump format is stable; pg_dump is forward-compatible
#    to older servers.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/pg_dump" -p $PORT_OLD -d $TEST_DB \
	--schema-only -f pre_upgrade_schema.sql
"$NEW_PREFIX/bin/pg_dump" -p $PORT_OLD -d $TEST_DB \
	--data-only -f pre_upgrade_data.sql

# ----------------------------------------------------------------------
# 4. Stop PG16.
# ----------------------------------------------------------------------
"$OLD_PREFIX/bin/pg_ctl" -D "$OLD_DATA" -m fast -w stop

# ----------------------------------------------------------------------
# 5. Initialize a fresh PG17 cluster with matching config.  pg_upgrade
#    expects the new cluster to be initdb'd but not started.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/initdb" -N --encoding=UTF-8 --locale=C -D "$NEW_DATA"
write_pg_conf "$NEW_DATA"

# ----------------------------------------------------------------------
# 6. pg_upgrade --check first to surface incompatibilities cleanly,
#    then the real upgrade.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/pg_upgrade" --check \
	-b "$OLD_PREFIX/bin" \
	-B "$NEW_PREFIX/bin" \
	-d "$OLD_DATA" \
	-D "$NEW_DATA" \
	-p $PORT_OLD \
	-P $PORT_NEW

"$NEW_PREFIX/bin/pg_upgrade" \
	-b "$OLD_PREFIX/bin" \
	-B "$NEW_PREFIX/bin" \
	-d "$OLD_DATA" \
	-D "$NEW_DATA" \
	-p $PORT_OLD \
	-P $PORT_NEW

# ----------------------------------------------------------------------
# 7. Start the upgraded PG17 cluster.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/pg_ctl" -D "$NEW_DATA" \
	-o "-p $PORT_NEW" -l "$GITHUB_WORKSPACE/pg17.log" -w start

# ----------------------------------------------------------------------
# 8. Dump the post-upgrade state with the same pg_dump binary.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/pg_dump" -p $PORT_NEW -d $TEST_DB \
	--schema-only -f post_upgrade_schema.sql
"$NEW_PREFIX/bin/pg_dump" -p $PORT_NEW -d $TEST_DB \
	--data-only -f post_upgrade_data.sql

# ----------------------------------------------------------------------
# 9. Diff pre vs post dumps.  Any difference for the deterministic
#    $TEST_DB database is a failure.
# ----------------------------------------------------------------------
status=0
if ! diff -u pre_upgrade_schema.sql post_upgrade_schema.sql > schema_diff.txt; then
	echo "ERROR: $TEST_DB schema differs after pg_upgrade"
	head -200 schema_diff.txt
	status=1
else
	rm -f schema_diff.txt
fi
if ! diff -u pre_upgrade_data.sql post_upgrade_data.sql > data_diff.txt; then
	echo "ERROR: $TEST_DB data differs after pg_upgrade"
	head -200 data_diff.txt
	status=1
else
	rm -f data_diff.txt
fi

# ----------------------------------------------------------------------
# 10. Verify the upgraded cluster is functional by running PG17's
#     orioledb-aware regression schedule.  This drops/recreates the
#     `regression` database, which is fine -- $TEST_DB is left intact.
# ----------------------------------------------------------------------
(
	cd "$GITHUB_WORKSPACE/postgresql17/src/test/regress"
	PATH="$NEW_PREFIX/bin:$PATH" PGPORT=$PORT_NEW \
		make installcheck-oriole \
		EXTRA_REGRESS_OPTS="--load-extension=orioledb" \
		-j "$(nproc)" || \
		{ echo "ERROR: regression suite failed on upgraded cluster"; status=1; }
)

"$NEW_PREFIX/bin/pg_ctl" -D "$NEW_DATA" -m fast -w stop

exit $status

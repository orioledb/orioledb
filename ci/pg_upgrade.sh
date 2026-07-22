#!/bin/bash
#
# pg_upgrade smoke test: populate a PG_OLD_VERSION cluster with orioledb
# data, upgrade in-place to PG_NEW_VERSION (both set by the CI matrix;
# default 16 -> 17), then verify that
#
#   1. pg_upgrade itself runs (with --check and the real run),
#   2. a pg_dump of the upgraded $TEST_DB matches the pre-upgrade dump
#      (schema and data),
#   3. pg_upgrade preserves every row of the regression database too
#      (data-only; see note in step 3),
#   4. the orioledb regression suite still runs on the upgraded cluster.

set -eux

# Which major versions we migrate from/to (set by the CI matrix; default to
# the original 16 -> 17 pair so the script still runs standalone).
OLD_VERSION="${PG_OLD_VERSION:-16}"
NEW_VERSION="${PG_NEW_VERSION:-17}"
OLD_PREFIX="$GITHUB_WORKSPACE/pgsql${OLD_VERSION}"
NEW_PREFIX="$GITHUB_WORKSPACE/pgsql${NEW_VERSION}"
OLD_DATA="$OLD_PREFIX/pgdata"

# PG18 turns data checksums on by default in initdb; PG16/17 default to off
# and don't accept --no-data-checksums.  pg_upgrade requires the old and new
# clusters to agree, so force checksums off wherever the flag is available
# (>= 18) to match the older default on both ends.
OLD_INITDB_CHECKSUMS=""
NEW_INITDB_CHECKSUMS=""
[ "$OLD_VERSION" -ge 18 ] && OLD_INITDB_CHECKSUMS="--no-data-checksums"
[ "$NEW_VERSION" -ge 18 ] && NEW_INITDB_CHECKSUMS="--no-data-checksums"
NEW_DATA="$NEW_PREFIX/pgdata"
PORT_OLD=5432
PORT_NEW=5433
TEST_DB=upgrade_test
SORT_DUMP="$GITHUB_WORKSPACE/orioledb/ci/sort_dump.py"

# Capture core dumps produced by any backend during the test (mirrors
# ci/check.sh), so a crash is diagnosable instead of just showing up as a
# failed regression test.
CORE_DIR="/tmp/cores-${GITHUB_SHA:-pg_upgrade}-${TIMESTAMP:-0}"
ulimit -c unlimited -S || true
mkdir -p "$CORE_DIR"
sudo sh -c "echo '$CORE_DIR/%t_%p.core' > /proc/sys/kernel/core_pattern" || true

# Dump backtraces for any postgres core left in CORE_DIR and return
# non-zero if at least one postgres core was found.  Non-postgres cores
# (shell/cp helpers killed during cleanup) are reported but ignored.
collect_core_dumps() {
	local cores corefile binary found=0
	cores=$(find "$CORE_DIR" -name '*.core' 2>/dev/null)
	[ -n "$cores" ] || return 0
	for corefile in $cores; do
		binary=$(gdb -quiet -core "$corefile" -batch -ex 'info auxv' 2>/dev/null \
			| grep AT_EXECFN | perl -pe 's/^.*"(.*)"$/$1/g')
		if [ "${binary##*/}" != "postgres" ]; then
			echo "Skipping non-postgres core $corefile (binary: ${binary:-unknown})"
			continue
		fi
		echo "======== core dump $corefile ($binary)"
		gdb --batch --quiet -x "$GITHUB_WORKSPACE/orioledb/ci/cmds.gdb" "$binary" "$corefile" || true
		found=1
	done
	return $found
}

# Print core-dump backtraces on any exit -- including the `set -e` exit taken
# when a backend crashes mid-test (which otherwise surfaces only as "server
# closed the connection unexpectedly" with no cause).  This mirrors the check
# workflow's post-run core inspection; the short sleep lets the kernel finish
# writing the core before we read it.  The original exit code is preserved.
dump_cores_on_exit() {
	local rc=$?
	sleep 2
	collect_core_dumps || true
	exit $rc
}
trap dump_cores_on_exit EXIT

write_pg_conf() {
	local data=$1
	cat >> "$data/postgresql.conf" <<EOF
wal_level = logical
shared_preload_libraries = 'orioledb'
default_table_access_method = 'orioledb'
EOF
}

# Does a database exist and accept connections?  Always uses PG17's psql
# (forward-compatible to the PG16 server) so callers don't have to care
# which cluster is up.
db_exists() {		# port db
	"$NEW_PREFIX/bin/psql" -p "$1" -tAc "SELECT 1" "$2" >/dev/null 2>&1
}

# pg_upgrade writes the per-database dump/restore failures into
# pg_upgrade_output.d/<ts>/log/*.log and prints only the path.  Echo
# those logs to stdout so the cause is visible directly in CI output.
dump_pg_upgrade_logs() {
	echo "=== pg_upgrade failed; dumping pg_upgrade_output.d logs ==="
	find "$NEW_DATA/pg_upgrade_output.d" -type f \( -name '*.log' -o -name '*.txt' \) 2>/dev/null \
		| sort | while read -r f; do
			echo "----- $f -----"
			cat "$f"
		done
}

# Dump one database's schema to a normalized .sql file.  PG17's pg_dump
# is used for every dump (pre and post) so the format is identical; the
# leading \restrict / \unrestrict psql meta-commands carry a per-dump
# nonce, and the "Dumped from database version" header reflects the
# source server (16 pre-upgrade vs 17 post-upgrade), so strip both before
# comparing.
dump_schema() {		# port db outfile
	"$NEW_PREFIX/bin/pg_dump" -s -p "$1" "$2" \
		| grep -v '^\\\(un\)\{0,1\}restrict' \
		| grep -v '^-- Dumped from database version ' > "$3"
}

# Dump one database's data as a row-sorted directory tree for a
# deterministic diff (mirrors the primary/replica comparison in
# check.sh).  Unlogged table contents are not preserved across
# pg_upgrade, so exclude them.  Returns non-zero if either dump step
# fails so best-effort callers can skip a broken database.
dump_data() {		# port db out_sorted_dir
	local port=$1 db=$2 out=$3 raw="$3.raw" exclude="" tbl
	rm -rf "$raw" "$out"
	for tbl in $("$NEW_PREFIX/bin/psql" -p "$port" -tAc \
		"SELECT schemaname || '.' || tablename FROM pg_tables WHERE tablename IN (SELECT relname FROM pg_class WHERE relpersistence = 'u')" \
		"$db" 2>/dev/null); do
		exclude="$exclude --exclude-table=$tbl"
	done
	"$NEW_PREFIX/bin/pg_dump" -a -Fd --compress=0 -p "$port" $exclude -f "$raw" "$db" || return 1
	PATH="$NEW_PREFIX/bin:$PATH" python3 "$SORT_DUMP" "$raw" "$out" || return 1
}

# ----------------------------------------------------------------------
# 1. Initialize and start the PG16 cluster with orioledb preloaded.
# ----------------------------------------------------------------------
"$OLD_PREFIX/bin/initdb" -N --encoding=UTF-8 --locale=C $OLD_INITDB_CHECKSUMS -D "$OLD_DATA"
write_pg_conf "$OLD_DATA"
"$OLD_PREFIX/bin/pg_ctl" -D "$OLD_DATA" \
	-o "-p $PORT_OLD" -l "$GITHUB_WORKSPACE/pg${OLD_VERSION}.log" -w start

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
-- Expression + partial index: its node trees are version-specific, so a
-- cross-major upgrade must rewrite them (see step 7a / 7b).
CREATE INDEX numbers_expr_idx ON numbers ((val || '_x')) WHERE id > 5;

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
	cd "$GITHUB_WORKSPACE/postgresql${OLD_VERSION}/src/test/regress"
	PATH="$OLD_PREFIX/bin:$PATH" PGPORT=$PORT_OLD \
		make installcheck-oriole \
		EXTRA_REGRESS_OPTS="--load-extension=orioledb" \
		-j "$(nproc)" || \
		echo "[WARN] some installcheck-oriole tests failed in PG16; continuing"
)

# ----------------------------------------------------------------------
# 2b. Move every user tablespace's contents back to pg_default and drop
#     the tablespace before pg_upgrade --check.
#
#     pg_upgrade cannot migrate in-place tablespaces (LOCATION ''): for
#     those pg_tablespace_location() returns a *relative* path such as
#     "pg_tblspc/16577", which pg_upgrade stat()s against its own CWD
#     rather than the data directory, so --check dies with "tablespace
#     directory does not exist".  The regression suite leaves such
#     tablespaces behind, especially when it aborts mid-way (which we
#     tolerate above).
#
#     Relocating the objects to pg_default keeps their data -- so it is
#     still upgraded and verified below -- and lets us drop the
#     un-migratable tablespace.  A tablespace whose backing storage was
#     already torn down can't be relocated; for those we fall back to
#     recreating an empty stub directory so --check can still proceed
#     (their data is already gone).
# ----------------------------------------------------------------------
ts_names=$("$OLD_PREFIX/bin/psql" -p $PORT_OLD -d postgres -tA -c \
	"SELECT spcname FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')")
dbs=$("$OLD_PREFIX/bin/psql" -p $PORT_OLD -d postgres -tA -c \
	"SELECT datname FROM pg_database WHERE datallowconn AND datname <> 'template0'")
for ts in $ts_names; do
	[ -n "$ts" ] || continue
	for db in $dbs; do
		"$OLD_PREFIX/bin/psql" -p $PORT_OLD -d "$db" -v ON_ERROR_STOP=0 -q <<EOF
ALTER TABLE ALL IN TABLESPACE "$ts" SET TABLESPACE pg_default;
ALTER INDEX ALL IN TABLESPACE "$ts" SET TABLESPACE pg_default;
ALTER MATERIALIZED VIEW ALL IN TABLESPACE "$ts" SET TABLESPACE pg_default;
EOF
	done
	if "$OLD_PREFIX/bin/psql" -p $PORT_OLD -d postgres -v ON_ERROR_STOP=1 \
			-c "DROP TABLESPACE \"$ts\";"; then
		echo "Relocated contents to pg_default and dropped tablespace: $ts"
	else
		echo "[WARN] could not drop tablespace $ts; will stub its directory"
	fi
done

# Safety net: any tablespace we could not drop still needs a backing
# directory or pg_upgrade --check refuses to run.
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
# 2c. Strip references to test-only loadable libraries from the
#     regression database before pg_upgrade --check.
#
#     The regression suite defines many C functions backed by build-tree
#     test libraries (regress.so, refint, autoinc, ...).  pg_upgrade's
#     "presence of required libraries" check tries to LOAD each library
#     referenced by the old cluster on the new PG17 server; those
#     PG16-ABI / not-installed libraries are not loadable there, so
#     --check aborts.  They are pure PG-core test scaffolding, unrelated
#     to orioledb, and cannot be migrated across versions.
#
#     Drop every user-defined C function that is not part of an extension
#     (and whatever depends on it).  orioledb's own functions and any
#     installed contrib are extension members, so they are kept -- their
#     libraries load fine on the new cluster.  This runs before the
#     pre-upgrade dump, so the pre/post data comparison is unaffected.
# ----------------------------------------------------------------------
if db_exists $PORT_OLD regression; then
	"$OLD_PREFIX/bin/psql" -p $PORT_OLD -d regression -v ON_ERROR_STOP=1 <<'EOF'
DO $$
DECLARE
	sigs text[];
	sig text;
BEGIN
	-- Materialize all signatures up front, while every function still
	-- exists: a lazily-fetched regprocedure renders as a bare OID once a
	-- prior CASCADE has dropped it, which would make DROP ROUTINE a
	-- syntax error rather than a catchable "does not exist".
	SELECT array_agg(p.oid::regprocedure::text)
	INTO sigs
	FROM pg_proc p
	JOIN pg_language l ON l.oid = p.prolang
	WHERE l.lanname = 'c'
	  AND p.oid >= 16384			-- FirstNormalObjectId: skip built-ins
	  AND NOT EXISTS (SELECT 1 FROM pg_depend d
					  WHERE d.classid = 'pg_proc'::regclass
						AND d.objid = p.oid
						AND d.deptype = 'e');

	IF sigs IS NULL THEN
		RETURN;
	END IF;

	FOREACH sig IN ARRAY sigs
	LOOP
		BEGIN
			EXECUTE format('DROP ROUTINE %s CASCADE', sig);
		EXCEPTION
			WHEN undefined_function OR undefined_object THEN
				-- already removed by an earlier CASCADE
				NULL;
		END;
	END LOOP;
END
$$;
EOF
fi

# ----------------------------------------------------------------------
# 3. Dump the pre-upgrade state while PG16 is still up.
#
#    $TEST_DB is deterministic, so we compare both its schema and its
#    data.  For the regression database we compare data only: a raw
#    cross-version schema dump is not directly comparable (PG17's
#    pg_dump renders a PG16 server differently from a PG17 server, which
#    PG core reconciles with AdjustUpgrade -- machinery we don't
#    replicate here), so instead we verify that pg_upgrade preserves
#    every row.  The regression DB may also be left in a half-broken
#    state by a tolerated mid-suite failure, so its comparison is
#    best-effort: if a dump fails we skip it rather than fail the job.
# ----------------------------------------------------------------------
dump_schema $PORT_OLD "$TEST_DB" pre_upgrade_schema.sql
dump_data   $PORT_OLD "$TEST_DB" pre_upgrade_data

REGRESSION_CHECKED=0
if db_exists $PORT_OLD regression; then
	if dump_data $PORT_OLD regression pre_regression_data; then
		REGRESSION_CHECKED=1
	else
		echo "[WARN] pre-upgrade data dump of regression failed; skipping its comparison"
	fi
fi

# ----------------------------------------------------------------------
# 4. Stop PG16.
# ----------------------------------------------------------------------
"$OLD_PREFIX/bin/pg_ctl" -D "$OLD_DATA" -m fast -w stop

# ----------------------------------------------------------------------
# 5. Initialize a fresh PG17 cluster with matching config.  pg_upgrade
#    expects the new cluster to be initdb'd but not started.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/initdb" -N --encoding=UTF-8 --locale=C $NEW_INITDB_CHECKSUMS -D "$NEW_DATA"
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
	-P $PORT_NEW || { dump_pg_upgrade_logs; exit 1; }

"$NEW_PREFIX/bin/pg_upgrade" \
	-b "$OLD_PREFIX/bin" \
	-B "$NEW_PREFIX/bin" \
	-d "$OLD_DATA" \
	-D "$NEW_DATA" \
	-p $PORT_OLD \
	-P $PORT_NEW || { dump_pg_upgrade_logs; exit 1; }

# ----------------------------------------------------------------------
# 6b. Migrate OrioleDB storage.
#
#     pg_upgrade only transfers the per-relation heap files it knows about
#     from pg_class.  OrioleDB keeps its table/index data in its own
#     orioledb_data/ tree (plus undo logs in orioledb_undo/), which
#     pg_upgrade never touches -- so without this the upgraded tables come
#     up empty.
#
#     pg_upgrade preserves database OIDs, relation OIDs and relfilenodes,
#     and OrioleDB names its files by (datoid, relnode), so the old
#     cluster's files are valid as-is in the new cluster.  Both clusters
#     are stopped here and the old one was shut down cleanly (fully
#     checkpointed), so a wholesale copy needs no WAL replay.
# ----------------------------------------------------------------------
rm -rf "$NEW_DATA/orioledb_data" "$NEW_DATA/orioledb_undo"
cp -R "$OLD_DATA/orioledb_data" "$NEW_DATA/orioledb_data"
if [ -d "$OLD_DATA/orioledb_undo" ]; then
	cp -R "$OLD_DATA/orioledb_undo" "$NEW_DATA/orioledb_undo"
fi

# ----------------------------------------------------------------------
# 7. Start the upgraded PG17 cluster.
# ----------------------------------------------------------------------
"$NEW_PREFIX/bin/pg_ctl" -D "$NEW_DATA" \
	-o "-p $PORT_NEW" -l "$GITHUB_WORKSPACE/pg${NEW_VERSION}.log" -w start

# ----------------------------------------------------------------------
# 7a. Before anything triggers the automatic refresh, confirm that a
#     plain read of an index whose expression trees were written by the old
#     major raises a clean error instead of crashing.  A bare SELECT does not
#     go through ProcessUtility, so it does not trip the automatic refresh
#     (step 7b) and still exercises the read-time fallback guard.  Only
#     meaningful across majors; same-version carries compatible trees.
# ----------------------------------------------------------------------
if [ "$OLD_VERSION" != "$NEW_VERSION" ] && db_exists $PORT_NEW "$TEST_DB"; then
	set +e
	refresh_err=$("$NEW_PREFIX/bin/psql" -p $PORT_NEW -d "$TEST_DB" -v ON_ERROR_STOP=1 \
		-c "SELECT count(*) FROM numbers;" 2>&1)
	refresh_rc=$?
	set -e
	if [ $refresh_rc -eq 0 ]; then
		echo "ERROR: expression-index table was readable before the automatic refresh"
		echo "$refresh_err"
		exit 1
	fi
	if ! printf '%s' "$refresh_err" | grep -q "orioledb_upgrade_refresh"; then
		echo "ERROR: accessing an un-refreshed expression index did not raise the expected error:"
		echo "$refresh_err"
		exit 1
	fi
	if ! db_exists $PORT_NEW "$TEST_DB"; then
		echo "ERROR: server is down after accessing an un-refreshed index (crash, not error)"
		exit 1
	fi
	echo "OK: un-refreshed expression index raises a clean error and the server stays up"
fi

# ----------------------------------------------------------------------
# 7b. Exercise the AUTOMATIC refresh.  A cross-major upgrade carries over
#     expression/predicate node trees in the old major's nodeToString format,
#     which the new server cannot read; OrioleDB rewrites them into the running
#     server's format on the first utility command in each database
#     (maybe_auto_upgrade_refresh), so no manual orioledb_upgrade_refresh() call
#     is required.  Issue one trivial utility statement to trigger it, then
#     confirm the expression-index table that erred in step 7a is now readable.
#
#     $TEST_DB is fully controlled by this script, so a failure there is a real
#     bug -- fail the job immediately.  The regression database can be left
#     half-broken by a tolerated mid-suite failure (see step 2), so it stays
#     best-effort, but we still surface the actual error.
# ----------------------------------------------------------------------
if db_exists $PORT_NEW "$TEST_DB"; then
	"$NEW_PREFIX/bin/psql" -p $PORT_NEW -d "$TEST_DB" -v ON_ERROR_STOP=1 <<'SQL'
DISCARD ALL;					-- utility command: triggers the automatic refresh
SELECT count(*) FROM numbers;	-- erred in step 7a; must succeed after the refresh
SQL
	echo "Auto-refreshed OrioleDB expressions in $TEST_DB"
fi

if db_exists $PORT_NEW regression; then
	refresh_out=$("$NEW_PREFIX/bin/psql" -p $PORT_NEW -d regression -v ON_ERROR_STOP=1 \
		-c "DISCARD ALL;" 2>&1) \
		&& echo "Auto-refresh triggered in regression" \
		|| echo "[WARN] triggering auto-refresh in regression failed: $refresh_out"
fi

# ----------------------------------------------------------------------
# 8. Dump the post-upgrade state.  This must happen before step 10's
#    regression run, which drops and recreates the regression database.
# ----------------------------------------------------------------------
dump_schema $PORT_NEW "$TEST_DB" post_upgrade_schema.sql
dump_data   $PORT_NEW "$TEST_DB" post_upgrade_data

if [ "$REGRESSION_CHECKED" = 1 ]; then
	if ! dump_data $PORT_NEW regression post_regression_data; then
		echo "[WARN] post-upgrade data dump of regression failed; skipping its comparison"
		REGRESSION_CHECKED=0
	fi
fi

# ----------------------------------------------------------------------
# 9. Diff pre vs post dumps.  Any difference is a failure.
# ----------------------------------------------------------------------
status=0
if ! diff -u pre_upgrade_schema.sql post_upgrade_schema.sql > schema_diff.txt; then
	echo "ERROR: $TEST_DB schema differs after pg_upgrade"
	head -200 schema_diff.txt
	status=1
else
	rm -f schema_diff.txt
fi
if ! diff -ru pre_upgrade_data post_upgrade_data > data_diff.txt; then
	echo "ERROR: $TEST_DB data differs after pg_upgrade"
	head -200 data_diff.txt
	status=1
else
	rm -f data_diff.txt
fi
if [ "$REGRESSION_CHECKED" = 1 ]; then
	if ! diff -ru pre_regression_data post_regression_data > data_diff_regression.txt; then
		echo "ERROR: regression data differs after pg_upgrade"
		head -200 data_diff_regression.txt
		status=1
	else
		echo "regression data matches after pg_upgrade"
		rm -f data_diff_regression.txt
	fi
fi

# ----------------------------------------------------------------------
# 9b. Exercise the catalog-free paths that actually crash if the cross-major
#     cache handling regresses.  A foreground query alone would not: the
#     checkpointer and crash recovery read OrioleDB's version-dependent caches
#     (class cache tuple descriptors, database-cache locale) WITHOUT a catalog,
#     via the syscache hook, and that is exactly where a carried-over
#     old-major entry blows up.  So drive both explicitly on $TEST_DB (it has
#     an expression + partial index, whose descriptor fill reads those caches):
#
#       1. CHECKPOINT   -> checkpointer fills index descriptors.
#       2. INSERT into the expression-index table, then crash (-m immediate)
#          and restart -> recovery must REPLAY that insert, rebuilding the
#          expression-index tuple (reading the class + database caches) with no
#          catalog of its own.  This is the scenario that asserted in
#          o_class_cache_deserialize_entry / "default locale not initialized"
#          before the fix.
#
#     This runs AFTER the pre/post data comparison (step 9) on purpose: it
#     mutates $TEST_DB (the INSERT below), so running it earlier would make the
#     post-upgrade dump differ from the pre-upgrade baseline.  A crash here is
#     still caught by the core-dump check in step 11.
#
#     Cross-major only; a same-version upgrade carries compatible caches.
# ----------------------------------------------------------------------
if [ "$OLD_VERSION" != "$NEW_VERSION" ] && db_exists $PORT_NEW "$TEST_DB"; then
	"$NEW_PREFIX/bin/psql" -p $PORT_NEW -d "$TEST_DB" -v ON_ERROR_STOP=1 <<'SQL'
CHECKPOINT;
INSERT INTO numbers SELECT i, 'v_' || i FROM generate_series(1000001, 1000050) i;
SQL
	# Crash without a clean shutdown so recovery has to replay the insert.
	"$NEW_PREFIX/bin/pg_ctl" -D "$NEW_DATA" -m immediate stop
	"$NEW_PREFIX/bin/pg_ctl" -D "$NEW_DATA" \
		-o "-p $PORT_NEW" -l "$GITHUB_WORKSPACE/pg${NEW_VERSION}.log" -w start
	if ! db_exists $PORT_NEW "$TEST_DB"; then
		echo "ERROR: server did not come up after crash recovery of an expression-index insert"
		exit 1
	fi
	rowcount=$("$NEW_PREFIX/bin/psql" -p $PORT_NEW -d "$TEST_DB" -tAc \
		"SELECT count(*) FROM numbers WHERE (val || '_x') = 'v_1000001_x'")
	if [ "$rowcount" != "1" ]; then
		echo "ERROR: expression index wrong after crash recovery (got '$rowcount', want 1)"
		exit 1
	fi
	echo "OK: checkpoint + crash recovery of an expression-index insert survived"
fi

# ----------------------------------------------------------------------
# 10. Verify the upgraded cluster is functional by running PG17's
#     orioledb-aware regression schedule.  This drops/recreates the
#     `regression` database, which is fine -- $TEST_DB is left intact
#     and the regression data comparison above already ran.
#
#     The schedule is run only as a smoke test that the upgraded binary
#     stays alive.  Individual regression diffs are expected (the same
#     orioledb-vs-heap drift the main CI filters, and which step 2
#     tolerates on PG16), so they are not fatal here; an actual crash is
#     caught by the core-dump check below instead.
# ----------------------------------------------------------------------
(
	cd "$GITHUB_WORKSPACE/postgresql${NEW_VERSION}/src/test/regress"
	PATH="$NEW_PREFIX/bin:$PATH" PGPORT=$PORT_NEW \
		make installcheck-oriole \
		EXTRA_REGRESS_OPTS="--load-extension=orioledb" \
		-j "$(nproc)" || \
		echo "[WARN] some installcheck-oriole tests failed on the upgraded cluster; continuing (diffs expected, crashes caught below)"
)

"$NEW_PREFIX/bin/pg_ctl" -D "$NEW_DATA" -m fast -w stop

# ----------------------------------------------------------------------
# 11. Fail if any backend dumped core at any point during the test.
# ----------------------------------------------------------------------
if ! collect_core_dumps; then
	echo "ERROR: a postgres backend dumped core during the pg_upgrade test"
	status=1
fi

exit $status

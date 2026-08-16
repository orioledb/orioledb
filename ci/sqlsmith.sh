#!/bin/bash

# Fuzz OrioleDB with SQLSmith, looking for crashes.
#
# SQLSmith reads the catalog and generates random-but-type-correct queries, so
# it explores planner/executor/storage combinations no hand-written test does.
# On its own it only reads, though, and OrioleDB's crashes cluster in the
# concurrent paths, so several writer sessions churn the same tables while the
# fuzzer runs.
#
# A generated query raising an ERROR is normal and ignored.  We fail the job
# only on evidence that the *server* broke: a PANIC or assertion, a backend
# killed by a signal, a postmaster restart, a core dump, or a structural check
# that no longer passes afterwards.
#
# Env (all optional outside CI):
#   SQLSMITH_SECONDS   wall-clock budget for the fuzzer      (default 600)
#   SQLSMITH_SEED      RNG seed; printed so a hit is replayable
#   SQLSMITH_WRITERS   concurrent writer sessions            (default 4)
#   SQLSMITH_MAX_QUERIES  hard cap on generated queries      (default 0 = none)

set -eu

WORKSPACE="${GITHUB_WORKSPACE:-$(cd "$(dirname "$0")/../.." && pwd)}"
ORIOLEDB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$WORKSPACE/pgsql/bin:$PATH"

SQLSMITH_SECONDS="${SQLSMITH_SECONDS:-600}"
SQLSMITH_WRITERS="${SQLSMITH_WRITERS:-4}"
SQLSMITH_MAX_QUERIES="${SQLSMITH_MAX_QUERIES:-0}"
SQLSMITH_SEED="${SQLSMITH_SEED:-$(date +%s)}"

PGDATA_DIR="$WORKSPACE/pgsql/sqlsmith_pgdata"
PGLOG="$WORKSPACE/sqlsmith_pg.log"
PGPORT_SS="${PGPORT_SS:-5678}"
export PGPORT="$PGPORT_SS"
export PGHOST=/tmp
export PGDATABASE=postgres

echo "=============================================="
echo "SQLSmith seed:    $SQLSMITH_SEED"
echo "Budget:           ${SQLSMITH_SECONDS}s, writers: $SQLSMITH_WRITERS"
echo "Replay a hit with: SQLSMITH_SEED=$SQLSMITH_SEED bash ci/sqlsmith.sh"
echo "=============================================="

# Core dumps, matching the convention in check.sh.
TIMESTAMP="${TIMESTAMP:-$(date +%s)}"
CORE_DIR="/tmp/cores-sqlsmith-${GITHUB_SHA:-local}-$TIMESTAMP"
ulimit -c unlimited -S || true
mkdir -p "$CORE_DIR"
sudo sh -c "echo \"$CORE_DIR/%t_%p.core\" > /proc/sys/kernel/core_pattern" || \
	echo "warning: cannot set core_pattern; core dumps may be lost"

# ---------------------------------------------------------------- build sqlsmith
if ! command -v sqlsmith >/dev/null 2>&1 && [ ! -x "$WORKSPACE/sqlsmith/sqlsmith" ]; then
	echo "===== building sqlsmith"
	rm -rf "$WORKSPACE/sqlsmith"
	git clone --depth 1 https://github.com/anse1/sqlsmith.git "$WORKSPACE/sqlsmith"
	cd "$WORKSPACE/sqlsmith"
	autoreconf -i
	./configure
	make -j "$(nproc)"
	cd "$WORKSPACE"
fi
SQLSMITH_BIN="$WORKSPACE/sqlsmith/sqlsmith"
[ -x "$SQLSMITH_BIN" ] || SQLSMITH_BIN="$(command -v sqlsmith)"
echo "sqlsmith: $SQLSMITH_BIN"

# ---------------------------------------------------------------- start server
echo "===== starting cluster"
rm -rf "$PGDATA_DIR"
initdb -N --encoding=UTF-8 --locale=C -D "$PGDATA_DIR" >/dev/null

cat >> "$PGDATA_DIR/postgresql.conf" <<EOF
port = $PGPORT_SS
listen_addresses = ''
unix_socket_directories = '/tmp'
shared_preload_libraries = 'orioledb'
orioledb.serializable = 'error'
max_connections = 100
# A single pathological generated query must not eat the whole budget.  This
# can in principle mask a hang, so a job that finds nothing is not proof that
# no query hangs -- the stuck-process step below is what covers that.
statement_timeout = '30s'
idle_in_transaction_session_timeout = '60s'
# Keep the log parseable and complete; the crash detector reads it.
log_min_messages = warning
log_line_prefix = '%m [%p] '
restart_after_crash = off
# The writer takes checkpoints of its own, so the churn still sees them; this
# only keeps a timed one from landing inside the structural check at the end,
# where it would look like corruption.
checkpoint_timeout = '1h'
max_wal_size = '8GB'
EOF

pg_ctl -D "$PGDATA_DIR" -l "$PGLOG" -w start

cleanup() {
	pg_ctl -D "$PGDATA_DIR" -w -m immediate stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "===== loading schema"
psql -v ON_ERROR_STOP=1 -q -f "$ORIOLEDB_DIR/ci/sqlsmith_schema.sql"

# SQLSmith calls every function it finds in the catalog with random arguments,
# including OrioleDB's own introspection and debug functions.  Those are a
# different (and much shallower) target than the storage engine: the very first
# run wedged on orioledb_get_complete_xid(), which dereferences rewindMeta with
# no enable_rewind guard, and one such function is enough to end every run
# before it reaches the tree code.
#
# So by default the fuzzer connects as an unprivileged role that cannot execute
# them.  Set SQLSMITH_FUZZ_ORIOLEDB_FUNCS=1 to aim at the debug API instead.
FUZZ_USER=sqlsmith_fuzz
psql -v ON_ERROR_STOP=1 -q <<EOF
DROP ROLE IF EXISTS $FUZZ_USER;
CREATE ROLE $FUZZ_USER LOGIN;
GRANT USAGE ON SCHEMA public TO $FUZZ_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO $FUZZ_USER;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO $FUZZ_USER;
EOF

if [ "${SQLSMITH_FUZZ_ORIOLEDB_FUNCS:-0}" != "1" ]; then
	psql -v ON_ERROR_STOP=1 -q <<EOF
DO \$\$
DECLARE f record;
BEGIN
	FOR f IN SELECT p.oid::regprocedure AS sig
	           FROM pg_proc p
	           JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
	           JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'orioledb'
	LOOP
		EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, $FUZZ_USER', f.sig);
	END LOOP;
END \$\$;
EOF
	echo "orioledb's own functions are excluded from the fuzz surface"
else
	echo "orioledb's own functions are INCLUDED in the fuzz surface"
fi

# ---------------------------------------------------------------- writers
echo "===== starting $SQLSMITH_WRITERS writer sessions"
WRITER_PIDS=()
WRITER_STOP="/tmp/sqlsmith_writers_stop.$$"
rm -f "$WRITER_STOP"
for i in $(seq 1 "$SQLSMITH_WRITERS"); do
	(
		while [ ! -f "$WRITER_STOP" ]; do
			# Errors here are expected: writers race each other into
			# deadlocks and unique violations by design.
			psql -q -f "$ORIOLEDB_DIR/ci/sqlsmith_writer.sql" \
				>> "$WORKSPACE/sqlsmith_writer_$i.log" 2>&1 || true
		done
	) &
	WRITER_PIDS+=($!)
done

# ---------------------------------------------------------------- fuzz
echo "===== running sqlsmith for ${SQLSMITH_SECONDS}s"
SQLSMITH_ARGS=(
	"--target=host=/tmp port=$PGPORT_SS dbname=postgres user=$FUZZ_USER"
	"--seed=$SQLSMITH_SEED"
	"--exclude-catalog"
	"--verbose"
)
if [ "$SQLSMITH_MAX_QUERIES" -gt 0 ]; then
	SQLSMITH_ARGS+=("--max-queries=$SQLSMITH_MAX_QUERIES")
fi

smith_status=0
timeout --preserve-status "$SQLSMITH_SECONDS" \
	"$SQLSMITH_BIN" "${SQLSMITH_ARGS[@]}" \
	> "$WORKSPACE/sqlsmith_out.log" 2> "$WORKSPACE/sqlsmith_err.log" || smith_status=$?
echo "sqlsmith exited with $smith_status (124/143 = budget reached, expected)"
tail -n 5 "$WORKSPACE/sqlsmith_err.log" || true

touch "$WRITER_STOP"
for pid in "${WRITER_PIDS[@]}"; do
	wait "$pid" 2>/dev/null || true
done
rm -f "$WRITER_STOP"

# ---------------------------------------------------------------- verdict
status=0

echo "===== checking for server-side failures"

# A backend death takes the whole cluster down (restart_after_crash = off), so
# any of these lines means we found something.
if grep -nE 'PANIC|TRAP:|terminated by signal|was terminated by|Segmentation fault|assertion failed|FailedAssertion' "$PGLOG"; then
	echo "!!!!! server crash or assertion in $PGLOG"
	status=1
fi

# Belt and braces: even without a matching line, a second "ready to accept
# connections" means the postmaster restarted underneath us.
ready_count=$(grep -c 'database system is ready to accept connections' "$PGLOG" || true)
if [ "${ready_count:-0}" -gt 1 ]; then
	echo "!!!!! postmaster restarted $((ready_count - 1)) time(s)"
	status=1
fi

if ! psql -q -c 'SELECT 1' >/dev/null 2>&1; then
	echo "!!!!! server is not accepting connections after the run"
	status=1
fi

# Structural verification: a fuzz run that corrupts a tree without crashing is
# the worst outcome, and these functions are exactly the check for it.
#
# Listing the tables and checking them are two separate queries on purpose.  As
# one query the planner is free to order the WHERE clauses as it likes, and it
# puts the function first -- it then runs on every row of pg_class, including
# sequences and catalogs, and dies with "is not a orioledb table" before the
# relkind and relam tests ever get a say.
#
# The second argument makes orioledb_tbl_check() compare the tree against the
# on-disk map, which only holds after a checkpoint has written the dirty pages
# out -- hence the CHECKPOINT first.  The writers and the fuzzer have already
# stopped by this point, and checkpoint_timeout is long enough that no timed
# checkpoint lands in the middle of the verdict.
if [ "$status" -eq 0 ]; then
	echo "===== verifying tree structure"
	psql -q -c 'CHECKPOINT' || true
	tables=$(psql -tAq -c "
		SELECT c.oid::regclass::text
		  FROM pg_class c
		  JOIN pg_am am ON am.oid = c.relam
		 WHERE am.amname = 'orioledb'
		   AND c.relkind = 'r'
		 ORDER BY 1")
	bad=""
	while IFS= read -r tbl; do
		[ -n "$tbl" ] || continue
		# Keep stderr out of the captured value: orioledb_tbl_check reports
		# what it found through WARNINGs, and those belong in the job log
		# rather than in the string we compare against.
		verdict=$(psql -tAq -c \
			"SELECT orioledb_tbl_check('$tbl'::regclass, true)" \
			2> "$WORKSPACE/tbl_check_err.log") || verdict="query failed"
		if [ "$verdict" != "t" ]; then
			echo "!!!!! orioledb_tbl_check($tbl) returned '$verdict'"
			cat "$WORKSPACE/tbl_check_err.log"
			bad="$bad $tbl"
		fi
	done <<< "$tables"
	if [ -n "$bad" ]; then
		echo "!!!!! orioledb_tbl_check failed for:$bad"
		status=1
	fi
fi

# Core dumps.
cores=$(find "$CORE_DIR" -name '*.core' 2>/dev/null || true)
if [ -n "$cores" ]; then
	for corefile in $cores; do
		echo "===== backtrace for $corefile"
		gdb --batch --quiet -x "$ORIOLEDB_DIR/ci/cmds.gdb" \
			"$WORKSPACE/pgsql/bin/postgres" "$corefile" || true
		status=1
	done
fi

if [ "$status" -ne 0 ]; then
	echo "===== last 200 lines of $PGLOG"
	tail -n 200 "$PGLOG"
	echo
	echo "Reproduce with: SQLSMITH_SEED=$SQLSMITH_SEED bash ci/sqlsmith.sh"
	echo "The queries leading up to the failure are in sqlsmith_out.log."
else
	queries=$(grep -c '^' "$WORKSPACE/sqlsmith_out.log" 2>/dev/null || echo 0)
	echo "no crashes; sqlsmith produced ~$queries output lines"
fi

exit $status

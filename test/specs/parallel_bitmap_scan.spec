# Parallel bitmap heap scan under concurrency.
#
# The scanning session parks a parallel bitmap scan in the middle of reading an
# on-disk leaf (the scan_disk_page stop event, which the leader hits while
# building the key bitmap or fetching the primary tree cooperatively).  While
# it is parked, the other session mutates the matching range -- inserting (page
# splits), deleting (merges), updating, checkpoint, re-eviction -- and only then
# releases the scan.  The resumed scan must still return its snapshot-consistent
# result, proving the shared read-only bitmap and the cooperative primary fetch
# survive the tree changing shape underneath it.
#
# An always-true but per-row-expensive sqrt() filter makes the parallel bitmap
# path win, so the planner picks Gather over a parallel custom bitmap scan.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_bm (
		id int8 NOT NULL,
		val int8 NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	TRUNCATE o_bm;
	INSERT INTO o_bm SELECT i, i % 1000 FROM generate_series(1, 40000) i;
	CREATE INDEX IF NOT EXISTS o_bm_val ON o_bm (val);
	ANALYZE o_bm;
	CHECKPOINT;
	SELECT orioledb_evict_pages('o_bm'::regclass::oid, 0);
}

teardown
{
	DROP TABLE o_bm;
}

# --- scanning session -------------------------------------------------------
session "s1"
step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET enable_seqscan = off;
	SET enable_indexscan = off;
	SET enable_indexonlyscan = off;
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	SET min_parallel_table_scan_size = 0;
	SET min_parallel_index_scan_size = 0;
	SET max_parallel_workers_per_gather = 3; }
step "s1_begin_rr" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_rc" { BEGIN ISOLATION LEVEL READ COMMITTED; }
# Parallel bitmap scan: parks on the first on-disk leaf, resumes after reset.
step "s1_scan" {
	SELECT count(*), min(id), max(id), sum(id)
		FROM o_bm WHERE val < 50 AND sqrt(id::float8) >= 0; }
# BitmapOr variant.
step "s1_scan_or" {
	SELECT count(*), min(id), max(id), sum(id)
		FROM o_bm WHERE (val < 30 OR val > 970) AND sqrt(id::float8) >= 0; }
step "s1_commit" { COMMIT; }

# --- controller / modifier session ------------------------------------------
session "s2"
step "s2_arm" {
	SELECT pg_stopevent_set('scan_disk_page', 'true'); }
step "s2_insert" {
	INSERT INTO o_bm SELECT i, i % 1000
		FROM generate_series(1000000, 1030000) i; }
step "s2_delete" {
	DELETE FROM o_bm WHERE id BETWEEN 3000 AND 12000; }
step "s2_update" {
	UPDATE o_bm SET val = val + 1 WHERE id BETWEEN 5000 AND 8000; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_evict" {
	SELECT orioledb_evict_pages('o_bm'::regclass::oid, 0); }
step "s2_reset" {
	SELECT pg_stopevent_reset('scan_disk_page'); }

# REPEATABLE READ: paused bitmap scan is unaffected by concurrent inserts
# (page splits) committed while it is parked.
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan" "s2_insert" "s2_checkpoint" "s2_reset" "s1_commit"

# REPEATABLE READ: paused bitmap scan unaffected by concurrent deletes (merges).
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan" "s2_delete" "s2_checkpoint" "s2_reset" "s1_commit"

# REPEATABLE READ: paused bitmap scan unaffected by concurrent update + re-eviction.
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan" "s2_update" "s2_evict" "s2_reset" "s1_commit"

# REPEATABLE READ: BitmapOr scan, concurrent insert + delete + checkpoint.
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan_or" "s2_insert" "s2_delete" "s2_checkpoint" "s2_reset" "s1_commit"

# READ COMMITTED: the scan's snapshot is fixed at statement start, so a commit
# landing while it is parked is still not visible; result stays consistent.
permutation "s2_arm" "s1_setup" "s1_begin_rc" "s1_scan" "s2_delete" "s2_reset" "s1_commit"

# Autocommit (no explicit BEGIN): paused bitmap scan + concurrent update.
permutation "s2_arm" "s1_setup" "s1_scan" "s2_update" "s2_checkpoint" "s2_reset"

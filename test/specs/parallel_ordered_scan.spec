# Parallel ordered (key-order) index scan under concurrency.
#
# The scanning session pauses a parallel ordered scan in the middle of reading
# an on-disk leaf (the scan_disk_page stop event, which every backend -- leader
# included -- hits independently while reading evicted leaves inline in key
# order).  While it is parked, the other session mutates the very range being
# scanned -- inserting (page splits), deleting (merges), updating, checkpoint,
# re-eviction -- and only then releases the scan.  The resumed scan must still
# return its snapshot-consistent result in exact key order, proving the ordered
# walk (backward re-descent by low key, cooperative double-buffering, undo-based
# snapshot reads) survives the tree changing shape underneath it.
#
# An always-true but per-row-expensive sqrt() filter makes the parallel ordered
# path win over the naturally-ordered serial index scan, so the planner picks
# Gather Merge deterministically.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_ord (
		id int8 NOT NULL,
		val int8 NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	TRUNCATE o_ord;
	INSERT INTO o_ord SELECT i, i % 97 FROM generate_series(1, 60000) i;
	ANALYZE o_ord;
	CHECKPOINT;
	SELECT orioledb_evict_pages('o_ord'::regclass::oid, 0);
}

teardown
{
	DROP TABLE o_ord;
}

# --- scanning session -------------------------------------------------------
session "s1"
step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	SET min_parallel_table_scan_size = 0;
	SET min_parallel_index_scan_size = 0;
	SET max_parallel_workers_per_gather = 3; }
step "s1_begin_rr" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_rc" { BEGIN ISOLATION LEVEL READ COMMITTED; }
# Backward ordered scan: parks on the first on-disk leaf, resumes after reset.
step "s1_scan_desc" {
	SELECT count(*), min(id), max(id),
		(count(*) = count(*) FILTER (WHERE prev IS NULL OR id <= prev)) AS sorted
	FROM (SELECT id, lag(id) OVER () AS prev FROM (
		SELECT id FROM o_ord
			WHERE id BETWEEN 5000 AND 25000 AND sqrt(val::float8) >= 0
			ORDER BY id DESC) o) t; }
# Forward ordered scan.
step "s1_scan_asc" {
	SELECT count(*), min(id), max(id),
		(count(*) = count(*) FILTER (WHERE prev IS NULL OR id >= prev)) AS sorted
	FROM (SELECT id, lag(id) OVER () AS prev FROM (
		SELECT id FROM o_ord
			WHERE id BETWEEN 5000 AND 25000 AND sqrt(val::float8) >= 0
			ORDER BY id) o) t; }
step "s1_commit" { COMMIT; }

# --- controller / modifier session ------------------------------------------
session "s2"
step "s2_arm" {
	SELECT pg_stopevent_set('scan_disk_page', 'true'); }
# Insert into the scanned range -> page splits under the parked scan.
step "s2_insert_mid" {
	INSERT INTO o_ord SELECT i, i % 97
		FROM generate_series(1000000, 1030000) i
		WHERE i % 3 = 0; }
# Delete a big chunk of the scanned range -> merges under the parked scan.
step "s2_delete_mid" {
	DELETE FROM o_ord WHERE id BETWEEN 8000 AND 20000; }
# Move rows via update (val is filtered on, id is the scan key).
step "s2_update" {
	UPDATE o_ord SET val = val + 1 WHERE id BETWEEN 5000 AND 25000; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_evict" {
	SELECT orioledb_evict_pages('o_ord'::regclass::oid, 0); }
step "s2_reset" {
	SELECT pg_stopevent_reset('scan_disk_page'); }

# REPEATABLE READ: paused backward scan is unaffected by concurrent inserts
# (page splits) committed while it is parked.
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan_desc" "s2_insert_mid" "s2_checkpoint" "s2_reset" "s1_commit"

# REPEATABLE READ: paused backward scan unaffected by concurrent deletes (merges).
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan_desc" "s2_delete_mid" "s2_checkpoint" "s2_reset" "s1_commit"

# REPEATABLE READ: paused backward scan unaffected by concurrent update + re-eviction.
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan_desc" "s2_update" "s2_evict" "s2_reset" "s1_commit"

# REPEATABLE READ: forward scan, concurrent insert + delete + checkpoint.
permutation "s2_arm" "s1_setup" "s1_begin_rr" "s1_scan_asc" "s2_insert_mid" "s2_delete_mid" "s2_checkpoint" "s2_reset" "s1_commit"

# READ COMMITTED: the scan's snapshot is fixed at statement start, so a commit
# landing while it is parked is still not visible; result stays consistent.
permutation "s2_arm" "s1_setup" "s1_begin_rc" "s1_scan_desc" "s2_delete_mid" "s2_reset" "s1_commit"

# Autocommit (no explicit BEGIN): paused backward scan + concurrent update.
permutation "s2_arm" "s1_setup" "s1_scan_desc" "s2_update" "s2_checkpoint" "s2_reset"

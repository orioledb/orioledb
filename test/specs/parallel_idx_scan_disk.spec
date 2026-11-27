# A parallel index range scan that runs without workers (the leader alone, as
# when no worker can be launched) must count every row of the range, whatever
# part of the tree is in memory and wherever its leaves sit in the data file.
#
# The scan reads in-memory leaves during the internal-page walk and on-disk
# leaves afterwards in file order, so the leaf holding the first row past the
# range is not necessarily the last one it reads.  An ordered scan (the one
# under Gather Merge) reads every leaf in key order instead, on-disk ones
# included.  With the table analyzed and enable_sort off, the always-true
# sqrt() filter makes the planner pick it for ORDER BY b.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_pis_disk (
		a int4 NOT NULL,
		b int4 NOT NULL,
		v int4 NOT NULL,
		PRIMARY KEY (a, b)
	) USING orioledb;
	INSERT INTO o_pis_disk
		SELECT a, b, 0 FROM generate_series(1, 10) a, generate_series(1, 2000) b;
}

teardown
{
	DROP TABLE o_pis_disk;
}

session "s1"
step "s1_update_head" {
	UPDATE o_pis_disk SET v = v + 1 WHERE a = 5 AND b <= 1000; }
step "s1_update_boundary" {
	UPDATE o_pis_disk SET v = v + 1 WHERE a = 6 AND b = 1; }

session "s2"
setup {
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	SET min_parallel_table_scan_size = 0;
	SET min_parallel_index_scan_size = 0;
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET max_parallel_workers_per_gather = 2;
	SET max_parallel_workers = 0;
	RESET enable_sort; }
step "s2_explain" {
	EXPLAIN (COSTS OFF) SELECT count(*) FROM o_pis_disk WHERE a = 5; }
step "s2_count" {
	SELECT count(*) FROM o_pis_disk WHERE a = 5; }
step "s2_evict" {
	SELECT orioledb_evict_pages('o_pis_disk'::regclass, 0); }
step "s2_load_boundary" {
	SELECT a, b FROM o_pis_disk WHERE a = 6 AND b = 1; }
step "s2_load_low_boundary" {
	SELECT a, b FROM o_pis_disk WHERE a = 4 AND b = 2000; }
step "s2_ordered_setup" {
	ANALYZE o_pis_disk;
	SET enable_sort = off; }
step "s2_explain_ordered" {
	EXPLAIN (COSTS OFF)
	SELECT b FROM o_pis_disk WHERE a = 5 AND sqrt(v::float8) >= 0 ORDER BY b; }
step "s2_explain_ordered_desc" {
	EXPLAIN (COSTS OFF)
	SELECT b FROM o_pis_disk WHERE a = 5 AND sqrt(v::float8) >= 0
		ORDER BY b DESC; }
step "s2_count_ordered" {
	SELECT count(*), min(b), max(b),
		(count(*) = count(*) FILTER (WHERE prev IS NULL OR b > prev)) AS sorted
	FROM (SELECT b, lag(b) OVER () AS prev FROM (
		SELECT b FROM o_pis_disk
			WHERE a = 5 AND sqrt(v::float8) >= 0
			ORDER BY b) o) t; }
step "s2_count_ordered_desc" {
	SELECT count(*), min(b), max(b),
		(count(*) = count(*) FILTER (WHERE prev IS NULL OR b < prev)) AS sorted
	FROM (SELECT b, lag(b) OVER () AS prev FROM (
		SELECT b FROM o_pis_disk
			WHERE a = 5 AND sqrt(v::float8) >= 0
			ORDER BY b DESC) o) t; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_begin_rr" {
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT 1; }
step "s2_commit" { COMMIT; }

# Whole tree in memory
permutation "s2_explain" "s2_count"

# Every leaf on disk, in key order in the file
permutation "s2_evict" "s2_count"

# The leaf holding the first row past the range in memory, the rest on disk
permutation "s2_evict" "s2_load_boundary" "s2_count"

# Every leaf on disk; the head of the range written again after a checkpoint,
# behind the leaf holding the first row past the range, so that leaf is read
# before them
permutation "s2_evict" "s2_checkpoint" "s1_update_head" "s2_evict" "s2_count"

# The leaf holding the first row past the range in memory and changed after
# the snapshot
permutation "s2_evict" "s2_begin_rr" "s1_update_boundary" "s2_count" "s2_commit"

# Ordered scan, whole tree in memory
permutation "s2_ordered_setup" "s2_explain_ordered" "s2_explain_ordered_desc" "s2_count_ordered" "s2_count_ordered_desc"

# Ordered scan, the leaf holding the first row past the range in memory, the
# rest on disk
permutation "s2_ordered_setup" "s2_evict" "s2_load_boundary" "s2_count_ordered"

# Ordered scan, every leaf on disk and the head of the range written again
# after a checkpoint, behind the leaf holding the first row past the range
permutation "s2_ordered_setup" "s2_evict" "s2_checkpoint" "s1_update_head" "s2_evict" "s2_count_ordered"

# Backward ordered scan, the leaf holding the last row before the range in
# memory, the rest on disk
permutation "s2_ordered_setup" "s2_evict" "s2_load_low_boundary" "s2_count_ordered_desc"

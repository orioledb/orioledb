setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_par_idx_scan (
		id int4 NOT NULL,
		val int4 NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	CREATE INDEX IF NOT EXISTS o_par_idx_scan_val_ix ON o_par_idx_scan (val);
	TRUNCATE o_par_idx_scan;
	INSERT INTO o_par_idx_scan
		SELECT i, i % 100 FROM generate_series(1, 10000) AS i;
}

teardown
{
	DROP TABLE o_par_idx_scan;
}

session "s1"
step "s1_begin_rr" {
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_rc" {
	BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s1_insert" {
	INSERT INTO o_par_idx_scan
		SELECT i, i % 100 FROM generate_series(10001, 11000) AS i; }
step "s1_update" {
	UPDATE o_par_idx_scan SET val = val + 1000 WHERE id <= 500; }
step "s1_delete" {
	DELETE FROM o_par_idx_scan WHERE id BETWEEN 5001 AND 6000; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"
step "s2_setup" {
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	SET min_parallel_table_scan_size = 0;
	SET min_parallel_index_scan_size = 0;
	SET max_parallel_workers_per_gather = 4; }
step "s2_begin_rr" {
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_begin_rc" {
	BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s2_count_all" {
	SELECT count(*) FROM o_par_idx_scan; }
step "s2_count_range" {
	SELECT count(*) FROM o_par_idx_scan WHERE id BETWEEN 1 AND 5000; }
step "s2_count_val" {
	SELECT count(*) FROM o_par_idx_scan WHERE val = 0; }
step "s2_sum_val" {
	SELECT sum(val) FROM o_par_idx_scan WHERE id BETWEEN 1 AND 1000; }
step "s2_commit" { COMMIT; }

# REPEATABLE READ: parallel scan sees snapshot taken at BEGIN
permutation "s2_setup" "s2_begin_rr" "s2_count_all" "s1_insert" "s2_count_all" "s2_commit"
permutation "s2_setup" "s2_begin_rr" "s2_count_all" "s1_delete" "s2_count_all" "s2_commit"
permutation "s2_setup" "s2_begin_rr" "s2_count_all" "s1_update" "s2_count_all" "s2_commit"

# REPEATABLE READ: parallel index range scan under concurrent DML
permutation "s2_setup" "s2_begin_rr" "s2_count_range" "s1_insert" "s2_count_range" "s2_commit"
permutation "s2_setup" "s2_begin_rr" "s2_count_range" "s1_delete" "s2_count_range" "s2_commit"
permutation "s2_setup" "s2_begin_rr" "s2_count_range" "s1_update" "s2_count_range" "s2_commit"

# REPEATABLE READ: parallel index scan on val column under DML
permutation "s2_setup" "s2_begin_rr" "s2_count_val" "s1_update" "s2_count_val" "s2_commit"

# READ COMMITTED: parallel scan sees committed changes mid-transaction
permutation "s2_setup" "s2_begin_rc" "s2_count_all" "s1_begin_rc" "s1_insert" "s1_commit" "s2_count_all" "s2_commit"
permutation "s2_setup" "s2_begin_rc" "s2_count_all" "s1_begin_rc" "s1_delete" "s1_commit" "s2_count_all" "s2_commit"
permutation "s2_setup" "s2_begin_rc" "s2_count_all" "s1_begin_rc" "s1_update" "s1_commit" "s2_count_all" "s2_commit"

# READ COMMITTED: parallel scan does NOT see rolled-back changes
permutation "s2_setup" "s2_begin_rc" "s2_count_all" "s1_begin_rc" "s1_insert" "s1_rollback" "s2_count_all" "s2_commit"

# REPEATABLE READ: parallel scan with index sum under update
permutation "s2_setup" "s2_begin_rr" "s2_sum_val" "s1_update" "s2_sum_val" "s2_commit"

# Concurrent parallel scans from two sessions
permutation "s1_begin_rr" "s2_setup" "s2_begin_rr" "s2_count_all" "s1_insert" "s2_count_all" "s1_commit" "s2_commit"

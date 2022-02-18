setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_bitmap_hist_scan_test
	(
		id int8 NOT NULL,
		val int8 NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	CREATE INDEX ON o_bitmap_hist_scan_test(val);
	TRUNCATE o_bitmap_hist_scan_test;
}

teardown
{
	DROP TABLE o_bitmap_hist_scan_test;
}

session "s1"
step "s1_begin" {
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }

step "s1_insert_big1" {
	INSERT INTO o_bitmap_hist_scan_test
		SELECT i, i * 11 FROM generate_series(1, 1500) AS i; }
step "s1_delete_big" {
	DELETE FROM o_bitmap_hist_scan_test WHERE id BETWEEN 1 and 90; }
step "s1_count" {
	SET enable_seqscan = off;
	SELECT count(*) FROM o_bitmap_hist_scan_test WHERE val < 2000;
	RESET enable_seqscan;
}
step "s1_commit" { COMMIT; }

session "s2"
step "s2_insert_big2"  {
	INSERT INTO o_bitmap_hist_scan_test
	SELECT i - 200, i * 33 FROM generate_series(1, 150) AS i; }
step "s2_count" { SELECT count(*) FROM o_bitmap_hist_scan_test; }

permutation "s1_insert_big1" "s1_begin" "s1_count" "s1_delete_big" "s1_count" "s2_insert_big2" "s1_count" "s2_count" "s1_commit"
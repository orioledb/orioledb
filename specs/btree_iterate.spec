setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_btree_iterate
	(
		id int8 NOT NULL,
		t text NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_btree_iterate;

}

teardown
{
	DROP TABLE o_btree_iterate;
}

session "s1"

step "s1_setup"  { SET orioledb.enable_stopevents = true; SET enable_seqscan = off; }
step "s1_setup_seq"  { SET orioledb.enable_stopevents = true; }
step "s1_begin" { BEGIN; }
step "s1_delete" {
	DELETE FROM o_btree_iterate WHERE id % 15 = 2; }
step "s1_insert1" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(1, 1500, 5) id; }
step "s1_insert2" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(1499, 1, -5) id; }
step "s1_insert3" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(1498, 1, -5) id; }
step "s1_insert4" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(2, 1500, 5) id; }

step "s1_get1" {
	SELECT * FROM o_btree_iterate
	WHERE id = 1; }
step "s1_bget1" {
	SELECT * FROM o_btree_iterate
	WHERE id > 0 and id < 2 ORDER BY id DESC; }
step "s1_get2" {
	SELECT * FROM o_btree_iterate
	WHERE id = 301; }
step "s1_bget2" {
	SELECT * FROM o_btree_iterate
	WHERE id > 300 and id < 302 ORDER BY id DESC; }
step "s1_get3" {
	SELECT * FROM o_btree_iterate
	WHERE id = 766; }
step "s1_bget3" {
	SELECT * FROM o_btree_iterate
	WHERE id > 765 and id < 767 ORDER BY id DESC; }
step "s1_get4" {
	SELECT * FROM o_btree_iterate
	WHERE id = 1216; }
step "s1_bget4" {
	SELECT * FROM o_btree_iterate
	WHERE id > 1215 and id < 1217 ORDER BY id DESC; }

step "s1_count" { SELECT count(*) FROM o_btree_iterate; }
step "s1_bcount" { SELECT count(*) FROM ( SELECT * FROM o_btree_iterate ORDER BY id DESC) t;}

step "s1_commit" { COMMIT; }

session "s2"

step "s2_break_step_down" { SELECT pg_stopevent_set('step_down', 'true'); }
step "s2_break_step_down_300" { SELECT pg_stopevent_set('step_down', '$.downlink.id >= 300'); }
step "s2_break_step_down_750" { SELECT pg_stopevent_set('step_down', '$.downlink.id >= 750'); }
step "s2_break_step_left_1200" { SELECT pg_stopevent_set('step_left', '$.hikey.id <= 1200'); }
step "s2_break_step_left_750" { SELECT pg_stopevent_set('step_left', '$.hikey.id <= 750'); }
step "s2_break_step_right_300" { SELECT pg_stopevent_set('step_right', '$.hikey.id >= 300'); }
step "s2_break_step_right_750" { SELECT pg_stopevent_set('step_right', '$.hikey.id >= 750'); }
step "s2_bp_scan_end" { SELECT pg_stopevent_set('scan_end', 'true'); }
step "s2_insert1" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(1, 1500, 5) id; }
step "s2_insert2" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(1499, 1, -5) id; }
step "s2_insert3" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(1498, 1, -5) id; }
step "s2_insert4" {
	INSERT INTO o_btree_iterate
	SELECT id, repeat('abcde', id % 10) FROM generate_series(2, 1500, 5) id; }
step "s2_delete34" { DELETE FROM o_btree_iterate WHERE id % 5 IN (2, 3); }
step "s2_delete12" { DELETE FROM o_btree_iterate WHERE id % 5 IN (1, 4) }
step "s2_delete_mass" { DELETE FROM o_btree_iterate WHERE id % 9 > 0; }
step "s2_continue" {
	SELECT
		(pg_stopevent_reset('step_down')::int + 
		 pg_stopevent_reset('step_left')::int +
		 pg_stopevent_reset('step_right')::int +
		 pg_stopevent_reset('scan_end')::int)::bool; }
step "s2_evict" { SELECT orioledb_evict_pages('o_btree_iterate'::regclass, 0); }

# forward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get1" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get1" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_get1" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_get1" "s2_insert3" "s2_continue"

# backward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget1" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget1" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_bget1" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_bget1" "s2_insert3" "s2_continue"

# forward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get2" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get2" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get2" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_get2" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_get2" "s2_insert3" "s2_continue"

# backward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget2" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget2" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget2" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_bget2" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_bget2" "s2_insert3" "s2_continue"

# forward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get3" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get3" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get3" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_get3" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_get3" "s2_insert3" "s2_continue"

# backward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget3" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget3" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget3" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_bget3" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_bget3" "s2_insert3" "s2_continue"

# forward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get4" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get4" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_get4" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_get4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_get4" "s2_insert3" "s2_continue"

# backward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget4" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget4" "s2_insert4" "s2_insert2" "s2_break_step_down" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down" "s1_bget4" "s2_insert3" "s2_insert2" "s2_break_step_down" "s2_insert4" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down" "s1_bget4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_insert2" "s2_break_step_down" "s1_bget4" "s2_insert3" "s2_continue"

# forward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_right_750" "s1_count" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert3" "s2_break_step_right_750" "s1_count" "s2_insert2" "s2_insert4" "s2_insert1" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_right_300" "s1_count" "s2_insert2" "s2_break_step_right_750" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert3" "s2_insert2" "s2_break_step_right_300" "s1_count" "s2_insert4" "s2_break_step_right_750" "s2_insert1" "s2_continue"

# backward
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_break_step_left_750" "s1_bcount" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert3" "s2_break_step_left_750" "s1_bcount" "s2_insert2" "s2_insert4" "s2_insert1" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_left_1200" "s1_bcount" "s2_insert2" "s2_break_step_left_750" "s2_insert3" "s2_continue"
permutation "s1_setup" "s2_bp_scan_end" "s2_insert3" "s2_insert2" "s2_break_step_left_1200" "s1_bcount" "s2_insert4" "s2_break_step_left_750" "s2_insert1" "s2_continue"

# sequential
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert1" "s2_break_step_down_750" "s1_count" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert3" "s2_break_step_down_750" "s1_count" "s2_insert2" "s2_insert4" "s2_insert1" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_break_step_down_300" "s1_count" "s2_insert2" "s2_break_step_down_750" "s2_insert3" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert3" "s2_insert2" "s2_break_step_down_300" "s1_count" "s2_insert4" "s2_break_step_down_750" "s2_insert1" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert1" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_insert4" "s2_insert2" "s2_insert3" "s2_evict" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert3" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_insert2" "s2_insert4" "s2_insert1" "s2_evict" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert1" "s2_insert4" "s2_evict" "s2_break_step_down_300" "s1_count" "s2_insert2" "s2_break_step_down_750" "s2_insert3" "s2_evict" "s2_continue"
permutation "s1_setup_seq" "s2_bp_scan_end" "s2_insert3" "s2_insert2" "s2_evict" "s2_break_step_down_300" "s1_count" "s2_insert4" "s2_break_step_down_750" "s2_insert1" "s2_evict" "s2_continue"

# forward
permutation "s1_setup" "s2_insert1" "s1_begin" "s1_delete" "s2_break_step_right_750" "s2_bp_scan_end" "s1_count" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s1_begin" "s1_delete" "s2_break_step_right_750" "s2_bp_scan_end" "s1_count" "s2_insert2" "s2_insert4" "s2_insert1" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s2_insert4" "s1_begin" "s1_delete" "s2_break_step_right_300" "s2_bp_scan_end" "s1_count" "s2_insert2" "s2_break_step_right_750" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s2_insert2" "s1_begin" "s1_delete" "s2_break_step_right_300" "s2_bp_scan_end" "s1_count" "s2_insert4" "s2_break_step_right_750" "s2_insert1" "s2_continue" "s1_commit"

# backward
permutation "s1_setup" "s2_insert1" "s1_begin" "s1_delete" "s2_break_step_left_750" "s2_bp_scan_end" "s1_bcount" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s1_begin" "s1_delete" "s2_break_step_left_750" "s2_bp_scan_end" "s1_bcount" "s2_insert2" "s2_insert4" "s2_insert1" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s2_insert4" "s1_begin" "s1_delete" "s2_break_step_left_1200" "s2_bp_scan_end" "s1_bcount" "s2_insert2" "s2_break_step_left_750" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s2_insert2" "s1_begin" "s1_delete" "s2_break_step_left_1200" "s2_bp_scan_end" "s1_bcount" "s2_insert4" "s2_break_step_left_750" "s2_insert1" "s2_continue" "s1_commit"

# sequential
permutation "s1_setup_seq" "s2_insert1" "s1_begin" "s1_delete" "s2_break_step_down_750" "s2_bp_scan_end" "s1_count" "s2_insert4" "s2_insert2" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s1_begin" "s1_delete" "s2_break_step_down_750" "s2_bp_scan_end" "s1_count" "s2_insert2" "s2_insert4" "s2_insert1" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s1_begin" "s1_delete" "s2_break_step_down_300" "s2_bp_scan_end" "s1_count" "s2_insert2" "s2_break_step_down_750" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s2_insert2" "s1_begin" "s1_delete" "s2_break_step_down_300" "s2_bp_scan_end" "s1_count" "s2_insert4" "s2_break_step_down_750" "s2_insert1" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s1_begin" "s1_delete" "s2_evict" "s2_break_step_down_750" "s2_bp_scan_end" "s1_count" "s2_insert4" "s2_insert2" "s2_insert3" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s1_begin" "s1_delete" "s2_evict" "s2_break_step_down_750" "s2_bp_scan_end" "s1_count" "s2_insert2" "s2_insert4" "s2_insert1" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s1_begin" "s1_delete" "s2_evict" "s2_break_step_down_300" "s2_bp_scan_end" "s1_count" "s2_insert2" "s2_evict" "s2_break_step_down_750" "s2_insert3" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s2_insert2" "s1_begin" "s1_delete" "s2_evict" "s2_break_step_down_300" "s2_bp_scan_end" "s1_count" "s2_insert4" "s2_evict" "s2_break_step_down_750" "s2_insert1" "s2_evict" "s2_continue" "s1_commit"

# forward
permutation "s1_setup" "s2_insert1" "s2_insert4" "s1_begin" "s2_break_step_right_750" "s1_count" "s2_delete_mass" "s2_insert2" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s2_insert4" "s2_insert3" "s1_begin" "s2_break_step_right_750" "s1_count" "s2_delete_mass" "s2_insert2" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s2_insert4" "s2_insert2" "s1_begin" "s2_break_step_right_750" "s1_count" "s2_delete_mass" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s2_insert4" "s2_insert2" "s1_begin" "s2_break_step_right_750" "s1_count" "s2_delete_mass" "s2_insert1" "s2_continue" "s1_commit"

# backward
permutation "s1_setup" "s2_insert1" "s2_insert4" "s1_begin" "s2_break_step_left_750" "s1_bcount" "s2_delete_mass" "s2_insert2" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s2_insert4" "s2_insert3" "s1_begin" "s2_break_step_left_1200" "s1_bcount" "s2_delete_mass" "s2_insert2" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s2_insert4" "s2_insert2" "s1_begin" "s2_break_step_left_750" "s1_bcount" "s2_delete_mass" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s2_insert4" "s2_insert2" "s1_begin" "s2_break_step_left_1200" "s1_bcount" "s2_delete_mass" "s2_insert1" "s2_continue" "s1_commit"

# sequential
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s1_begin" "s2_break_step_down_750" "s1_count" "s2_delete_mass" "s2_insert2" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s2_insert3" "s1_begin" "s2_break_step_down_750" "s1_count" "s2_delete_mass" "s2_insert2" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s2_insert2" "s1_begin" "s2_break_step_down_750" "s1_count" "s2_delete_mass" "s2_insert3" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s2_insert4" "s2_insert2" "s1_begin" "s2_break_step_right_750" "s1_count" "s2_delete_mass" "s2_insert1" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s1_begin" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_delete_mass" "s2_insert2" "s2_insert3" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s2_insert3" "s1_begin" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_delete_mass" "s2_insert2" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s2_insert4" "s2_insert2" "s1_begin" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_delete_mass" "s2_insert3" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s2_insert4" "s2_insert2" "s1_begin" "s2_evict" "s2_break_step_right_750" "s1_count" "s2_delete_mass" "s2_insert1" "s2_evict" "s2_continue" "s1_commit"

# forward
permutation "s1_setup" "s2_insert4" "s1_begin" "s1_insert2" "s2_break_step_right_750" "s1_count" "s2_insert3" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s1_begin" "s1_insert1" "s2_break_step_right_750" "s1_count" "s2_insert4" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s1_begin" "s1_insert3" "s2_break_step_right_750" "s1_count" "s2_insert2" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert2" "s1_begin" "s1_insert4" "s2_break_step_right_750" "s1_count" "s2_insert1" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_continue" "s1_commit"

# backward
permutation "s1_setup" "s2_insert4" "s1_begin" "s1_insert2" "s2_break_step_left_750" "s1_bcount" "s2_insert3" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert3" "s1_begin" "s1_insert1" "s2_break_step_left_1200" "s1_bcount" "s2_insert4" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert1" "s1_begin" "s1_insert3" "s2_break_step_left_750" "s1_bcount" "s2_insert2" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_continue" "s1_commit"
permutation "s1_setup" "s2_insert2" "s1_begin" "s1_insert4" "s2_break_step_left_1200" "s1_bcount" "s2_insert1" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_continue" "s1_commit"

# sequential
permutation "s1_setup_seq" "s2_insert4" "s1_begin" "s1_insert2" "s2_break_step_down_750" "s1_count" "s2_insert3" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s1_begin" "s1_insert1" "s2_break_step_down_750" "s1_count" "s2_insert4" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s1_begin" "s1_insert3" "s2_break_step_down_750" "s1_count" "s2_insert2" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert2" "s1_begin" "s1_insert4" "s2_break_step_down_750" "s1_count" "s2_insert1" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert4" "s1_begin" "s1_insert2" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_insert3" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert3" "s1_begin" "s1_insert1" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_insert4" "s2_delete34" "s2_insert4" "s2_insert3" "s2_delete34" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert1" "s1_begin" "s1_insert3" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_insert2" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_evict" "s2_continue" "s1_commit"
permutation "s1_setup_seq" "s2_insert2" "s1_begin" "s1_insert4" "s2_evict" "s2_break_step_down_750" "s1_count" "s2_insert1" "s2_delete12" "s2_insert1" "s2_insert2" "s2_delete12" "s2_evict" "s2_continue" "s1_commit"

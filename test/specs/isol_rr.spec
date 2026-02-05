setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_iso_rr (
		id int4 NOT NULL,
		t text NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	TRUNCATE o_iso_rr;

	CREATE OR REPLACE PROCEDURE update_table(n integer)
	LANGUAGE plpgsql AS $$
	BEGIN
	WHILE n > 0 LOOP
		UPDATE o_iso_rr SET t = t::int + 1 WHERE id=1;
		n = n - 1;
		COMMIT;
	END LOOP;
	END;
	$$;
}

teardown
{
	DROP TABLE o_iso_rr;
	DROP PROCEDURE update_table;
}

session "s1"
step "s1_begin" {
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	SET enable_seqscan = off; }

step "s1_insert1" {
	INSERT INTO o_iso_rr
		SELECT i, repeat('x', i)
		FROM generate_series(1, 50) AS i; }
step "s1_insert2" {
	INSERT INTO o_iso_rr
		SELECT i - 20, repeat('y', i)
		FROM generate_series(1, 15) AS i; }
step "s1_insert50" {
	INSERT INTO o_iso_rr
		VALUES (50, 'abc'); }
step "s1_update50" {
	UPDATE o_iso_rr SET t = 'abcde' WHERE id = 50; }
step "s1_delete" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 1 and 10; }
step "s1_delete11" {
	DELETE FROM o_iso_rr
	WHERE id = 11; }
step "s1_delete10" {
	DELETE FROM o_iso_rr
	WHERE id = 10; }

step "s1_insert_big1" {
	INSERT INTO o_iso_rr
		SELECT i, repeat('z', i % 50)
		FROM generate_series(1, 500) AS i; }
step "s1_insert_big2" {
	INSERT INTO o_iso_rr
		SELECT i - 200, i
		FROM generate_series(1, 150) AS i; }
step "s1_delete_big" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 1 and 90; }
step "s1_delete_big_end" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 401 AND 500; }
step "s1_delete150" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 150 and 200; }
step "s1_delete100" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 100 and 140; }
step "s1_update10000" { call update_table(10000); }
step "s1_update100000" { call update_table(100000); }

step "s1_select" { SELECT * FROM o_iso_rr; }
step "s1_select20" {
	SELECT * FROM o_iso_rr
	WHERE id BETWEEN -10 and 10;
	SELECT * FROM o_iso_rr
	WHERE id > -11 and id < 11; }
step "s1_count200" {
	SELECT count(*) FROM o_iso_rr
	WHERE id BETWEEN 0 and 200;
	SELECT count(*) FROM o_iso_rr
	WHERE id > -1 and id < 201; }
step "s1_count200_end" {
	SELECT count(*) FROM o_iso_rr
	WHERE id BETWEEN 301 and 500;
	SELECT count(*) FROM o_iso_rr
	WHERE id > 300 and id < 501; }
step "s1_select49" { SELECT * FROM o_iso_rr WHERE id = 49; }
step "s1_select50" { SELECT * FROM o_iso_rr WHERE id = 50; }
step "s1_count" { SELECT count(*) FROM o_iso_rr; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"
step "s2_begin" {
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	SET enable_seqscan = off;}
step "s2_begin_rc" {
	BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET enable_seqscan = off;}

step "s2_delete" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 1 and 10; }
step "s2_insert2" {
	INSERT INTO o_iso_rr
	SELECT i - 20, repeat('x', i) FROM generate_series(1, 15) AS i; }

step "s2_insert_one" {
	INSERT INTO o_iso_rr VALUES (1, '1'); }
step "s2_delete_big" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 1 and 100; }
step "s2_delete_big_end" {
	DELETE FROM o_iso_rr
	WHERE id BETWEEN 401 AND 500; }
step "s2_insert_big2"  {
	INSERT INTO o_iso_rr
	SELECT i - 200, repeat('z', i % 50) FROM generate_series(1, 150) AS i; }
step "s2_insert50" {
	INSERT INTO o_iso_rr
		VALUES (50, 'abc'); }
step "s2_insert_before50" {
	INSERT INTO o_iso_rr (SELECT i, repeat('x', 120) FROM generate_series(1, 20) i); }
step "s2_insert_after50" {
	INSERT INTO o_iso_rr (SELECT i, repeat('x', 120) FROM generate_series(55, 100) i); }
step "s2_select" { SELECT * FROM o_iso_rr; }
step "s2_select20" {
	SELECT * FROM o_iso_rr WHERE id BETWEEN -10 and 10;
	SELECT * FROM o_iso_rr WHERE id > -11 and id < 11; }
step "s2_select50_key_share" { SELECT * FROM o_iso_rr WHERE id = 50 FOR KEY SHARE; }
step "s2_count200" {
	SELECT count(*) FROM o_iso_rr WHERE id BETWEEN 0 and 200;
	SELECT count(*) FROM o_iso_rr WHERE id > -1 and id < 201; }
step "s2_count200_end" {
	SELECT count(*) FROM o_iso_rr WHERE id BETWEEN 301 and 500;
	SELECT count(*) FROM o_iso_rr WHERE id > 300 and id < 501; }
step "s2_count" { SELECT count(*) FROM o_iso_rr; }
step "s2_select_one" { SELECT * FROM o_iso_rr WHERE id=1; }
step "s2_rollback" { ROLLBACK; }
step "s2_commit" { COMMIT; }

permutation "s1_insert1" "s1_begin" "s1_select" "s2_begin" "s2_select" "s1_delete" "s2_delete" "s1_select" "s1_commit" "s2_select" "s2_rollback"
permutation "s1_insert1" "s1_begin" "s2_begin" "s2_delete" "s1_select" "s1_delete11" "s1_delete10" "s2_commit" "s1_rollback"
permutation "s1_insert1" "s1_begin" "s1_select" "s2_begin" "s2_delete" "s2_commit" "s1_delete11" "s1_delete10" "s1_rollback"
permutation "s1_insert1" "s1_begin" "s2_begin" "s2_delete" "s2_commit" "s1_select" "s1_delete11" "s1_delete10" "s1_rollback"
permutation "s1_insert1" "s1_begin" "s1_select" "s1_insert2" "s1_select" "s2_delete" "s1_select" "s2_select" "s1_commit"
permutation "s1_insert1" "s1_begin" "s1_select" "s2_insert2" "s1_select" "s1_delete" "s1_select" "s2_select" "s1_commit"
permutation "s1_insert1" "s1_begin" "s1_select" "s1_delete" "s1_select" "s2_insert2" "s1_select" "s2_select" "s1_commit"
permutation "s1_insert1" "s1_begin" "s1_select" "s2_delete" "s1_select" "s1_insert2" "s1_select" "s2_select" "s1_commit"

permutation "s1_insert1" "s1_begin" "s1_select20" "s2_begin" "s2_select20" "s1_delete" "s2_delete" "s1_select20" "s1_commit" "s2_select20" "s2_rollback"
permutation "s1_insert1" "s1_begin" "s2_begin" "s2_delete" "s1_select20" "s1_delete11" "s1_delete10" "s2_commit" "s1_rollback"
permutation "s1_insert1" "s1_begin" "s1_select20" "s2_begin" "s2_delete" "s2_commit" "s1_delete11" "s1_delete10" "s1_rollback"
permutation "s1_insert1" "s1_begin" "s2_begin" "s2_delete" "s2_commit" "s1_select20" "s1_delete11" "s1_delete10" "s1_rollback"
permutation "s1_insert1" "s1_begin" "s1_select20" "s1_insert2" "s1_select20" "s2_delete" "s1_select20" "s2_select20" "s1_commit"
permutation "s1_insert1" "s1_begin" "s1_select20" "s2_insert2" "s1_select20" "s1_delete" "s1_select20" "s2_select20" "s1_commit"
permutation "s1_insert1" "s1_begin" "s1_select20" "s1_delete" "s1_select20" "s2_insert2" "s1_select20" "s2_select20" "s1_commit"
permutation "s1_insert1" "s1_begin" "s1_select20" "s2_delete" "s1_select20" "s1_insert2" "s1_select20" "s2_select20" "s1_commit"

permutation "s1_insert_big1" "s1_begin" "s1_count" "s2_begin" "s2_count" "s1_delete_big" "s2_delete_big" "s1_count" "s1_commit" "s2_count" "s2_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big" "s1_count" "s1_delete150" "s1_delete100" "s2_commit" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s2_begin" "s2_delete_big" "s2_commit" "s1_delete150" "s1_delete100" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big" "s2_commit" "s1_count" "s1_delete150" "s1_delete100" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s1_delete_big" "s1_count" "s2_insert_big2" "s1_count" "s2_count" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s2_delete_big" "s1_count" "s1_insert_big2" "s1_count" "s2_count" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s1_insert_big2" "s1_count" "s2_delete_big" "s1_count" "s2_count" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s2_insert_big2" "s1_count" "s1_delete_big" "s1_count" "s2_count" "s1_commit"

permutation "s1_insert_big1" "s1_begin" "s1_count" "s1_delete_big_end" "s1_count" "s2_insert_big2" "s1_count" "s2_count" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s2_begin" "s2_count" "s1_delete_big" "s2_delete_big_end" "s1_count" "s1_commit" "s2_count" "s2_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big_end" "s1_count" "s1_delete150" "s1_delete100" "s2_commit" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s1_count" "s2_begin" "s2_delete_big_end" "s2_commit" "s1_delete150" "s1_delete100" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big_end" "s2_commit" "s1_count" "s1_delete150" "s1_delete100" "s1_rollback"

permutation "s1_insert_big1" "s1_begin" "s1_count200" "s2_delete_big" "s1_count200" "s1_insert_big2" "s1_count200" "s2_count200" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200" "s1_insert_big2" "s1_count200" "s2_delete_big" "s1_count200" "s2_count200" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200" "s2_insert_big2" "s1_count200" "s1_delete_big" "s1_count200" "s2_count200" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200" "s1_delete_big" "s1_count200" "s2_insert_big2" "s1_count200" "s2_count200" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200" "s2_begin" "s2_count200" "s1_delete_big" "s2_delete_big" "s1_count200" "s1_commit" "s2_count200" "s2_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big" "s1_count200" "s1_delete150" "s1_delete100" "s2_commit" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s1_count200" "s2_begin" "s2_delete_big" "s2_commit" "s1_delete150" "s1_delete100" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big" "s2_commit" "s1_count200" "s1_delete150" "s1_delete100" "s1_rollback"

permutation "s1_insert_big1" "s1_begin" "s1_count200_end" "s2_delete_big_end" "s1_count200_end" "s1_insert_big2" "s1_count200_end" "s2_count200_end" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200_end" "s1_insert_big2" "s1_count200_end" "s2_delete_big_end" "s1_count200_end" "s2_count200_end" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200_end" "s2_insert_big2" "s1_count200_end" "s1_delete_big_end" "s1_count200_end" "s2_count200_end" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200_end" "s1_delete_big_end" "s1_count200_end" "s2_insert_big2" "s1_count200_end" "s2_count200_end" "s1_commit"
permutation "s1_insert_big1" "s1_begin" "s1_count200_end" "s2_begin" "s2_count200_end" "s1_delete_big_end" "s2_delete_big_end" "s1_count200_end" "s1_commit" "s2_count200_end" "s2_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big_end" "s1_count200_end" "s1_delete150" "s1_delete100" "s2_commit" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s1_count200_end" "s2_begin" "s2_delete_big_end" "s2_commit" "s1_delete150" "s1_delete100" "s1_rollback"
permutation "s1_insert_big1" "s1_begin" "s2_begin" "s2_delete_big_end" "s2_commit" "s1_count200_end" "s1_delete150" "s1_delete100" "s1_rollback"

permutation "s2_insert_one" "s2_begin" "s2_select_one" "s1_update10000" "s1_update100000" "s2_commit"

permutation "s1_begin" "s1_insert50" "s2_insert_before50" "s2_insert_after50" "s1_select49" "s1_commit"
permutation "s2_insert50" "s1_begin" "s1_select" "s2_insert_before50" "s2_insert_after50" "s1_update50" "s2_begin_rc" "s2_select50_key_share" "s1_select50" "s2_commit" "s1_commit"

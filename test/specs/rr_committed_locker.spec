# A row lock is not a row version.  Once its locker finishes, a REPEATABLE
# READ transaction may still modify the row: only a newer version is a
# serialization conflict.  row_lock_conflicts() used to report the finished
# locker's record as the row's version, so the callback compared the locker's
# commit csn with the snapshot and raised a serialization failure.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_rr_committed_locker (
		id int PRIMARY KEY,
		v int
	) USING orioledb;
	INSERT INTO o_rr_committed_locker VALUES (1, 0);
}

teardown
{
	DROP TABLE o_rr_committed_locker;
}

session s1
step s1_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_select	{ SELECT * FROM o_rr_committed_locker; }
step s1_update	{ UPDATE o_rr_committed_locker SET v = v + 1 WHERE id = 1; }
step s1_delete	{ DELETE FROM o_rr_committed_locker WHERE id = 1; }
step s1_commit	{ COMMIT; }
step s1_check	{ SELECT * FROM o_rr_committed_locker; }

session s2
step s2_begin	{ BEGIN; }
step s2_fu		{ SELECT * FROM o_rr_committed_locker WHERE id = 1 FOR UPDATE; }
step s2_fnku	{ SELECT * FROM o_rr_committed_locker WHERE id = 1 FOR NO KEY UPDATE; }
step s2_commit	{ COMMIT; }
step s2_rollback	{ ROLLBACK; }

permutation s1_begin s1_select s2_begin s2_fu s2_commit s1_update s1_commit s1_check
permutation s1_begin s1_select s2_begin s2_fnku s2_commit s1_delete s1_commit s1_check
permutation s1_begin s1_select s2_begin s2_fu s2_rollback s1_update s1_commit s1_check

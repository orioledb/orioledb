# The callback can raise the row lock mode mid-modify: an UPDATE that turns
# out to touch key attributes asks for RowLockUpdate where the operation
# started with RowLockNoKeyUpdate, and o_btree_normal_modify() retries.  Our
# own FOR NO KEY UPDATE record is redundant for the first mode and gets
# removed on the way, so the retry must still leave the row locked -- if it
# does not, s2 stops blocking here.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_rll_mode_raise (
		id int PRIMARY KEY,
		v int
	) USING orioledb;
	INSERT INTO o_rll_mode_raise VALUES (1, 10);
}

teardown
{
	DROP TABLE o_rll_mode_raise;
}

session s1
step s1_begin	{ BEGIN; }
step s1_nku		{ SELECT * FROM o_rll_mode_raise WHERE id = 1 FOR NO KEY UPDATE; }
step s1_raise	{ UPDATE o_rll_mode_raise SET id = 2 WHERE id = 1; }
step s1_commit	{ COMMIT; }

session s2
step s2_begin	{ BEGIN; }
step s2_upd		{ UPDATE o_rll_mode_raise SET v = 99 WHERE id = 1; }
step s2_commit	{ COMMIT; }

permutation s1_begin s1_nku s1_raise s2_begin s2_upd s1_commit s2_commit

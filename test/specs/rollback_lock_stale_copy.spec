# A reader working from a copy of a leaf page must not lose the undo record
# a concurrent rollback restores into the page.
#
# x updates a non-key column of row 50, and y takes FOR KEY SHARE on the row,
# which NO KEY UPDATE allows; a foreign key check takes the same lock.  y's
# lock-only undo record holds the header of x's version.  z's seq scan copies
# the page while row 50 holds x's 'B'.  x rolls back and puts 'A' back into
# the page.  z then resolves row 50 from its copy and must still see 'A'.
#
# The rollback leaves the lock-only records alone and leads the page to a lock
# dispatch record holding the restored version.  The other permutations check
# that row-level locks under a dispatch record still work: they conflict, they
# go away when their transaction or subtransaction rolls back, and further
# updates, locks and rollbacks of the row stack on top.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_rb_lock (id int PRIMARY KEY, val text) USING orioledb;
	INSERT INTO o_rb_lock SELECT i, 'A' FROM generate_series(1, 100) i;
	CREATE TABLE o_rb_lock_child (pid int REFERENCES o_rb_lock) USING orioledb;
}

teardown
{
	DROP TABLE o_rb_lock_child;
	DROP TABLE o_rb_lock;
}

session x
step x_update	{ BEGIN; UPDATE o_rb_lock SET val = 'B' WHERE id = 50; }
step x_update2	{ SAVEPOINT s; UPDATE o_rb_lock SET val = 'C' WHERE id = 50; }
step x_rb_sp	{ ROLLBACK TO SAVEPOINT s; }
step x_rollback	{ ROLLBACK; }

session y
step y_lock		{ BEGIN; SELECT id FROM o_rb_lock WHERE id = 50 FOR KEY SHARE; }
step y_fk		{ BEGIN; INSERT INTO o_rb_lock_child VALUES (50); }
step y_sp_lock	{ BEGIN; SAVEPOINT s;
				  SELECT id FROM o_rb_lock WHERE id = 50 FOR KEY SHARE; }
step y_rb_sp	{ ROLLBACK TO SAVEPOINT s; }
step y_commit	{ COMMIT; }
step y_rollback	{ ROLLBACK; }

session y2
step y2_lock	{ BEGIN; SELECT id FROM o_rb_lock WHERE id = 50 FOR KEY SHARE; }
step y2_commit	{ COMMIT; }

session w
step w_update	{ BEGIN; UPDATE o_rb_lock SET val = 'D' WHERE id = 50; }
step w_lock		{ BEGIN; SELECT id, val FROM o_rb_lock WHERE id = 50 FOR UPDATE NOWAIT; }
step w_rollback	{ ROLLBACK; }
step w_commit	{ COMMIT; }

session z
setup			{ SET enable_indexscan = off; SET enable_bitmapscan = off; }
step z_open		{ BEGIN ISOLATION LEVEL REPEATABLE READ;
				  DECLARE c CURSOR FOR SELECT id, val FROM o_rb_lock;
				  FETCH 1 FROM c; }
step z_fetch	{ MOVE 48 IN c; FETCH 1 FROM c; COMMIT; }
step z_check	{ SELECT id, val FROM o_rb_lock WHERE id = 50; }

# FOR KEY SHARE: row 50 must be 'A'
permutation x_update y_lock z_open x_rollback z_fetch y_commit z_check

# The foreign key check of an insert into a referencing table
permutation x_update y_fk z_open x_rollback z_fetch y_commit z_check

# No lock: row 50 is 'A'
permutation x_update z_open x_rollback z_fetch z_check

# Rollback to a savepoint under the lock, then of the whole transaction
permutation x_update x_update2 y_lock z_open x_rb_sp z_fetch x_rollback y_commit z_check

# A second update, lock and rollback on top of a dispatch record
permutation x_update y_lock x_rollback w_update y2_lock z_open w_rollback z_fetch y2_commit y_commit z_check

# The lock under the dispatch record still conflicts, until its holder ends
permutation x_update y_lock x_rollback w_lock w_rollback y_commit w_lock w_commit z_check

# Two locks under it: one ending leaves the other
permutation x_update y_lock y2_lock x_rollback y2_commit w_lock w_rollback y_commit w_lock w_commit

# A rolled back subtransaction takes its lock away from under it
permutation x_update y_sp_lock x_rollback y_rb_sp w_lock w_commit y_rollback

# The holder's rollback does too
permutation x_update y_lock x_rollback y_rollback w_lock w_commit

# A committed update on top of it
permutation x_update y_lock x_rollback w_update w_commit y_commit z_check

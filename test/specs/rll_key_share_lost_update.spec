# A FOR KEY SHARE row lock taken by a third session while a modification
# waits for a concurrent update must not make the waiter miss that update.
#
# In most permutations s1 updates the row, s2 updates, deletes or locks it
# before s1 commits and waits for s1, and s3 takes FOR KEY SHARE, which does
# not conflict with s1's update, so its lock-only record ends up above s1's
# version.  Once s1 commits, s2 has to see s1's version: in READ COMMITTED it
# rechecks the row on that version, in REPEATABLE READ it fails with a
# serialization error.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE ks_parent (
		id int NOT NULL PRIMARY KEY,
		counter int NOT NULL,
		other int NOT NULL
	) USING orioledb;
	CREATE TABLE ks_child (
		id int NOT NULL PRIMARY KEY,
		parent_id int NOT NULL REFERENCES ks_parent (id),
		note int NOT NULL DEFAULT 0
	) USING orioledb;
	CREATE TABLE ks_grandchild (
		id int NOT NULL PRIMARY KEY,
		child_id int NOT NULL REFERENCES ks_child (id)
	) USING orioledb;
	INSERT INTO ks_parent VALUES (1, 0, 0);
	INSERT INTO ks_child VALUES (10, 1);
}

teardown
{
	DROP TABLE ks_grandchild;
	DROP TABLE ks_child;
	DROP TABLE ks_parent;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_inc" { UPDATE ks_parent SET counter = counter + 1 WHERE id = 1; }
step "s1_grandchild_insert" { INSERT INTO ks_grandchild VALUES (1, 10); }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_begin_rr" { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s2_key_share" { SELECT id FROM ks_parent WHERE id = 1 FOR KEY SHARE; }
step "s2_snapshot" { SELECT counter, other FROM ks_parent WHERE id = 1; }
step "s2_update" { UPDATE ks_parent SET other = other + 1 WHERE id = 1; }
step "s2_delete" { DELETE FROM ks_parent WHERE id = 1 AND counter = 0; }
step "s2_for_no_key_update" { SELECT counter, other FROM ks_parent WHERE id = 1 FOR NO KEY UPDATE; }
step "s2_for_share" { SELECT counter, other FROM ks_parent WHERE id = 1 FOR SHARE; }
step "s2_child_update" { UPDATE ks_child SET note = note + 1 WHERE id = 10; }
step "s2_commit" { COMMIT; }

session "s3"
step "s3_begin" { BEGIN; }
step "s3_key_share" { SELECT id FROM ks_parent WHERE id = 1 FOR KEY SHARE; }
step "s3_fk_insert" { INSERT INTO ks_child VALUES (1, 1); }
step "s3_parent_for_update_nowait" { SELECT id FROM ks_parent WHERE id = 1 FOR UPDATE NOWAIT; }
step "s3_commit" { COMMIT; }
step "s3_rollback" { ROLLBACK; }
step "s3_select" { SELECT * FROM ks_parent; }

session "s4"
step "s4_begin" { BEGIN; }
step "s4_key_share" { SELECT id FROM ks_parent WHERE id = 1 FOR KEY SHARE; }
step "s4_commit" { COMMIT; }

# UPDATE: s1's increment must survive
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_update" "s3_key_share" "s1_commit" "s2_commit" "s3_commit" "s3_select"
# the same with the lock taken by a foreign key check
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_update" "s3_fk_insert" "s1_commit" "s2_commit" "s3_commit" "s3_select"
# the locker has rolled back before s2 goes on
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_update" "s3_key_share" "s3_rollback" "s1_commit" "s2_commit" "s3_select"
# the waiter itself holds FOR KEY SHARE above s1's version
permutation "s1_begin" "s2_begin" "s1_inc" "s2_key_share" "s2_update" "s1_commit" "s2_commit" "s3_select"
# DELETE conflicts with FOR KEY SHARE and waits for s3 as well; the row no
# longer matches counter = 0 once s1 committed
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_delete" "s3_key_share" "s1_commit" "s3_commit" "s2_commit" "s3_select"
# row locks that conflict with s1's update must return s1's version
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_for_no_key_update" "s3_key_share" "s1_commit" "s2_commit" "s3_commit" "s3_select"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_for_share" "s3_key_share" "s1_commit" "s2_commit" "s3_commit" "s3_select"
# REPEATABLE READ: s2 must fail to serialize instead of overwriting s1
permutation "s2_begin_rr" "s2_snapshot" "s1_begin" "s3_begin" "s1_inc" "s2_update" "s3_key_share" "s1_commit" "s2_commit" "s3_commit" "s3_select"
# s1's foreign key check locks the child row FOR KEY SHARE.  s2 leaves
# parent_id unchanged on a child row it did not write, so it must not check
# the foreign key again, and s3 can lock the parent row.
permutation "s1_begin" "s2_begin" "s3_begin" "s1_grandchild_insert" "s2_child_update" "s3_parent_for_update_nowait" "s1_commit" "s2_commit" "s3_commit"
# s3 and s4 both take FOR KEY SHARE, so a lock-only record also lies below
# the one on the page
permutation "s1_begin" "s2_begin" "s3_begin" "s4_begin" "s1_inc" "s2_update" "s3_key_share" "s4_key_share" "s1_commit" "s2_commit" "s3_commit" "s4_commit" "s3_select"
permutation "s1_begin" "s2_begin" "s3_begin" "s4_begin" "s1_inc" "s2_for_share" "s3_key_share" "s4_key_share" "s1_commit" "s2_commit" "s3_commit" "s4_commit" "s3_select"
permutation "s2_begin_rr" "s2_snapshot" "s1_begin" "s3_begin" "s4_begin" "s1_inc" "s2_update" "s3_key_share" "s4_key_share" "s1_commit" "s2_commit" "s3_commit" "s4_commit" "s3_select"
# s1 rolls back under the two locks
permutation "s1_begin" "s2_begin" "s3_begin" "s4_begin" "s1_inc" "s2_update" "s3_key_share" "s4_key_share" "s1_rollback" "s2_commit" "s3_commit" "s4_commit" "s3_select"
# one of the two lockers commits before s1
permutation "s1_begin" "s2_begin" "s3_begin" "s4_begin" "s1_inc" "s2_update" "s3_key_share" "s4_key_share" "s3_commit" "s1_commit" "s2_commit" "s4_commit" "s3_select"
# s2's own FOR KEY SHARE lies below s3's when its update waits
permutation "s1_begin" "s2_begin" "s3_begin" "s1_inc" "s2_key_share" "s3_key_share" "s2_update" "s1_commit" "s2_commit" "s3_commit" "s3_select"

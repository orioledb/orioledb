# Regression test: SELECT ... FOR UPDATE must not wait on the in-progress
# re-inserter of a key whose committed DELETE happened after our snapshot.
# s1 fixes an RR snapshot while key 5 is alive; s2 deletes key 5 (commits);
# s3 re-inserts key 5 (in-progress); s1's FOR UPDATE for the minimum key must
# raise a serialization error immediately, NOT block on s3.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE fu_reins (
		id int NOT NULL,
		val int NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	INSERT INTO fu_reins VALUES (5, 100), (10, 100), (20, 100);
}

teardown
{
	DROP TABLE fu_reins;
}

session "s1"
step "s1_begin_rr" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_rc" { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1_snapshot"  { SELECT count(*) FROM fu_reins; }
step "s1_forupdate" { SELECT id FROM fu_reins ORDER BY id ASC LIMIT 1 FOR UPDATE; }
step "s1_commit"    { COMMIT; }

session "s2"
step "s2_delete"    { DELETE FROM fu_reins WHERE id = 5; }

session "s3"
setup { BEGIN; }
step "s3_reinsert"  { INSERT INTO fu_reins VALUES (5, 200); }
step "s3_commit"    { COMMIT; }
step "s3_rollback"  { ROLLBACK; }

permutation "s1_begin_rr" "s1_snapshot" "s2_delete" "s3_reinsert" "s1_forupdate" "s3_commit" "s1_commit"
permutation "s1_begin_rr" "s1_snapshot" "s2_delete" "s3_reinsert" "s3_rollback" "s1_forupdate" "s1_commit"
permutation "s1_begin_rc" "s1_snapshot" "s2_delete" "s3_reinsert" "s1_forupdate" "s3_commit" "s1_commit"

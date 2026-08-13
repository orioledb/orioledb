setup
{
  CREATE EXTENSION IF NOT EXISTS orioledb;
  CREATE TABLE o_test_1 (
    val_1	int PRIMARY KEY,
    val_2	int
  )USING orioledb;

  INSERT INTO o_test_1 VALUES (1, 1);
}

teardown
{
  DROP TABLE o_test_1;
}

session s1
setup		{ BEGIN; }
step "savepoint_1"	{ SAVEPOINT f; }
step "update_1"	{ UPDATE o_test_1 SET val_1 = 2; }
step "rollback_1"	{ ROLLBACK TO f; } 
step "select_1_key_share"	{ SELECT * FROM o_test_1 FOR KEY SHARE; }
step "select_1_no_key_update" { SELECT * FROM o_test_1 FOR NO KEY UPDATE; }
step "lock_1"	{ SELECT * FROM o_test_1 WHERE val_1 = 1 FOR UPDATE; }
step "savepoint_g"	{ SAVEPOINT g; }
step "insert_g"	{ INSERT INTO o_test_1 VALUES (2, 2); }
step "rollback_g"	{ ROLLBACK TO g; }
step "commit_1"	{ COMMIT; }

session s2
setup		{ BEGIN; }
step "select_2_key_share"	{ SELECT * FROM o_test_1 FOR KEY SHARE; }
step "select_2_no_key_update" { SELECT * FROM o_test_1 FOR NO KEY UPDATE; }
step "update_2"	{ UPDATE o_test_1 SET val_2 = 100 WHERE val_1 = 1; }
step "upsert_2"	{ INSERT INTO o_test_1 VALUES (1, 5) ON CONFLICT (val_1) DO UPDATE SET val_2 = o_test_1.val_2 + 1; }
step "commit_2"	{ COMMIT; }
step "select_2_all"	{ SELECT * FROM o_test_1 ORDER BY val_1; }

permutation "savepoint_1" "update_1" "select_2_key_share" "rollback_1" "select_1_key_share" "commit_2" "commit_1"
permutation "select_1_key_share" "savepoint_1" "select_1_no_key_update" "select_2_no_key_update"  "rollback_1"  "commit_1"  "commit_2"

# A subtransaction rollback runs oxid_notify_all(), which releases every
# waiter -- including one whose row is still locked, here s2 on row 1, locked
# by s1 before the savepoint.  That wake-up goes through
# RemoveFromWaitQueue(), which frees the waiter's proclock, so the waiter must
# conclude nothing from it and go back to sleep; it may only proceed once s1
# commits.  See "Re-find the proclock after waiting instead of trusting a
# stale pointer" in the PostgreSQL fork (ORI-247).
permutation "lock_1" "savepoint_g" "insert_g" "update_2" "rollback_g" "commit_1" "commit_2" "select_2_all"
# same, with the waiter blocked inside INSERT ... ON CONFLICT DO UPDATE, which
# is the statement the crash was reported on
permutation "lock_1" "savepoint_g" "insert_g" "upsert_2" "rollback_g" "commit_1" "commit_2" "select_2_all"

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
step "commit_1"	{ COMMIT; }

session s2
setup		{ BEGIN; }
step "select_2_key_share"	{ SELECT * FROM o_test_1 FOR KEY SHARE; }
step "select_2_no_key_update" { SELECT * FROM o_test_1 FOR NO KEY UPDATE; }
step "commit_2"	{ COMMIT; }

permutation "savepoint_1" "update_1" "select_2_key_share" "rollback_1" "select_1_key_share" "commit_2" "commit_1"
permutation "select_1_key_share" "savepoint_1" "select_1_no_key_update" "select_2_no_key_update"  "rollback_1"  "commit_1"  "commit_2"


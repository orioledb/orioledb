setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_test_1 (
		val_1 int PRIMARY KEY,
		val_2 int
	) USING orioledb;

	INSERT INTO o_test_1 VALUES(1, 1);
}

teardown
{
	DROP TABLE o_test_1;
}

session s1
step "begin_1" { BEGIN; }
step "key_share_1" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR KEY SHARE;}
step "share_1" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR SHARE; }
step "fornokeyupd_1" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR NO KEY UPDATE; }
step "update_1" { UPDATE o_test_1 set val_2 = 2 WHERE val_1 = 1;  }
step "savepoint_1_a" { savepoint a; }
step "savepoint_1_b" { savepoint b; }
step "rollback_1_a" { ROLLBACK to a; }
step "rollback_1_b" { ROLLBACK to b; }
step "rollback_1" { ROLLBACK; }
step "commit_1" { COMMIT; }

session s2
step "begin_2" { BEGIN; }
step "forkeyshare_2" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR KEY SHARE; }
step "fornokeyupd_2" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR NO KEY UPDATE; }
step "for_update_2" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR UPDATE; }
step "rollback_2" { ROLLBACK; }

session s3
step "begin_3" { BEGIN; }
step "key_share_3" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR KEY SHARE; }
step "share_3" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR SHARE; }
step "for_update_3" { SELECT val_1 FROM o_test_1 WHERE val_1 = 1 FOR UPDATE; }
step "update_3" { UPDATE o_test_1 SET val_2 = 2 WHERE val_1 = 1; }
step "delete_3" { DELETE FROM o_test_1 WHERE val_1 = 1; }
step "rollback_3" { ROLLBACK; }
step "commit_3" { COMMIT; }

permutation "begin_1" "begin_2" "begin_3" "share_1" "for_update_2" "share_3" "for_update_3" "rollback_1" "rollback_3" "rollback_2"
permutation "begin_1" "begin_2" "begin_3" "key_share_1" "for_update_2" "key_share_3" "update_1" "update_3" "rollback_1" "rollback_3" "rollback_2"
permutation "begin_1" "begin_2" "begin_3" "key_share_1" "for_update_2" "key_share_3" "update_1" "update_3" "commit_1" "rollback_3" "rollback_2"
permutation "begin_1" "begin_2" "begin_3" "key_share_1" "for_update_2" "key_share_3"  "delete_3" "rollback_1" "rollback_3" "rollback_2"
permutation "begin_1" "begin_2" "begin_3" "key_share_1" "for_update_2" "key_share_3"  "delete_3" "rollback_1" "commit_3" "rollback_2"
permutation "begin_1" "begin_2" "begin_3" "key_share_1" "for_update_3" "forkeyshare_2" "savepoint_1_b" "share_1" "savepoint_1_b" "fornokeyupd_1" "fornokeyupd_2" "rollback_1_b" "rollback_1_a" "rollback_1" "rollback_2" "rollback_3"
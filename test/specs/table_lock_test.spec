setup
{
    CREATE EXTENSION IF NOT EXISTS orioledb;

    CREATE TABLE o_test_1(
        val_1 int,
        val_2 int,
        val_3 int,
        val_4 int,
        val_6 int,
        val_7 int
    )USING orioledb;

    INSERT INTO o_test_1(val_1, val_2, val_3, val_4, val_6, val_7)
        (SELECT val_1, val_1 + 100, val_1, val_1 + 50, val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);

    CREATE INDEX val_ind_1 ON o_test_1(val_1);

    CREATE TABLE o_test_2(
        val_1 int,
        val_2 int
    )USING orioledb;

    INSERT INTO o_test_2(val_1, val_2)
        (SELECT val_1, val_1 + 100 FROM generate_series(1, 10) AS val_1);

}

teardown
{
    DROP TABLE IF EXISTS o_test_1;
    DROP TABLE IF EXISTS o_test_2;
}

session "s1"

step "s1_begin" { BEGIN; }
step "s1_commit" { COMMIT; }

step "s1_lock_access_share_1" { LOCK TABLE o_test_1 IN ACCESS SHARE MODE;}
step "s1_lock_access_share_2" { LOCK TABLE o_test_2 IN ACCESS SHARE MODE;}
step "s1_lock_row_exclusive_1" { LOCK TABLE o_test_1 IN ROW EXCLUSIVE MODE; }
step "s1_lock_access_exclusive_1" { LOCK TABLE o_test_1 IN ACCESS EXCLUSIVE MODE; }

step "s1_select_1" { SELECT * FROM o_test_1; }
step "s1_update" { UPDATE o_test_1 SET val_1 = val_1 + 100; }
step "s1_delete" { DELETE FROM o_test_1 WHERE val_2 % 2 = 0; }
step "s1_alter_drop" { ALTER TABLE o_test_1 DROP COLUMN val_3;}
step "s1_alter_rename" { ALTER TABLE o_test_1 RENAME COLUMN val_4 TO val_44;}
step "s1_alter_add" { ALTER TABLE o_test_1 ADD COLUMN val_5 int;}
step "s1_insert" { INSERT INTO o_test_1
        (SELECT val_1, val_1 + 100 FROM generate_series(1, 10) AS val_1); }
step "s1_vacuum" { VACUUM o_test_1; }
step "s1_rollback" { ROLLBACK; }
step "s1_truncate" { TRUNCATE o_test_1; }
step "s1_drop_table_1" { DROP TABLE IF EXISTS o_test_1; }
step "s1_alter_index_1" { ALTER INDEX val_ind_1 RENAME TO
									val_ind_11; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_commit" { COMMIT; }

step "s2_lock_access_share_1" { LOCK TABLE o_test_1 IN ACCESS SHARE MODE;}
step "s2_lock_row_share_1" { LOCK TABLE o_test_1 IN ROW SHARE MODE; }
step "s2_lock_row_exclusive_1" { LOCK TABLE o_test_1 IN ROW EXCLUSIVE MODE; }
step "s2_lock_share_update_exclusive_1" { LOCK TABLE o_test_1 IN SHARE UPDATE EXCLUSIVE MODE; }
step "s2_lock_share_1" { LOCK TABLE o_test_1 IN SHARE MODE; }
step "s2_lock_share_row_exclusive_1" { LOCK TABLE o_test_1 IN SHARE ROW EXCLUSIVE MODE; }
step "s2_lock_exclusive_1" { LOCK TABLE o_test_1 IN EXCLUSIVE MODE; }
step "s2_lock_access_exclusive_1" { LOCK TABLE o_test_1 IN ACCESS EXCLUSIVE MODE; }

step "s2_select_1" { SELECT * FROM o_test_1; }
step "s2_select_2" { SELECT * FROM o_test_1; }
step "s2_update" { UPDATE o_test_1 SET val_1 = val_1 + 100; }
step "s2_delete" { DELETE FROM o_test_1 WHERE val_2 % 2 = 0; }
step "s2_alter_rename" { ALTER TABLE o_test_1 RENAME COLUMN val_7 TO val_77;}
step "s2_insert" { INSERT INTO o_test_1
        (SELECT val_1, val_1 + 100 FROM generate_series(1, 10) AS val_1); }
step "s2_truncate" { TRUNCATE o_test_1; }
step "s2_drop_table_2" { DROP TABLE IF EXISTS o_test_2; }

permutation "s1_begin" "s2_begin" "s1_lock_access_share_1" "s2_lock_access_exclusive_1" "s1_select_1" "s1_commit" "s2_select_1" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s1_select_1" "s2_lock_access_share_1" "s1_commit" "s1_select_1" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_share_2" "s2_lock_access_exclusive_1" "s1_select_1" "s2_select_2" "s2_commit" "s1_commit"
permutation "s1_begin" "s2_begin" "s1_lock_row_exclusive_1" "s2_lock_row_share_1" "s1_update" "s2_delete" "s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin" "s1_lock_row_exclusive_1" "s1_insert" "s2_lock_row_exclusive_1" "s1_insert" "s2_delete" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_row_exclusive_1" "s2_lock_access_exclusive_1" "s1_insert" "s1_commit" "s2_update" "s2_commit"
permutation "s2_begin" "s2_lock_share_update_exclusive_1" "s2_lock_share_update_exclusive_1" "s1_vacuum" "s2_commit"
permutation "s2_begin" "s2_lock_share_update_exclusive_1" "s2_lock_share_1" "s1_vacuum" "s2_commit"

permutation "s2_begin" "s2_lock_share_update_exclusive_1" "s2_lock_share_row_exclusive_1" "s1_vacuum" "s2_commit"
permutation "s2_begin" "s2_lock_share_update_exclusive_1" "s2_lock_exclusive_1" "s1_vacuum" "s2_commit"
permutation "s2_begin" "s2_lock_share_update_exclusive_1" "s2_lock_access_exclusive_1" "s1_vacuum" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_access_share_1" "s1_truncate" "s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_access_share_1" "s1_drop_table_1" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_access_share_1" "s1_alter_add" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s1_insert" "s2_lock_row_share_1" "s1_alter_rename" "s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin" "s2_insert" "s1_lock_access_exclusive_1" "s2_lock_row_exclusive_1" "s2_commit" "s1_truncate" "s1_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_share_update_exclusive_1" "s1_alter_index_1" "s1_alter_add" "s1_alter_drop" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_update" "s1_lock_access_exclusive_1" "s1_update" "s2_lock_share_1" "s1_alter_add" "s1_commit" "s2_truncate" "s2_commit"
permutation "s2_begin" "s2_lock_access_exclusive_1" "s1_vacuum" "s2_lock_share_row_exclusive_1" "s2_commit"

permutation "s1_begin" "s2_begin" "s1_truncate" "s1_insert" "s1_lock_access_exclusive_1" "s2_lock_exclusive_1" "s1_alter_rename" "s1_alter_add" "s1_alter_drop" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_exclusive_1" "s1_delete" "s1_commit" "s2_drop_table_2" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_access_exclusive_1" "s1_alter_add" "s1_rollback" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_lock_access_exclusive_1" "s2_lock_access_exclusive_1" "s1_update" "s1_update" "s1_update" "s1_commit" "s2_alter_rename" "s2_commit"



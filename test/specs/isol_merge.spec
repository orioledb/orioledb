setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_test_1(
	    val_1 int,
	    val_2 int
    )USING orioledb;

    CREATE TABLE o_test_2(
	    val_3 int,
	    val_4 int,
        val_5 int
    )USING orioledb;

    INSERT INTO o_test_1(val_1, val_2)
        (SELECT val_1, val_1 * 100 FROM generate_series (1, 11) val_1);

    INSERT INTO o_test_2(val_3, val_4)
	    (SELECT val_3, val_3 * 200 FROM generate_series (8, 14) val_3);

}

teardown
{
	DROP TABLE IF EXISTS o_test_1;
    DROP TABLE IF EXISTS o_test_2;
}

session "s1"

step "s1_begin_1" { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1_begin_2" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_begin_3" { BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s1_merge_1" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN NOT MATCHED THEN
                    INSERT (val_2) VALUES (333)
                    WHEN MATCHED THEN
                    DELETE; }

step "s1_merge_2" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN NOT MATCHED THEN
                    INSERT (val_2) VALUES (333)
                    WHEN MATCHED THEN
                    UPDATE SET val_2 = val_1 + val_2; }

step "s1_merge_3" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN NOT MATCHED THEN
                    INSERT (val_2) VALUES (333)
                    WHEN MATCHED THEN
                    DELETE; }

step "s1_merge_4" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN MATCHED THEN
                    DELETE; }
step "s1_merge_5" {
    MERGE INTO o_test_1 t
	USING (SELECT 1 AS val_1) s
	ON s.val_1 = t.val_1
	WHEN MATCHED THEN
	UPDATE SET val_2 = 33;
}

step "s1_commit" { COMMIT; }

step "s1_select_1" { SELECT * FROM o_test_1; }
step "s1_select_1_2" { SELECT * FROM o_test_1 WHERE val_1 = 1; }
step "s1_select_2" { SELECT * FROM o_test_2; }

step "s1_alter_rename" { ALTER TABLE o_test_1 RENAME val_1 TO val_11; }
step "s1_alter_add" { ALTER TABLE o_test_1 ADD COLUMN val_12 int; }
step "s1_alter_drop" { ALTER TABLE o_test_1 DROP COLUMN val_2; }

step "s1_update" { UPDATE o_test_1 SET val_2 = 40 WHERE val_2 % 2 = 0;}
step "s1_drop" { DROP TABLE o_test_1; }
step "s1_drop_2" { DROP TABLE o_test_5; }
step "s1_create_table" { CREATE TABLE o_test_3 (
                            val_11 int,
                            val_22 int
                        )USING orioledb; }
step "s1_create_table_2" { CREATE TABLE o_test_5 (
                            val_1 int,
                            val_2 int
                        )USING orioledb;
                        INSERT INTO o_test_5(val_1, val_2)
                            (SELECT val_1, val_1 * 100 FROM generate_series (1, 11) val_1);}

session "s2"

step "s2_begin_1" { BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2_begin_2" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_begin_3" { BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }

step "s2_commit" { COMMIT; }

step "s2_update" { UPDATE o_test_2 SET val_4 = 40 WHERE val_4 % 2 = 0;}
step "s2_update_2" { UPDATE o_test_1 SET val_2 = val_2 + 10 WHERE val_1 = 1; }
step "s2_drop" { DROP TABLE o_test_3; }
step "s2_drop_2" { DROP TABLE o_test_4; }
step "s2_create_table" { CREATE TABLE o_test_4 (
                            val_11 int,
                            val_22 int
                        )USING orioledb; }

step "s2_rollback" {ROLLBACK;}

step "s2_alter_add" { ALTER TABLE o_test_1 ADD COLUMN val_32 int; }

step "s2_select_1" { SELECT * FROM o_test_1; }
step "s2_select_1_2" { SELECT * FROM o_test_1 WHERE val_1 = 1; }
step "s2_select_2" { SELECT * FROM o_test_2; }

step "s2_alter_rename" { ALTER TABLE o_test_2 RENAME val_5 TO val_55; }

step "s2_merge_1" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN NOT MATCHED THEN
                    INSERT (val_2) VALUES (333)
                    WHEN MATCHED THEN
                    DELETE; }

step "s2_merge_2" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN NOT MATCHED THEN
                    INSERT (val_2) VALUES (333)
                    WHEN MATCHED THEN
                    UPDATE SET val_2 = val_1 + val_2; }

step "s2_merge_3" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN NOT MATCHED THEN
                    INSERT (val_2) VALUES (333)
                    WHEN MATCHED THEN
                    DELETE; }

step "s2_merge_4" { MERGE INTO o_test_1 t
                    USING o_test_2 s
                    ON t.val_1 = s.val_3
                    WHEN MATCHED THEN
                    DELETE; }

step "s2_merge_5" { MERGE INTO o_test_1 t
                    USING o_test_5 s
                    ON t.val_1 = s.val_3
                    WHEN MATCHED THEN
                    DELETE; }

permutation "s1_begin_2" "s1_merge_1" "s2_begin_1" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_2" "s2_begin_2" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_3" "s2_begin_3" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_4" "s2_begin_1" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"

permutation "s1_begin_2" "s1_merge_1" "s2_begin_2" "s2_select_1" "s2_select_2" "s1_commit"  "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_2" "s2_begin_3" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_3" "s2_begin_1" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_4" "s2_begin_2" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"
permutation "s1_begin_2" "s1_merge_3" "s2_begin_3" "s2_select_1" "s2_select_2" "s1_commit" "s2_select_1" "s2_select_2" "s2_commit"

permutation "s1_begin_3" "s2_begin_3" "s1_merge_1" "s1_merge_2" "s1_merge_3" "s1_commit" "s2_commit"
permutation "s1_begin_1" "s2_begin_3" "s1_merge_1" "s1_merge_2" "s1_merge_3" "s1_commit" "s2_commit"
permutation "s1_begin_1" "s2_begin_1" "s1_merge_1" "s1_merge_2" "s1_merge_4" "s1_commit" "s2_commit"

permutation "s2_begin_2" "s2_alter_add" "s1_begin_1" "s2_merge_3" "s2_rollback" "s1_select_1" "s1_select_2" "s1_commit"
permutation "s1_begin_2" "s2_create_table" "s2_begin_1" "s2_merge_2" "s1_drop" "s2_commit" "s1_commit" "s2_drop_2"
permutation "s1_begin_1" "s1_create_table_2" "s1_commit" "s1_begin_3" "s2_begin_1" "s2_merge_5" "s1_create_table" "s1_commit" "s2_commit" "s1_drop_2"
permutation "s2_begin_1" "s2_update" "s2_merge_4" "s1_begin_3" "s1_alter_drop" "s2_commit" "s1_alter_add" "s1_commit"

permutation "s2_begin_3" "s2_alter_rename" "s2_drop" "s1_begin_1"  "s2_merge_1" "s1_alter_rename" "s2_commit" "s1_update" "s1_commit"

permutation "s1_begin_1" "s2_begin_1"
            "s2_update_2" "s1_select_1_2" "s2_select_1_2"
            "s1_merge_5"
            "s2_commit"
            "s1_select_1_2" "s2_select_1_2"
            "s1_commit"
            "s1_select_1_2" "s2_select_1_2"

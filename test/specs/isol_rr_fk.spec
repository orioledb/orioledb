setup {

	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_test_1 (
		val_1 int PRIMARY KEY,
		val_2 varchar,
		val_3 varchar
	) USING orioledb;

	CREATE TABLE o_test_2 (
		val_1 int PRIMARY KEY,
		val_2 varchar,
		val_3 int REFERENCES o_test_1 (val_1)
	) USING orioledb;

	INSERT INTO o_test_1 (val_1, val_2) VALUES (1, 'a');
	INSERT INTO o_test_2 (val_1, val_2) VALUES (1, 'a1');
}

teardown
{
	DROP TABLE o_test_1, o_test_2;
}

session s1

step begin_s1		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }

step update1_s1		{ UPDATE o_test_2 SET val_2 = 'a2', val_3 = 1 WHERE val_1 = 1; }
step update2_s1		{ UPDATE o_test_2 SET val_2 = 'a3', val_3 = 1 WHERE val_1 = 1; }
step commit_s1		{ COMMIT; }

session s2

step begin_s2		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }

step update_s2		{ UPDATE o_test_1 SET val_3 = 'b' WHERE val_1 = 1; }
step commit_s2		{ COMMIT; }

permutation begin_s1 begin_s2 update1_s1 update_s2 commit_s2 update2_s1 commit_s1
permutation begin_s1 update1_s1 begin_s2 update_s2 commit_s2 update2_s1 commit_s1

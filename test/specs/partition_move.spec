setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_test (
		key integer,
		val text
	) PARTITION BY LIST (key);

	CREATE TABLE o_test_part1 (key integer, val text) USING orioledb;
	CREATE TABLE o_test_part2 (val text, key integer) USING orioledb;
	CREATE TABLE o_test_part3 (key integer, val text) USING orioledb;

	ALTER TABLE o_test ATTACH PARTITION o_test_part1 FOR VALUES IN (1, 4);
	ALTER TABLE o_test ATTACH PARTITION o_test_part2 FOR VALUES IN (2, 5, 6);
	ALTER TABLE o_test ATTACH PARTITION o_test_part3 DEFAULT;

	INSERT INTO o_test VALUES (1, 'b');
	INSERT INTO o_test VALUES (2, 'b');
}

teardown
{
	DROP TABLE o_test CASCADE;
}

session "s1"
setup { BEGIN; }

step "s1_cross_update" {
	UPDATE o_test SET key = key + 1 WHERE key = 1;
}

step "s1_commit" { COMMIT; }

session "s2"
setup { BEGIN; }
step "s2_value_update"
{
	UPDATE o_test SET val = val || 'xxx' WHERE key = 1;
}
step "s2_key_update"
{
	UPDATE o_test SET key = 4 WHERE key = 1;
}
step "s2_delete"
{
	DELETE FROM o_test WHERE key = 1;
}
step "s2_commit" { COMMIT; }

permutation "s1_cross_update" "s2_value_update" "s1_commit" "s2_commit"
permutation "s1_cross_update" "s2_key_update" "s1_commit" "s2_commit"
permutation "s1_cross_update" "s2_delete" "s1_commit" "s2_commit"

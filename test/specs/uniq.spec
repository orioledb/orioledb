setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_uniq (
		key int8 NOT NULL,
		value integer,
		PRIMARY KEY(key)
	) USING orioledb;
	TRUNCATE o_uniq;
}

teardown
{
	DROP TABLE o_uniq;
}

session "s1"

step "s1_begin"  { BEGIN; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }
step "s1_insert" { INSERT INTO o_uniq VALUES (1, 'abc'); }


session "s2"

step "s2_begin"  { BEGIN; }
step "s2_commit" { COMMIT; }
step "s2_rollback" { ROLLBACK; }
step "s2_insert" { INSERT INTO o_uniq VALUES (1, 'def'); }
step "s2_update" { UPDATE o_uniq SET value = value || 'ghi' WHERE id = 1 RETURNING *; }

permutation "s1_begin" "s1_insert" "s2_begin" "s1_commit" "s2_update" "s2_commit"
permutation "s1_begin" "s1_insert" "s1_commit" "s2_begin" "s2_update" "s2_commit"
permutation "s1_begin" "s1_insert" "s2_begin" "s2_update" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_insert" "s1_commit" "s2_insert" "s2_commit"

permutation "s1_begin" "s2_begin" "s2_insert" "s1_insert" "s2_commit" "s1_commit"
permutation "s1_begin" "s1_insert" "s1_commit" "s2_begin" "s2_insert" "s2_commit"
permutation "s1_begin" "s2_begin" "s2_insert" "s2_commit" "s1_insert" "s1_commit"
permutation "s1_begin" "s1_insert" "s2_begin" "s1_commit" "s2_insert" "s2_commit"

permutation "s1_begin" "s2_begin" "s2_insert" "s2_rollback" "s1_insert" "s1_commit"
permutation "s1_begin" "s1_insert" "s1_rollback" "s2_begin" "s2_insert" "s2_commit"
permutation "s1_begin" "s2_begin" "s2_insert" "s1_insert" "s2_rollback" "s1_commit"
permutation "s1_begin" "s1_insert" "s2_begin" "s1_rollback" "s2_insert" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_insert" "s1_rollback" "s2_insert" "s2_commit"

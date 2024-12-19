setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_test
	(
		id integer NOT NULL,
		val text,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_test;
}

teardown
{
	DROP TABLE o_test;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_insert" { INSERT INTO o_test (SELECT id, id || 'val' FROM generate_series(1, 30, 5) id); }
step "s1_rollback" { ROLLBACK; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_insert" { INSERT INTO o_test (SELECT id, id || 'val' FROM generate_series(2, 30, 5) id); }
step "s2_rollback" { ROLLBACK; }

session "s3"
step "s3_begin" { BEGIN; }
step "s3_insert" { INSERT INTO o_test (SELECT id, id || 'val' FROM generate_series(3, 30, 5) id);
				   INSERT INTO o_test (SELECT id, id || 'val' FROM generate_series(5, 30, 5) id); }
step "s3_rollback" { ROLLBACK; }

session "s4"
step "s4_begin" { BEGIN; }
step "s4_insert" { INSERT INTO o_test (SELECT id, id || 'val' FROM generate_series(4, 30, 5) id); }
step "s4_rollback" { ROLLBACK; }

session "s_str"
step "s_str_structure" { SELECT orioledb_tbl_structure('o_test'::regclass, 'neb'); }

permutation "s1_begin" "s1_insert" "s2_begin" "s2_insert" "s3_begin" "s3_insert" "s4_begin" "s4_insert" "s_str_structure" "s1_rollback" "s2_rollback" "s3_rollback" "s4_rollback"
permutation "s1_insert" "s2_insert" "s3_begin" "s3_insert" "s4_begin" "s4_insert" "s_str_structure" "s3_rollback" "s4_rollback"

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_iso_ddl (
		id1 int8 NOT NULL,
		id2 int8 NOT NULL,
		id3 int8 NOT NULL
	) USING orioledb;
	TRUNCATE o_iso_ddl;
}

teardown
{
	DROP TABLE IF EXISTS o_iso_ddl;
}


session "s1"
step "s1_begin" { BEGIN; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }
step "s1_drop_null_id1" { ALTER TABLE o_iso_ddl ALTER id1 DROP NOT NULL; }
step "s1_drop_null_id3" { ALTER TABLE o_iso_ddl ALTER id3 DROP NOT NULL; }
step "s1_drop_id1" { ALTER TABLE o_iso_ddl DROP id1; }
step "s1_drop_id3" { ALTER TABLE o_iso_ddl DROP id3; }
step "s1_drop_table" { DROP TABLE o_iso_ddl; }
step "s1_drop_all" { DROP EXTENSION orioledb CASCADE; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_commit" { COMMIT; }
step "s2_rollback" { ROLLBACK; }
step "s2_drop_null_id2" { ALTER TABLE o_iso_ddl ALTER id2 DROP NOT NULL; }
step "s2_drop_id2" { ALTER TABLE o_iso_ddl DROP id2; }

session "s3"
step "s3_descr" { SELECT orioledb_table_description('o_iso_ddl'::regclass); }

# s1_commit s2_commit
permutation "s1_begin" "s2_begin" "s1_drop_null_id1" "s2_drop_null_id2" "s1_drop_null_id3" "s1_commit" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_id3" "s1_commit" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_null_id3" "s1_commit" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_table" "s2_drop_id2" "s1_commit" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_all" "s2_drop_id2" "s1_commit" "s2_commit" "s3_descr"

# s1_rollback s2_commit
permutation "s1_begin" "s2_begin" "s1_drop_null_id1" "s2_drop_null_id2" "s1_drop_null_id3" "s1_rollback" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_id3" "s1_rollback" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_null_id3" "s1_rollback" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_table" "s2_drop_id2" "s1_rollback" "s2_commit" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_all" "s2_drop_id2" "s1_rollback" "s2_commit" "s3_descr"

# s1_commit s2_rollback
permutation "s1_begin" "s2_begin" "s1_drop_null_id1" "s2_drop_null_id2" "s1_drop_null_id3" "s1_commit" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_id3" "s1_commit" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_null_id3" "s1_commit" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_table" "s2_drop_id2" "s1_commit" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_all" "s2_drop_id2" "s1_commit" "s2_rollback" "s3_descr"

# s1_rollback s2_rollback (the same output of s3_descr in all cases)
permutation "s1_begin" "s2_begin" "s1_drop_null_id1" "s2_drop_null_id2" "s1_drop_null_id3" "s1_rollback" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_id3" "s1_rollback" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_id1" "s2_drop_id2" "s1_drop_null_id3" "s1_rollback" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_table" "s2_drop_id2" "s1_rollback" "s2_rollback" "s3_descr"
permutation "s1_begin" "s2_begin" "s1_drop_all" "s2_drop_id2" "s1_rollback" "s2_rollback" "s3_descr"

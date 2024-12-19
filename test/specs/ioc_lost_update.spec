setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_ioc (
		id int8 NOT NULL,
		t  text NOT NULL,
		UNIQUE(id)
	) USING orioledb;
	TRUNCATE o_ioc;

	CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
		AS $$
			SELECT substr(string_agg(
							substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
					''), 1, length)
			FROM generate_series(1, (length + 20) / 21) i; $$
	LANGUAGE SQL;
}

teardown
{
	DROP TABLE o_ioc;
	DROP FUNCTION generate_string;
}

session "s1"

step "s1_setup"  { SET orioledb.enable_stopevents = true; }
step "s1_begin"  { BEGIN; }
step "s1_commit" { COMMIT; }
step "s1_insert" { INSERT INTO o_ioc VALUES (1, '2') ON CONFLICT (id) DO UPDATE SET id = 1, t = 'first' || generate_string(1, 5000) || o_ioc.t RETURNING *;}
step "s1_insert2" { INSERT INTO o_ioc VALUES (1, 'first') ON CONFLICT (id) DO UPDATE SET id = 1, t = 'first' RETURNING *; }
step "s1_insert3" { INSERT INTO o_ioc VALUES (1, '2') ON CONFLICT (id) DO UPDATE SET id = 1, t = o_ioc.t RETURNING *;}

session "s2"

step "s2_begin" { BEGIN; }
step "s2_commit" { COMMIT; }
step "s2_insert" { INSERT INTO o_ioc VALUES (1, '2') ON CONFLICT (id) DO UPDATE SET id = 1, t = 'second' || generate_string(2, 5000) || o_ioc.t RETURNING *; }
step "s2_insert2" { INSERT INTO o_ioc VALUES (1, 'second') ON CONFLICT (id) DO UPDATE SET id = 1, t = 'second' RETURNING *; }

session "s3"

step "s3_init" { BEGIN; INSERT INTO o_ioc VALUES (1, 'init'); COMMIT; }
step "s3_init2" { BEGIN; INSERT INTO o_ioc VALUES (1, generate_string(3, 5000)); COMMIT; }
step "s3_setup" { SELECT pg_stopevent_set('ioc_before_update', 'true'); }
step "s3_reset" { SELECT pg_stopevent_reset('ioc_before_update'); }
step "s3_select" { SELECT * FROM o_ioc; }
step "s3_struct" { SELECT orioledb_tbl_structure('o_ioc'::regclass, 'nue'); }

permutation "s3_init" "s3_struct" "s1_begin" "s1_insert" "s1_commit" "s3_struct" "s2_begin" "s2_insert" "s2_commit" "s3_struct" "s3_select"
permutation "s3_init" "s3_struct" "s1_setup" "s3_setup" "s1_begin" "s1_insert" "s2_begin" "s2_insert" "s2_commit" "s3_struct" "s3_select" "s3_reset" "s1_commit" "s3_struct" "s3_select"
permutation "s3_init2" "s3_struct" "s1_setup" "s3_setup" "s1_begin" "s1_insert" "s2_begin" "s2_insert" "s2_commit" "s3_struct" "s3_select" "s3_reset" "s1_commit" "s3_struct" "s3_select"
permutation "s3_init2" "s3_struct" "s1_setup" "s3_setup" "s1_begin" "s1_insert3" "s2_begin" "s2_insert" "s2_commit" "s3_struct" "s3_select" "s3_reset" "s1_commit" "s3_struct" "s3_select"
permutation "s3_init2" "s3_struct" "s1_setup" "s3_setup" "s1_begin" "s1_insert2" "s2_begin" "s2_insert" "s2_commit" "s3_struct" "s3_select" "s3_reset" "s1_commit" "s3_struct" "s3_select"
permutation "s3_init2" "s3_struct" "s1_setup" "s3_setup" "s1_begin" "s1_insert" "s2_begin" "s2_insert2" "s2_commit" "s3_struct" "s3_select" "s3_reset" "s1_commit" "s3_struct" "s3_select"

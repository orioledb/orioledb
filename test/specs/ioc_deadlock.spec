# INSERT .. ON CONFLICT deadlocks test
setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_ioc (
		id1 int8 NOT NULL,
		id2 int8 NOT NULL,
		id3 int8 NOT NULL,
		PRIMARY KEY(id1)
	) USING orioledb;

	CREATE UNIQUE INDEX o_ioc_ix1 ON o_ioc (id2);
	CREATE UNIQUE INDEX o_ioc_ix2 ON o_ioc (id3);
	TRUNCATE o_ioc;
}

teardown
{
    DROP TABLE o_ioc;
}

session "s1"

step "s1_setup" { SELECT pg_stopevent_set('index_insert', 'true'); }
step "s1_release_s3" { SELECT pg_stopevent_set('index_insert', '$applicationName == "s4"'); }
step "s1_release_s4" { SELECT pg_stopevent_set('index_insert', '$applicationName == "s3"'); }
step "s1_release" { SELECT pg_stopevent_reset('index_insert'); }

step "s1_begin" { BEGIN; }
step "s1_rollback" { ROLLBACK; }
# blocks s3
step "s1_insert" { INSERT INTO o_ioc VALUES (1, 3, 1) RETURNING *; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_rollback" { ROLLBACK; }
# blocks s4
step "s2_insert" { INSERT INTO o_ioc VALUES (2, 4, 2) RETURNING *; }

session "s3"

step "s3_setup"  {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's3'; }
step "s3_begin" { BEGIN; }
step "s3_commit" { COMMIT; }
step "s3_insert" { INSERT INTO o_ioc VALUES (3, 3, 3) RETURNING *; }

session "s4"

step "s4_setup"  {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's4'; }
step "s4_begin" { BEGIN; }
step "s4_commit" { COMMIT; }
step "s4_insert" { INSERT INTO o_ioc VALUES (3, 4, 3) ON CONFLICT (id3) DO NOTHING RETURNING *; }

permutation "s1_begin" "s2_begin" "s3_begin" "s4_begin" "s1_insert" "s2_insert" "s3_insert" "s4_insert" "s2_rollback" "s1_rollback"  "s3_commit" "s4_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s4_begin" "s1_insert" "s2_insert" "s3_insert" "s4_insert" "s1_rollback" "s2_rollback"  "s3_commit" "s4_commit"

permutation "s3_begin" "s3_setup" "s4_begin" "s4_setup" "s1_setup" "s3_insert" "s4_insert" "s1_release_s3" "s1_release" "s3_commit" "s4_commit"
permutation "s3_begin" "s3_setup" "s4_begin" "s4_setup" "s1_setup" "s3_insert" "s4_insert" "s1_release_s4" "s1_release" "s3_commit" "s4_commit"

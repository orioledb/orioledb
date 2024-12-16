# read committed isolation level tests
setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_isorc (
		id int8 NOT NULL,
		val int8 NOT NULL
	) USING orioledb;
	INSERT INTO o_isorc SELECT id, 1 FROM generate_series(1, 20) id;

	CREATE TABLE IF NOT EXISTS o_isorc_pk (
		id int8 NOT NULL,
		val int8 NOT NULL
	) USING orioledb;
	INSERT INTO o_isorc_pk SELECT id, id FROM generate_series(1, 20) id;

	CREATE TABLE IF NOT EXISTS o_isorc_toast (
		id int8 NOT NULL,
		val text NOT NULL
	) USING orioledb;
	INSERT INTO o_isorc_toast SELECT id, 'test' FROM generate_series(1, 5) id;

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
	DROP TABLE o_isorc;
	DROP TABLE o_isorc_pk;
	DROP TABLE o_isorc_toast;
	DROP FUNCTION generate_string;
}

session "s1"

step "s1_begin" { BEGIN ISOLATION LEVEL READ COMMITTED; }

step "s1_update" { UPDATE o_isorc SET val = val + 1; }
step "s1_update1_byid" { UPDATE o_isorc SET val = val + 1 WHERE id = 1; }
step "s1_update2_byid" { UPDATE o_isorc SET val = val + 1 WHERE id = 2; }
step "s1_update1_byval" { UPDATE o_isorc SET val = val + 1 WHERE val = 1; }
step "s1_update1_10" { UPDATE o_isorc SET val = val + 1 WHERE id <= 10; }
step "s1_update11_20" { UPDATE o_isorc SET val = val + 2 WHERE id > 10; }

step "s1_update_toast" { UPDATE o_isorc_toast SET val = generate_string(1, 600) RETURNING *;}
step "s1_update_toast2" { UPDATE o_isorc_toast SET val = val || generate_string(1, 600) RETURNING *;}

step "s1_select" { SELECT * FROM o_isorc; }
step "s1_select1" { SELECT * FROM o_isorc WHERE id = 1; }

step "s1_delete_pk_id" { DELETE FROM o_isorc_pk WHERE id = 1; }
step "s1_update_pk_val" { UPDATE o_isorc_pk SET val = val + 1 WHERE id = 1; }

step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

# stopevents at beginning of btree_modify_normal()
step "s1_se_set" { SELECT pg_stopevent_set('modify_start', 'true'); }
step "s1_se_set2" { SELECT pg_stopevent_set('modify_start', '$applicationName == "s3"'); }
step "s1_reset" { SELECT pg_stopevent_reset('modify_start'); }

session "s2"

step "s2_begin" { BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s2_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's2'; }

step "s2_update" { UPDATE o_isorc SET val = val + 1; }
step "s2_delete1_val1" { DELETE FROM o_isorc WHERE id = 1 AND val = 1; }
step "s2_update_id1" { UPDATE o_isorc SET id = val WHERE id = 1; }
step "s2_update_id2" { UPDATE o_isorc SET id = val WHERE id = 2; }
step "s2_update1_byid" { UPDATE o_isorc SET val = val + 1 WHERE id = 1; }
step "s2_update1_byval" { UPDATE o_isorc SET val = val + 1 WHERE val = 1; }
step "s2_update_toast" { UPDATE o_isorc_toast SET val = generate_string(2, 500) RETURNING *;}
step "s2_update_toast2" { UPDATE o_isorc_toast SET val = val || generate_string(2, 500) RETURNING *;}

step "s2_delete1" { DELETE FROM o_isorc WHERE val = 1; }
step "s2_delete2" { DELETE FROM o_isorc WHERE val = 2; }
step "s2_delete12" { DELETE FROM o_isorc WHERE val BETWEEN 1 and 2; }

step "s2_select" { SELECT * FROM o_isorc; }
step "s2_select1" { SELECT * FROM o_isorc WHERE id = 1; }

step "s2_delete_pk_val" { DELETE FROM o_isorc_pk WHERE val = 1; }
step "s2_update_pk_id" { UPDATE o_isorc_pk SET id = id + 1 WHERE val = 1; }
step "s2_update_pk_val" { UPDATE o_isorc_pk SET val = val + 1 WHERE val = 1; }

step "s2_commit" { COMMIT; }

session "s3"

step "s3_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's3'; }

step "s3_update_toast" { UPDATE o_isorc_toast SET val = generate_string(3, 400) RETURNING *;}
step "s3_update_toast2" { UPDATE o_isorc_toast SET val = val || generate_string(3, 400) RETURNING *;}

# simple lost update tests
permutation "s1_begin" "s1_update1_byid" "s1_select1" "s2_begin" "s2_update1_byid" "s1_commit" "s2_select1" "s2_commit" "s1_select1" "s2_select1"
permutation "s1_begin" "s1_update1_byval" "s1_select1" "s2_begin" "s2_update1_byval" "s1_commit" "s2_select1" "s2_commit" "s1_select1" "s2_select1"
permutation "s1_begin" "s1_update" "s1_select" "s2_begin" "s2_update" "s1_commit" "s2_select" "s2_commit" "s1_select" "s2_select"
permutation "s1_begin" "s1_update" "s2_begin" "s2_update" "s1_commit" "s1_begin" "s1_update" "s2_commit" "s2_begin" "s2_update" "s1_commit" "s2_commit" "s1_select" "s2_select"
permutation "s1_begin" "s1_update_toast" "s2_begin" "s2_update_toast" "s1_commit" "s2_commit"

# TOAST and lost update tests
permutation "s2_setup" "s3_setup" "s1_se_set" "s1_begin" "s1_update_toast" "s2_update_toast" "s3_update_toast" "s1_commit" "s1_se_set2" "s1_reset"
permutation "s2_setup" "s3_setup" "s1_se_set" "s1_begin" "s1_update_toast2" "s2_update_toast2" "s3_update_toast2" "s1_commit" "s1_se_set2" "s1_reset"

# delete behavion tests
permutation "s1_begin" "s1_update1_byid" "s1_select1" "s2_begin" "s2_delete1" "s1_commit" "s2_select1" "s2_commit" "s1_select1" "s2_select1"
permutation "s1_begin" "s1_update1_byid" "s1_select1" "s2_begin" "s2_delete2" "s1_commit" "s2_select1" "s2_commit" "s1_select1" "s2_select1"
permutation "s1_begin" "s1_update1_byid" "s1_select1" "s2_begin" "s2_delete12" "s1_commit" "s2_select1" "s2_commit" "s1_select1" "s2_select1"

permutation "s1_begin" "s1_update1_10" "s1_update11_20" "s1_select" "s2_begin" "s2_delete1" "s1_commit" "s2_select" "s2_commit" "s1_select" "s2_select"
permutation "s1_begin" "s1_update1_10" "s1_update11_20" "s1_select" "s2_begin" "s2_delete2" "s1_commit" "s2_select" "s2_commit" "s1_select" "s2_select"
permutation "s1_begin" "s1_update1_10" "s1_update11_20" "s1_select" "s2_begin" "s2_delete12" "s1_commit" "s2_select" "s2_commit" "s1_select" "s2_select"

# Concurrent update test: override <=> reinsert
permutation "s1_begin" "s2_begin" "s1_update1_byid" "s2_update_id1" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update1_byid" "s2_update_id1" "s1_rollback"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update2_byid" "s2_update_id2" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update2_byid" "s2_update_id2" "s1_rollback"  "s2_commit" "s1_select"

# Concurrent update test: delete
permutation "s1_begin" "s2_begin" "s1_update1_byid" "s2_delete1_val1" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update1_byid" "s2_delete1_val1" "s1_rollback"  "s2_commit" "s1_select"

# Concurrent delete tests
permutation "s1_begin" "s2_begin" "s1_delete_pk_id" "s2_update_pk_val" "s1_commit"  "s2_commit"
permutation "s1_begin" "s2_begin" "s1_delete_pk_id" "s2_update_pk_id" "s1_commit"  "s2_commit"

# Concurrent qual change tests
permutation "s1_begin" "s2_begin" "s1_update_pk_val" "s2_update_pk_val" "s1_commit"  "s2_commit"
permutation "s1_begin" "s2_begin" "s1_update_pk_val" "s2_delete_pk_val" "s1_commit"  "s2_commit"

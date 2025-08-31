setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_btree_scan (
		id text NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	INSERT INTO o_btree_scan (SELECT to_char(id, 'fm0000') || repeat('x', 500) FROM generate_series(1, 1000, 1) id);
}

teardown
{
	DROP TABLE o_btree_scan;
}

session "s1"

step "s1_setup"  { SET orioledb.enable_stopevents = true; }
step "s1_scan" { SELECT COUNT(*) FROM o_btree_scan; }

session "s2"

step "s2_setup" { SELECT pg_stopevent_set('seq_scan_load_internal_page', 'true'); }
step "s2_delete1" { DELETE FROM o_btree_scan WHERE id < '0500'; }
step "s2_delete2" { DELETE FROM o_btree_scan WHERE id < '0550'; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_reset" { SELECT pg_stopevent_reset('seq_scan_load_internal_page'); }


permutation "s1_setup" "s2_setup" "s1_scan" "s2_delete1" "s2_checkpoint" "s2_delete2" "s2_checkpoint"  "s2_reset"

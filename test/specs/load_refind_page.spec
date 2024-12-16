setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_loadpage (
		id text NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_loadpage;
}

teardown
{
	DROP TABLE o_loadpage;
}

session "s1"

step "s1_setup"  {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1'; }

step "s1_insert1" { INSERT INTO o_loadpage (SELECT id || repeat('x', 2600) FROM generate_series(10, 13, 1) id); }
step "s1_insert2" { INSERT INTO o_loadpage (SELECT id || repeat('x', 2600) FROM generate_series(14, 17, 1) id); }
step "s1_insert3" { INSERT INTO o_loadpage (SELECT id || repeat('x', 2600) FROM generate_series(18, 21, 1) id); }

step "s1_insert4" { INSERT INTO o_loadpage (SELECT id || repeat('x', 2600) FROM generate_series(10, 19, 1) id); }
step "s1_insert5" { INSERT INTO o_loadpage (SELECT id || repeat('x', 2600) FROM generate_series(20, 29, 1) id); }
step "s1_insert6" { INSERT INTO o_loadpage (SELECT id || repeat('x', 2600) FROM generate_series(30, 39, 1) id); }

step "s1_se_refind0" { SELECT pg_stopevent_set('load_page_refind', '$.level == 0 && $applicationName == "s2"'); }
step "s1_se_refind1" { SELECT pg_stopevent_set('load_page_refind', '$.level == 1 && $applicationName == "s2"'); }

step "s1_evict" { SELECT orioledb_evict_pages('o_loadpage'::regclass, 1); }
step "s1_continue" { SELECT pg_stopevent_reset('load_page_refind'); }

session "s2"

step "s2_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's2';
	SET enable_seqscan = off; }

step "s2_select" { SELECT substr(id, 1, 2) FROM o_loadpage ORDER BY id; }
step "s2_bselect" { SELECT substr(id, 1, 2) FROM o_loadpage ORDER BY id DESC; }

permutation "s1_setup" "s2_setup" "s1_insert3" "s1_evict" "s1_se_refind0" "s2_bselect" "s1_insert1" "s1_insert2" "s1_continue"
permutation "s1_setup" "s2_setup" "s1_insert2" "s1_insert3" "s1_evict" "s1_se_refind0" "s2_bselect" "s1_insert1" "s1_continue"

permutation "s1_setup" "s2_setup" "s1_insert6" "s1_evict" "s1_se_refind1" "s2_bselect" "s1_insert4" "s1_insert5" "s1_continue"
permutation "s1_setup" "s2_setup" "s1_insert5" "s1_insert6" "s1_evict" "s1_se_refind1" "s2_bselect" "s1_insert4" "s1_continue"

permutation "s1_setup" "s2_setup" "s1_insert1" "s1_evict" "s1_se_refind0" "s2_select" "s1_insert2" "s1_insert3" "s1_continue"
permutation "s1_setup" "s2_setup" "s1_insert1" "s1_insert2" "s1_evict" "s1_se_refind0" "s2_select" "s1_insert3" "s1_continue"

permutation "s1_setup" "s2_setup" "s1_insert4" "s1_evict" "s1_se_refind1" "s2_select" "s1_insert5" "s1_insert6" "s1_continue"
permutation "s1_setup" "s2_setup" "s1_insert4" "s1_insert5" "s1_evict" "s1_se_refind1" "s2_select" "s1_insert6" "s1_continue"

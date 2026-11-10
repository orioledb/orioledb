setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_conc_idx (
		id bigint NOT NULL PRIMARY KEY,
		a text,
		b text
	) USING orioledb;
	INSERT INTO o_conc_idx VALUES (1, 'x', 'y');
}

teardown
{
	DROP TABLE IF EXISTS o_conc_idx;
}

session "s1"

step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

step "s1_create_idx_a" {
	CREATE INDEX o_conc_idx_a ON o_conc_idx (a);
}

step "s1_check" {
	SET enable_seqscan = off;
	SELECT * FROM o_conc_idx WHERE a = 'x';
	SELECT * FROM o_conc_idx WHERE b = 'y';
}

session "s2"

step "s2_arm" {
	SELECT pg_stopevent_set('define_index_before_update',
		'$applicationName == "s1"');
}

step "s2_create_idx_b" {
	CREATE INDEX o_conc_idx_b ON o_conc_idx (b);
}

step "s2_release" {
	SELECT pg_stopevent_reset('define_index_before_update');
}

# s1 pauses before systrees_modify_start, s2 creates its index while s1 is
# paused.  When s1 resumes it re-reads the OTable under oSysTreesLock and
# merges; both indexes must be usable.
permutation "s1_setup" "s2_arm" "s1_create_idx_a" "s2_create_idx_b" "s2_release" "s1_check"

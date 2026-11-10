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

step "s1_begin" { BEGIN; }
step "s1_rollback" { ROLLBACK; }

step "s1_create_idx_a" {
	CREATE INDEX o_conc_idx_a ON o_conc_idx (a);
}

step "s1_check" {
	SET enable_seqscan = off;
	SELECT * FROM o_conc_idx WHERE a = 'x';
	SELECT * FROM o_conc_idx WHERE b = 'y';
}

step "s1_check_table" {
	SELECT * FROM o_conc_idx;
}

session "s2"

step "s2_create_idx_b" {
	CREATE INDEX o_conc_idx_b ON o_conc_idx (b);
}

step "s2_check_idx_b" {
	SET enable_seqscan = off;
	SELECT * FROM o_conc_idx WHERE b = 'y';
}

session "s3"

step "s3_arm_define" {
	SELECT pg_stopevent_set('define_index_before_update',
		'$applicationName == "s1"');
}

step "s3_release_define" {
	SELECT pg_stopevent_reset('define_index_before_update');
}

step "s3_arm_build" {
	SELECT pg_stopevent_set('build_index_placeholder_inserted',
		'$applicationName == "s1"');
}

step "s3_release_build" {
	SELECT pg_stopevent_reset('build_index_placeholder_inserted');
}

# s1 pauses before systrees_modify_start, s2 creates its index while s1 is
# paused.  When s1 resumes it re-reads the OTable under oSysTreesLock and
# merges; both indexes must be usable.
permutation "s1_setup" "s3_arm_define" "s1_create_idx_a" "s2_create_idx_b" "s3_release_define" "s1_check"

# s1 creates index A in a transaction, paused after OTable write at
# build_index_placeholder_inserted.  s2 creates index B and encounters s1's
# uncommitted OTable entry in the sys tree btree modify (XidWait).  Releasing
# the stopevent and rolling back s1 exercises three fixes:
#   - releasing oSysTreesLock before XidWait avoids an LWLock deadlock
#   - skipping the relation lock in the undo callback avoids a heavyweight
#     lock deadlock with s2's ShareLock on the table
#   - repairing stale index references in the OTable after the aborted
#     transaction's o_indices entries are removed by undo
# After both sessions complete, index B must be usable.
permutation "s1_setup" "s3_arm_build" "s1_begin" "s1_create_idx_a" "s2_create_idx_b" "s3_release_build" "s1_rollback" "s1_check_table" "s2_check_idx_b"

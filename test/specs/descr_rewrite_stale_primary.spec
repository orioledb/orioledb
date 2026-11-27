# A descriptor refill must not lose the primary index of its table.
#
# recreate_table_descr() reads the OTable, then drops the indices whose
# o_indices entries are gone: a concurrent CREATE INDEX may have aborted.
# But a rewrite of the table committed in between removes the primary
# index's entry too.  Built with no indices at all, the descriptor was used
# as if it had a primary: o_find_toastable_attrs() read past the empty
# indices[] array and crashed (issue #1594).  Now the refill gives up, and
# the descriptor is built again from the current OTable.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_rewrite_pk (
		id int PRIMARY KEY,
		v text
	) USING orioledb;
	INSERT INTO o_rewrite_pk VALUES (1, 'a');
}

teardown
{
	DROP TABLE IF EXISTS o_rewrite_pk;
}

session "s1"

step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

step "s1_cache" {
	SELECT * FROM o_rewrite_pk;
}

step "s1_select" {
	SELECT id, v, c1 FROM o_rewrite_pk;
}

session "s2"

step "s2_add_c1" {
	ALTER TABLE o_rewrite_pk ADD COLUMN c1 int DEFAULT 1;
}

session "s3"

step "s3_arm" {
	SELECT pg_stopevent_set('recreate_table_descr',
		'$applicationName == "s1"');
}

# A volatile default rewrites the table: new relnode, new indices
step "s3_rewrite" {
	ALTER TABLE o_rewrite_pk ADD COLUMN r float8 DEFAULT random();
}

step "s3_release" {
	SELECT pg_stopevent_reset('recreate_table_descr');
}

# s1 caches the descriptor, s2's ALTER queues an invalidation for it, and
# s1's next query refills it, parked right after reading the OTable.  s3
# rewrites the table meanwhile; then s1 goes on.
permutation "s1_setup" "s1_cache" "s2_add_c1" "s3_arm" "s1_select" "s3_rewrite" "s3_release"

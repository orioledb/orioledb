setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_torn_pair (
		a int
	) USING orioledb;
	INSERT INTO o_torn_pair VALUES (1);
}

teardown
{
	DROP TABLE IF EXISTS o_torn_pair;
}

session "s1"

step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

step "s1_cache" {
	SELECT * FROM o_torn_pair;
}

step "s1_select" {
	SELECT * FROM o_torn_pair;
}

session "s2"

step "s2_add_c1" {
	ALTER TABLE o_torn_pair ADD COLUMN c1 int DEFAULT 1;
}

session "s3"

step "s3_arm" {
	SELECT pg_stopevent_set('recreate_table_descr',
		'$applicationName == "s1"');
}

step "s3_add_c2" {
	ALTER TABLE o_torn_pair ADD COLUMN c2 int DEFAULT 2;
}

step "s3_release" {
	SELECT pg_stopevent_reset('recreate_table_descr');
}

# s1 caches the descriptor, s2 adds c1 (queues invalidation for s1),
# s3 arms the stopevent, s1 starts a new SELECT which processes the
# invalidation and calls recreate_table_descr. The stopevent fires
# after OTable is read. While s1 is paused, s3 adds c2 in a separate
# step so the ALTER commits before s1 resumes. Then s3 releases the
# stopevent. s1 resumes and reads OIndex: the committed newer version
# is accepted by o_find_tuple_version (break at committed+INPROGRESS),
# producing a torn OTable/OIndex pair.
# Without the fix, this asserts; with the fix, the torn pair is
# detected and the descriptor is rebuilt cleanly.
permutation "s1_setup" "s1_cache" "s2_add_c1" "s3_arm" "s1_select" "s3_add_c2" "s3_release"

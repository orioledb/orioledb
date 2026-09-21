setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_raw_refind
	(
		id int4 NOT NULL,
		val int4 NOT NULL,
		pad text NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_raw_refind;
	INSERT INTO o_raw_refind
		SELECT i, i, repeat('x', 50) FROM generate_series(1, 200) i;
}

teardown
{
	DROP TABLE o_raw_refind;
}

session "s1"

step "s1_setup" { SET orioledb.enable_stopevents = true; }
# orioledb_index_rows() drains the primary index through exactly the
# iterator under test -- o_in_progress_snapshot, forward, btree_iterate_raw --
# and counts a NULL result as a dead row.  A stale result after the refind
# reads its tuple header from the old image, which is what makes one appear.
step "s1_iterate" {
	SELECT total, dead FROM orioledb_index_rows('o_raw_refind_pkey'::regclass); }

session "s2"

step "s2_set_stopevent" {
	SELECT pg_stopevent_set('raw_iterate_chunk_crossing', 'true'); }
step "s2_bloat" {
	UPDATE o_raw_refind SET pad = repeat('z', 500) WHERE id <= 30; }
step "s2_reset" {
	SELECT pg_stopevent_reset('raw_iterate_chunk_crossing'); }

permutation "s1_setup" "s2_set_stopevent" "s1_iterate" "s2_bloat" "s2_reset"

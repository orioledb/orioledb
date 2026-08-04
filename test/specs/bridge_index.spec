setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_bridge_refind (
		id int NOT NULL PRIMARY KEY,
		pos point,
		val int
	) USING orioledb;

	INSERT INTO o_bridge_refind
		SELECT i, point(i, i), i FROM generate_series(1, 500) i;
	CREATE INDEX ON o_bridge_refind USING gist(pos);

	CREATE TABLE o_bridge_conc (
		id int NOT NULL PRIMARY KEY,
		pos point,
		val text
	) USING orioledb;

	INSERT INTO o_bridge_conc
		SELECT i, point(i, i), 'initial' FROM generate_series(1, 10) i;
	CREATE INDEX ON o_bridge_conc USING gist(pos);
}

teardown
{
	DROP TABLE o_bridge_refind;
	DROP TABLE o_bridge_conc;
}

session "s1"

step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

step "s1_count" {
	SELECT count(*) FROM o_bridge_refind;
}

step "s1_select" {
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT id, pos, val FROM o_bridge_conc
		WHERE pos <@ box '(0,0),(20,20)' ORDER BY id;
}

session "s2"

step "s2_arm" {
	SELECT pg_stopevent_set('iterator_next',
		'$applicationName == "s1"');
}

step "s2_delete_and_release" {
	DELETE FROM o_bridge_refind WHERE id BETWEEN 50 AND 80;
	SELECT pg_stopevent_reset('iterator_next');
}

step "s2_begin" {
	BEGIN;
}

step "s2_update" {
	UPDATE o_bridge_conc SET pos = point(id + 900, id + 900), val = 'updated'
		WHERE id BETWEEN 3 AND 7;
}

step "s2_delete" {
	DELETE FROM o_bridge_conc WHERE id BETWEEN 3 AND 7;
}

step "s2_insert" {
	INSERT INTO o_bridge_conc
		SELECT i, point(i, i), 'inserted' FROM generate_series(11, 15) i;
}

step "s2_commit" {
	COMMIT;
}

permutation "s1_setup" "s2_arm" "s1_count" "s2_delete_and_release"
permutation "s2_begin" "s2_update" "s1_select" "s2_commit"
permutation "s2_begin" "s2_delete" "s1_select" "s2_commit"
permutation "s2_begin" "s2_insert" "s1_select" "s2_commit"

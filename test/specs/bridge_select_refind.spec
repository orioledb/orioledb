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
}

teardown
{
	DROP TABLE o_bridge_refind;
}

session "s1"

step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

step "s1_count" {
	SELECT count(*) FROM o_bridge_refind;
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

permutation "s1_setup" "s2_arm" "s1_count" "s2_delete_and_release"

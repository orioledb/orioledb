setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_split (
		id text NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_split;
}

teardown
{
    DROP TABLE o_split;
}

session "s1"

step "s1_setup"  { SET orioledb.enable_stopevents = true; }

step "s1_split_prepare" { INSERT INTO o_split (SELECT repeat('x', 400) || id FROM generate_series(1, 20, 1) id); }
step "s1_split" { INSERT INTO o_split (SELECT repeat('x', 400) || id FROM generate_series(131, 139, 1) id); }

session "s2"

step "s2_setup" { SET orioledb.enable_stopevents = true; }

step "s2_insert" { INSERT INTO o_split (SELECT repeat('x', 400) || id  || repeat('x', 1200) FROM generate_series(110, 111, 1) id); }

session "s3"

step "s3_setup" { SET orioledb.enable_stopevents = true; }

step "s3_insert" { INSERT INTO o_split (SELECT repeat('x', 400) || 123); }

session "s4"

step "s4_bp" {
	SELECT pg_stopevent_set('page_split', 'true');
	SELECT pg_stopevent_set('relock_page', 'true');
}

step "s4_reset" {
	SELECT
		pg_stopevent_reset('page_split') a,
		pg_stopevent_reset('relock_page') b; }

permutation "s1_setup" "s2_setup" "s3_setup" "s1_split_prepare" "s4_bp" "s1_split" "s2_insert" "s3_insert" "s4_reset"

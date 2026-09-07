# Two rows that share the index key columns but differ in the primary key
# (which sits in the INCLUDE list) are unrelated: the second insert must not
# wait for the first transaction, and both must end up in the index.
setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_include_pk (
		id text PRIMARY KEY,
		grp text,
		created_at timestamptz,
		ttl int
	) USING orioledb;
	CREATE INDEX IF NOT EXISTS o_include_pk_ix ON o_include_pk (grp, created_at)
		INCLUDE (id, ttl);
	TRUNCATE o_include_pk;
}

teardown
{
	DROP TABLE o_include_pk;
}

session "s1"
step "s1_begin"    { BEGIN; }
step "s1_insert_a" { INSERT INTO o_include_pk VALUES ('a', 'g-0', '2026-01-01 00:00:00+00', 1); }
step "s1_commit"   { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"
step "s2_insert_b" { INSERT INTO o_include_pk VALUES ('b', 'g-0', '2026-01-01 00:00:00+00', 1); }
step "s2_update_b" { UPDATE o_include_pk SET ttl = 2 WHERE id = 'b'; }
step "s2_select"   { SET enable_seqscan = off; SELECT id, ttl FROM o_include_pk WHERE grp = 'g-0' ORDER BY id; }

permutation "s1_begin" "s1_insert_a" "s2_insert_b" "s2_update_b" "s1_commit" "s2_select"
permutation "s1_begin" "s1_insert_a" "s2_insert_b" "s1_rollback" "s2_select"

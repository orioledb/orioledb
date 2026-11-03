# INSERT ... ON CONFLICT DO NOTHING must not skip the row because of a
# committed row the snapshot cannot see, except in Read Committed.
# A plain INSERT reports a unique violation, as heap does.  An INSERT over
# a row deleted after the snapshot still raises a serialization failure.
# When s1 is still in progress, s2 waits: s1 commit gives a serialization
# failure, s1 rollback lets s2 insert.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_ioc_rr (
		k int PRIMARY KEY,
		u int,
		v text
	) USING orioledb;
	CREATE UNIQUE INDEX o_ioc_rr_u ON o_ioc_rr (u);
}

teardown
{
	DROP TABLE o_ioc_rr;
}

session "s1"

step "s1_begin" { BEGIN; }
step "s1_insert" { INSERT INTO o_ioc_rr VALUES (1, 10, '1'); }
step "s1_delete" { DELETE FROM o_ioc_rr WHERE k = 1; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"

step "s2_begin_rr" { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s2_begin_rc" { BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s2_snap" { SELECT count(*) FROM o_ioc_rr; }
step "s2_ioc_u" { INSERT INTO o_ioc_rr VALUES (2, 10, '2') ON CONFLICT (u) DO NOTHING RETURNING k; }
step "s2_ioc_k" { INSERT INTO o_ioc_rr VALUES (1, 20, '2') ON CONFLICT (k) DO NOTHING RETURNING k; }
step "s2_ins_k" { INSERT INTO o_ioc_rr VALUES (1, 20, '2'); }
step "s2_ins_u" { INSERT INTO o_ioc_rr VALUES (2, 10, '2'); }
step "s2_commit" { COMMIT; }
step "s2_rollback" { ROLLBACK; }
step "s2_select" { SELECT k, u, v FROM o_ioc_rr ORDER BY k; }

permutation "s2_begin_rr" "s2_snap" "s1_insert" "s2_ioc_u" "s2_rollback"
permutation "s2_begin_rr" "s2_snap" "s1_insert" "s2_ioc_k" "s2_rollback"
permutation "s2_begin_rc" "s2_snap" "s1_insert" "s2_ioc_u" "s2_commit" "s2_select"
permutation "s2_begin_rr" "s2_snap" "s1_insert" "s2_ins_k" "s2_rollback"
permutation "s2_begin_rr" "s2_snap" "s1_insert" "s2_ins_u" "s2_rollback"
permutation "s1_insert" "s2_begin_rr" "s2_snap" "s1_delete" "s2_ins_k" "s2_rollback"

# s2 waits for s1, on both arbiters
permutation "s2_begin_rr" "s2_snap" "s1_begin" "s1_insert" "s2_ioc_u" "s1_commit" "s2_rollback"
permutation "s2_begin_rr" "s2_snap" "s1_begin" "s1_insert" "s2_ioc_k" "s1_commit" "s2_rollback"
permutation "s2_begin_rr" "s2_snap" "s1_begin" "s1_insert" "s2_ioc_u" "s1_rollback" "s2_commit" "s2_select"
permutation "s2_begin_rr" "s2_snap" "s1_begin" "s1_insert" "s2_ioc_k" "s1_rollback" "s2_commit" "s2_select"

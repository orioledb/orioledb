# Isolation tests for orioledb.serializable = 'table_lock' (default).
#
# OrioleDB does not implement SSI; SERIALIZABLE is approximated by taking
# a heavyweight ExclusiveLock on every relation a SERIALIZABLE transaction
# touches.  These permutations exercise the resulting blocking behavior:
#
#   - two SERIALIZABLE writers on the same table serialize via lockmgr;
#   - a SERIALIZABLE writer blocks a concurrent non-SERIALIZABLE writer
#     (RowExclusiveLock vs ExclusiveLock) on the same table;
#   - a SERIALIZABLE writer does NOT block a concurrent non-SERIALIZABLE
#     reader (AccessShareLock is compatible with ExclusiveLock);
#   - SERIALIZABLE transactions on different tables run in parallel.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_iso_ser (
		id int4 NOT NULL,
		val int4 NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	INSERT INTO o_iso_ser SELECT i, i FROM generate_series(1, 5) i;

	CREATE TABLE o_iso_ser_other (
		id int4 NOT NULL,
		val int4 NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	INSERT INTO o_iso_ser_other SELECT i, i FROM generate_series(1, 5) i;
}

teardown
{
	DROP TABLE o_iso_ser;
	DROP TABLE o_iso_ser_other;
}

session "s1"
step "s1_begin_ser"      { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "s1_update"         { UPDATE o_iso_ser SET val = val + 100 WHERE id = 1; }
step "s1_insert"         { INSERT INTO o_iso_ser VALUES (10, 10); }
step "s1_commit"         { COMMIT; }

session "s2"
step "s2_begin_ser"      { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "s2_begin_rc"       { BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s2_update"         { UPDATE o_iso_ser SET val = val + 1000 WHERE id = 2; }
step "s2_update_other"   { UPDATE o_iso_ser_other SET val = val + 1000 WHERE id = 2; }
step "s2_insert"         { INSERT INTO o_iso_ser VALUES (11, 11); }
step "s2_select"         { SELECT id, val FROM o_iso_ser ORDER BY id; }
step "s2_commit"         { COMMIT; }

# Two SERIALIZABLE writers on the same table serialize (s2 blocks until s1 commits).
permutation "s1_begin_ser" "s2_begin_ser" "s1_update" "s2_update" "s1_commit" "s2_commit"

# A SERIALIZABLE writer blocks a concurrent READ COMMITTED writer on the same table.
permutation "s1_begin_ser" "s2_begin_rc" "s1_update" "s2_update" "s1_commit" "s2_commit"

# A SERIALIZABLE writer does not block a concurrent READ COMMITTED reader.
permutation "s1_begin_ser" "s2_begin_rc" "s1_update" "s2_select" "s1_commit" "s2_commit"

# SERIALIZABLE writers on different tables run in parallel.
permutation "s1_begin_ser" "s2_begin_ser" "s1_update" "s2_update_other" "s1_commit" "s2_commit"

# Two SERIALIZABLE INSERTs on the same table serialize.
permutation "s1_begin_ser" "s2_begin_ser" "s1_insert" "s2_insert" "s1_commit" "s2_commit"

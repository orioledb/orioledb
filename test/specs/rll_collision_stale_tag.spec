setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_coll (
		id bigint PRIMARY KEY,
		v int
	) USING orioledb;
	/* 47-bit hash collision, see rll_tuple_lock_collision.spec */
	INSERT INTO o_coll VALUES (140737490556223, 1), (140737501603420, 1);

	CREATE TABLE t1 (i int);
	CREATE TABLE t2 (i int);
}

teardown
{
	DROP TABLE o_coll;
	DROP TABLE t1;
	DROP TABLE t2;
}

session s1
step "begin_1" { BEGIN; }
step "ks_1_B" { SELECT id FROM o_coll WHERE id = 140737501603420 FOR KEY SHARE; }
step "fu_1_AB" { SELECT id FROM o_coll ORDER BY id FOR UPDATE; }
step "sleep_1" { SELECT pg_sleep(0.5); }
step "rb_1" { ROLLBACK; }

session s2
step "dt_short_2" { SET deadlock_timeout = '10ms'; }
step "dt_long_2" { SET deadlock_timeout = '10s'; }
step "lt_2" { SET lock_timeout = '100ms'; }
step "lt_off_2" { RESET lock_timeout; }
step "begin_2" { BEGIN; }
step "ks_2_A" { SELECT id FROM o_coll WHERE id = 140737490556223 FOR KEY SHARE; }
step "fu_2_A" { SELECT id FROM o_coll WHERE id = 140737490556223 FOR UPDATE; }
step "fu_2_B" { SELECT id FROM o_coll WHERE id = 140737501603420 FOR UPDATE; }
step "adv_2_1" { SELECT pg_advisory_lock(1); }
step "adv_2_2" { SELECT pg_advisory_lock(2); }
step "lk_2_t1" { LOCK TABLE t1; }
step "lk_2_t2" { LOCK TABLE t2; }
step "rb_2" { ROLLBACK; }
step "unl_2" { SELECT pg_advisory_unlock_all(); }

session s3
step "dt_short_3" { SET deadlock_timeout = '100ms'; }
step "dt_long_3" { SET deadlock_timeout = '10s'; }
step "lt_3" { SET lock_timeout = '100ms'; }
step "lt_off_3" { RESET lock_timeout; }
step "begin_3" { BEGIN; }
step "fu_3_A" { SELECT id FROM o_coll WHERE id = 140737490556223 FOR UPDATE; }
step "fu_3_B" { SELECT id FROM o_coll WHERE id = 140737501603420 FOR UPDATE; }
step "adv_3_1" { SELECT pg_advisory_lock(1); }
step "adv_3_2" { SELECT pg_advisory_lock(2); }
step "adv_3_held" { SELECT objid FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted ORDER BY objid; }
step "lk_3_t1" { LOCK TABLE t1; }
step "lk_3_t2" { LOCK TABLE t2; }
step "rb_3" { ROLLBACK; }
step "unl_3" { SELECT pg_advisory_unlock_all(); }

# 1. Waiter (s2) is the deadlock victim: s3 holds the tuple lock and waits
# for s2; s2 queues on the same (colliding) tag and detects the cycle.
permutation
	"dt_long_3" "dt_short_2"
	"begin_1" "begin_2" "begin_3"
	"ks_1_B" "ks_2_A"
	"fu_3_A"
	"fu_2_B"
	"rb_1"
	"rb_2" "rb_3"

# 2. Stale published tag + real advisory lock deadlock.  s2 and s3 time out
# while waiting for colliding keys A and B; their published tags stay.
# Then s3 runs into a real deadlock with s2 on advisory locks.  Expected:
# "deadlock detected" for s3.
permutation
	"dt_long_2" "dt_short_3" "lt_2" "lt_3"
	"begin_1" "fu_1_AB"
	"fu_2_A" "fu_3_B"
	"sleep_1" "rb_1" "lt_off_2" "lt_off_3"
	"adv_2_1" "adv_3_2"
	"adv_2_2"("adv_3_1") "adv_3_1"
	"adv_3_held"
	"unl_3" "unl_2"

# 3. Same, with a real LOCK TABLE deadlock.  Expected: "deadlock detected"
# for s3.
permutation
	"dt_long_2" "dt_short_3" "lt_2" "lt_3"
	"begin_1" "fu_1_AB"
	"fu_2_A" "fu_3_B"
	"sleep_1" "rb_1" "lt_off_2" "lt_off_3"
	"begin_2" "begin_3"
	"lk_2_t1" "lk_3_t2"
	"lk_2_t2"("lk_3_t1") "lk_3_t1"
	"rb_3" "rb_2"

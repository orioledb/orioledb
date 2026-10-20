setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_hwlock (
		id int PRIMARY KEY,
		v int
	) USING orioledb;

	/*
	 * Keys 211 and 9660 produce the same orioledb tuple hash (0x86c292eb),
	 * so they share a single LOCKTAG_TUPLE heavyweight lock tag.
	 */
	INSERT INTO o_hwlock VALUES (1, 1), (211, 1), (9660, 1);
}

teardown
{
	DROP TABLE o_hwlock;
}

session s1
step "begin_1" { BEGIN; }
step "ks_1_1" { SELECT id FROM o_hwlock WHERE id = 1 FOR KEY SHARE; }
step "ks_1_211" { SELECT id FROM o_hwlock WHERE id = 211 FOR KEY SHARE; }
step "ks_1_9660" { SELECT id FROM o_hwlock WHERE id = 9660 FOR KEY SHARE; }
step "fu_1_211" { SELECT id FROM o_hwlock WHERE id = 211 FOR UPDATE; }
step "rb_1" { ROLLBACK; }

session s2
step "begin_2" { BEGIN; }
step "ks_2_211" { SELECT id FROM o_hwlock WHERE id = 211 FOR KEY SHARE; }
step "fu_2_9660" { SELECT id FROM o_hwlock WHERE id = 9660 FOR UPDATE; }
step "rb_2" { ROLLBACK; }

session s3
step "begin_3" { BEGIN; }
step "ks_3_9660" { SELECT id FROM o_hwlock WHERE id = 9660 FOR KEY SHARE; }
step "fu_3_1" { SELECT id FROM o_hwlock WHERE id = 1 FOR UPDATE; }
step "fu_3_211" { SELECT id FROM o_hwlock WHERE id = 211 FOR UPDATE; }
step "rb_3" { ROLLBACK; }

session s4
step "begin_4" { BEGIN; }
step "ks_4_211" { SELECT id FROM o_hwlock WHERE id = 211 FOR KEY SHARE; }
step "rb_4" { ROLLBACK; }

# Permutation 1: stale LOCKTAG_TUPLE across retry (2-way).
#
# Keys 211 and 9660 share the same tuple hash, so both map to the same
# LOCKTAG_TUPLE.  Schedule:
#
#   s1: KEY SHARE on 211 and 9660
#   s3: FOR UPDATE on 211 -- blocks on s1; acquires LOCKTAG_TUPLE
#   s2: KEY SHARE on 211 (compatible with s1, succeeds)
#   s2: FOR UPDATE on 9660 -- blocks on LOCKTAG_TUPLE (held by s3)
#   s1: ROLLBACK           -- wakes s3 to retry
#
# Without the fix s3 still holds LOCKTAG_TUPLE at the retry label,
# sees s2's KEY SHARE on 211, calls wait_for_oxid(s2).  s2 is blocked
# on LOCKTAG_TUPLE held by s3; cycle, deadlock.
#
# With the fix s3 releases LOCKTAG_TUPLE at retry, s2 unblocks and
# both complete normally.
permutation
	"begin_1" "begin_3"
	"ks_1_211" "ks_1_9660"
	"fu_3_211"
	"begin_2" "ks_2_211"
	"fu_2_9660"
	"rb_1"
	"rb_2" "rb_3"

# Permutation 2: 3-way false deadlock from hash collision (no retry needed).
#
#   s4: KEY SHARE on 211
#   s1: KEY SHARE on 1
#   s3: KEY SHARE on 9660
#   s3: FOR UPDATE on 1   -- conflicts with s1, wait_for_oxid(s1)
#   s2: FOR UPDATE on 9660 -- conflicts with s3, acquires LOCKTAG_TUPLE(hash),
#                             wait_for_oxid(s3)
#   s1: FOR UPDATE on 211  -- conflicts with s4, needs LOCKTAG_TUPLE(hash)
#
# Without dontWait, s1 blocks on LOCKTAG_TUPLE held by s2.
# Cycle: s1->s2 (LOCKTAG), s2->s3 (oxid), s3->s1 (oxid).
# s1's dependency on s2 is false: s1 wants key 211, s2 holds the lock
# for key 9660.
#
# With dontWait, s1 skips the lock and calls wait_for_oxid(s4).
# No cycle; s4 rolls back, s1 completes, s3 completes, s2 completes.
permutation
	"begin_1" "begin_3" "begin_4"
	"ks_4_211" "ks_1_1" "ks_3_9660"
	"fu_3_1"
	"begin_2" "fu_2_9660"
	"fu_1_211"
	"rb_4"
	"rb_1" "rb_3" "rb_2"

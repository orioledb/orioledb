setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_hwlock (
		id int PRIMARY KEY,
		v int
	) USING orioledb;

	/*
	 * Keys 211 and 9660 used to produce the same 32-bit tuple hash
	 * (0x86c292eb), and so shared a single LOCKTAG_TUPLE heavyweight lock
	 * tag.  The tag now carries a 4-byte key itself.
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

# Scenarios by Pavel Borisov (#1408).  Each waiter that holds no lock on
# its tuple queues on the tuple lock of that tuple.  When unrelated keys
# share the lock tag, a waiter queues behind a session it holds a row lock
# for, and the two deadlock.

# Permutation 1 (2-way):
#
#   s1: KEY SHARE on 211 and 9660
#   s3: FOR UPDATE on 211  -- waits for s1, holding the tuple lock of 211
#   s2: KEY SHARE on 211   (compatible with s1)
#   s2: FOR UPDATE on 9660 -- waits for s1; with a shared tag it also queues
#                             on the tuple lock s3 holds
#   s1: ROLLBACK           -- s3 retries and now waits for s2's KEY SHARE
#
# A shared tag closes s3 -> s2 -> s3.  With distinct tags s2 takes the lock
# of 9660 and both complete.
permutation
	"begin_1" "begin_3"
	"ks_1_211" "ks_1_9660"
	"fu_3_211"
	"begin_2" "ks_2_211"
	"fu_2_9660"
	"rb_1"
	"rb_2" "rb_3"

# Permutation 2 (3-way, no retry needed):
#
#   s4: KEY SHARE on 211
#   s1: KEY SHARE on 1
#   s3: KEY SHARE on 9660
#   s3: FOR UPDATE on 1    -- waits for s1
#   s2: FOR UPDATE on 9660 -- waits for s3, holding the tuple lock of 9660
#   s1: FOR UPDATE on 211  -- waits for s4; with a shared tag it also queues
#                             on the tuple lock s2 holds
#
# A shared tag closes s1 -> s2 -> s3 -> s1.
permutation
	"begin_1" "begin_3" "begin_4"
	"ks_4_211" "ks_1_1" "ks_3_9660"
	"fu_3_1"
	"begin_2" "fu_2_9660"
	"fu_1_211"
	"rb_4"
	"rb_1" "rb_3" "rb_2"

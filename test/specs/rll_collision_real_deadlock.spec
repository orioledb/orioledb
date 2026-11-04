setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_coll (
		id bigint PRIMARY KEY,
		v int
	) USING orioledb;
	/* 47-bit hash collision, see rll_tuple_lock_collision.spec */
	INSERT INTO o_coll VALUES (140737490556223, 1), (140737501603420, 1);

	CREATE TABLE o_plain (
		id bigint PRIMARY KEY,
		v int
	) USING orioledb;
	/* Both above 2^47, so hashed too, but no collision */
	INSERT INTO o_plain VALUES (140737490556223, 1), (140737490556224, 1);
}

teardown
{
	DROP TABLE o_coll;
	DROP TABLE o_plain;
}

session s2
setup { SET deadlock_timeout = '10s'; SET statement_timeout = '3s'; }
step "begin_2" { BEGIN; }
step "u_2_coll_A" { UPDATE o_coll SET v = v + 1 WHERE id = 140737490556223; }
step "u_2_coll_B" { UPDATE o_coll SET v = v + 1 WHERE id = 140737501603420; }
step "u_2_plain_A" { UPDATE o_plain SET v = v + 1 WHERE id = 140737490556223; }
step "u_2_plain_B" { UPDATE o_plain SET v = v + 1 WHERE id = 140737490556224; }
step "rb_2" { ROLLBACK; }

session s3
setup { SET deadlock_timeout = '100ms'; SET statement_timeout = '3s'; }
step "begin_3" { BEGIN; }
step "u_3_coll_A" { UPDATE o_coll SET v = v + 1 WHERE id = 140737490556223; }
step "u_3_coll_B" { UPDATE o_coll SET v = v + 1 WHERE id = 140737501603420; }
step "u_3_plain_A" { UPDATE o_plain SET v = v + 1 WHERE id = 140737490556223; }
step "u_3_plain_B" { UPDATE o_plain SET v = v + 1 WHERE id = 140737490556224; }
step "rb_3" { ROLLBACK; }

# Control: classic two-row deadlock, keys do not collide.  s3 gets
# "deadlock detected" after 100ms.
permutation
	"begin_2" "begin_3"
	"u_2_plain_A" "u_3_plain_B"
	"u_2_plain_B"("u_3_plain_A") "u_3_plain_A"
	"rb_3" "rb_2"

# Same deadlock on two rows whose hashed tuple lock tags collide.  It is a
# real deadlock: each session updates a row the other one has updated.
# Expected: "deadlock detected" for s3 after 100ms, like the control.
permutation
	"begin_2" "begin_3"
	"u_2_coll_A" "u_3_coll_B"
	"u_2_coll_B"("u_3_coll_A") "u_3_coll_A"
	"rb_3" "rb_2"

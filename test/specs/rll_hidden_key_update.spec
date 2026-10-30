# A FOR KEY SHARE lock taken under REPEATABLE READ on a row whose key was
# changed by a transaction that committed after the snapshot must fail to
# serialize, even when an in-progress non-key update lies above that change.
# Run on a heap table and on an OrioleDB table: the outputs must agree.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE hk_heap (id int PRIMARY KEY, u int UNIQUE NOT NULL, v int NOT NULL) USING heap;
	CREATE TABLE hk_o (id int PRIMARY KEY, u int UNIQUE NOT NULL, v int NOT NULL) USING orioledb;
	CREATE TABLE hk_heap_child (id int PRIMARY KEY, pu int NOT NULL REFERENCES hk_heap (u)) USING heap;
	CREATE TABLE hk_o_child (id int PRIMARY KEY, pu int NOT NULL REFERENCES hk_o (u)) USING orioledb;
	INSERT INTO hk_heap VALUES (1, 1, 0);
	INSERT INTO hk_o VALUES (1, 1, 0);
}

teardown
{
	DROP TABLE hk_heap_child;
	DROP TABLE hk_o_child;
	DROP TABLE hk_heap;
	DROP TABLE hk_o;
}

session "s1"
step "s1_key_heap" { UPDATE hk_heap SET u = u + 100 WHERE id = 1; }
step "s1_key_o" { UPDATE hk_o SET u = u + 100 WHERE id = 1; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_nokey_heap" { UPDATE hk_heap SET v = v + 1 WHERE id = 1; }
step "s2_nokey_o" { UPDATE hk_o SET v = v + 1 WHERE id = 1; }
step "s2_commit" { COMMIT; }

session "s3"
step "s3_begin_rr" { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s3_snap" { SELECT count(*) FROM hk_heap; SELECT count(*) FROM hk_o; }
step "s3_ks_heap" { SELECT id, u FROM hk_heap WHERE id = 1 FOR KEY SHARE; }
step "s3_ks_o" { SELECT id, u FROM hk_o WHERE id = 1 FOR KEY SHARE; }
step "s3_fk_heap" { INSERT INTO hk_heap_child VALUES (1, 1); }
step "s3_fk_o" { INSERT INTO hk_o_child VALUES (1, 1); }
step "s3_commit" { COMMIT; }
step "s3_orphans" { SELECT 'heap' AS t, count(*) FROM hk_heap_child c WHERE NOT EXISTS (SELECT 1 FROM hk_heap p WHERE p.u = c.pu) UNION ALL SELECT 'orioledb', count(*) FROM hk_o_child c WHERE NOT EXISTS (SELECT 1 FROM hk_o p WHERE p.u = c.pu); }

permutation "s3_begin_rr" "s3_snap" "s1_key_heap" "s2_begin" "s2_nokey_heap" "s3_ks_heap" "s2_commit" "s3_commit"
permutation "s3_begin_rr" "s3_snap" "s1_key_o" "s2_begin" "s2_nokey_o" "s3_ks_o" "s2_commit" "s3_commit"
# control: no in-progress update above the key change
permutation "s3_begin_rr" "s3_snap" "s1_key_heap" "s3_ks_heap" "s3_commit"
permutation "s3_begin_rr" "s3_snap" "s1_key_o" "s3_ks_o" "s3_commit"
# the same through a foreign key check: the child must not reference the
# key that s1 changed
permutation "s3_begin_rr" "s3_snap" "s1_key_heap" "s2_begin" "s2_nokey_heap" "s3_fk_heap" "s2_commit" "s3_commit" "s3_orphans"
permutation "s3_begin_rr" "s3_snap" "s1_key_o" "s2_begin" "s2_nokey_o" "s3_fk_o" "s2_commit" "s3_commit" "s3_orphans"

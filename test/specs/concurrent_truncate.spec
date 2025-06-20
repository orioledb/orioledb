setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE t (i int) USING orioledb;
    INSERT INTO t VALUES (45), (66), (233);
}

teardown
{
	DROP TABLE t;
}

session "s1"

step "s1_begin" { BEGIN; }
step "s1_truncate" { TRUNCATE t; }
step "s1_insert" { INSERT INTO t VALUES (6), (19), (40); }
step "s1_select" { SELECT * FROM t; }
step "s1_commit" { COMMIT; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_truncate" { TRUNCATE t; }
step "s2_insert" { INSERT INTO t VALUES (79), (13), (22); }
step "s2_select" { SELECT * FROM t; }
step "s2_commit" { COMMIT; }

permutation "s1_begin"      "s2_begin" 
            "s1_truncate"
                            "s2_truncate"
            "s1_commit"
            "s1_truncate"
                            "s2_truncate" # one non-transactional truncate just to spice things up a bit
                            "s2_commit"
            "s1_insert"
                            "s2_insert"
            "s1_select"
                            "s2_select" 

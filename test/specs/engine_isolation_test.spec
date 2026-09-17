setup
{
    CREATE EXTENSION IF NOT EXISTS orioledb;
    CREATE TABLE o_iso_test (
        id int PRIMARY KEY,
        val text
    ) USING orioledb;
}

teardown
{
    DROP TABLE o_iso_test;
}

session "s1"
step "s1_begin"    { BEGIN; }
step "s1_insert"   { INSERT INTO o_iso_test VALUES (1, 's1_veri'); }
step "s1_stats"    { SELECT orioledb_custom_page_stats('o_iso_test'); }
step "s1_commit"   { COMMIT; }

session "s2"
step "s2_begin"    { BEGIN; }
step "s2_status"   { SELECT orioledb_engine_status(); }
step "s2_read"     { SELECT * FROM o_iso_test; }
step "s2_commit"   { COMMIT; }

permutation "s1_begin" "s2_begin" "s1_insert" "s2_status" "s1_stats" "s2_read" "s1_commit" "s2_read" "s2_commit"
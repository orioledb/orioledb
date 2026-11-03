# A transaction that keeps updating a row stops looking for conflicts at its
# own latest version, unless a lock-only record lies below it.  A key share
# lock of another transaction, under or over those versions, must still make
# a key update wait.

setup
{
    CREATE EXTENSION IF NOT EXISTS orioledb;
    CREATE TABLE own_chain
    (
        id int not null PRIMARY KEY,
        v int not null
    ) USING orioledb;
    INSERT INTO own_chain VALUES (1, 0);
}

teardown
{
    DROP TABLE own_chain;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_key_share" { SELECT * FROM own_chain WHERE id = 1 FOR KEY SHARE; }
step "s1_commit" { COMMIT; }
step "s1_select" { SELECT * FROM own_chain ORDER BY id; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_update1" { UPDATE own_chain SET v = v + 1 WHERE id = 1; }
step "s2_update2" { UPDATE own_chain SET v = v + 1 WHERE id = 1; }
step "s2_update3" { UPDATE own_chain SET v = v + 1 WHERE id = 1; }
step "s2_update_key" { UPDATE own_chain SET id = 2 WHERE id = 1; }
step "s2_commit" { COMMIT; }

# the lock is below our versions
permutation "s1_begin" "s1_key_share" "s2_begin" "s2_update1" "s2_update2" "s2_update3" "s2_update_key" "s1_commit" "s2_commit" "s1_select"
# the lock is over our versions
permutation "s2_begin" "s2_update1" "s2_update2" "s1_begin" "s1_key_share" "s2_update3" "s2_update_key" "s1_commit" "s2_commit" "s1_select"
# no lock: the key update goes through
permutation "s2_begin" "s2_update1" "s2_update2" "s2_update3" "s2_update_key" "s2_commit" "s1_select"


setup
{
    CREATE EXTENSION IF NOT EXISTS orioledb;
    CREATE TABLE rll_test
    (
        key int8 not null,
        value int8 not null,
        uniq int8 not null,
        PRIMARY KEY(key)
    ) USING orioledb;
    CREATE OR REPLACE FUNCTION update_trg_func() RETURNS trigger AS
    $$BEGIN
        RAISE NOTICE 'old: (%, %, %), new: (%, %, %)', OLD.key, OLD.value, OLD.uniq, NEW.key, NEW.value, NEW.uniq;
        RETURN NEW;
    END;$$ LANGUAGE plpgsql;
    TRUNCATE rll_test;
    INSERT INTO rll_test (SELECT i, i, i FROM generate_series(1, 10) AS i);
}

teardown
{
    DROP TABLE rll_test;
}

session "s1"

step "s1_trigger" { CREATE TRIGGER before_update_trg BEFORE UPDATE ON rll_test FOR EACH ROW EXECUTE PROCEDURE update_trg_func();}
step "s1_begin" { BEGIN; }
step "s1_select" { SELECT * FROM rll_test; }

step "s1_select_key_share" { SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE; }
step "s1_select_share" { SELECT * FROM rll_test WHERE key = 1 FOR SHARE; }
step "s1_select_update" { SELECT * FROM rll_test WHERE key = 1 FOR UPDATE; }
step "s1_select_no_key_update" { SELECT * FROM rll_test WHERE key = 1 FOR NO KEY UPDATE; }

step "s1_update" { UPDATE rll_test SET value = value + 10 WHERE key = 1; }
step "s1_update_key" { UPDATE rll_test SET key = value WHERE key = 1; }
step "s1_delete" { DELETE FROM rll_test WHERE key = 1; }
step "s1_ioc" { INSERT INTO rll_test VALUES (1, 20, 200)
                    ON CONFLICT (key) DO UPDATE SET key = 1; }

step "s1_savepoint" { SAVEPOINT point_name_1; }
step "s1_rollbak_to_savepoint" { ROLLBACK TO SAVEPOINT point_name_1; }
step "s1_release" { RELEASE SAVEPOINT point_name_1; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }


session "s2"

step "s2_begin" { BEGIN; }
step "s2_select" { SELECT * FROM rll_test; }

step "s2_select_key_share" { SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE; }
step "s2_select_share" { SELECT * FROM rll_test WHERE key = 1 FOR SHARE; }
step "s2_select_update" { SELECT * FROM rll_test WHERE key = 1 FOR UPDATE; }
step "s2_select_no_key_update" { SELECT * FROM rll_test WHERE key = 1 FOR NO KEY UPDATE; }

step "s2_update" { UPDATE rll_test SET value = value + 10 WHERE key = 1; }
step "s2_update_key" { UPDATE rll_test SET key = value WHERE key = 1; }
step "s2_delete" { DELETE FROM rll_test WHERE key = 1; }

step "s2_savepoint" { SAVEPOINT point_name_2; }
step "s2_rollbak_to_savepoint" {ROLLBACK TO SAVEPOINT point_name_2; }
step "s2_release" { RELEASE SAVEPOINT point_name_2; }
step "s2_commit" { COMMIT; }
step "s2_rollback" { ROLLBACK; }


session "s3"

step "s3_begin" { BEGIN; }
step "s3_select" { SELECT * FROM rll_test; }

step "s3_select_key_share" { SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE; }
step "s3_select_share" { SELECT * FROM rll_test WHERE key = 1 FOR SHARE; }
step "s3_select_update" { SELECT * FROM rll_test WHERE key = 1 FOR UPDATE; }
step "s3_select_no_key_update" { SELECT * FROM rll_test WHERE key = 1 FOR NO KEY UPDATE; }

step "s3_update" { UPDATE rll_test SET value = value + 10 WHERE key = 1; }
step "s3_update_key" { UPDATE rll_test SET key = value WHERE key = 1; }
step "s3_delete" { DELETE FROM rll_test WHERE key = 1; }

step "s3_savepoint" { SAVEPOINT point_name_2; }
step "s3_rollbak_to_savepoint" {ROLLBACK TO SAVEPOINT point_name_2; }
step "s3_release" { RELEASE SAVEPOINT point_name_2; }
step "s3_commit" { COMMIT; }
step "s3_rollback" { ROLLBACK; }


permutation "s1_begin" "s2_begin"
"s1_select_update"
"s2_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s2_delete"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s2_select_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s2_select_no_key_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s2_select_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s2_select_key_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s2_select_key_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s2_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s2_delete"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s2_select_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s2_select_no_key_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s2_select_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s2_select_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s2_select_key_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s2_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s2_delete"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s2_select_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s2_select_no_key_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_select_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_select_key_share"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_select_no_key_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_update_key"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_delete"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_select_update"
"s1_commit" "s2_commit"




permutation "s1_begin" "s2_begin"
"s1_select_update"
"s1_rollback"
"s2_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s1_rollback"
"s2_delete"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s1_rollback"
"s2_select_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s1_rollback"
"s2_select_no_key_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s1_rollback"
"s2_select_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_update"
"s1_rollback"
"s2_select_key_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s1_rollback"
"s2_select_key_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s1_rollback"
"s2_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s1_rollback"
"s2_delete"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s1_rollback"
"s2_select_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s1_rollback"
"s2_select_no_key_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_no_key_update"
"s1_rollback"
"s2_select_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s1_rollback"
"s2_select_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s1_rollback"
"s2_select_key_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s1_rollback"
"s2_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s1_rollback"
"s2_delete"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s1_rollback"
"s2_select_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_share"
"s1_rollback"
"s2_select_no_key_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_rollback"
"s2_select_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_rollback"
"s2_select_key_share"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_rollback"
"s2_select_no_key_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_rollback"
"s2_update"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_rollback"
"s2_delete"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_rollback"
"s2_select_update"
"s2_commit"


permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_update"
"s2_commit"
"s1_select_share"
"s1_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_update"
"s2_commit"
"s1_select_key_share"
"s1_commit"

permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s2_update"
"s2_commit"
"s1_select_no_key_update"
"s1_commit"


permutation "s1_trigger"
"s1_begin" "s2_begin"
"s1_delete"
"s1_commit"
"s2_select_update"
"s2_commit"

permutation "s1_trigger"
"s1_begin" "s2_begin"
"s1_delete"
"s1_rollback"
"s2_select_update"
"s2_commit"



permutation "s1_begin" "s2_begin"
"s2_update"
"s1_select_update"
"s2_rollback"
"s2_begin"
"s2_update"
"s1_commit" "s2_commit"


permutation "s1_begin" "s2_begin"
"s1_select_key_share"
"s1_savepoint"
"s1_update" "s1_select"
"s1_rollbak_to_savepoint"
"s2_update" "s2_select"
"s1_select"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_savepoint"
"s2_select_key_share"
"s1_update"
"s2_select_update"
"s1_delete"
"s1_rollbak_to_savepoint"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_savepoint"
"s1_select_key_share"
"s1_update"
"s1_release" "s1_savepoint"
"s1_update"
"s1_rollbak_to_savepoint"
"s1_select" "s1_commit"
"s2_update_key"
"s2_commit"
"s2_select"

permutation "s1_begin" "s2_begin"
"s2_select_key_share"
"s1_savepoint"
"s1_update"
"s1_rollbak_to_savepoint"
"s1_select_no_key_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s2_savepoint"
"s1_select_update"
"s2_delete"
"s1_update_key"
"s1_commit"
"s2_select"
"s2_rollbak_to_savepoint"
"s2_select"
"s2_commit"

permutation "s1_begin"
"s1_select_no_key_update"
"s2_begin"
"s2_select_key_share"
"s1_update"
"s2_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin"
"s1_update" "s1_select"
"s1_savepoint"
"s1_delete" "s1_select"
"s2_select_update"
"s1_rollbak_to_savepoint" "s1_select"
"s1_commit"
"s2_update" "s2_select"
"s2_commit"

permutation "s1_begin" "s2_begin"
"s1_update_key" "s1_select"
"s1_savepoint"
"s1_update" "s1_select"
"s1_release" "s1_savepoint"
"s1_update" "s1_select"
"s2_delete"
"s1_rollbak_to_savepoint"
"s1_select"
"s1_commit"
"s2_select"
"s2_commit"





permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_delete"
"s1_select_key_share"
"s2_update_key"
"s3_savepoint"
"s1_rollback"
"s2_update"
"s2_savepoint"
"s2_delete"
"s2_rollbak_to_savepoint" "s2_commit"
"s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s1_delete"
"s1_rollback"
"s3_savepoint"
"s3_select_update"
"s2_update_key"
"s3_rollbak_to_savepoint" "s3_commit"
"s2_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s3_savepoint"
"s3_select_key_share"
"s2_savepoint"
"s2_update"
"s3_rollbak_to_savepoint"
"s2_rollbak_to_savepoint"
"s1_update_key"
"s1_select_update"
"s1_update"
"s1_commit" "s2_commit" "s3_commit"

permutation "s1_begin"  "s3_begin"
"s3_savepoint"
"s3_select_key_share"
"s1_select_key_share"
"s3_release"
"s3_savepoint"
"s3_update"
"s3_rollbak_to_savepoint"
"s3_commit"
"s1_delete"
"s1_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s2_savepoint"
"s3_select_key_share"
"s2_select_no_key_update"
"s2_rollbak_to_savepoint"
"s3_select_share"
"s1_select_share"
"s1_commit" "s3_commit" "s2_commit"


permutation "s1_begin" "s2_begin" "s3_begin"
"s3_savepoint"
"s1_select_key_share"
"s2_savepoint"
"s1_select_no_key_update"
"s1_savepoint"
"s1_select_share"
"s2_rollbak_to_savepoint"
"s3_rollbak_to_savepoint"
"s1_rollbak_to_savepoint"
"s1_commit" "s2_commit" "s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s3_savepoint"
"s1_select_key_share"
"s2_savepoint"
"s1_select_no_key_update"
"s1_savepoint"
"s1_select_share"
"s2_rollbak_to_savepoint"
"s3_rollbak_to_savepoint"
"s1_rollback"
"s2_commit" "s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s2_savepoint"
"s3_delete"
"s3_update_key"
"s2_rollbak_to_savepoint"
"s3_commit"
"s1_select_update"
"s1_commit" "s2_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_select_update"
"s1_release" "s1_savepoint"
"s2_select_share"
"s1_commit"
"s3_delete"
"s2_commit" "s3_commit"


permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_select_update"
"s1_release" "s1_savepoint"
"s2_select_share"
"s1_commit"
"s3_update_key"
"s2_commit" "s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_select_update"
"s1_release" "s1_savepoint"
"s2_select_share"
"s1_commit"
"s3_update"
"s2_commit" "s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_select_update"
"s1_release" "s1_savepoint"
"s2_select_share"
"s1_commit"
"s3_select_update"
"s2_commit" "s3_commit"


permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_select_update"
"s1_release" "s1_savepoint"
"s2_select_share"
"s1_commit"
"s3_select_no_key_update"
"s2_commit" "s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_update"
"s2_delete"
"s1_rollback"
"s2_savepoint"
"s2_update"
"s2_commit"
"s1_select_share"
"s3_commit"


permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_update"
"s2_update"
"s1_rollback"
"s2_savepoint"
"s2_update"
"s2_commit"
"s1_select_share"
"s3_commit"

permutation "s1_begin" "s2_begin" "s3_begin"
"s1_savepoint"
"s1_update"
"s2_update_key"
"s1_rollback"
"s2_savepoint"
"s2_update"
"s2_commit"
"s1_select_share"
"s3_commit"

permutation "s1_begin" "s1_ioc" "s2_select_key_share" "s1_commit"
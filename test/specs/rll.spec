setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE rll_test
	(
		key int8 not null,
		value int8 not null,
		uniq int8 not null,
		PRIMARY KEY(key),
		UNIQUE(uniq)
	) USING orioledb;
	CREATE OR REPLACE FUNCTION update_trg_func() RETURNS trigger AS
	$$BEGIN
		RAISE NOTICE 'old: (%, %, %), new: (%, %, %)', OLD.key, OLD.value, OLD.uniq, NEW.key, NEW.value, NEW.uniq;
		RETURN NEW;
	END;$$ LANGUAGE plpgsql;
	TRUNCATE rll_test;
	INSERT INTO rll_test (SELECT i, i, i FROM generate_series(1, 10) AS i);

	CREATE TABLE rll_test_text_pkey_update (
		val_1 text PRIMARY KEY,
		val_2 int not null
	) USING orioledb;

	INSERT INTO rll_test_text_pkey_update VALUES ('a', 600), ('b', 600);
}

teardown
{
	DROP TABLE rll_test;
	DROP TABLE rll_test_text_pkey_update;
}

session "s1"

step "s1_trigger" { CREATE TRIGGER before_update_trg BEFORE UPDATE ON rll_test FOR EACH ROW EXECUTE PROCEDURE update_trg_func();}
step "s1_begin" { BEGIN; }
step "s1_select" { SELECT * FROM rll_test; }
step "s1_select_key_share" { SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE; }
step "s1_select_share" { SELECT * FROM rll_test WHERE key = 1 FOR SHARE; }
step "s1_select_update" { SELECT * FROM rll_test WHERE key = 1 FOR UPDATE; }
step "s1_select_update_skip_locked" { SELECT * FROM rll_test ORDER BY key FOR UPDATE SKIP LOCKED LIMIT 1; }
step "s1_select_2" { SELECT * FROM rll_test_text_pkey_update; }
step "s1_update" { UPDATE rll_test SET value = value + 10 WHERE key = 1; }
step "s1_update_2" { UPDATE rll_test_text_pkey_update SET val_2 = val_2 - 200 WHERE val_1 = 'a'; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_select_key_share" { SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE; }
step "s2_select_share" { SELECT * FROM rll_test WHERE key = 1 FOR SHARE; }
step "s2_select_no_key_update" { SELECT * FROM rll_test WHERE key = 1 FOR NO KEY UPDATE; }
step "s2_select_update" { SELECT * FROM rll_test WHERE key = 1 FOR UPDATE; }
step "s2_select_update_skip_locked" { SELECT * FROM rll_test ORDER BY key FOR UPDATE SKIP LOCKED LIMIT 1; }
step "s2_update" { UPDATE rll_test SET value = value + 10 WHERE key = 1; }
step "s2_update_2" { UPDATE rll_test_text_pkey_update SET val_2 = val_2 + 450 WHERE val_1 = 'a'; }
step "s2_update_key" { UPDATE rll_test SET key = value WHERE key = 1; }
step "s2_commit" { COMMIT; }

session "s3"

step "s3_begin" { BEGIN; }
step "s3_update" { UPDATE rll_test SET value = value + 1 WHERE key = 1; }
step "s3_key_update" { UPDATE rll_test SET key = key + 10 WHERE key = 1; }
step "s3_delete" { DELETE FROM rll_test WHERE key = 1; }
step "s3_commit" { COMMIT; }

permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_key_share" "s2_select_key_share" "s3_update" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_key_share" "s2_select_key_share" "s3_update" "s3_commit" "s3_begin" "s3_key_update" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s3_update" "s1_select_key_share" "s2_select_key_share" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_share" "s2_select_share" "s3_update" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_key_share" "s2_select_share" "s3_update" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_key_share" "s2_select_no_key_update" "s3_update" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_key_share" "s2_select_key_share" "s3_key_update" "s1_commit" "s2_commit" "s3_commit"
permutation "s1_begin" "s2_begin" "s1_select_key_share" "s2_select_update" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_select_share" "s2_select_no_key_update" "s1_commit" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_select_share" "s2_select_update" "s1_commit" "s2_commit"
permutation "s1_begin" "s3_begin" "s1_select_share" "s3_update" "s1_commit" "s3_commit"
permutation "s1_begin" "s3_begin" "s1_select_key_share" "s3_delete" "s1_commit" "s3_commit"
permutation "s1_begin" "s3_begin" "s3_update" "s1_select_update" "s3_commit"  "s1_commit"
permutation "s1_begin" "s3_begin" "s3_key_update" "s1_select_update" "s3_commit"  "s1_commit"
permutation "s1_begin" "s3_begin" "s3_delete" "s1_select_update" "s3_commit"  "s1_commit"
permutation "s1_begin" "s2_begin" "s3_begin" "s1_select_key_share" "s3_update" "s2_select_share"  "s3_commit" "s1_commit" "s2_commit"

# Concurrent update
permutation "s1_begin" "s2_begin" "s1_update" "s2_update" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update" "s2_update" "s1_rollback"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update" "s2_update_key" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_begin" "s2_begin" "s1_update" "s2_update_key" "s1_rollback"  "s2_commit" "s1_select"

# Concurrent update + trigger
permutation "s1_trigger" "s1_begin" "s2_begin" "s1_update" "s2_update" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_trigger" "s1_begin" "s2_begin" "s1_update" "s2_update" "s1_rollback"  "s2_commit" "s1_select"
permutation "s1_trigger" "s1_begin" "s2_begin" "s1_update" "s2_update_key" "s1_commit"  "s2_commit" "s1_select"
permutation "s1_trigger" "s1_begin" "s2_begin" "s1_update" "s2_update_key" "s1_rollback"  "s2_commit" "s1_select"

# Concurrent update text pkey
permutation "s1_begin" "s2_begin" "s1_update_2" "s2_update_2" "s1_commit"  "s2_commit" "s1_select_2"

# Check for double locking the same row
permutation "s1_begin" "s2_begin" "s1_select_update_skip_locked" "s1_select_update_skip_locked" "s2_select_update_skip_locked" "s1_commit"  "s2_commit"


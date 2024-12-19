CREATE SCHEMA row_level_locks;
SET SESSION search_path = 'row_level_locks';
CREATE EXTENSION orioledb;

CREATE TABLE rll_test
(
	key int8 not null,
	value int8 not null,
	uniq int8 not null,
	PRIMARY KEY(key),
	UNIQUE(uniq)
) USING orioledb;

INSERT INTO rll_test VALUES (1, 1, 1);

-- Check row-level locks are correctly placed into undo chain

BEGIN;
SELECT * FROM rll_test WHERE key = 1 FOR SHARE;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SAVEPOINT s1;
SELECT * FROM rll_test WHERE key = 1 FOR UPDATE;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SAVEPOINT s2;
UPDATE rll_test SET value = value + 1 WHERE key = 1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK TO s2;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK TO s1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');

BEGIN;
SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SELECT * FROM rll_test WHERE key = 1 FOR SHARE;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SAVEPOINT s1;
UPDATE rll_test SET value = value + 1 WHERE key = 1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
UPDATE rll_test SET uniq = uniq + 1 WHERE key = 1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SAVEPOINT s2;
UPDATE rll_test SET uniq = uniq WHERE key = 1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK TO s1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');

BEGIN;
SELECT * FROM rll_test WHERE key = 1 FOR SHARE;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
SAVEPOINT s1;
UPDATE rll_test SET uniq = uniq + 1 WHERE key = 1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
UPDATE rll_test SET value = value + 1 WHERE key = 1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK TO s1;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');
ROLLBACK;
SELECT orioledb_tbl_structure('rll_test'::regclass, 'ne');

DROP TABLE rll_test;

CREATE TABLE trigger_test
(
	id int4 not null primary key,
	value text not null
) USING orioledb;

CREATE OR REPLACE FUNCTION update_trg_func() RETURNS trigger AS
$$BEGIN
	RAISE NOTICE 'old: (%, %), new: (%, %)', OLD.id, OLD.value, NEW.id, NEW.value;
	RETURN NEW;
END;$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_trg_func() RETURNS trigger AS
$$BEGIN
	RAISE NOTICE 'old: (%, %)', OLD.id, OLD.value;
	RETURN OLD;
END;$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_trg
	BEFORE UPDATE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE update_trg_func();

CREATE TRIGGER after_update_trg
	AFTER UPDATE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE update_trg_func();

CREATE TRIGGER before_delete_trg
	BEFORE DELETE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE delete_trg_func();

CREATE TRIGGER after_delete_trg
	AFTER DELETE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE delete_trg_func();

INSERT INTO trigger_test VALUES (1, 'abc');
UPDATE trigger_test SET value = 'def' WHERE id = 1;
DELETE FROM trigger_test WHERE id = 1;

DROP TABLE trigger_test;
CREATE TABLE trigger_test
(
	id int4 not null,
	value text not null
) USING orioledb;

CREATE TRIGGER before_update_trg
	BEFORE UPDATE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE update_trg_func();

CREATE TRIGGER after_update_trg
	AFTER UPDATE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE update_trg_func();

CREATE TRIGGER before_delete_trg
	BEFORE DELETE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE delete_trg_func();

CREATE TRIGGER after_delete_trg
	AFTER DELETE ON trigger_test FOR EACH ROW
	EXECUTE PROCEDURE delete_trg_func();

INSERT INTO trigger_test VALUES (1, 'abc');
UPDATE trigger_test SET value = 'def' WHERE id = 1;
DELETE FROM trigger_test WHERE id = 1;

BEGIN;
INSERT INTO trigger_test VALUES (2, 'a');
UPDATE trigger_test SET value = 'b' WHERE id = 2;
DELETE FROM trigger_test WHERE id = 2;
COMMIT;

CREATE TABLE rll_test2
(
	key int8 not null,
	value int8 not null,
	spacer text,
	PRIMARY KEY(key)
) USING orioledb;

INSERT INTO rll_test2 (SELECT i, i FROM generate_series(1, 100) i);
-- Add record with prevHasLocks flag
SELECT * FROM rll_test2 WHERE key = 1 FOR KEY SHARE;
UPDATE rll_test2 SET value = value + 1 WHERE key = 1;
-- Issue some amount of undo to update the retained location
UPDATE rll_test2 SET spacer = repeat('a', 1000) WHERE key > 1;
UPDATE rll_test2 SET spacer = repeat('b', 1000) WHERE key > 1;
UPDATE rll_test2 SET spacer = repeat('c', 1000) WHERE key > 1;
-- Check how we cleanup prevHasLocks
SELECT * FROM rll_test2 WHERE key = 1 FOR UPDATE;

CREATE TABLE o_test_update_locked_for_update (
	key int NOT NULL PRIMARY KEY,
	val_1 text,
	val_2 int
) USING orioledb;

INSERT INTO o_test_update_locked_for_update VALUES (1, 'a', 0);

SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

BEGIN;
UPDATE o_test_update_locked_for_update SET val_2 = val_2 - 1 WHERE key = 1;
SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

SELECT * FROM o_test_update_locked_for_update FOR UPDATE;
SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

UPDATE o_test_update_locked_for_update SET val_2 = val_2 - 1 WHERE key = 1;
SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

DELETE FROM o_test_update_locked_for_update WHERE KEY = 1;
SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

ROLLBACK;

SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

UPDATE o_test_update_locked_for_update SET val_2 = val_2 - 1 WHERE key = 1;
SELECT orioledb_tbl_structure('o_test_update_locked_for_update'::regclass,
							  'fen');
SELECT * FROM o_test_update_locked_for_update;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA row_level_locks CASCADE;
RESET search_path;

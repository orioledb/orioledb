CREATE SCHEMA trigger;
SET SESSION search_path = 'trigger';
CREATE EXTENSION IF NOT EXISTS orioledb;

CREATE TABLE o_test_1 (
	val_1 int,
	val_2 int
) USING orioledb;

INSERT INTO o_test_1 (val_1, val_2)
	(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

CREATE OR REPLACE FUNCTION func_trig_o_test_1() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO o_test_1(val_1) VALUES (OLD.val_1);
	RETURN OLD;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER trig_o_test_1 AFTER DELETE ON o_test_1 FOR EACH STATEMENT
	EXECUTE PROCEDURE func_trig_o_test_1();

SELECT * FROM o_test_1;
DELETE FROM o_test_1 WHERE val_1 = 3;
SELECT * FROM o_test_1;

CREATE TABLE o_test_2 (
  val_1 int,
  val_2 int
) USING orioledb;

INSERT INTO o_test_2 (val_1, val_2)
  (SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

CREATE OR REPLACE FUNCTION func_trig_o_test_2() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO o_test_2(val_1) VALUES (OLD.val_1);
	RETURN OLD;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER trig_o_test_2 AFTER UPDATE ON o_test_2 FOR EACH STATEMENT
	EXECUTE PROCEDURE func_trig_o_test_2();

SELECT * FROM o_test_2;
UPDATE o_test_2 SET val_1 = val_1 + 100;
SELECT * FROM o_test_2;

CREATE TABLE o_test_3 (
    val_1 int,
    val_2 int
) USING orioledb;

INSERT INTO o_test_3 (val_1, val_2)
    (SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

CREATE OR REPLACE FUNCTION func_trig_o_test_3() RETURNS TRIGGER AS $$
BEGIN
	UPDATE o_test_3 SET val_1 = val_1 WHERE val_1 = OLD.val_1;
	RETURN OLD;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER trig_o_test_3 AFTER INSERT ON o_test_3 FOR EACH STATEMENT
	EXECUTE PROCEDURE func_trig_o_test_3();

SELECT * FROM o_test_3;
INSERT INTO o_test_3 (val_1, val_2)
    (SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);
SELECT * FROM o_test_3;

CREATE TABLE o_test_4 (
  val_1 int PRIMARY KEY,
  val_2 text
) USING orioledb;

INSERT INTO o_test_4 (val_1, val_2)
	(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

CREATE FUNCTION func_trig_o_test_4() RETURNS TRIGGER AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_1 AFTER UPDATE ON o_test_4
    REFERENCING OLD TABLE AS a NEW TABLE AS i
    FOR EACH STATEMENT EXECUTE FUNCTION func_trig_o_test_4();

SELECT * FROM o_test_4;
UPDATE o_test_4 SET val_1 = val_1;
SELECT * FROM o_test_4;

CREATE TABLE o_test_copy_trigger (
	val_1 serial,
	val_2 int,
	val_3 text,
	val_4 text,
	val_5 text
) USING orioledb;

CREATE FUNCTION func_1 () RETURNS TRIGGER
AS $$
BEGIN
	NEW.val_5 := 'abc'::text;
	return NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trig_1 BEFORE INSERT ON o_test_copy_trigger
	FOR EACH ROW EXECUTE PROCEDURE func_1();

COPY o_test_copy_trigger (val_1, val_2, val_3, val_4, val_5) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

SELECT * FROM o_test_copy_trigger;

BEGIN;

CREATE TABLE o_test_alter_type_after_update (
  val_1 int PRIMARY KEY,
  val_2 text
) USING orioledb;

CREATE FUNCTION alter_type_after_update_trigger() RETURNS TRIGGER
AS $$
BEGIN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_1
  AFTER UPDATE ON o_test_alter_type_after_update
  REFERENCING OLD TABLE AS a NEW TABLE AS b
  FOR EACH STATEMENT EXECUTE PROCEDURE alter_type_after_update_trigger();

INSERT INTO o_test_alter_type_after_update VALUES (1, '1'), (2, '2'), (3, '3');

ALTER TABLE o_test_alter_type_after_update
	ALTER COLUMN val_2 TYPE int USING val_2::integer;

UPDATE o_test_alter_type_after_update SET val_2 = val_2 + 1;

COMMIT;

-- Test trigger enable/disable commands via ALTER TABLE
-- AT_EnableTrig, AT_DisableTrig, AT_EnableAlwaysTrig, AT_EnableReplicaTrig
-- AT_EnableTrigAll, AT_DisableTrigAll, AT_EnableTrigUser, AT_DisableTrigUser

-- Create a table for trigger testing
CREATE TABLE test_trigger_table (
	i int PRIMARY KEY,
	val text,
	modified_count int DEFAULT 0
) USING orioledb;

-- Create a log table to track trigger executions
CREATE TABLE test_trigger_log (
	log_id serial PRIMARY KEY,
	trigger_name text,
	operation text,
	old_val text,
	new_val text,
	fired_at timestamp DEFAULT now()
) USING orioledb;

-- Create trigger function that logs operations
CREATE OR REPLACE FUNCTION test_trigger_func() RETURNS trigger AS $$
BEGIN
	INSERT INTO test_trigger_log (trigger_name, operation, old_val, new_val)
	VALUES (TG_NAME, TG_OP,
		CASE WHEN TG_OP = 'DELETE' THEN OLD.val ELSE NULL END,
		CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN NEW.val ELSE NULL END);

	IF TG_OP = 'UPDATE' THEN
		NEW.modified_count := OLD.modified_count + 1;
	END IF;

	RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

-- Create multiple triggers with different types
CREATE TRIGGER trigger_before_insert
	BEFORE INSERT ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

CREATE TRIGGER trigger_after_insert
	AFTER INSERT ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

CREATE TRIGGER trigger_before_update
	BEFORE UPDATE ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

CREATE TRIGGER trigger_after_delete
	AFTER DELETE ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

-- Check initial trigger states (all should be enabled: tgenabled = 'O' for origin)
SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Insert data and verify triggers fire
INSERT INTO test_trigger_table (i, val) VALUES (1, 'first');
INSERT INTO test_trigger_table (i, val) VALUES (2, 'second');

-- Check trigger log (should have 4 entries: 2 before_insert + 2 after_insert)
SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

-- Clear log
TRUNCATE test_trigger_log;

-- Test AT_DisableTrig - disable specific trigger
ALTER TABLE test_trigger_table DISABLE TRIGGER trigger_before_insert;

-- Verify trigger is disabled (tgenabled = 'D')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Insert should only fire after_insert trigger (not before_insert)
INSERT INTO test_trigger_table (i, val) VALUES (3, 'third');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

TRUNCATE test_trigger_log;

-- Test AT_EnableTrig - re-enable the trigger (origin mode)
ALTER TABLE test_trigger_table ENABLE TRIGGER trigger_before_insert;

-- Verify trigger is enabled again (tgenabled = 'O')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
  AND tgname = 'trigger_before_insert';

-- Insert should fire both triggers again
INSERT INTO test_trigger_table (i, val) VALUES (4, 'fourth');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

TRUNCATE test_trigger_log;

-- Test AT_EnableReplicaTrig - enable for replica mode (tgenabled = 'R')
ALTER TABLE test_trigger_table ENABLE REPLICA TRIGGER trigger_before_update;

-- Verify trigger mode changed to replica
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
  AND tgname = 'trigger_before_update';

-- Test AT_EnableAlwaysTrig - enable to always fire (tgenabled = 'A')
ALTER TABLE test_trigger_table ENABLE ALWAYS TRIGGER trigger_after_delete;

-- Verify trigger mode changed to always
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
  AND tgname = 'trigger_after_delete';

-- Test AT_DisableTrigAll - disable all triggers on the table
ALTER TABLE test_trigger_table DISABLE TRIGGER ALL;

-- Verify all triggers are disabled (tgenabled = 'D')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Operations should not fire any triggers
INSERT INTO test_trigger_table (i, val) VALUES (5, 'fifth');
UPDATE test_trigger_table SET val = 'updated' WHERE i = 1;
DELETE FROM test_trigger_table WHERE i = 5;

-- Log should be empty (no triggers fired)
SELECT COUNT(*) as trigger_fire_count FROM test_trigger_log;

-- Test AT_EnableTrigAll - enable all triggers
ALTER TABLE test_trigger_table ENABLE TRIGGER ALL;

-- Verify all triggers are enabled (tgenabled = 'O')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Operations should fire triggers again
INSERT INTO test_trigger_table (i, val) VALUES (6, 'sixth');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

TRUNCATE test_trigger_log;

-- Test AT_DisableTrigUser - disable user triggers only
-- First, let's create a constraint trigger to differentiate
CREATE TABLE test_trigger_ref (
	ref_id int PRIMARY KEY
) USING orioledb;

INSERT INTO test_trigger_ref VALUES (1), (2), (3), (4), (6), (7), (8);

-- Add foreign key which creates a constraint trigger (internal trigger)
ALTER TABLE test_trigger_table
	ADD CONSTRAINT fk_test_ref FOREIGN KEY (i) REFERENCES test_trigger_ref(ref_id);

-- Check triggers now (should have user triggers + internal FK triggers)
SELECT
	CASE
		WHEN tgname ~ '^RI_ConstraintTrigger_c_[0-9]+$'
		THEN 'RI_ConstraintTrigger'
		ELSE tgname
	END AS tgname,
	tgenabled,
	tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

-- Disable only user triggers (not internal FK triggers)
ALTER TABLE test_trigger_table DISABLE TRIGGER USER;

-- Verify: user triggers disabled, internal triggers still enabled
SELECT
	CASE
		WHEN tgname ~ '^RI_ConstraintTrigger_c_[0-9]+$'
		THEN 'RI_ConstraintTrigger'
		ELSE tgname
	END AS tgname,
	tgenabled,
	tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

-- Insert should not fire user triggers but FK constraint should still work
INSERT INTO test_trigger_table (i, val) VALUES (7, 'seventh');

-- Log should be empty (user triggers disabled)
SELECT COUNT(*) as trigger_fire_count FROM test_trigger_log;

-- Try to violate FK constraint (should still fail - internal trigger works)
INSERT INTO test_trigger_table (i, val) VALUES (99, 'invalid');  -- Should fail FK

-- Test AT_EnableTrigUser - enable user triggers only
ALTER TABLE test_trigger_table ENABLE TRIGGER USER;

-- Verify: user triggers enabled back to origin mode
SELECT
	CASE
		WHEN tgname ~ '^RI_ConstraintTrigger_c_[0-9]+$'
		THEN 'RI_ConstraintTrigger'
		ELSE tgname
	END AS tgname,
	tgenabled,
	tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

-- Insert should fire user triggers again
INSERT INTO test_trigger_table (i, val) VALUES (8, 'eighth');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

-- Test combination: DISABLE ALL then ENABLE USER
ALTER TABLE test_trigger_table DISABLE TRIGGER ALL;

SELECT
	CASE
		WHEN tgname ~ '^RI_ConstraintTrigger_c_[0-9]+$'
		THEN 'RI_ConstraintTrigger'
		ELSE tgname
	END AS tgname,
	tgenabled,
	tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

ALTER TABLE test_trigger_table ENABLE TRIGGER USER;

-- User triggers should be enabled, internal triggers still disabled
SELECT
	CASE
		WHEN tgname ~ '^RI_ConstraintTrigger_c_[0-9]+$'
		THEN 'RI_ConstraintTrigger'
		ELSE tgname
	END AS tgname,
	tgenabled,
	tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

TRUNCATE test_trigger_log;

INSERT INTO test_trigger_table (i, val) VALUES (99, 'invalid');  -- Now succeed, due to disabled FK trigger

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

-- Cleanup
DROP TABLE test_trigger_table CASCADE;
DROP TABLE test_trigger_ref CASCADE;
DROP TABLE test_trigger_log CASCADE;
DROP FUNCTION test_trigger_func();

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA trigger CASCADE;
RESET search_path;
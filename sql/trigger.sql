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

DROP FUNCTION func_trig_o_test_1 CASCADE;
DROP FUNCTION func_trig_o_test_2 CASCADE;
DROP FUNCTION func_trig_o_test_3 CASCADE;
DROP FUNCTION func_trig_o_test_4 CASCADE;
DROP EXTENSION orioledb CASCADE;
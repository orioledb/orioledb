CREATE SCHEMA foreign_keys;
SET SESSION search_path = 'foreign_keys';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_text(
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id),
	UNIQUE(val)
) USING orioledb;

CREATE TABLE o_test_text_child(
	id integer NOT NULL,
	o_test_val text NOT NULL REFERENCES o_test_text (val) ON UPDATE CASCADE,
	PRIMARY KEY(id)
) USING orioledb;


CREATE TABLE o_test(
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id),
	UNIQUE(id, val)
) USING orioledb;

CREATE TABLE o_test_child(
	id integer NOT NULL,
	o_test_ID integer NOT NULL REFERENCES o_test (id) DEFERRABLE,
	PRIMARY KEY(id)
) USING orioledb;

CREATE TABLE o_test_child_del(
	id integer NOT NULL,
	o_test_ID  integer NOT NULL REFERENCES o_test (id) ON DELETE CASCADE,
	PRIMARY KEY(id)
) USING orioledb;

CREATE TABLE o_test_child_upd(
	id integer NOT NULL,
	o_test_ID  integer NOT NULL REFERENCES o_test (id) ON UPDATE CASCADE,
	PRIMARY KEY(id)
) USING orioledb;

CREATE TABLE o_test_child_compose(
	id integer NOT NULL,
	o_test_ID integer NOT NULL,
	o_test_val text NOT NULL,
	FOREIGN KEY(o_test_ID, o_test_val)
		REFERENCES o_test (id, val) ON UPDATE CASCADE ON DELETE CASCADE,
	PRIMARY KEY(id)
) USING orioledb;

-- correct insert
INSERT INTO o_test_text(id, val) VALUES(1, 'text');

INSERT INTO o_test_text_child(id, o_test_val) VALUES(1, 'text');

-- correct update
UPDATE o_test_text SET val='hello' where id = 1;

SELECT * FROM o_test_text;
SELECT * FROM o_test_text_child;

-- correct insert
INSERT INTO o_test(id, val) VALUES (1, 'hello');
INSERT INTO o_test(id, val) VALUES (2, 'hey');
INSERT INTO o_test(id, val) VALUES (3, 'hi');
INSERT INTO o_test(id, val) VALUES (4, 'mey');
INSERT INTO o_test(id, val) VALUES (5, 'gogo');
INSERT INTO o_test(id, val) VALUES (6, 'neo');

BEGIN;
INSERT INTO o_test(id, val) VALUES (7, 'cat');
UPDATE o_test SET val = 'dog' WHERE id = 7;
COMMIT;

INSERT INTO o_test_child(id, o_test_ID) VALUES (1, 1);
INSERT INTO o_test_child(id, o_test_ID) VALUES (2, 2);

BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO o_test_child(id, o_test_ID) VALUES (3, 10);
UPDATE o_test_child SET id = 4 WHERE id = 3;
COMMIT;

INSERT INTO o_test_child_upd(id, o_test_ID) VALUES (1, 3);

INSERT INTO o_test_child_del(id, o_test_ID) VALUES (1, 4);

INSERT INTO o_test_child_compose(id, o_test_ID, o_test_val) VALUES (1, 6, 'neo');
INSERT INTO o_test_child_compose(id, o_test_ID, o_test_val) VALUES (2, 7, 'dog');

-- fail insert
INSERT INTO o_test_child(id, o_test_ID) VALUES (3, 10);
INSERT INTO o_test_child_compose(id, o_test_ID, o_test_val) VALUES (3, 7, 'xxxxx');
INSERT INTO o_test_child_compose(id, o_test_ID, o_test_val) VALUES (4, 10, 'neo');

-- fail update
UPDATE o_test SET id = 10 where id = 1;
UPDATE o_test SET id = 10 where id = 4;

UPDATE o_test_child SET o_test_ID = 10 where id = 1;

UPDATE o_test_child_del SET o_test_ID = 10 where id = 1;
UPDATE o_test_child_upd SET o_test_ID = 10 where id = 1;

UPDATE o_test_child_compose SET o_test_val = 'xxxx' where id = 1;
UPDATE o_test_child_compose SET o_test_ID = 10 where id = 1;

-- fail delete
DELETE FROM o_test where id = 1;
DELETE FROM o_test where id = 3;

-- correct delete
DELETE FROM o_test_child where o_test_ID = 1;
DELETE FROM o_test where id = 1;
DELETE FROM o_test where id = 4;

-- correct update
UPDATE o_test SET val = 'new_dog' where id = 7;
UPDATE o_test SET id = 10 where id = 3;

UPDATE o_test_child SET o_test_ID = 3 where o_test_ID = 1;

UPDATE o_test_child_upd SET o_test_ID = 10 where id = 1;

UPDATE o_test_child_compose SET o_test_val = 'gogo', o_test_ID = 5 where id = 2;

SELECT * FROM o_test;
SELECT * FROM o_test_child;
SELECT * FROM o_test_child_upd;
SELECT * FROM o_test_child_del;
SELECT * FROM o_test_child_compose;

-- correct delete
DELETE FROM o_test where id = 6 or id = 7;
SELECT * FROM o_test_child_compose;

CREATE TABLE o_test_1 (
	val_1 int NOT NULL
)USING orioledb;

CREATE TABLE o_test_2 (
	val_2 int,
	PRIMARY KEY(val_1),
	UNIQUE(val_1, val_2)
) INHERITS (o_test_1) USING orioledb;

INSERT INTO o_test_2 (val_1)
	VALUES (1), (2), (3);

UPDATE o_test_2 SET val_1 = val_1 * 4;

DELETE FROM o_test_2;

CREATE TABLE o_test_3 (
	val_3 int,
	val_4 int,
	FOREIGN KEY (val_3, val_4)
	REFERENCES o_test_2 (val_1, val_2)
)USING orioledb;

INSERT INTO o_test_2 (val_1, val_2)
	VALUES (1, 1), (2, 2), (3, 1);

INSERT INTO o_test_3 (val_3, val_4)
	VALUES (3, 1), (3, 1);

UPDATE o_test_2 SET val_1 = val_1 * 4;
UPDATE o_test_2 SET val_1 = val_1 * 4 WHERE val_1 < 3;

BEGIN;

CREATE TABLE o_test_reference_to_self_update_cascade (
	val_1 int PRIMARY KEY,
	val_2 int,
	FOREIGN KEY (val_2)
		REFERENCES o_test_reference_to_self_update_cascade (val_1)
			ON UPDATE CASCADE
) USING orioledb;

INSERT INTO o_test_reference_to_self_update_cascade (val_1, val_2)
	VALUES (0, 0);

UPDATE o_test_reference_to_self_update_cascade SET val_1 = 3 WHERE val_1 = 0;

COMMIT;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA foreign_keys CASCADE;
RESET search_path;

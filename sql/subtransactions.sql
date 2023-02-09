--
-- Tests for (sub)transactions
--
CREATE SCHEMA subtransactions;
SET SESSION search_path = 'subtransactions';
CREATE EXTENSION orioledb;

CREATE TABLE o_subtrans (
	id integer NOT NULL,
	value text NOT NULL,
	PRIMARY KEY (id)
) USING orioledb;

---
-- Tests for relfilenode undo
---
-- TRUNCATE undo
BEGIN;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
COMMIT;

SELECT * FROM o_subtrans;

BEGIN;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
ROLLBACK;

SELECT * FROM o_subtrans;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(0, 555, 5) i;
SAVEPOINT s1;
UPDATE o_subtrans SET value = value || 'def' WHERE id % 10 = 5;
DELETE FROM o_subtrans WHERE id % 10 = 0;
SAVEPOINT s2;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(0, 555, 10) i;
SAVEPOINT s3;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(3, 555, 10) i;
UPDATE o_subtrans SET value = value || 'qwer';
SAVEPOINT s4;
UPDATE o_subtrans SET value = value || 'asdf';
SAVEPOINT s5;
DELETE FROM o_subtrans WHERE id % 4 = 3;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s5;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s4;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s3;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s2;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s1;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
COMMIT;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(0, 555, 5) i;
SAVEPOINT s1;
UPDATE o_subtrans SET value = value || 'def' WHERE id % 10 = 5;
DELETE FROM o_subtrans WHERE id % 10 = 0;
SAVEPOINT s2;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(0, 555, 10) i;
SAVEPOINT s3;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(3, 555, 10) i;
UPDATE o_subtrans SET value = value || 'qwer';
SAVEPOINT s4;
UPDATE o_subtrans SET value = value || 'asdf';
SAVEPOINT s5;
DELETE FROM o_subtrans WHERE id % 4 = 3;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s5;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s4;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s3;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s2;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK TO s1;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
ROLLBACK;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(0, 555, 5) i;
SAVEPOINT s1;
UPDATE o_subtrans SET value = value || 'def' WHERE id % 10 = 5;
DELETE FROM o_subtrans WHERE id % 10 = 0;
SAVEPOINT s2;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(0, 555, 10) i;
SAVEPOINT s3;
INSERT INTO o_subtrans
SELECT i, repeat('abc', i % 5) FROM generate_series(3, 555, 10) i;
UPDATE o_subtrans SET value = value || 'qwer';
SAVEPOINT s4;
UPDATE o_subtrans SET value = value || 'asdf';
SAVEPOINT s5;
DELETE FROM o_subtrans WHERE id % 4 = 3;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
RELEASE s5;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
RELEASE s4;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
RELEASE s3;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
RELEASE s2;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
RELEASE s1;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;
COMMIT;
SELECT array_agg(id), value
FROM o_subtrans GROUP BY value ORDER BY value;


-- INSERT/DELETE undo
TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 10, 1) AS i;
DELETE FROM o_subtrans WHERE id <= 5;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
COMMIT;

SELECT * FROM o_subtrans;

BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 10, 1) AS i;
DELETE FROM o_subtrans WHERE id <= 5;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
ROLLBACK;

SELECT * FROM o_subtrans;

-- UPDATE undo
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3)FROM generate_series(1, 10, 1) AS i;
BEGIN;
UPDATE o_subtrans SET value = value || 'abc';
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
COMMIT;

SELECT * FROM o_subtrans;

INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 10, 1) AS i;
BEGIN;
UPDATE o_subtrans SET value = value || 'abc';
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
TRUNCATE o_subtrans;
ROLLBACK;
SELECT * FROM o_subtrans;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 5, 1) AS i;
SAVEPOINT s1;
INSERT INTO o_subtrans
SELECT i, repeat('asdf', i % 3) FROM generate_series(6, 10, 1) AS i;
SELECT * FROM o_subtrans;
ROLLBACK TO s1;
SELECT * FROM o_subtrans;
COMMIT;
SELECT * FROM o_subtrans;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 5, 1) AS i;
SAVEPOINT s1;
INSERT INTO o_subtrans
SELECT i, repeat('asdf', i % 3) FROM generate_series(6, 10, 1) AS i;
SELECT * FROM o_subtrans;
ROLLBACK TO s1;
SELECT * FROM o_subtrans;
ROLLBACK;
SELECT * FROM o_subtrans;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 5, 1) AS i;
DELETE FROM o_subtrans WHERE id BETWEEN 1 AND 5;
SAVEPOINT s1;
INSERT INTO o_subtrans
SELECT i, repeat('zxcv', i % 3) FROM generate_series(1, 5, 1) AS i;
UPDATE o_subtrans SET value = repeat('rtyu', id % 4) WHERE id BETWEEN 1 AND 5;
INSERT INTO o_subtrans
SELECT i, repeat('asdf', i % 3) FROM generate_series(6, 10, 1) AS i;
UPDATE o_subtrans SET value = repeat('ghjk', id % 5) WHERE id BETWEEN 1 AND 5;
UPDATE o_subtrans SET value = repeat('bnm,', id % 6) WHERE id BETWEEN 1 AND 5;
SAVEPOINT s2;
DELETE FROM o_subtrans WHERE id BETWEEN 6 AND 10;
SELECT * FROM o_subtrans;
ROLLBACK TO s2;
COMMIT;
SELECT * FROM o_subtrans;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('qwer', i % 3) FROM generate_series(1, 5, 1) AS i;
DELETE FROM o_subtrans WHERE id BETWEEN 1 AND 5;
SAVEPOINT s1;
INSERT INTO o_subtrans
SELECT i, repeat('zxcv', i % 3) FROM generate_series(1, 5, 1) AS i;
UPDATE o_subtrans SET value = repeat('rtyu', id % 4) WHERE id BETWEEN 1 AND 5;
INSERT INTO o_subtrans
SELECT i, repeat('asdf', i % 3) FROM generate_series(6, 10, 1) AS i;
UPDATE o_subtrans SET value = repeat('ghjk', id % 5) WHERE id BETWEEN 1 AND 5;
UPDATE o_subtrans SET value = repeat('bnm,', id % 6) WHERE id BETWEEN 1 AND 5;
SAVEPOINT s2;
DELETE FROM o_subtrans WHERE id BETWEEN 6 AND 10;
SELECT * FROM o_subtrans;
ROLLBACK TO s1;
COMMIT;
SELECT * FROM o_subtrans;

TRUNCATE o_subtrans;
BEGIN;
SAVEPOINT s1;
SET TRANSACTION READ ONLY;
SAVEPOINT s2;
SET TRANSACTION READ WRITE;
ROLLBACK;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 5) id;
SAVEPOINT s1;
DELETE FROM o_subtrans WHERE id % 10 = 0;
SAVEPOINT s2;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 10) id;
SAVEPOINT s3;
UPDATE o_subtrans SET value = '12345' || value;
ROLLBACK TO s3;
ROLLBACK TO s2;
COMMIT;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 5) id;
SAVEPOINT s1;
DELETE FROM o_subtrans WHERE id % 10 = 0;
SAVEPOINT s2;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 10) id;
DELETE FROM o_subtrans WHERE id % 10 = 0;
UPDATE o_subtrans SET value = '12345' || value;
ROLLBACK TO s2;
SELECT * FROM o_subtrans WHERE id = 10;
ROLLBACK TO s1;
SELECT * FROM o_subtrans WHERE id = 10;
COMMIT;

TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 5) id;
SAVEPOINT s1;
DELETE FROM o_subtrans WHERE id % 5 = 0;
COMMIT;
BEGIN;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 5) id;
DELETE FROM o_subtrans WHERE id % 5 = 0;
ROLLBACK;

TRUNCATE o_subtrans;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 5) id;
DELETE FROM o_subtrans WHERE id % 5 = 0;
BEGIN;
INSERT INTO o_subtrans
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 10) id;
DELETE FROM o_subtrans WHERE id % 5 = 0;
SELECT id, repeat('abcdef', id % 7) FROM generate_series(0, 500, 5) id;
COMMIT;

-- Subtransaction check
TRUNCATE o_subtrans;
BEGIN;
INSERT INTO o_subtrans
SELECT i, repeat('a', i) FROM generate_series(1, 10, 1) AS i;
SAVEPOINT s1;
TRUNCATE o_subtrans;
CREATE TABLE o_subtrans_1
(
	id int8 not null,
	PRIMARY KEY(id)
) USING orioledb;

SAVEPOINT s2;
CREATE TABLE o_subtrans_2
(
	id int8 not null,
	PRIMARY KEY(id)
) USING orioledb;
TRUNCATE o_subtrans;

SAVEPOINT s3;
CREATE TABLE o_subtrans_3
(
	id int8 not null,
	PRIMARY KEY(id)
) USING orioledb;
TRUNCATE o_subtrans;
DROP TABLE o_subtrans;

ROLLBACK TO s3;
ROLLBACK TO s2;
CREATE TABLE o_subtrans_4
(
	id int8 not null,
	PRIMARY KEY(id)
) USING orioledb;
ROLLBACK;

SELECT * FROM o_subtrans;
DROP TABLE o_subtrans_1;
DROP TABLE o_subtrans_2;
DROP TABLE o_subtrans_3;
DROP TABLE o_subtrans_4;

-- Test some interesting cases to check that orioledb doesn't break them
BEGIN;
	SAVEPOINT one;
	ROLLBACK TO one;
	RELEASE one;
	SAVEPOINT two;
	RELEASE two;
	SAVEPOINT three;
	ROLLBACK TO three;
	RELEASE three;
COMMIT;

BEGIN;
	SAVEPOINT one;
	ROLLBACK TO one;
	RELEASE one;
	SAVEPOINT two;
	RELEASE two;
	SAVEPOINT three;
		SAVEPOINT four;
			SAVEPOINT five;
				SAVEPOINT six;
				ROLLBACK TO four;
		ROLLBACK TO four;
		RELEASE four;
	ROLLBACK TO three;
	RELEASE three;
COMMIT;

BEGIN;

-- Check subtransaction vs cursors
CREATE TABLE o_test_1(
    val_1 int
) USING orioledb;
INSERT INTO o_test_1 VALUES (10);
DECLARE abc CURSOR for SELECT * FROM o_test_1;
SAVEPOINT s1;
FETCH FROM abc;
ROLLBACK TO s1;
FETCH FROM abc;
ROLLBACK;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA subtransactions CASCADE;
RESET search_path;

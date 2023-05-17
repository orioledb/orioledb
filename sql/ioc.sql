-- INSERT .. ON CONFLICT

CREATE SCHEMA ioc;
SET SESSION search_path = 'ioc';
CREATE EXTENSION orioledb;

---
-- easy cases
---
CREATE TABLE o_test_ioc1
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	PRIMARY KEY (id1)
) USING orioledb;

INSERT INTO o_test_ioc1 VALUES (1, 1) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc1 VALUES (1, 2) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc1 VALUES (1, 3) ON CONFLICT (not_exist) DO NOTHING;
INSERT INTO o_test_ioc1 VALUES (1, 4) ON CONFLICT DO NOTHING;
INSERT INTO o_test_ioc1 VALUES (1, 5) ON CONFLICT DO NOTHING RETURNING *;

SELECT * FROM o_test_ioc1;

TRUNCATE o_test_ioc1;
INSERT INTO o_test_ioc1 AS t
	VALUES (1, 1), (1, 2)
		ON CONFLICT (id1) DO UPDATE SET id2 = t.id2 + EXCLUDED.id2;

CREATE UNIQUE INDEX o_test_ioc1_uniq on o_test_ioc1(id2);
ALTER TABLE o_test_ioc1 ADD COLUMN val int;

INSERT INTO o_test_ioc1 AS t
	VALUES (1, 1, 1), (2, 1, 2)
		ON CONFLICT (id2) DO UPDATE SET val = t.val + EXCLUDED.val;

CREATE TABLE o_test_ioc2
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	PRIMARY KEY (id1)
) USING orioledb;

INSERT INTO o_test_ioc2 VALUES (1, 1) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc2 VALUES (1, 2) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc2 VALUES (1, 3) ON CONFLICT (not_exist) DO NOTHING;
INSERT INTO o_test_ioc2 VALUES (1, 4) ON CONFLICT DO NOTHING;
INSERT INTO o_test_ioc2 VALUES (1, 5) ON CONFLICT DO NOTHING RETURNING *;

SELECT * FROM o_test_ioc2;

CREATE TABLE o_test_ioc3
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL
) USING orioledb;
CREATE INDEX o_test_ioc3_reg ON o_test_ioc3 (id1);

INSERT INTO o_test_ioc3 VALUES (1, 1) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc3 VALUES (1, 2) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc3 VALUES (1, 3) ON CONFLICT (not_exist) DO NOTHING;
INSERT INTO o_test_ioc3 VALUES (1, 4) ON CONFLICT DO NOTHING;
INSERT INTO o_test_ioc3 VALUES (1, 5) ON CONFLICT DO NOTHING RETURNING *;

SELECT * FROM o_test_ioc3;

---
-- conflict_target tests
---

CREATE TABLE o_test_ioc4
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	id3 int8 NOT NULL,
	id4 int8 NOT NULL,
	id5 int8 NOT NULL,
	id6 int8 NOT NULL,
	id7 int8 NOT NULL,
	id8 int8 NOT NULL,
	PRIMARY KEY(id4)
) USING orioledb;
CREATE UNIQUE INDEX o_test_ioc4_uniq1 on o_test_ioc4(id5);
CREATE UNIQUE INDEX o_test_ioc4_uniq2 on o_test_ioc4(id4, id5);
CREATE UNIQUE INDEX o_test_ioc4_uniq3 on o_test_ioc4(id6, id7);
CREATE INDEX o_test_ioc4_reg1 on o_test_ioc4(id3);
CREATE UNIQUE INDEX o_test_ioc4_uniq4 on o_test_ioc4(id2);
INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8);

INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (9, 2, 9, 9, 9, 9, 9, 9) ON CONFLICT (id2) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id3) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (9, 9, 9, 4, 9, 9, 9, 9) ON CONFLICT (id4) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (9, 9, 9, 9, 5, 9, 9, 9) ON CONFLICT (id5) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id2, id3) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id3, id4) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (9, 9, 9, 4, 5, 9, 9, 9) ON CONFLICT (id4, id5) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id5, id6) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (9, 9, 9, 9, 9, 6, 7, 9) ON CONFLICT (id6, id7) DO NOTHING;
INSERT INTO o_test_ioc4 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id3, id4, id5) DO NOTHING;

SELECT * FROM o_test_ioc4;

CREATE TABLE o_test_ioc5
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	id3 int8 NOT NULL,
	id4 int8 NOT NULL,
	id5 int8 NOT NULL,
	id6 int8 NOT NULL,
	id7 int8 NOT NULL,
	id8 int8 NOT NULL,
	PRIMARY KEY(id1)
) USING orioledb;
CREATE UNIQUE INDEX o_test_ioc5_uniq1 on o_test_ioc5(id4);
CREATE UNIQUE INDEX o_test_ioc5_uniq2 on o_test_ioc5(id4);
CREATE UNIQUE INDEX o_test_ioc5_uniq3 on o_test_ioc5(id5);
CREATE UNIQUE INDEX o_test_ioc5_uniq4 on o_test_ioc5(id4, id5);
CREATE UNIQUE INDEX o_test_ioc5_uniq5 on o_test_ioc5(id6, id7);
CREATE INDEX o_test_ioc5_reg1 on o_test_ioc5(id3);
CREATE UNIQUE INDEX o_test_ioc5_uniq6 on o_test_ioc5(id2);

INSERT INTO o_test_ioc5 VALUES (1, 2, 3, 4, 5, 6, 7, 8);

INSERT INTO o_test_ioc5 VALUES (1, 9, 9, 9, 9, 9, 9, 9) ON CONFLICT (id1) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 2, 9, 9, 9, 9, 9, 9) ON CONFLICT (id2) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id3) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 9, 9, 4, 9, 9, 9, 9) ON CONFLICT (id4) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 9, 9, 9, 5, 9, 9, 9) ON CONFLICT (id5) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id2, id3) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id3, id4) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 9, 9, 4, 5, 9, 9, 9) ON CONFLICT (id4, id5) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 9, 9, 4, 9, 9, 9, 9) ON CONFLICT (id4, id5) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 9, 9, 9, 5, 9, 9, 9) ON CONFLICT (id4, id5) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id5, id6) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (9, 9, 9, 9, 9, 6, 7, 9) ON CONFLICT (id6, id7) DO NOTHING;
INSERT INTO o_test_ioc5 VALUES (1, 2, 3, 4, 5, 6, 7, 8) ON CONFLICT (id3, id4, id5) DO NOTHING;

SELECT * FROM o_test_ioc5;

---
-- UPDATE SET
---
CREATE TABLE o_test_ioc6
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	id3 int8 NOT NULL,
	id4 int8 NOT NULL,
	id5 int8 NOT NULL,
	id6 int8 NOT NULL,
	id7 int8 NOT NULL,
	id8 int8 NOT NULL,
	PRIMARY KEY(id1)
) USING orioledb;
CREATE UNIQUE INDEX o_test_ioc6_uniq1 on o_test_ioc6(id4);
CREATE UNIQUE INDEX o_test_ioc6_uniq2 on o_test_ioc6(id4);
CREATE UNIQUE INDEX o_test_ioc6_uniq3 on o_test_ioc6(id5);
CREATE UNIQUE INDEX o_test_ioc6_uniq4 on o_test_ioc6(id4, id5);
CREATE UNIQUE INDEX o_test_ioc6_uniq5 on o_test_ioc6(id6, id7);
CREATE INDEX o_test_ioc6_reg1 on o_test_ioc6(id3);
CREATE UNIQUE INDEX o_test_ioc6_uniq6 on o_test_ioc6(id2);
INSERT INTO o_test_ioc6 VALUES (1, 2, 3, 4, 5, 6, 7, 8);

INSERT INTO o_test_ioc6 VALUES (1, 9, 9, 9, 9, 9, 9, 9)
ON CONFLICT (id1) DO UPDATE SET
   id1 = 10
RETURNING *;

INSERT INTO o_test_ioc6 VALUES (9, 2, 9, 9, 9, 9, 9, 9)
ON CONFLICT (id2) DO UPDATE SET
   id2 = 10
RETURNING *;

INSERT INTO o_test_ioc6 VALUES (9, 9, 9, 4, 9, 9, 9, 9)
ON CONFLICT (id4) DO UPDATE SET
   id4 = 10
RETURNING *;

INSERT INTO o_test_ioc6 VALUES (9, 9, 9, 9, 5, 9, 9, 9)
ON CONFLICT (id5) DO UPDATE SET
   id5 = 10
RETURNING *;

INSERT INTO o_test_ioc6 VALUES (9, 9, 9, 10, 10, 9, 9, 9)
ON CONFLICT (id4, id5) DO UPDATE SET
   id4 = 11, id5 = 11
RETURNING *;

INSERT INTO o_test_ioc6 VALUES (9, 9, 9, 9, 9, 6, 7, 9)
ON CONFLICT (id6, id7) DO UPDATE SET
   id6 = 10, id7 = 10
RETURNING *;

SELECT * FROM o_test_ioc6;

INSERT INTO o_test_ioc6 VALUES (10, 10, 10, 10, 10, 10, 10, 10)
ON CONFLICT (id1) DO UPDATE SET
       id1 = EXCLUDED.id1 - 100,
       id2 = EXCLUDED.id2 - 100,
       id3 = EXCLUDED.id3 - 100,
       id4 = EXCLUDED.id4 - 100,
       id5 = o_test_ioc6.id5 + 100,
       id6 = o_test_ioc6.id6 + 100,
       id7 = o_test_ioc6.id7 + 100,
       id8 = o_test_ioc6.id8 + 100
RETURNING *;

SELECT * FROM o_test_ioc6;

---
-- UPDATE SET WHERE
---
INSERT INTO o_test_ioc6 VALUES (-90, 1, 1, 1, 1, 1, 1, 1)
ON CONFLICT (id1) DO UPDATE SET
       id1 = 1,
       id5 = 112
WHERE o_test_ioc6.id5 <> 111
RETURNING *;

SELECT * FROM o_test_ioc6;

INSERT INTO o_test_ioc6 VALUES (-90, 1, 1, 1, 1, 1, 1, 1)
ON CONFLICT (id1) DO UPDATE SET
       id1 = 1,
       id5 = 112
WHERE o_test_ioc6.id5 = 111
RETURNING *;

SELECT * FROM o_test_ioc6;

CREATE TABLE o_test_ioc_change_pkey (
	val_1 int,
	val_2 int,
	PRIMARY KEY(val_1)
) USING orioledb;
INSERT INTO o_test_ioc_change_pkey VALUES (1, 2), (2, 3);
TABLE o_test_ioc_change_pkey;

BEGIN;
SELECT orioledb_tbl_structure('o_test_ioc_change_pkey'::regclass, 'ne');
UPDATE o_test_ioc_change_pkey SET val_1 = 13 WHERE val_1 = 1;
TABLE o_test_ioc_change_pkey;
SELECT orioledb_tbl_structure('o_test_ioc_change_pkey'::regclass, 'ne');
INSERT INTO o_test_ioc_change_pkey VALUES (1, 15) ON CONFLICT (val_1)
	DO UPDATE SET val_1 = 3 RETURNING *;
COMMIT;
TABLE o_test_ioc_change_pkey;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ioc CASCADE;
RESET search_path;

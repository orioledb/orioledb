CREATE SCHEMA getsomeattrs;
SET SESSION search_path = 'getsomeattrs';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_0
(
	id1 text NOT NULL,
	id2 float8 NOT NULL,
	PRIMARY KEY (id2, id1)
) USING orioledb;
CREATE UNIQUE INDEX o_test_0_idx ON o_test_0 (id1 DESC, id2 DESC);
INSERT INTO o_test_0
	SELECT repeat('x', a % 3) || repeat ('y', a % 2), a
		FROM generate_series(1, 5) as a;
EXPLAIN (COSTS off) SELECT * FROM o_test_0 ORDER BY id2, id1 LIMIT 5;
SELECT * FROM o_test_0 ORDER BY id2, id1 LIMIT 5;
EXPLAIN (COSTS off) SELECT id2, id1, id2 FROM o_test_0 ORDER BY id2, id1 LIMIT 5;
SELECT id2, id1, id2 FROM o_test_0 ORDER BY id2, id1 LIMIT 5;
EXPLAIN (COSTS off) SELECT * FROM o_test_0 ORDER BY id1, id2;
SELECT * FROM o_test_0 ORDER BY id1, id2;

CREATE TABLE o_test_1
(
	id1 float8 NOT NULL,
	id2 text NOT NULL,
	PRIMARY KEY(id1, id2)
) USING orioledb;
INSERT INTO o_test_1
	SELECT a, repeat('x', a % 3) || repeat ('y', a % 2)
		FROM generate_series(1, 5) as a;

EXPLAIN (COSTS off) SELECT * FROM o_test_1 ORDER BY id1, id2 LIMIT 5;
SELECT * FROM o_test_1 ORDER BY id1, id2 LIMIT 5;

CREATE TABLE o_test_2 (
	val_1 int2,
	val_2 int4,
	val_3 int8,
	PRIMARY KEY (val_3, val_2)
) USING orioledb;
ALTER TABLE o_test_2 DROP COLUMN val_1;
INSERT INTO o_test_2 VALUES (1, 2), (3, 4);
EXPLAIN (COSTS off) SELECT * FROM o_test_2 ORDER BY val_3, val_2;
SELECT * FROM o_test_2 ORDER BY val_3, val_2;

CREATE TABLE o_test_3 (
	val_1 int,
	val_2 int,
	PRIMARY KEY (val_2, val_1)
) USING orioledb;
INSERT INTO o_test_3 VALUES (1, 2), (3, 4);
EXPLAIN (COSTS off) SELECT * FROM o_test_3 ORDER BY val_2;
SELECT * FROM o_test_3 ORDER BY val_2;

CREATE TABLE o_test_4 (
	id integer NOT NULL,
	val integer,
	PRIMARY KEY(id)
) USING orioledb;
INSERT INTO o_test_4 (id, val) VALUES (1, 2);
INSERT INTO o_test_4 (id, val) VALUES (3, 4);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_4;
SELECT * FROM o_test_4;
DELETE FROM o_test_4 where val = 2;
SELECT * FROM o_test_4;

CREATE TABLE o_test_4a (
	id int2 NOT NULL,
	val int4,
	PRIMARY KEY(id)
) USING orioledb;
INSERT INTO o_test_4a (id, val) VALUES (1, 2);
INSERT INTO o_test_4a (id, val) VALUES (3, 4);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_4a;
SELECT * FROM o_test_4a;
DELETE FROM o_test_4a where val = 2;
SELECT * FROM o_test_4a;

CREATE TABLE o_test_5 (
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id),
	UNIQUE(id, val)
) USING orioledb;
INSERT INTO o_test_5(id, val) VALUES (1, 'hello');
INSERT INTO o_test_5(id, val) VALUES (2, 'hey');
EXPLAIN (COSTS off) DELETE FROM o_test_5 where id = 1;
DELETE FROM o_test_5 where id = 1;

CREATE TABLE o_test_6
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	PRIMARY KEY(id1)
) USING orioledb;
CREATE UNIQUE INDEX o_test_6_uniq6 on o_test_6(id2);
INSERT INTO o_test_6 VALUES (1, 2);
EXPLAIN (COSTS off)
	INSERT INTO o_test_6 VALUES (1, 9)
		ON CONFLICT (id1) DO UPDATE SET id1 = 10 RETURNING *;
INSERT INTO o_test_6 VALUES (1, 9)
	ON CONFLICT (id1) DO UPDATE SET id1 = 10 RETURNING *;
EXPLAIN (COSTS off)
	INSERT INTO o_test_6 VALUES (9, 2)
		ON CONFLICT (id2) DO UPDATE SET id2 = 10 RETURNING *;
INSERT INTO o_test_6 VALUES (9, 2)
	ON CONFLICT (id2) DO UPDATE SET id2 = 10 RETURNING *;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
	SELECT substr(sha256(($1::text || ' ' || $2::text)::bytea)::text,2,16)::bit(52)::bigint::float8 / pow(2.0, 52.0);
$$ LANGUAGE sql;

CREATE TABLE o_test_7
(
	id bigserial,
	id2 bigserial,
	i int,
	PRIMARY KEY (id, id2)
) USING orioledb;
ALTER SEQUENCE o_test_7_id_seq RESTART WITH 100000;
ALTER SEQUENCE o_test_7_id2_seq RESTART WITH 200000;
INSERT INTO o_test_7 (i)
	SELECT pseudo_random(7, v) * 20000 FROM generate_series(1,5) v;
CREATE INDEX o_test_7_ix1 ON o_test_7 (i);
SELECT * FROM o_test_7;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_7 ORDER BY i;
SELECT * FROM o_test_7 ORDER BY i;
COMMIT;

CREATE TABLE o_test_8
(
	id1 text NOT NULL,
	id2 float8 NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_test_8_idx1 ON o_test_8 (id2, id1);
CREATE UNIQUE INDEX o_test_8_idx2 ON o_test_8 (id1 DESC, id2 DESC);
INSERT INTO o_test_8
	SELECT repeat('x', a % 3) || repeat ('y', a % 2), a
		FROM generate_series(1, 5) as a;
EXPLAIN (COSTS off) SELECT * FROM o_test_8 ORDER BY id2, id1 LIMIT 5;
SELECT * FROM o_test_8 ORDER BY id2, id1 LIMIT 5;
EXPLAIN (COSTS off) SELECT id2, id1, id2 FROM o_test_8 ORDER BY id2, id1 LIMIT 5;
SELECT id2, id1, id2 FROM o_test_8 ORDER BY id2, id1 LIMIT 5;
EXPLAIN (COSTS off) SELECT * FROM o_test_8 ORDER BY id1, id2;
SELECT * FROM o_test_8 ORDER BY id1, id2;

CREATE TABLE o_test_9
(
	id1 float8 NOT NULL,
	id2 text NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_test_9_idx1 ON o_test_9 (id1, id2);
INSERT INTO o_test_9
	SELECT a, repeat('x', a % 3) || repeat ('y', a % 2)
		FROM generate_series(1, 5) as a;

EXPLAIN (COSTS off) SELECT * FROM o_test_9 ORDER BY id1, id2 LIMIT 5;
SELECT * FROM o_test_9 ORDER BY id1, id2 LIMIT 5;

CREATE TABLE o_test_10 (
	val_1 int,
	val_2 int,
	val_3 int
) USING orioledb;
CREATE UNIQUE INDEX o_test_10_idx1 ON o_test_10 (val_3, val_2);
ALTER TABLE o_test_10 DROP COLUMN val_1;
INSERT INTO o_test_10 VALUES (1, 2), (3, 4);
EXPLAIN (COSTS off) SELECT * FROM o_test_10 ORDER BY val_3, val_2;
SELECT * FROM o_test_10 ORDER BY val_3, val_2;

CREATE TABLE o_test_11 (
	val_1 int,
	val_2 int
) USING orioledb;
CREATE UNIQUE INDEX o_test_11_idx1 ON o_test_11 (val_2, val_1);
INSERT INTO o_test_11 VALUES (1, 2), (3, 4);
EXPLAIN (COSTS off) SELECT * FROM o_test_11 ORDER BY val_2;
SELECT * FROM o_test_11 ORDER BY val_2;

CREATE TABLE o_test_12 (
	id integer NOT NULL,
	val integer
) USING orioledb;
CREATE UNIQUE INDEX o_test_12_idx1 ON o_test_12 (id);
INSERT INTO o_test_12 (id, val) VALUES (1, 2);
INSERT INTO o_test_12 (id, val) VALUES (3, 4);
SELECT * FROM o_test_12;
DELETE FROM o_test_12 where val = 2;
SELECT * FROM o_test_12;

CREATE TABLE o_test_13 (
	id integer NOT NULL,
	val text NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_test_13_idx1 ON o_test_13 (id);
CREATE UNIQUE INDEX o_test_13_idx2 ON o_test_13 (id, val);
INSERT INTO o_test_13(id, val) VALUES (1, 'hello');
INSERT INTO o_test_13(id, val) VALUES (2, 'hey');
EXPLAIN (COSTS off) DELETE FROM o_test_13 where id = 1;
DELETE FROM o_test_13 where id = 1;

CREATE TABLE o_test_14
(
	id1 int8 NOT NULL,
	id2 int8 NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_test_14_idx1 ON o_test_14 (id1);
CREATE UNIQUE INDEX o_test_14_uniq6 on o_test_14(id2);
INSERT INTO o_test_14 VALUES (1, 2);
EXPLAIN (COSTS off)
	INSERT INTO o_test_14 VALUES (1, 9)
		ON CONFLICT (id1) DO UPDATE SET id1 = 10 RETURNING *;
INSERT INTO o_test_14 VALUES (1, 9)
	ON CONFLICT (id1) DO UPDATE SET id1 = 10 RETURNING *;
EXPLAIN (COSTS off)
	INSERT INTO o_test_14 VALUES (9, 2)
		ON CONFLICT (id2) DO UPDATE SET id2 = 10 RETURNING *;
INSERT INTO o_test_14 VALUES (9, 2)
	ON CONFLICT (id2) DO UPDATE SET id2 = 10 RETURNING *;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
	SELECT substr(sha256(($1::text || ' ' || $2::text)::bytea)::text,2,16)::bit(52)::bigint::float8 / pow(2.0, 52.0);
$$ LANGUAGE sql;

CREATE TABLE o_test_15
(
	id bigserial,
	id2 bigserial,
	i int
) USING orioledb;
CREATE UNIQUE INDEX o_test_15_idx1 ON o_test_15 (id, id2);
ALTER SEQUENCE o_test_15_id_seq RESTART WITH 100000;
ALTER SEQUENCE o_test_15_id2_seq RESTART WITH 200000;
INSERT INTO o_test_15 (i)
	SELECT pseudo_random(7, v) * 20000 FROM generate_series(1,5) v;
CREATE INDEX o_test_15_ix1 ON o_test_15 (i);
SELECT * FROM o_test_15;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_15 ORDER BY i;
SELECT * FROM o_test_15 ORDER BY i;
COMMIT;

CREATE TABLE o_test_16
(
	id serial primary key,
	i int4
) USING orioledb;
CREATE SEQUENCE o_test_16_j_seq;

INSERT INTO o_test_16 (i) SELECT generate_series(1,2);
ALTER TABLE o_test_16 ADD COLUMN y int4;
SELECT * FROM o_test_16;
ALTER TABLE o_test_16
	ADD COLUMN j int4 not null default nextval('o_test_16_j_seq');
SELECT * FROM o_test_16;

CREATE TABLE o_test_17 (
	val_1 int,
	val_1a int,
	val_2 int,
	val_3 int,
	PRIMARY KEY(val_1a, val_1) INCLUDE(val_3, val_2)
) USING orioledb;

INSERT INTO o_test_17 SELECT v, v * 10, NULL, NULL FROM generate_series(1,2) v;
SELECT * FROM o_test_17;
SELECT * FROM o_test_17 WHERE (val_1) < (2);

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA getsomeattrs CASCADE;
RESET search_path;

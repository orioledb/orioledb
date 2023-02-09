CREATE SCHEMA primary_key;
SET SESSION search_path = 'primary_key';
CREATE EXTENSION orioledb;

-- Test for integer primary key
CREATE TABLE o_pk1
(
	key integer NOT NULL,
	payload text,
	PRIMARY KEY(key)
) USING orioledb;

INSERT INTO o_pk1 (key, payload)
SELECT i, '*' || i || repeat('*', i % 5) FROM generate_series(1, 60, 3) AS i;

SELECT * FROM o_pk1;

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = 1;
SELECT * FROM o_pk1 WHERE key = 1;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = 4;
SELECT * FROM o_pk1 WHERE key = 4;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = 20;
SELECT * FROM o_pk1 WHERE key = 20;
EXPLAIN (COSTS off) SELECT payload FROM o_pk1 WHERE key = 19;
SELECT payload FROM o_pk1 WHERE key = 19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key IN (1, 4, 13, 20);
SELECT * FROM o_pk1 WHERE key IN (1, 4, 13, 20);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = 1 OR key = 4 OR key = 13 OR key = 20;
SELECT * FROM o_pk1 WHERE key = 1 OR key = 4 OR key = 13 OR key = 20;

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 AND key >  19;
SELECT * FROM o_pk1 WHERE key >  10 AND key >  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 AND key >  19;
SELECT * FROM o_pk1 WHERE key >= 10 AND key >  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 AND key >= 19;
SELECT * FROM o_pk1 WHERE key >  10 AND key >= 19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 AND key >= 19;
SELECT * FROM o_pk1 WHERE key >= 10 AND key >= 19;

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 AND key <  19;
SELECT * FROM o_pk1 WHERE key >  10 AND key <  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 AND key <  19;
SELECT * FROM o_pk1 WHERE key >= 10 AND key <  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 AND key <= 19;
SELECT * FROM o_pk1 WHERE key >  10 AND key <= 19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 AND key <= 19;
SELECT * FROM o_pk1 WHERE key >= 10 AND key <= 19;

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 AND key >  19;
SELECT * FROM o_pk1 WHERE key <  10 AND key >  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 AND key >  19;
SELECT * FROM o_pk1 WHERE key <= 10 AND key >  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 AND key >= 19;
SELECT * FROM o_pk1 WHERE key <  10 AND key >= 19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 AND key >= 19;
SELECT * FROM o_pk1 WHERE key <= 10 AND key >= 19;

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 AND key <  19;
SELECT * FROM o_pk1 WHERE key <  10 AND key <  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 AND key <  19;
SELECT * FROM o_pk1 WHERE key <= 10 AND key <  19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 AND key <= 19;
SELECT * FROM o_pk1 WHERE key <  10 AND key <= 19;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 AND key <= 19;
SELECT * FROM o_pk1 WHERE key <= 10 AND key <= 19;

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 OR  key >  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >  10 OR  key >  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 OR  key >  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >= 10 OR  key >  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 OR  key >= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >  10 OR  key >= 19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 OR  key >= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >= 10 OR  key >= 19 AND payload LIKE '%7*%';

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 OR  key <  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >  10 OR  key <  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 OR  key <  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >= 10 OR  key <  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >  10 OR  key <= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >  10 OR  key <= 19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key >= 10 OR  key <= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key >= 10 OR  key <= 19 AND payload LIKE '%7*%';

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 OR  key >  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <  10 OR  key >  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 OR  key >  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <= 10 OR  key >  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 OR  key >= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <  10 OR  key >= 19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 OR  key >= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <= 10 OR  key >= 19 AND payload LIKE '%7*%';

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 OR  key <  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <  10 OR  key <  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 OR  key <  19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <= 10 OR  key <  19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <  10 OR  key <= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <  10 OR  key <= 19 AND payload LIKE '%7*%';
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10 OR  key <= 19 AND payload LIKE '%7*%';
SELECT * FROM o_pk1 WHERE key <= 10 OR  key <= 19 AND payload LIKE '%7*%';

EXPLAIN (COSTS off) SELECT * FROM o_pk1
WHERE key >  10 AND key <  19 AND key = (SELECT i FROM generate_series(13,13) AS i);
SELECT * FROM o_pk1
WHERE key >  10 AND key <  19 AND key = (SELECT i FROM generate_series(13,13) AS i);
EXPLAIN (COSTS off) SELECT * FROM o_pk1
WHERE key <  10 OR  key >  19 OR key = (SELECT i FROM generate_series(13,13) AS i);
SELECT * FROM o_pk1
WHERE key <  10 OR  key >  19 OR key = (SELECT i FROM generate_series(13,13) AS i);

EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = ANY (ARRAY[4]);
SELECT * FROM o_pk1 WHERE key = ANY (ARRAY[4]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = ANY (ARRAY[4,10]);
SELECT * FROM o_pk1 WHERE key = ANY (ARRAY[4,10]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = ANY (ARRAY[4,10, NULL]);
SELECT * FROM o_pk1 WHERE key = ANY (ARRAY[4,10, NULL]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = ALL (ARRAY[4]);
SELECT * FROM o_pk1 WHERE key = ALL (ARRAY[4]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = ALL (ARRAY[4,10]);
SELECT * FROM o_pk1 WHERE key = ALL (ARRAY[4,10]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = ALL (ARRAY[4,10, NULL]);
SELECT * FROM o_pk1 WHERE key = ALL (ARRAY[4,10, NULL]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key < ANY (ARRAY[4]);
SELECT * FROM o_pk1 WHERE key < ANY (ARRAY[4]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key < ANY (ARRAY[4,10]);
SELECT * FROM o_pk1 WHERE key < ANY (ARRAY[4,10]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key < ANY (ARRAY[4,10, NULL]);
SELECT * FROM o_pk1 WHERE key < ANY (ARRAY[4,10, NULL]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key < ALL (ARRAY[4]);
SELECT * FROM o_pk1 WHERE key < ALL (ARRAY[4]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key < ALL (ARRAY[4,10]);
SELECT * FROM o_pk1 WHERE key < ALL (ARRAY[4,10]);
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key < ALL (ARRAY[4,5, NULL]);
SELECT * FROM o_pk1 WHERE key < ALL (ARRAY[4,5, NULL]);

-- Check for non-coersible case
SET enable_seqscan = OFF;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key = 1.0;
SELECT * FROM o_pk1 WHERE key = 1.0;
EXPLAIN (COSTS off) SELECT * FROM o_pk1 WHERE key <= 10.0;
SELECT * FROM o_pk1 WHERE key <= 10.0;
RESET enable_seqscan;

-- Test for float8 primary key
CREATE TABLE o_pk2
(
	value text,
	id float8 NOT NULL,
	PRIMARY KEY(id)
) USING orioledb;

INSERT INTO o_pk2
SELECT 'abc' || repeat('def', i % 3), i FROM generate_series(1, 100) AS i;
ANALYZE o_pk2;
SELECT id, value FROM o_pk2;
SELECT count(*) FROM o_pk2;

SET enable_seqscan = off;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 ORDER BY id;
SELECT id, value FROM o_pk2 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 ORDER BY id DESC;
SELECT id, value FROM o_pk2 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id = 71;
SELECT id, value FROM o_pk2 WHERE id = 71;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id = 71 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id = 71 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id = 71 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id = 71 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71;
SELECT id, value FROM o_pk2 WHERE id <= 71;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id <= 71 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id <= 71 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71;
SELECT id, value FROM o_pk2 WHERE id >= 71;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id >= 71 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id >= 71 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 AND id > 19;
SELECT id, value FROM o_pk2 WHERE id <= 71 AND id > 19;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 AND id > 19 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id <= 71 AND id > 19 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 AND id > 19 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id <= 71 AND id > 19 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 AND id < 91;
SELECT id, value FROM o_pk2 WHERE id >= 71 AND id < 91;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 AND id < 91 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id >= 71 AND id < 91 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 AND id < 91 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id >= 71 AND id < 91 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 OR id > 91;
SELECT id, value FROM o_pk2 WHERE id <= 71 OR id > 91;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 OR id > 91 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id <= 71 OR id > 91 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id <= 71 OR id > 91 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id <= 71 OR id > 91 ORDER BY id DESC;

EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 OR id < 19;
SELECT id, value FROM o_pk2 WHERE id >= 71 OR id < 19;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 OR id < 19 ORDER BY id;
SELECT id, value FROM o_pk2 WHERE id >= 71 OR id < 19 ORDER BY id;
EXPLAIN (COSTS off) SELECT id, value FROM o_pk2 WHERE id >= 71 OR id < 19 ORDER BY id DESC;
SELECT id, value FROM o_pk2 WHERE id >= 71 OR id < 19 ORDER BY id DESC;

RESET enable_seqscan;

-- Test for composite primary key

CREATE TABLE o_pk3
(
	id1 float8 NOT NULL,
	id2 text NOT NULL,
	PRIMARY KEY(id1, id2)
) USING orioledb;
SET enable_seqscan = off;

INSERT INTO o_pk3
SELECT a, repeat('x', b) || repeat ('y', c)
FROM
	generate_series(1, 4) as a,
	generate_series(1, 4) as b,
	generate_series(1, 4) as c;
SELECT count(*) FROM o_pk3;
SELECT id1, id2 FROM o_pk3;

\i 'sql/composite_pk_quals'

DROP TABLE o_pk3;
RESET enable_seqscan;

CREATE TABLE o_pk3
(
	id2 text NOT NULL,
	id1 float8 NOT NULL,
	PRIMARY KEY(id1, id2)
) USING orioledb;
SET enable_seqscan = off;

INSERT INTO o_pk3
SELECT a, repeat('x', b) || repeat ('y', c)
FROM
	generate_series(1, 4) as a,
	generate_series(1, 4) as b,
	generate_series(1, 4) as c;
SELECT count(*) FROM o_pk3;
SELECT id1, id2 FROM o_pk3;

\i 'sql/composite_pk_quals'

DROP TABLE o_pk3;
RESET enable_seqscan;

CREATE TABLE o_pk3
(
	id1 float8 NOT NULL,
	id2 text NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_pk3_idx ON o_pk3 (id1, id2 DESC);
SET enable_seqscan = off;

INSERT INTO o_pk3
SELECT a, repeat('x', b) || repeat ('y', c)
FROM
	generate_series(1, 4) as a,
	generate_series(1, 4) as b,
	generate_series(1, 4) as c;
SELECT count(*) FROM o_pk3;
SELECT id1, id2 FROM o_pk3;

\i 'sql/composite_pk_quals'

DROP TABLE o_pk3;
RESET enable_seqscan;

CREATE TABLE o_pk3
(
	id2 text NOT NULL,
	id1 float8 NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_pk3_idx ON o_pk3 (id1 DESC, id2 DESC);
SET enable_seqscan = off;

INSERT INTO o_pk3
SELECT a, repeat('x', b) || repeat ('y', c)
FROM
	generate_series(1, 4) as a,
	generate_series(1, 4) as b,
	generate_series(1, 4) as c;
SELECT count(*) FROM o_pk3;
SELECT id1, id2 FROM o_pk3;

\i 'sql/composite_pk_quals'

DROP TABLE o_pk3;
RESET enable_seqscan;

-- Check for key length limits
CREATE TABLE o_pk4
(
	idx integer NOT NULL,
	txt text NOT NULL,
	PRIMARY KEY (txt)
) USING orioledb;
INSERT INTO o_test1 VALUES (1, repeat('x', 2673));
INSERT INTO o_test1 VALUES (2, repeat('x', 2680));
INSERT INTO o_test1 VALUES (3, repeat('x', 2672));
SELECT * FROM o_pk4;

-- Check for datetime types
CREATE TABLE o_pk5 (
	i int4 NOT NULL,
	dt date NOT NULL,
	PRIMARY KEY (i, dt)
) USING orioledb;

INSERT INTO o_pk5
SELECT i % 5, '2021-01-01'::date + interval '1 month' * (i / 5) FROM generate_series(0, 24) i;
SET enable_seqscan = OFF;
EXPLAIN (COSTS off) SELECT * FROM o_pk5 WHERE i = 2 AND dt >= '2021-03-01'::timestamp;
SELECT * FROM o_pk5 WHERE i = 2 AND dt >= '2021-03-01'::timestamp;
EXPLAIN (COSTS off) SELECT * FROM o_pk5 WHERE i = 2 AND dt >= '2021-03-01'::date;
SELECT * FROM o_pk5 WHERE i = 2 AND dt >= '2021-03-01'::date;
RESET enable_seqscan;

CREATE TABLE o_pk6 (
	i int4 NOT NULL,
	dt timestamp NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_pk6_idx ON o_pk6 (dt DESC, i DESC);

INSERT INTO o_pk6
SELECT i % 5, '2021-01-01'::date + interval '1 month' * (i / 5) FROM generate_series(0, 24) i;
SET enable_seqscan = OFF;
EXPLAIN (COSTS off) SELECT * FROM o_pk6 WHERE i <= 2 AND dt = '2021-03-01'::timestamp;
SELECT * FROM o_pk6 WHERE i = 2 AND dt >= '2021-03-01'::timestamp;
EXPLAIN (COSTS off) SELECT * FROM o_pk6 WHERE i <= 2 AND dt = '2021-03-01'::date;
SELECT * FROM o_pk6 WHERE i = 2 AND dt >= '2021-03-01'::date;
RESET enable_seqscan;

-- Test null clauses in index scan
create table o_test_null_clauses (
	id bigint PRIMARY KEY,
	val int
) USING orioledb;
CREATE INDEX ON o_test_null_clauses(val);
INSERT INTO o_test_null_clauses
	SELECT id, id + 2 val FROM generate_series(1, 10) id;
INSERT INTO o_test_null_clauses VALUES (11, NULL), (12, NULL);
SET enable_bitmapscan = off;
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses ORDER BY val NULLS FIRST;
SELECT * FROM o_test_null_clauses ORDER BY val NULLS FIRST;
-- Test primary index scan with null clause
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NULL ORDER BY val;
SELECT * FROM o_test_null_clauses WHERE val IS NULL ORDER BY val;
-- Test primary index scan with not null clause
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL ORDER BY val;
SELECT * FROM o_test_null_clauses WHERE val IS NOT NULL ORDER BY val;
-- Test primary index scan with not null and another clause
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val;
SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val;
-- Test primary index scan with not null and another clause and nullsfirst
EXPLAIN (COSTS off) SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val NULLS FIRST;
SELECT * FROM o_test_null_clauses
	WHERE val IS NOT NULL AND val > 5 ORDER BY val NULLS FIRST;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- Test primary index scan with null array clause
EXPLAIN (COSTS off) select id from o_test_null_clauses
	WHERE id = ANY(NULL::int4[]);
select id from o_test_null_clauses WHERE id = ANY(NULL::int4[]);
-- Test nested primary index scan with not null clause
EXPLAIN (COSTS off) select max(id) from o_test_null_clauses;
select max(id) from o_test_null_clauses;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA primary_key CASCADE;
RESET search_path;

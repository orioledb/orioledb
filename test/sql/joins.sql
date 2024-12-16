CREATE SCHEMA joins;
SET SESSION search_path = 'joins';
CREATE EXTENSION orioledb;

CREATE TABLE o_joins1
(
	id1 float8 NOT NULL,
	id2 text NOT NULL,
	PRIMARY KEY(id1, id2)
) USING orioledb;
CREATE TABLE o_joins2
(
	id1 float8 NOT NULL,
	id2 text NOT NULL,
	PRIMARY KEY(id1, id2)
) USING orioledb;

INSERT INTO o_joins1
SELECT a, repeat('x', b) || repeat ('z', c)
FROM
	generate_series(1, 4) as a,
	generate_series(1, 4) as b,
	generate_series(1, 4) as c;
INSERT INTO o_joins2
SELECT a, repeat('x', b) || repeat ('y', c)
FROM
	generate_series(1, 4) as a,
	generate_series(1, 4) as b,
	generate_series(1, 4) as c;

SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 LEFT JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 LEFT JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 FULL JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
SET enable_nestloop = off;
RESET enable_hashjoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 LEFT JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 LEFT JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 FULL JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
SET enable_hashjoin = off;
RESET enable_mergejoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 LEFT JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 LEFT JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
EXPLAIN (COSTS off) SELECT * FROM o_joins2 FULL JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 RIGHT JOIN o_joins1 USING(id1, id2);
RESET enable_nestloop;
RESET enable_hashjoin;

DROP TABLE o_joins1;
CREATE TABLE o_joins1
(
	id2 text NOT NULL,
	id1 float8 NOT NULL,
	PRIMARY KEY (id1, id2)
) USING orioledb;
INSERT INTO o_joins1 (SELECT id2, id1 FROM o_joins2);

SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SET enable_nestloop = off;
RESET enable_hashjoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SET enable_hashjoin = off;
RESET enable_mergejoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
RESET enable_nestloop;
RESET enable_hashjoin;

DROP TABLE o_joins1;
CREATE TABLE o_joins1
(
	id1 float8 NOT NULL,
	id2 text NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_joins1_idx ON o_joins1 (id1, id2 DESC);
INSERT INTO o_joins1 (SELECT id1, id2 FROM o_joins2);

SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SET enable_nestloop = off;
RESET enable_hashjoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SET enable_hashjoin = off;
RESET enable_mergejoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
RESET enable_nestloop;
RESET enable_hashjoin;

DROP TABLE o_joins1;
CREATE TABLE o_joins1
(
	id2 text NOT NULL,
	id1 float8 NOT NULL
) USING orioledb;
CREATE UNIQUE INDEX o_joins1_idx ON o_joins1 (id1 DESC, id2 DESC);
INSERT INTO o_joins1 (SELECT id2, id1 FROM o_joins2);

SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SET enable_nestloop = off;
RESET enable_hashjoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SET enable_hashjoin = off;
RESET enable_mergejoin;
EXPLAIN (COSTS off) SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
SELECT * FROM o_joins2 JOIN o_joins1 USING(id1, id2);
RESET enable_nestloop;
RESET enable_hashjoin;

DROP TABLE o_joins2;
DROP TABLE o_joins1;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA joins CASCADE;
RESET search_path;


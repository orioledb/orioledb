-- A primary key column named in an INCLUDE list is stored once, in the
-- deduplicated PK segment, and remains available to index-only scans.
CREATE SCHEMA include_pk;
SET SESSION search_path = 'include_pk';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_include_pk
(
	id text PRIMARY KEY,
	grp text,
	created_at timestamptz,
	ttl bigint,
	deleted bool NOT NULL DEFAULT false
) USING orioledb;
CREATE INDEX o_test_include_pk_ix ON o_test_include_pk (grp, created_at)
	INCLUDE (id, ttl) WHERE NOT deleted AND ttl IS NOT NULL;

INSERT INTO o_test_include_pk VALUES ('a', 'g-0', '2026-01-01 00:00:00+00', 3600);
INSERT INTO o_test_include_pk VALUES ('b', 'g-0', '2026-01-01 00:00:00+00', 3600);
INSERT INTO o_test_include_pk VALUES ('c', 'g-0', '2026-01-01 00:00:00+00', 3600)
	ON CONFLICT (id) DO NOTHING;
INSERT INTO o_test_include_pk VALUES ('d', 'g-1', '2026-01-01 00:00:00+00', NULL);

SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
	SELECT id FROM o_test_include_pk
		WHERE NOT deleted AND ttl IS NOT NULL ORDER BY grp, created_at, id;
SELECT id, grp FROM o_test_include_pk
	WHERE NOT deleted AND ttl IS NOT NULL ORDER BY grp, created_at, id;
RESET enable_seqscan;
SELECT orioledb_tbl_structure('o_test_include_pk'::regclass, 'ne');

-- leaving the predicate must remove exactly this row's entry
UPDATE o_test_include_pk SET deleted = true WHERE id = 'b';
UPDATE o_test_include_pk SET deleted = true WHERE id = 'a';
-- and coming back must add it again
UPDATE o_test_include_pk SET deleted = false WHERE id = 'b';
SET enable_seqscan = off;
SELECT id, grp FROM o_test_include_pk
	WHERE NOT deleted AND ttl IS NOT NULL ORDER BY grp, created_at, id;
RESET enable_seqscan;
DELETE FROM o_test_include_pk WHERE id = 'c';
SET enable_seqscan = off;
SELECT id, grp FROM o_test_include_pk
	WHERE NOT deleted AND ttl IS NOT NULL ORDER BY grp, created_at, id;
RESET enable_seqscan;
SELECT orioledb_tbl_structure('o_test_include_pk'::regclass, 'ne');

-- non-partial variant, and a UNIQUE index whose INCLUDE list names the key
CREATE TABLE o_test_include_pk2
(
	id int PRIMARY KEY,
	grp int,
	payload text
) USING orioledb;
CREATE INDEX o_test_include_pk2_grp ON o_test_include_pk2 (grp) INCLUDE (id, payload);
CREATE UNIQUE INDEX o_test_include_pk2_payload ON o_test_include_pk2 (payload) INCLUDE (id);
INSERT INTO o_test_include_pk2 SELECT i, i % 3, 'p' || i FROM generate_series(1, 9) i;
SET enable_seqscan = off;
SELECT grp, id FROM o_test_include_pk2 WHERE grp = 1 ORDER BY id;
SELECT id FROM o_test_include_pk2 WHERE payload = 'p5';
RESET enable_seqscan;
UPDATE o_test_include_pk2 SET grp = 1 WHERE id = 6;
DELETE FROM o_test_include_pk2 WHERE id = 4;
SET enable_seqscan = off;
SELECT grp, id FROM o_test_include_pk2 WHERE grp = 1 ORDER BY id;
RESET enable_seqscan;
SELECT orioledb_tbl_structure('o_test_include_pk2'::regclass, 'ne');

-- composite primary key with only one of its columns included
CREATE TABLE o_test_include_pk3
(
	a int,
	b int,
	v int,
	PRIMARY KEY (a, b)
) USING orioledb;
CREATE INDEX o_test_include_pk3_v ON o_test_include_pk3 (v) INCLUDE (b);
INSERT INTO o_test_include_pk3 VALUES (1, 1, 7), (1, 2, 7), (2, 1, 7), (2, 2, 8);
SET enable_seqscan = off;
SELECT a, b FROM o_test_include_pk3 WHERE v = 7 ORDER BY a, b;
RESET enable_seqscan;
DELETE FROM o_test_include_pk3 WHERE a = 1 AND b = 2;
SET enable_seqscan = off;
SELECT a, b FROM o_test_include_pk3 WHERE v = 7 ORDER BY a, b;
RESET enable_seqscan;
SELECT orioledb_tbl_structure('o_test_include_pk3'::regclass, 'ne');

-- composite PK delete correctness without any secondary index
CREATE TABLE o_test_include_pk4
(
	a int,
	b int,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_test_include_pk4 VALUES (1, 1), (1, 2), (2, 1), (2, 2);
SELECT a, b FROM o_test_include_pk4 ORDER BY a, b;
DELETE FROM o_test_include_pk4 WHERE a = 1 AND b = 2;
SELECT a, b FROM o_test_include_pk4 ORDER BY a, b;

-- PK fields can be interleaved with INCLUDE fields of different types.
-- Exercise both secondary-to-primary lookup and ON CONFLICT row locking.
CREATE TABLE o_test_include_pk5
(
	a text,
	b int,
	v int,
	token text,
	note text,
	PRIMARY KEY (a, b)
) USING orioledb;
CREATE INDEX o_test_include_pk5_v ON o_test_include_pk5 (v)
	INCLUDE (b, note);
CREATE UNIQUE INDEX o_test_include_pk5_token ON o_test_include_pk5 (token)
	INCLUDE (b);
INSERT INTO o_test_include_pk5 VALUES
	('one', 1, 7, 'first', repeat('x', 10000)),
	('two', 2, 7, 'second', repeat('y', 10000));
SET enable_seqscan = off;
SELECT a, b, token, length(note) FROM o_test_include_pk5
	WHERE v = 7 ORDER BY a, b;
RESET enable_seqscan;
INSERT INTO o_test_include_pk5 VALUES
	('unused', 9, 9, 'first', 'updated')
	ON CONFLICT (token) DO UPDATE SET note = EXCLUDED.note
	RETURNING a, b, token, note;
DELETE FROM o_test_include_pk5 WHERE a = 'two' AND b = 2;
SET enable_seqscan = off;
SELECT a, b, token, length(note) FROM o_test_include_pk5
	WHERE v = 7 ORDER BY a, b;
RESET enable_seqscan;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA include_pk CASCADE;
RESET search_path;

-- A primary key column named in an INCLUDE list is stored once, in the
-- INCLUDE position, but it must still take part in the index key: two rows
-- that agree on the key columns are two distinct entries.
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

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA include_pk CASCADE;
RESET search_path;

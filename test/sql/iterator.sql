-- Whitebox tests for the orioledb B-tree iterator.  The C helpers exercised
-- here live at the bottom of src/btree/iterator.c and are registered in the
-- 1.8->1.9 dev migration.

CREATE SCHEMA iterator;
SET SESSION search_path = 'iterator';
CREATE EXTENSION orioledb;

-- it->curKeyReturned set BEFORE the end-key bound check in
-- btree_iterate_raw_internal().  orioledb_test_endkey_returned_skip()
-- drives an iterator into the buggy state via an end-bound rejection,
-- forces iterator_refind_partial_leaf() (the partial-read failure path),
-- then drains the iterator with a wide end and returns the PKs as int4[].
-- The bug drops the rejected row from the resumed scan, so the resulting
-- query has one fewer row than expected.
CREATE TABLE o_endkey_skip (id int4 PRIMARY KEY) USING orioledb;
INSERT INTO o_endkey_skip SELECT g FROM generate_series(1, 10) g;

SELECT unnest(orioledb_test_endkey_returned_skip('o_endkey_skip'::regclass, 5)) AS id;

DROP TABLE o_endkey_skip;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA iterator CASCADE;
RESET search_path;

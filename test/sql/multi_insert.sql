CREATE SCHEMA multi_insert;
SET SESSION search_path = 'multi_insert';
CREATE EXTENSION IF NOT EXISTS orioledb;

\getenv abs_builddir PG_ABS_BUILDDIR

-- The batched same-leaf primary insert path must not change visible
-- results regardless of the debug GUC.  Each scenario runs both with
-- the batched path enabled and disabled so a mishap in either branch
-- would show up as a row-count or ordering diff.

CREATE TABLE t_ord (
    id   bigint PRIMARY KEY,
    val  int,
    grp  int
) USING orioledb;
CREATE INDEX t_ord_val_idx ON t_ord(val);
CREATE INDEX t_ord_grp_idx ON t_ord(grp);

-- A) Ordered insert: the hot path.  Run with batched on then off.
SET orioledb.debug_disable_multi_insert = off;
INSERT INTO t_ord SELECT i, i * 7, i % 50 FROM generate_series(1, 2000) i;

SET orioledb.debug_disable_multi_insert = on;
INSERT INTO t_ord SELECT i, i * 7, i % 50 FROM generate_series(2001, 4000) i;

SELECT count(*), min(id), max(id), sum(val) FROM t_ord;
SELECT count(*) FROM t_ord WHERE val BETWEEN 700 AND 1400;
SELECT count(*) FROM t_ord WHERE grp = 7;

-- B) Out-of-order insertion: hikey checks must fall back cleanly.
TRUNCATE t_ord;
SET orioledb.debug_disable_multi_insert = off;
INSERT INTO t_ord
SELECT i, i * 3, i % 13
FROM   generate_series(1, 1000) i
ORDER BY md5(i::text);

SELECT count(*) FROM t_ord;
SELECT count(*) FROM t_ord WHERE val < 1500;

-- C) Unique violation still fires under the optimisation.
SET orioledb.debug_disable_multi_insert = off;
DO $$
BEGIN
    INSERT INTO t_ord VALUES (1, 99, 99);
    RAISE EXCEPTION 'duplicate accepted';
EXCEPTION WHEN unique_violation THEN
    NULL;
END$$;

-- D) Reverse-order insert: every row crosses the leaf hikey so the
-- batched helper must bail and refind_page each time.
CREATE TABLE t_rev (id bigint PRIMARY KEY, val int) USING orioledb;
INSERT INTO t_rev
SELECT i, i FROM generate_series(500, 1, -1) i;
SELECT count(*), min(id), max(id) FROM t_rev;

-- E) Unsorted COPY into explicit-PK table spanning multiple leaves.
-- This is the regression-tested case: probe must detect BeforeLeaf and
-- bail to re-find for the out-of-order key, otherwise a row would be
-- inserted onto a leaf whose lokey is greater than the row's key,
-- breaking the B-tree downlink invariant.
CREATE TABLE t_copy_unsorted (id integer PRIMARY KEY, val int) USING orioledb;
-- Build a multi-leaf tree (about ~80 rows per leaf with the default settings).
INSERT INTO t_copy_unsorted SELECT i * 10, i FROM generate_series(1, 2000) i;
SET orioledb.debug_disable_multi_insert = off;
COPY t_copy_unsorted FROM stdin;
9999	1
1	2
15555	3
3	4
12345	5
7	6
\.
SELECT count(*) FROM t_copy_unsorted;
-- Each freshly-COPYed row must be reachable via the PK index.
SELECT id FROM t_copy_unsorted WHERE id IN (1, 3, 7, 9999, 12345, 15555)
  ORDER BY id;
-- B-tree integrity: a corrupted downlink would make this fail.
SELECT orioledb_tbl_check('t_copy_unsorted'::regclass);

-- F) Larger unsorted COPY via temp file.
-- Builds a multi-leaf tree first, then COPYs an interleaving set of
-- keys in reverse order to keep the monotonicity-fallback path exercised
-- across multiple leaves.
CREATE TABLE t_copy_reverse (id integer PRIMARY KEY, val int) USING orioledb;
INSERT INTO t_copy_reverse SELECT i * 10, i FROM generate_series(1, 5000) i;
\set copyfile :abs_builddir '/results/copy_reverse.data'
COPY (SELECT 50001 + i, i FROM generate_series(999, 1, -2) i) TO :'copyfile';
SET orioledb.debug_disable_multi_insert = off;
COPY t_copy_reverse FROM :'copyfile';
SELECT count(*) FROM t_copy_reverse;
SELECT orioledb_tbl_check('t_copy_reverse'::regclass);

-- G) In-batch duplicate against existing row: must still surface a
-- unique violation when the conflict is the first probed.
CREATE TABLE t_dup_existing (id int PRIMARY KEY, v int) USING orioledb;
INSERT INTO t_dup_existing VALUES (5, 100);
SET orioledb.debug_disable_multi_insert = off;
DO $$
BEGIN
    INSERT INTO t_dup_existing VALUES (5, 200), (6, 300);
    RAISE EXCEPTION 'duplicate accepted';
EXCEPTION WHEN unique_violation THEN
    NULL;
END$$;
SELECT count(*) FROM t_dup_existing;
SELECT orioledb_tbl_check('t_dup_existing'::regclass);

-- H) Self-created table COPYed in the same transaction.  desc->createOxid
-- equals the inserting oxid so o_btree_modify_internal's "needsUndo = false"
-- shortcut fires.  The multi-insert helper mirrors that and the post-undo
-- callback receives WaitingSkUndoLoc instead of a real undo loc.
\set copyfile :abs_builddir '/results/selfcreated.data'
COPY (SELECT i, i * 3 FROM generate_series(1, 500) i) TO :'copyfile';
BEGIN;
CREATE TABLE t_selfcreated (id integer PRIMARY KEY, val int) USING orioledb;
CREATE INDEX ON t_selfcreated (val);
SET LOCAL orioledb.debug_disable_multi_insert = off;
COPY t_selfcreated FROM :'copyfile';
SELECT count(*) FROM t_selfcreated;
SELECT count(*) FROM t_selfcreated WHERE val BETWEEN 100 AND 300;
COMMIT;
SELECT orioledb_tbl_check('t_selfcreated'::regclass);

-- I) COPY of TOAST-eligible long values.  Phase 1 calls tts_orioledb_toast
-- to decide what to toast; Phase 3 calls o_toast_insert_values to write
-- the toast btree entries.  Verify both ends survive a multi-row COPY.
CREATE TABLE t_toast (id integer PRIMARY KEY, body text) USING orioledb;
\set copyfile :abs_builddir '/results/toast.data'
COPY (SELECT i, repeat('X', 6000) FROM generate_series(1, 200) i) TO :'copyfile';
SET orioledb.debug_disable_multi_insert = off;
COPY t_toast FROM :'copyfile';
SELECT count(*), min(length(body)), max(length(body)) FROM t_toast;
-- Verify TOASTed values round-trip intact for a sample of rows.
SELECT id, length(body) FROM t_toast WHERE id IN (1, 100, 200) ORDER BY id;
SELECT orioledb_tbl_check('t_toast'::regclass);

-- J) CTID-PK (no explicit PK) multi-leaf COPY.  Exercises the prefix
-- contract on monotone CTID keys at scale: Phase 1's btree_ctid_get_and_inc
-- assigns each row a monotone TID; the helper should drain runs cleanly,
-- crossing leaf hikeys via HikeyCrossed -> refind.
CREATE TABLE t_ctid_pk (val int, grp int) USING orioledb;
CREATE INDEX ON t_ctid_pk (val);
\set copyfile :abs_builddir '/results/ctid_pk.data'
COPY (SELECT i, i % 47 FROM generate_series(1, 5000) i) TO :'copyfile';
SET orioledb.debug_disable_multi_insert = off;
COPY t_ctid_pk FROM :'copyfile';
SELECT count(*), min(val), max(val) FROM t_ctid_pk;
SELECT count(*) FROM t_ctid_pk WHERE val BETWEEN 1000 AND 2000;
SELECT orioledb_tbl_check('t_ctid_pk'::regclass);

DROP TABLE t_ord;
DROP TABLE t_rev;
DROP TABLE t_copy_unsorted;
DROP TABLE t_copy_reverse;
DROP TABLE t_dup_existing;
DROP TABLE t_selfcreated;
DROP TABLE t_toast;
DROP TABLE t_ctid_pk;
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA multi_insert CASCADE;

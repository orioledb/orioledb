#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import string

from testgres.enums import NodeStatus
from testgres.connection import NodeConnection

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent


class MergeTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "log_min_messages = notice\n"
		    "orioledb.enable_stopevents = true\n")

		self.node.start()  # start PostgreSQL
		self.node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;"
		    "CREATE TABLE IF NOT EXISTS o_merge ("
		    "     id int NOT NULL,"
		    "     PRIMARY KEY (id)"
		    ") USING orioledb;"
		    "TRUNCATE o_merge;")

	def test_non_concurrent_merge_and_checkpoint(self):
		node = self.node
		node.execute("INSERT INTO o_merge"
		             "(SELECT id FROM generate_series(1, 10000, 1) id);")
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])
		node.execute("CHECKPOINT;")
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])

		node.execute("DELETE FROM o_merge WHERE id <= 5000;")
		node.execute("CHECKPOINT;")
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])
		node.stop()

		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 5000)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])

	def test_merge_diff_old_snapshot_read(self):
		"""
		Exercises the differential merge undo image and its read path.

		A low-fillfactor table keeps leaf pages sparse without any deletes, so
		CHECKPOINT's page-merge pass (checkpoint_try_merge_page ->
		btree_try_merge_pages) merges adjacent resident siblings while dropping
		no tuple -> the merge writes a differential image (boundary key only).
		A REPEATABLE READ snapshot taken before the CHECKPOINT has a csn below
		the merge csn, so its reads of the merged pages route through the
		page-level undo chain and must reconstruct the pre-merge halves by
		trimming the merged page at the boundary key -- returning the exact
		original key set.
		"""
		node = self.node
		node.safe_psql(
		    'postgres',
		    "CREATE TABLE IF NOT EXISTS o_merge_diff ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb WITH (fillfactor = 10);"
		    "TRUNCATE o_merge_diff;")
		node.execute("INSERT INTO o_merge_diff "
		             "(SELECT id, repeat('x', 100) FROM generate_series(1, 3000) id);")

		con_snap = node.connect()
		con_snap.begin()
		con_snap.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_merge_diff;")[0][0], 3000)

		# CHECKPOINT merges the sparse, co-resident leaves (no deletes -> the
		# merge drops nothing -> differential images), with a fresh csn above
		# the snapshot's.
		node.execute("CHECKPOINT;")

		# The old snapshot reads the merged pages through undo and must
		# reconstruct the exact original key set.
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_merge_diff;")[0][0], 3000)
		self.assertEqual(
		    con_snap.execute(
		        "SELECT count(*) FROM o_merge_diff "
		        "WHERE id BETWEEN 1000 AND 2000;")[0][0], 1001)
		self.assertEqual(
		    con_snap.execute(
		        "SELECT min(id), max(id), sum(id::bigint) FROM o_merge_diff;")[0],
		    (1, 3000, 4501500))
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_merge_diff'::regclass)")[0][0])

	def test_merge_diff_old_snapshot_index_read(self):
		"""
		Like test_merge_diff_old_snapshot_read, but the old snapshot reads the
		merged pages through an *index* (ordered) scan, which routes the
		page-level undo walk through the iterator (undo_it_find_internal) rather
		than the seq-scan path.  Confirms the iterator seeds its working image
		from the live merged leaf before reconstructing differential halves.
		"""
		node = self.node
		node.safe_psql(
		    'postgres',
		    "CREATE TABLE IF NOT EXISTS o_merge_diff_idx ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb WITH (fillfactor = 10);"
		    "TRUNCATE o_merge_diff_idx;")
		node.execute("INSERT INTO o_merge_diff_idx "
		             "(SELECT id, repeat('x', 100) FROM generate_series(1, 3000) id);")

		con_snap = node.connect()
		con_snap.begin()
		con_snap.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = off;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_merge_diff_idx;")[0][0],
		    3000)

		node.execute("CHECKPOINT;")

		# Ordered/range index reads route through the page-level undo iterator.
		self.assertEqual(
		    con_snap.execute(
		        "SELECT count(*) FROM o_merge_diff_idx "
		        "WHERE id BETWEEN 1000 AND 2000;")[0][0], 1001)
		self.assertEqual(
		    con_snap.execute(
		        "SELECT id FROM o_merge_diff_idx "
		        "WHERE id BETWEEN 500 AND 2500 ORDER BY id;"),
		    [(i,) for i in range(500, 2501)])
		self.assertEqual(
		    con_snap.execute(
		        "SELECT id FROM o_merge_diff_idx "
		        "WHERE id BETWEEN 500 AND 2500 ORDER BY id DESC;"),
		    [(i,) for i in range(2500, 499, -1)])
		self.assertEqual(
		    con_snap.execute(
		        "SELECT min(id), max(id), sum(id::bigint) "
		        "FROM o_merge_diff_idx;")[0],
		    (1, 3000, 4501500))
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_merge_diff_idx'::regclass)")[0][0])

	def test_merge_diff_old_snapshot_point_read(self):
		"""
		Like test_merge_diff_old_snapshot_read, but the old snapshot reads the
		merged pages through *point* and *bitmap* lookups, which route through
		o_btree_find_tuple_by_key_cb -> find_page -> o_btree_read_page with a
		partially-loaded page (BTREE_PAGE_FIND_FETCH).  A differential image must
		fully materialize that page before reconstructing in place; this test
		guards the partial-page materialization fix.
		"""
		node = self.node
		node.safe_psql(
		    'postgres',
		    "CREATE TABLE IF NOT EXISTS o_merge_diff_pt ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb WITH (fillfactor = 10);"
		    "TRUNCATE o_merge_diff_pt;")
		node.execute("INSERT INTO o_merge_diff_pt "
		             "(SELECT id, repeat('x', 100) FROM generate_series(1, 3000) id);")

		con_snap = node.connect()
		con_snap.begin()
		con_snap.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_merge_diff_pt;")[0][0], 3000)

		node.execute("CHECKPOINT;")

		# Point lookups (index scan) over merged pages through undo.
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = off;")
		for pk in (1, 2, 1500, 1501, 2999, 3000):
			self.assertEqual(
			    con_snap.execute(
			        "SELECT id, payload = repeat('x', 100) FROM o_merge_diff_pt "
			        "WHERE id = %d;" % pk), [(pk, True)])
		self.assertEqual(
		    con_snap.execute(
		        "SELECT count(*) FROM o_merge_diff_pt "
		        "WHERE id IN (700, 1300, 2200, 2900);")[0][0], 4)

		# Bitmap scan over merged pages through undo.
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = on;")
		con_snap.execute("SET enable_indexscan = off;")
		self.assertEqual(
		    con_snap.execute(
		        "SELECT count(*) FROM o_merge_diff_pt "
		        "WHERE id < 100 OR id > 2950;")[0][0], 99 + 50)
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_merge_diff_pt'::regclass)")[0][0])

	def test_split_diff_old_snapshot_read(self):
		"""
		Exercises the differential split undo image and its read paths.

		An old REPEATABLE READ snapshot is taken over a fully-populated table.
		Then a second transaction inserts keys interleaved between the existing
		ones, forcing the pre-existing (full) leaves to split.  Each split drops
		no tuple -> it writes a differential split image (split key only), with a
		csn above the snapshot's.  The old snapshot then reads the split leaves
		through the page-level undo chain via seq, index, point and bitmap scans
		-- each must reconstruct the exact original key set, with none of the
		newly inserted interleaved keys visible.

		Also covers UPDATE-driven splits (a single UPDATE that grows rows and
		re-fetches them through undo), which routes the page-level undo walk
		through the partially-loaded combined-fetch path.
		"""
		node = self.node
		node.safe_psql(
		    'postgres',
		    "CREATE TABLE IF NOT EXISTS o_split_diff ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb;"
		    "TRUNCATE o_split_diff;")
		# Even keys 2..10000 fill the leaves.
		node.execute("INSERT INTO o_split_diff "
		             "(SELECT id * 2, repeat('x', 100) "
		             "FROM generate_series(1, 5000) id);")

		con_snap = node.connect()
		con_snap.begin()
		con_snap.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_split_diff;")[0][0], 5000)

		# Odd keys 1..9999 interleave between the existing even keys, forcing the
		# pre-existing full leaves to split (no deletes -> differential images),
		# with fresh csns above the snapshot's.
		node.execute("INSERT INTO o_split_diff "
		             "(SELECT id * 2 - 1, repeat('y', 100) "
		             "FROM generate_series(1, 5000) id);")
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_split_diff;")[0][0], 10000)

		# Seq scan through undo.
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_split_diff;")[0][0], 5000)
		self.assertEqual(
		    con_snap.execute(
		        "SELECT min(id), max(id), sum(id::bigint), count(*) "
		        "FROM o_split_diff WHERE MOD(id, 2) = 1;")[0],
		    (None, None, None, 0))
		self.assertEqual(
		    con_snap.execute(
		        "SELECT min(id), max(id), sum(id::bigint) "
		        "FROM o_split_diff;")[0],
		    (2, 10000, 25005000))

		# Point + index reads through undo (partial-page combined-fetch path).
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = off;")
		for pk in (2, 1000, 5000, 9998, 10000):
			self.assertEqual(
			    con_snap.execute(
			        "SELECT id FROM o_split_diff WHERE id = %d;" % pk),
			    [(pk,)])
		for odd in (1, 4999, 9999):
			self.assertEqual(
			    con_snap.execute(
			        "SELECT count(*) FROM o_split_diff WHERE id = %d;" % odd)
			    [0][0], 0)
		self.assertEqual(
		    con_snap.execute(
		        "SELECT count(*) FROM o_split_diff "
		        "WHERE id BETWEEN 1000 AND 2000;")[0][0], 501)

		# Bitmap scan through undo.
		con_snap.execute("SET enable_bitmapscan = on;")
		con_snap.execute("SET enable_indexscan = off;")
		self.assertEqual(
		    con_snap.execute(
		        "SELECT count(*) FROM o_split_diff "
		        "WHERE id < 200 OR id > 9800;")[0][0], 99 + 100)
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_split_diff'::regclass)")[0][0])

	def test_split_diff_update_grow(self):
		"""
		A single UPDATE that grows every row, forcing in-statement page splits
		and re-fetches of the rows being updated through the (partially-loaded)
		combined-fetch undo path.  Regression guard for the differential split
		image + partial-page materialization.
		"""
		node = self.node
		node.safe_psql(
		    'postgres',
		    "CREATE TABLE IF NOT EXISTS o_split_upd ("
		    "    key int PRIMARY KEY, val int, spacer text"
		    ") USING orioledb;"
		    "TRUNCATE o_split_upd;")
		node.execute("INSERT INTO o_split_upd "
		             "(SELECT i, i, '' FROM generate_series(1, 100) i);")
		node.execute("UPDATE o_split_upd SET spacer = repeat('a', 96) "
		             "WHERE key > 1;")
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_split_upd;")[0][0], 100)
		self.assertEqual(
		    node.execute(
		        "SELECT bool_and(spacer = repeat('a', 96)) "
		        "FROM o_split_upd WHERE key > 1;")[0][0], True)
		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_split_upd'::regclass)")[0][0])

	def test_non_concurrent_merge(self):
		node = self.node
		node.execute("INSERT INTO o_merge"
		             "(SELECT id FROM generate_series(1, 10000, 1) id);")
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])
		node.execute("CHECKPOINT;")
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])

		node.execute("DELETE FROM o_merge WHERE id <= 5000;")
		node.stop()

		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 5000)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])

	def test_concurrent_checkpoint_begin(self):
		self.concurrent_checkpoint_base(0, 2600, 1)

	def test_concurrent_checkpoint_middle(self):
		self.concurrent_checkpoint_base(2500, 5000, 0)

	def test_concurrent_checkpoint_end(self):
		self.concurrent_checkpoint_base(4000, 800, 5)

	def concurrent_checkpoint_base(self, delete_offset, bp_value,
	                               checkpoint_after_count):
		node = self.node
		node.execute("INSERT INTO o_merge"
		             "(SELECT id FROM generate_series(1, 10000, 1) id);")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
		             "'$.action == \"walkDownwards\" && "
		             "$.treeName == \"o_merge_pkey\" && "
		             "$.lokey.id > %d');" % (bp_value))

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()

		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("DELETE FROM o_merge WHERE id > %d AND id <= %d;" %
		             (delete_offset, 5000 + delete_offset))

		t2 = ThreadQueryExecutor(con2, "COMMIT;")
		t2.start()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")

		t1.join()
		t2.join()

		for i in range(checkpoint_after_count):
			node.execute("CHECKPOINT;")
			self.assertTrue(
			    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")
			    [0][0])

		con1.close()
		con2.close()
		con3.close()
		node.stop()

		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 5000)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0]
		    [0])

#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import string
import time

from threading import Event, Thread

from testgres.enums import NodeStatus
from testgres.connection import NodeConnection

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent
from .base_test import wait_stopevent


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

	def _open_merge_diff_snapshot(self, table):
		"""
		Shared setup for the test_merge_diff_old_snapshot_* cases: populate a
		low-fillfactor table, open a REPEATABLE READ snapshot, then CHECKPOINT
		so its page-merge pass writes differential merge images above the
		snapshot's csn.
		"""
		node = self.node
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS %s ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb WITH (fillfactor = 10);"
		    "TRUNCATE %s;" % (table, table))
		node.execute(
		    "INSERT INTO %s "
		    "(SELECT id, repeat('x', 100) FROM generate_series(1, 3000) id);" %
		    table)

		con_snap = node.connect()
		con_snap.begin()
		con_snap.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM %s;" % table)[0][0], 3000)

		# CHECKPOINT merges the sparse, co-resident leaves (no deletes -> the
		# merge drops nothing -> differential images), with a fresh csn above
		# the snapshot's.
		node.execute("CHECKPOINT;")
		return con_snap

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
		table = 'o_merge_diff'
		con_snap = self._open_merge_diff_snapshot(table)

		# The old snapshot reads the merged pages through undo and must
		# reconstruct the exact original key set.
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM %s;" % table)[0][0], 3000)
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM %s "
		                     "WHERE id BETWEEN 1000 AND 2000;" % table)[0][0],
		    1001)
		self.assertEqual(
		    con_snap.execute(
		        "SELECT min(id), max(id), sum(id::bigint) FROM %s;" %
		        table)[0], (1, 3000, 4501500))
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    self.node.execute("SELECT orioledb_tbl_check('%s'::regclass)" %
		                      table)[0][0])

	def test_merge_diff_old_snapshot_index_read(self):
		"""
		Like test_merge_diff_old_snapshot_read, but the old snapshot reads the
		merged pages through an *index* (ordered) scan, which routes the
		page-level undo walk through the iterator (undo_it_find_internal) rather
		than the seq-scan path.  Confirms the iterator seeds its working image
		from the live merged leaf before reconstructing differential halves.
		"""
		table = 'o_merge_diff_idx'
		con_snap = self._open_merge_diff_snapshot(table)
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = off;")

		# Ordered/range index reads route through the page-level undo iterator.
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM %s "
		                     "WHERE id BETWEEN 1000 AND 2000;" % table)[0][0],
		    1001)
		self.assertEqual(
		    con_snap.execute("SELECT id FROM %s "
		                     "WHERE id BETWEEN 500 AND 2500 ORDER BY id;" %
		                     table), [(i, ) for i in range(500, 2501)])
		self.assertEqual(
		    con_snap.execute(
		        "SELECT id FROM %s "
		        "WHERE id BETWEEN 500 AND 2500 ORDER BY id DESC;" % table),
		    [(i, ) for i in range(2500, 499, -1)])
		self.assertEqual(
		    con_snap.execute(
		        "SELECT min(id), max(id), sum(id::bigint) FROM %s;" %
		        table)[0], (1, 3000, 4501500))
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    self.node.execute("SELECT orioledb_tbl_check('%s'::regclass)" %
		                      table)[0][0])

	def test_merge_diff_old_snapshot_point_read(self):
		"""
		Like test_merge_diff_old_snapshot_read, but the old snapshot reads the
		merged pages through *point* and *bitmap* lookups, which route through
		o_btree_find_tuple_by_key_cb -> find_page -> o_btree_read_page with a
		partially-loaded page (BTREE_PAGE_FIND_FETCH).  A differential image must
		fully materialize that page before reconstructing in place; this test
		guards the partial-page materialization fix.
		"""
		table = 'o_merge_diff_pt'
		con_snap = self._open_merge_diff_snapshot(table)

		# Point lookups (index scan) over merged pages through undo.
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = off;")
		for pk in (1, 2, 1500, 1501, 2999, 3000):
			self.assertEqual(
			    con_snap.execute(
			        "SELECT id, payload = repeat('x', 100) FROM %s "
			        "WHERE id = %d;" % (table, pk)), [(pk, True)])
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM %s "
		                     "WHERE id IN (700, 1300, 2200, 2900);" %
		                     table)[0][0], 4)

		# Bitmap scan over merged pages through undo.
		con_snap.execute("SET enable_bitmapscan = on;")
		con_snap.execute("SET enable_indexscan = off;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM %s "
		                     "WHERE id < 100 OR id > 2950;" % table)[0][0],
		    99 + 50)
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    self.node.execute("SELECT orioledb_tbl_check('%s'::regclass)" %
		                      table)[0][0])

	def test_split_diff_old_snapshot_read(self):
		"""
		Exercises a no-drop split's read paths under an old snapshot.

		An old REPEATABLE READ snapshot is taken over a fully-populated table.
		Then a second transaction inserts keys interleaved between the existing
		ones, forcing the pre-existing (full) leaves to split.  Each split drops
		no tuple and the leaves carry no retained pre-split history, so it writes
		no page-level undo image at all: both halves are frozen with an invalid
		undo location and read live.  The old snapshot then reads the split
		leaves through seq, index, point and bitmap scans -- each must return the
		exact original key set, with the newly inserted interleaved keys filtered
		out by per-tuple MVCC (physically present on the live halves but
		invisible to the old snapshot).

		Also covers UPDATE-driven splits (a single UPDATE that grows rows and
		re-fetches them through undo), which routes the page-level undo walk
		through the partially-loaded combined-fetch path.
		"""
		node = self.node
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_split_diff ("
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
		# pre-existing full leaves to split (no deletes -> no page-level undo
		# image; the halves are frozen with an invalid undo location).
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
		    con_snap.execute("SELECT min(id), max(id), sum(id::bigint) "
		                     "FROM o_split_diff;")[0], (2, 10000, 25005000))

		# Point + index reads through undo (partial-page combined-fetch path).
		con_snap.execute("SET enable_seqscan = off;")
		con_snap.execute("SET enable_bitmapscan = off;")
		for pk in (2, 1000, 5000, 9998, 10000):
			self.assertEqual(
			    con_snap.execute("SELECT id FROM o_split_diff WHERE id = %d;" %
			                     pk), [(pk, )])
		for odd in (1, 4999, 9999):
			self.assertEqual(
			    con_snap.execute(
			        "SELECT count(*) FROM o_split_diff WHERE id = %d;" %
			        odd)[0][0], 0)
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_split_diff "
		                     "WHERE id BETWEEN 1000 AND 2000;")[0][0], 501)

		# Bitmap scan through undo.
		con_snap.execute("SET enable_bitmapscan = on;")
		con_snap.execute("SET enable_indexscan = off;")
		self.assertEqual(
		    con_snap.execute("SELECT count(*) FROM o_split_diff "
		                     "WHERE id < 200 OR id > 9800;")[0][0], 99 + 100)
		con_snap.rollback()
		con_snap.close()

		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_split_diff'::regclass)")
		    [0][0])

	def test_split_diff_scan_during_split(self):
		"""
		A sequential scan on an old snapshot must not return a row twice while
		the leaves under it are splitting.

		Scanning keeps a page-level undo image alive for every split, so each
		live leaf reconstructs the page it came from.  A leaf produced by two
		splits sits strictly inside the older image, and the walk has to start
		at that leaf's own low boundary -- otherwise it re-emits the keys that
		went to a sibling the scan visits on its own.
		"""
		node = self.node
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_split_conc ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb;"
		    "TRUNCATE o_split_conc;")
		node.execute("INSERT INTO o_split_conc "
		             "(SELECT id * 2, repeat('x', 100) "
		             "FROM generate_series(1, 5000) id);")

		# The scan has to be in flight when the pages split -- that is what
		# makes the split keep a page-level undo image at all -- so it runs
		# from its own thread, started before the interleaving insert and
		# stopped after it.
		stop = Event()
		outcome = {}

		def scanner():
			con = node.connect()
			try:
				con.begin()
				con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
				con.execute("SET enable_indexscan = off;")
				con.execute("SET enable_bitmapscan = off;")
				outcome['first'] = con.execute(
				    "SELECT count(*) FROM o_split_conc;")[0][0]
				while not stop.is_set():
					# Any key the scan hands out twice shows up here.
					dups = con.execute("SELECT id, count(*) FROM o_split_conc "
					                   "GROUP BY id HAVING count(*) > 1 "
					                   "ORDER BY id;")
					if dups:
						outcome['bad'] = dups[:5]
						return
				outcome['last'] = con.execute(
				    "SELECT count(*), min(id), max(id), sum(id::bigint) "
				    "FROM o_split_conc;")[0]
			except Exception as e:
				outcome['error'] = repr(e)
			finally:
				con.rollback()
				con.close()

		t = Thread(target=scanner)
		t.start()
		try:
			deadline = time.time() + 30
			while ('first' not in outcome and t.is_alive()
			       and time.time() < deadline):
				time.sleep(0.01)
			time.sleep(0.3)

			node.execute("INSERT INTO o_split_conc "
			             "(SELECT id * 2 - 1, repeat('y', 100) "
			             "FROM generate_series(1, 5000) id);")
			time.sleep(0.2)
		finally:
			stop.set()
			t.join(timeout=60)

		self.assertNotIn('error', outcome)
		self.assertEqual(outcome.get('first'), 5000)
		self.assertNotIn('bad', outcome)
		self.assertEqual(outcome.get('last'), (5000, 2, 10000, 25005000))

		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_split_conc;")[0][0], 10000)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_split_conc'::regclass)")
		    [0][0])

	def test_split_diff_backward_scan_across_split(self):
		"""
		A backward ordered scan must not lose rows to a split under it.

		A cursor holds the scan in flight between two FETCHes, so the split
		lands at an exactly reproducible point rather than by timing.  While
		the scan is parked, interleaved keys split every leaf under it, twice
		over: the leaf the scan is standing on ends up as the right half of one
		split whose own history is another split.

		Both halves of a differential split image (UndoPageImageSplitDiff) keep
		their own narrow contents but their undo chains converge on the same
		older history.  A backward step that identified a historical image by
		where its chain stopped therefore could not tell the two halves apart,
		took the left one for already covered, and stepped past it -- dropping
		every row on it.  Issue #1036.
		"""
		node = self.node
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_backscan ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb;"
		    "TRUNCATE o_backscan;")
		node.execute("INSERT INTO o_backscan "
		             "(SELECT id * 2, repeat('x', 100) "
		             "FROM generate_series(1, 2000) id);")

		con = node.connect()
		try:
			con.begin()
			con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
			con.execute("SET enable_seqscan = off;")
			con.execute("SET enable_bitmapscan = off;")
			con.execute("DECLARE c NO SCROLL CURSOR FOR "
			            "SELECT id FROM o_backscan ORDER BY id DESC;")
			# Park the scan a few leaves in, still far from the low keys.
			got = [r[0] for r in con.execute("FETCH 100 FROM c;")]

			# Split every leaf under the parked scan.
			node.execute("INSERT INTO o_backscan "
			             "(SELECT id * 2 - 1, repeat('y', 100) "
			             "FROM generate_series(1, 2000) id);")

			got += [r[0] for r in con.execute("FETCH ALL FROM c;")]
			self.assertEqual(got, [i * 2 for i in range(2000, 0, -1)])
		finally:
			con.rollback()
			con.close()

	def test_split_diff_over_full_image_chain(self):
		"""
		A page-level undo chain must not mix a full image with a differential one.

		A full image (UndoPageImageSplit) carries the whole page, so applying it
		overwrites whatever the chain walk has built.  A differential split
		(UndoPageImageSplitDiff) carries no page bytes at all and transforms the
		image the walk is holding, in place.  Mixed, the differential record is
		applied to a base it was not computed against, and the bound the walk
		narrowed from its split key survives into a page the full image has
		since widened back -- so a backward scan re-reads the same leaves and
		returns every row several times.  Issue #1055.

		page_add_image_to_undo() picks the kind at write time: a split that
		drops no tuple writes a differential image, unless a sequential scan is
		registered on the tree, in which case it writes a full one.  So a cursor
		held open over a sequential scan decides the first split, and closing it
		decides the second -- no timing involved.  The old REPEATABLE READ
		snapshot held throughout retains the undo, so neither split may take the
		branch that writes no image at all.
		"""
		node = self.node
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_mixed_chain ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb;"
		    "TRUNCATE o_mixed_chain;")
		# Two free slots between each pair of keys, so both rounds of
		# interleaved inserts split the same leaves.
		node.execute("INSERT INTO o_mixed_chain "
		             "(SELECT id * 4, repeat('x', 100) "
		             "FROM generate_series(1, 5000) id);")
		expected = [i * 4 for i in range(1, 5001)]

		con_snap = node.connect()
		try:
			con_snap.begin()
			con_snap.execute(
			    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
			# Takes the snapshot and starts retaining undo.  The read finishes,
			# so it leaves no sequential scan registered.
			self.assertEqual(
			    con_snap.execute("SELECT count(*) FROM o_mixed_chain;")[0][0],
			    len(expected))

			# Round one: split every leaf while a sequential scan is parked on
			# the tree, which forces a full image.
			con_hold = node.connect()
			try:
				con_hold.begin()
				con_hold.execute("SET enable_indexscan = off;")
				con_hold.execute("SET enable_bitmapscan = off;")
				con_hold.execute("DECLARE ch NO SCROLL CURSOR FOR "
				                 "SELECT id FROM o_mixed_chain;")
				self.assertEqual(len(con_hold.execute("FETCH 1 FROM ch;")), 1)
				node.execute("INSERT INTO o_mixed_chain "
				             "(SELECT id * 4 + 1, repeat('y', 100) "
				             "FROM generate_series(1, 5000) id);")
				con_hold.execute("CLOSE ch;")
			finally:
				con_hold.rollback()
				con_hold.close()

			# Round two: nothing is parked now, so the same leaves split into
			# differential images stacked on the full ones.
			node.execute("INSERT INTO o_mixed_chain "
			             "(SELECT id * 4 + 2, repeat('z', 100) "
			             "FROM generate_series(1, 5000) id);")

			con_snap.execute("SET enable_seqscan = off;")
			con_snap.execute("SET enable_bitmapscan = off;")
			forward = [
			    r[0] for r in con_snap.execute(
			        "SELECT id FROM o_mixed_chain ORDER BY id;")
			]
			backward = [
			    r[0] for r in con_snap.execute(
			        "SELECT id FROM o_mixed_chain ORDER BY id DESC;")
			]
			# Compare the sizes first: the failure is a flood of duplicates, and
			# the count says so in one line.
			self.assertEqual(len(forward), len(expected))
			self.assertEqual(len(backward), len(expected))
			self.assertEqual(forward, expected)
			self.assertEqual(backward, list(reversed(expected)))
		finally:
			# An assert-enabled build catches the same defect as an ordering
			# assertion in the backend instead of returning the rows, so the
			# connection may already be gone by now.  Don't let tearing it down
			# mask what failed.
			try:
				con_snap.rollback()
				con_snap.close()
			except Exception:
				pass

	def test_split_diff_update_grow(self):
		"""
		A single UPDATE that grows every row, forcing in-statement page splits
		and re-fetches of the rows being updated through the (partially-loaded)
		combined-fetch undo path.  Regression guard for the differential split
		image + partial-page materialization.
		"""
		node = self.node
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_split_upd ("
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
		    node.execute("SELECT bool_and(spacer = repeat('a', 96)) "
		                 "FROM o_split_upd WHERE key > 1;")[0][0], True)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_split_upd'::regclass)")
		    [0][0])

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

	def test_merge_free_extent_concurrent_checkpoint(self):
		"""
		btree_try_merge_pages() captures the right page's checkpoint number while
		parent, left and right are all locked, then reports the retired page's
		extent via free_extent_for_checkpoint().  If the left page is unlocked
		before that report, the checkpointer can finish the checkpoint
		(checkpoint_ix() -> seq_buf_finalize() + free_seq_buf_pages()), so
		shared->pages[i] becomes OInvalidInMemoryBlkno and the report writes into
		a freed seq_buf.  The fix reports under the left page lock, which blocks
		the checkpointer until the report is done.
		"""
		node = self.node
		node.execute(
		    "INSERT INTO o_merge SELECT g FROM generate_series(1, 20000) g;")

		node.execute("CHECKPOINT;")

		node.execute("DELETE FROM o_merge WHERE mod(id, 8) <> 0;")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()
		con2_pid = con2.pid

		con1.execute("SELECT pg_stopevent_set('merge_before_free_extent',\n"
		             "'$.treeName == \"o_merge_pkey\" && "
		             "$.hadExtent == true && "
		             "$backendType == \"client backend\"');")

		t2 = ThreadQueryExecutor(
		    con2, "SELECT orioledb_write_pages('o_merge'::regclass);")
		t2.start()
		wait_stopevent(node, con2_pid)

		# Checkpointer should be blocked, because the left page is blocked
		t3 = ThreadQueryExecutor(con3, "CHECKPOINT;")
		t3.start()
		t3.join(10)
		self.assertTrue(
		    t3.is_alive(),
		    "checkpointer must block on the left page lock held by the merge")

		self.assertTrue(
		    con1.execute(
		        "SELECT pg_stopevent_reset('merge_before_free_extent')")[0][0])
		try:
			t2.join()
			t3.join()
		except Exception as e:
			raise AssertionError("The released merge lost its connection!")

		con1.close()
		con2.close()
		con3.close()

		node.stop()
		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 2500)
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

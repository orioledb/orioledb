#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest


class SeqScanUndoTest(BaseTest):

	def test_full_image_for_split_after_downlink_collection(self):
		"""
		A sequential scan that has already collected its on-disk downlinks must
		still see every pre-operation row when a split rewrites those extents
		underneath it.

		This is the shared-table (non-temp) counterpart of
		TempLocalPoolTest.test_evict_pages_with_concurrent_seq_scan.  It guards
		the rule that a page split or merge keeps a *full* page-level undo image
		while a sequential scan is active on the tree
		(meta_page_get_num_seq_scans() > 0), even when the page has no retained
		pre-op undo of its own -- a freshly written leaf has an invalid
		undoLocation, so the retain-horizon test alone would let the image be
		skipped and the scan would lose the tuples the split moved to a new page
		that is not in its collected downlink set.

		Arrangement (single backend, mirroring the temp-table test):

		  1. Fill a table so the primary-key btree has many leaves and push
		     every leaf to disk.
		  2. A PK point lookup on the max id brings just the rightmost leaf back
		     into the main pool; its parent downlink flips to IN_MEMORY while the
		     rest stay ON_DISK.
		  3. A cursor opens a seq scan.  The first FETCH walks the internal page,
		     queues an ON_DISK downlink for every other leaf and bumps
		     numSeqScans, leaving diskDownlinks[] populated but unconsumed.
		  4. A bulk UPDATE grows every row enough to split the leaves.  With a
		     seq scan active these splits must keep a full pre-split image.
		  5. orioledb_evict_pages() rewrites the dirty pages back over the same
		     on-disk extents the cursor's diskDownlinks[] still points at.
		  6. Draining the cursor reads the overwritten extents as of its
		     snapshot, reconstructing each pre-split page from the page-level
		     undo chain.  It must return the full original key set.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TABLE o_seqscan (
				id int PRIMARY KEY,
				val int NOT NULL,
				payload text
			) USING orioledb;
		""")
		con.commit()

		n = 5000
		con.execute("INSERT INTO o_seqscan SELECT g, g, repeat('x', 200) "
		            "FROM generate_series(1, %d) g;" % n)
		con.commit()

		# Push every leaf to disk.
		con.execute("SELECT orioledb_evict_pages('o_seqscan'::regclass, 0);")

		# Bring only the rightmost leaf back into the main pool.
		self.assertEqual(
		    con.execute("SELECT id FROM o_seqscan WHERE id = %d;" % n),
		    [(n, )])

		con.begin()
		con.execute("SET LOCAL enable_indexscan = off;")
		con.execute("SET LOCAL enable_bitmapscan = off;")
		con.execute(
		    "DECLARE c1 NO SCROLL CURSOR FOR SELECT id FROM o_seqscan;")

		# The first FETCH initializes the seq scan: iterate_internal_page walks
		# the internal page, queues ON_DISK downlinks for every leaf except the
		# rightmost (which is IN_MEMORY) and bumps numSeqScans, leaving
		# diskDownlinks[] populated but unconsumed.
		first_batch = con.execute("FETCH 10 FROM c1;")
		self.assertEqual(len(first_batch), 10)

		# Grow every row so the leaves split while the seq scan is active.  Each
		# split moves tuples to a new page that is not in the cursor's collected
		# downlink set; only a full pre-split undo image lets the cursor recover
		# them.
		con.execute("UPDATE o_seqscan SET val = val + 1000000, "
		            "payload = repeat('y', 600) WHERE id < %d;" % n)

		# Rewrite the dirty pages in place, overwriting the on-disk extents the
		# cursor's diskDownlinks[] still points at.
		con.execute("SELECT orioledb_evict_pages('o_seqscan'::regclass, 0);")

		rest = con.execute("FETCH ALL FROM c1;")
		con.execute("CLOSE c1;")
		con.commit()

		all_ids = sorted(row[0] for row in first_batch + rest)
		self.assertEqual(all_ids, list(range(1, n + 1)))

		self.assertTrue(
		    con.execute("SELECT orioledb_tbl_check('o_seqscan'::regclass);")[0]
		    [0])

		con.close()
		node.stop()

#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest

FAT = "x" * 180
BASE = 30


class SeqScanOwnWriteTest(BaseTest):
	"""A sequential scan misses the scanning transaction's own write.

	btree_seq_scan_getnext_internal() merges the live leaf image with a
	historical one, and where both hold the same key it decides which to emit
	with

	    if (XACT_INFO_OXID_IS_CURRENT(tuphdr->xactInfo))

	read off the *raw* leaf header.  A concurrent FOR KEY SHARE lock puts a
	lock-only undo record on top of our own version -- KEY SHARE and NO KEY
	UPDATE do not conflict -- so that header describes the locker, the test
	answers "not mine", and the scan emits the historical version instead.

	The iterator asks the same question through find_non_lock_only_undo_record()
	and gets it right, with a comment naming this exact hazard, which is why
	the reported shape is "the index scan finds the row, the sequential scan
	does not" (ORI-229 / orioledb#982).

	Reaching that branch needs a historical page image, and one is only loaded
	when the leaf's own csn is at or after the scan's snapshot -- i.e. a
	*page-level* change committed by someone else after we took our snapshot.
	A row update does not make one; a split does.  Order matters: the split
	has to happen before our own write, so the image it leaves behind predates
	it and emitting from it actually loses the write.

	Instrumented, the losing decision reads

	    ORI229PROBE: cmp==0 lockOnly=1 raw=0 resolved=1

	-- the row is ours, and only the raw header says otherwise.
	"""

	@unittest.skip("the bug is not fixed yet: the merge decides by the raw "
	               "leaf header instead of resolving the lock-only chain")
	def test_sequential_scan_sees_own_write_under_a_key_share_lock(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_seq (\n"
		    "  id int NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  pad text NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_seq SELECT g, 0, '%s' "
		    "FROM generate_series(1, %d) g;" % (FAT, BASE))
		node.safe_psql('postgres', "CHECKPOINT;")

		scanner = node.connect()
		splitter = node.connect()
		locker = node.connect()

		scanner.execute("SET enable_indexscan = off;")
		scanner.execute("SET enable_bitmapscan = off;")
		scanner.begin('repeatable read')
		# a live sequential scan, so a concurrent page change has a reason to
		# keep the old page image
		scanner.execute(
		    "DECLARE cur NO SCROLL CURSOR FOR SELECT id, v FROM o_seq;")
		scanner.execute("FETCH 1 FROM cur;")

		# split the page under our snapshot, before our own write
		splitter.begin()
		splitter.execute("INSERT INTO o_seq SELECT g, 0, '%s' "
		                 "FROM generate_series(%d, %d) g;" %
		                 (FAT, BASE + 1, BASE + 120))
		splitter.commit()

		scanner.execute("UPDATE o_seq SET v = 42 WHERE id = 1;")

		# a lock-only record on top of our own version
		locker.begin()
		locker.execute("SELECT id FROM o_seq WHERE id = 1 FOR KEY SHARE;")

		rows = scanner.execute("SELECT id, v FROM o_seq WHERE id = 1;")
		try:
			self.assertEqual(
			    rows, [(1, 42)], "the sequential scan did not see the "
			    "transaction's own write")
		finally:
			for con in (locker, splitter, scanner):
				try:
					con.rollback()
					con.close()
				except Exception:
					pass
		node.stop()

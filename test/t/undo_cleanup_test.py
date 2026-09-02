#!/usr/bin/env python3
# coding: utf-8

import os
import re
import threading
import time
import unittest

from .base_test import BaseTest

UNDO_FILE_SIZE = 0x4000000
WORKERS = 4
ROUNDS = 8
ROWS = 40000


class UndoCleanupTest(BaseTest):

	def undo_files(self, node):
		"""Numbers of the row-undo files currently on disk."""
		undo_dir = os.path.join(node.data_dir, 'orioledb_undo')
		result = []
		for name in os.listdir(undo_dir):
			match = re.match(r'^([0-9A-F]{10})row$', name)
			if match:
				result.append(int(match.group(1), 16))
		return sorted(result)

	def unretained_undo_files(self, node):
		"""Undo files holding nothing that any retain position still keeps.

		The retain set is the checkpoint range plus everything from the
		current retain location on; a file outside both is garbage that
		cleanup was supposed to have unlinked.
		"""
		(chkp_start, chkp_end, retain) = node.execute("""
			SELECT checkpointRetainStartLocation, checkpointRetainEndLocation,
			       minProcRetainLocation
			FROM orioledb_get_undo_meta() WHERE undo_type = 'row';
		""")[0]

		result = []
		for file_num in self.undo_files(node):
			low = file_num * UNDO_FILE_SIZE
			high = low + UNDO_FILE_SIZE
			live = high > retain
			in_chkp = chkp_start < chkp_end and low < chkp_end and chkp_start < high
			if not live and not in_chkp:
				result.append(file_num)
		return result

	def test_no_undo_file_outlives_its_retain(self):
		"""Cleanup must not leave an undo file no retain position keeps.

		Cleanup used to unlink only what a single caller-supplied range
		covered wholly.  The file holding the upper edge of a retired
		checkpoint retain range is covered only partially, and no later call
		revisits that range -- so it stayed on disk for good, one 64 MB file
		per checkpoint.

		Three things are needed to reach that state, and dropping any of them
		hides the bug: the retain position has to keep moving (a single
		unmoving snapshot blocks cleanup outright), several sessions have to
		churn concurrently, and a checkpoint has to retire a range whose edge
		falls inside a file rather than on its boundary.
		"""
		node = self.node
		node.append_conf('orioledb.undo_buffers = 256\n')
		node.append_conf('orioledb.main_buffers = 8MB\n')
		node.append_conf('checkpoint_timeout = 1h\n')
		node.append_conf('max_connections = 30\n')
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_churn (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_churn SELECT g, repeat('x', 2000)
				FROM generate_series(1, %d) g;
		""" % ROWS)

		stop = threading.Event()
		failures = []

		def churn(worker):
			# Each round runs under a fresh snapshot, so the retain position
			# advances instead of standing still.  Keep '%' out of the SQL:
			# psycopg2 reads it as a parameter placeholder.
			low = 1 + worker * (ROWS // WORKERS)
			high = low + (ROWS // WORKERS) - 1
			con = node.connect()
			try:
				while not stop.is_set():
					con.begin('repeatable read')
					con.execute("SELECT count(*) FROM o_churn "
					            "WHERE id BETWEEN %d AND %d;" % (low, high))
					con.execute("UPDATE o_churn SET pad = repeat('y', 2000) "
					            "WHERE id BETWEEN %d AND %d;" % (low, high))
					con.commit()
			except Exception as e:
				failures.append("worker %d: %s" % (worker, e))
			finally:
				con.close()

		threads = [
		    threading.Thread(target=churn, args=(worker, ), daemon=True)
		    for worker in range(WORKERS)
		]
		for thread in threads:
			thread.start()
		try:
			for _ in range(ROUNDS):
				time.sleep(1.5)
				node.safe_psql("CHECKPOINT;")
		finally:
			stop.set()
			for thread in threads:
				thread.join(timeout=60)
		self.assertEqual(failures, [])

		# Nothing is retained any more, so a last cleanup round has to leave
		# only the checkpoint range and the live tail behind.
		for _ in range(2):
			node.safe_psql("UPDATE o_churn SET pad = repeat('z', 2000);")
			node.safe_psql("CHECKPOINT;")

		unretained = self.unretained_undo_files(node)
		self.assertEqual(
		    unretained, [], "undo files kept by nothing: %r (of %r)" %
		    (unretained, self.undo_files(node)))

#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class IndexScanInterruptTest(BaseTest):
	"""An index scan must stay interruptible, however long one fetch runs.

	o_index_scan_getnext() returns to the executor only with a row to hand
	out, so a scan whose rows are all turned down below the executor -- by its
	scan keys in o_iterate_index(), or as invisible to its snapshot inside the
	B-tree iterator -- runs through its whole range in a single call.  If
	nothing in there checks for interrupts, a query cancel or a
	statement_timeout is only seen once the scan is over: a plain SELECT then
	completes, and the timeout fires on the next statement instead.

	An error thrown in that call also skips the code that clears the table
	descriptor's noInvalidation flag; the next invalidation of the descriptor
	then trips Assert(!tableDescr->noInvalidation) in cassert builds.
	"""

	ROWS = 100000

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "orioledb.enable_stopevents = true\n")

	def create_table(self, node, fill=True):
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_scan_intr (\n"
		    "  a int NOT NULL,\n"
		    "  b int NOT NULL,\n"
		    "  PRIMARY KEY (a, b)\n"
		    ") USING orioledb;\n")
		if fill:
			node.safe_psql(
			    'postgres', "INSERT INTO o_scan_intr "
			    "SELECT g, g FROM generate_series(1, %d) g;" % self.ROWS)

	def connect_index_scan(self, node):
		con = node.connect(autocommit=True)
		con.execute("SET enable_seqscan = off;")
		con.execute("SET enable_bitmapscan = off;")
		return con

	def wait_parked(self, node, pid, timeout):
		deadline = time.time() + timeout
		while time.time() < deadline:
			if node.execute("SELECT EXISTS (SELECT 1 FROM pg_stopevents() "
			                "WHERE waiter_pids @> ARRAY[%d]);" % pid)[0][0]:
				return True
			time.sleep(0.1)
		return False

	def assert_index_scan(self, con, query):
		plan = "\n".join(
		    row[0] for row in con.execute("EXPLAIN (COSTS OFF) " + query))
		self.assertIn("index only scan of: o_scan_intr_pkey", plan)

	def assert_times_out(self, con, query):
		"""The query, which returns no rows, must end on statement_timeout.

		The query is a plain SELECT on purpose: an aggregate on top would check
		for interrupts on its own once the scan is over, and would report the
		timeout late instead of not at all.  The timeout is a small fraction
		of the time the whole scan takes, so it fires while the scan runs.
		"""
		self.assertEqual(con.execute(query), [])
		start = time.time()
		self.assertEqual(con.execute(query), [])
		scan_ms = (time.time() - start) * 1000.0
		con.execute("SET statement_timeout = %d;" % max(1, int(scan_ms / 10)))
		with self.assertRaises(Exception) as e:
			con.execute(query)
		self.assertIn("canceling statement due to statement timeout",
		              str(e.exception))
		# The timeout must not be left pending for the next statement.
		con.execute("SET statement_timeout = 0;")
		self.assertEqual(con.execute("SELECT 1;"), [(1, )])

	def test_timeout_rows_rejected_by_scan_keys(self):
		node = self.node
		node.start()
		self.create_table(node)
		con = self.connect_index_scan(node)
		# "b < 0" bounds no prefix of the key, so the scan walks the whole
		# index and o_iterate_index() turns down every row.
		query = "SELECT * FROM o_scan_intr WHERE b < 0;"
		self.assert_index_scan(con, query)
		self.assert_times_out(con, query)
		con.close()
		node.stop()

	def test_timeout_rows_invisible_to_snapshot(self):
		node = self.node
		node.start()
		self.create_table(node, fill=False)
		# Rows of a transaction still in progress are invisible to other
		# sessions; the iterator steps over them page after page without
		# returning a tuple.
		writer = node.connect()
		writer.execute("INSERT INTO o_scan_intr "
		               "SELECT g, g FROM generate_series(1, %d) g;" %
		               self.ROWS)
		con = self.connect_index_scan(node)
		query = "SELECT * FROM o_scan_intr WHERE a > 0;"
		self.assert_index_scan(con, query)
		self.assert_times_out(con, query)
		writer.rollback()
		writer.close()
		con.close()
		node.stop()

	def test_cancel_inside_scan_keeps_descr_invalidatable(self):
		node = self.node
		node.start()
		self.create_table(node)
		con = self.connect_index_scan(node)
		con_pid = con.pid
		query = "SELECT * FROM o_scan_intr WHERE b < 0;"
		self.assert_index_scan(con, query)

		# Park the scan in the middle of the index, inside
		# o_index_scan_getnext(), and cancel it there.
		ctl = node.connect(autocommit=True)
		ctl.execute("SELECT pg_stopevent_set('iterator_next', "
		            "'$.treeName == \"o_scan_intr_pkey\"');")
		t = ThreadQueryExecutor(con, query)
		t.start()
		self.assertTrue(self.wait_parked(node, con_pid, timeout=60),
		                "the scan never reached iterator_next")
		ctl.execute("SELECT pg_cancel_backend(%d);" % con_pid)
		caught = None
		try:
			t.join(60)
		except Exception as e:
			caught = e
		self.assertFalse(t.is_alive(), "the canceled scan did not end")
		self.assertIn("canceling statement due to user request", str(caught))
		ctl.execute("SELECT pg_stopevent_reset('iterator_next');")

		# Invalidate the table descriptor the canceled scan was using; the
		# canceled backend processes that invalidation on its next access.
		ctl.execute("ALTER TABLE o_scan_intr ADD COLUMN c int;")
		self.assertEqual(con.execute("SELECT count(*) FROM o_scan_intr;"),
		                 [(self.ROWS, )])
		ctl.close()
		con.close()
		node.stop()

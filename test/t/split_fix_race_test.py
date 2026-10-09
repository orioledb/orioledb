#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from testgres.enums import NodeStatus
from testgres.connection import NodeConnection
from .base_test import wait_stopevent


class SplitFixRaceTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "log_min_messages = notice\n")

		self.node.start()
		self.node.safe_psql(
		    'postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;
				CREATE TABLE IF NOT EXISTS o_split (
					 id text NOT NULL,
					 PRIMARY KEY (id)
				) USING orioledb WITH (fillfactor = 90);
				TRUNCATE o_split;
				""")
		self.connections = []

	def tearDown(self):
		if self.node.status() == NodeStatus.Running:
			self.stopAll()
		super().tearDown()

	def test_concurrent_split_fix(self):
		"""
		Two backends concurrently detect BROKEN_SPLIT on the same page
		and both enter o_btree_split_fix_for_right_page_and_unlock().
		Without the fix the second backend hits
		Assert(O_PAGE_IS(O_GET_IN_MEMORY_PAGE(rightBlkno), BROKEN_SPLIT))
		in o_btree_fix_page_split() because the first already cleared it.

		Both capture the same pageChangeCount from the right page before
		unlocking it; both pass the rightLink validation because
		pageChangeCount is not bumped when BROKEN_SPLIT is cleared.

		Schedule (via stopevents):
		  1. before_split_fix_left_lock parks con2 between unlocking the
		     right page and locking the left page.
		  2. split_fix_before_downlink parks con3 after BROKEN_SPLIT is
		     cleared and both pages are unlocked, before downlink insert.

		DELETEs reach the fix path via refind_page: the index scan follows
		the rightlink to the BROKEN_SPLIT page, and the modify refinds it
		by hint, detecting the flag.
		"""
		node = self.node

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")
		con2 = self.createConnection()
		con2.execute("SET orioledb.enable_stopevents = true;")
		con2.execute("SET enable_seqscan = off;")
		con3 = self.createConnection()
		con3.execute("SET orioledb.enable_stopevents = true;")
		con3.execute("SET enable_seqscan = off;")
		con_setup = self.createConnection()

		con2_pid = con2.pid
		con3_pid = con3.pid

		self.insertToSplitTable(node, 10, 50, 4)
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 11)

		# Failed insert of key 32 splits in the middle, creating a
		# BROKEN_SPLIT page holding keys 30, 34, 38, 42.
		con_setup.execute("SELECT pg_stopevent_set('split_fail', 'true');")
		con1.begin()
		try:
			con1.execute("INSERT INTO o_split VALUES "
			             "(repeat('x', 708) || '32');")
			self.assertTrue(False)
		except AssertionError:
			raise
		except Exception:
			pass
		finally:
			con1.commit()
		con_setup.execute("SELECT pg_stopevent_reset('split_fail');")

		# Park con2 between unlocking right and locking left.
		con_setup.execute(
		    "SELECT pg_stopevent_set('before_split_fix_left_lock',"
		    " '$pid == %d');" % con2_pid)
		# Park con3 after clearing BROKEN_SPLIT, before downlink insert.
		con_setup.execute(
		    "SELECT pg_stopevent_set('split_fix_before_downlink',"
		    " '$pid == %d');" % con3_pid)

		# con2: DELETE a key on the BROKEN_SPLIT page.
		t2 = ThreadQueryExecutor(
		    con2, "DELETE FROM o_split "
		    "WHERE id = repeat('x', 708) || '34';")
		t2.start()
		wait_stopevent(node, con2_pid)

		# con3: DELETE another key on the same BROKEN_SPLIT page.
		t3 = ThreadQueryExecutor(
		    con3, "DELETE FROM o_split "
		    "WHERE id = repeat('x', 708) || '38';")
		t3.start()
		wait_stopevent(node, con3_pid)

		# con3 cleared BROKEN_SPLIT; con2 is about to pass rightLink
		# validation and call o_btree_fix_page_split.
		# Pre-fix: Assert fires, backend crashes.
		# Post-fix: con2 sees flag already cleared, returns, retries.
		con_setup.execute(
		    "SELECT pg_stopevent_reset('before_split_fix_left_lock');")
		con_setup.execute(
		    "SELECT pg_stopevent_reset('split_fix_before_downlink');")
		t2.join()
		t3.join()

		con2.commit()
		con3.commit()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 9)
		self.assertEqual(
		    node.execute("SELECT orioledb_tbl_check('o_split'::regclass)")[0]
		    [0], True)

		self.stopAll()

	def test_concurrent_parent_split_fix(self):
		"""
		Multiple backends concurrently encounter a parent-level
		BROKEN_SPLIT via o_btree_split_is_incomplete and enter
		o_btree_insert_stack_push_split_item.  Only one should proceed
		to clear BROKEN_SPLIT and insert the downlink; the others must
		see BROKEN_SPLIT already cleared and bail out.

		Uses split_fail with $.level == 1 to create a parent-level
		BROKEN_SPLIT, then concurrent inserts trigger the fix path.
		"""
		node = self.node
		con_setup = self.createConnection()

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		self.insertToSplitTable(node, 1, 400, 4)
		initial = node.execute("SELECT COUNT(*) FROM o_split;")[0][0]

		con_setup.execute(
		    "SELECT pg_stopevent_set('split_fail', '$.level == 1');")
		con1.begin()
		try:
			con1.execute("INSERT INTO o_split"
			             "       (SELECT repeat('x', 708) || id\n"
			             "FROM generate_series(1650, 1685, 1) id);")
			self.assertTrue(False)
		except AssertionError:
			raise
		except Exception:
			pass
		finally:
			con1.commit()
		con_setup.execute("SELECT pg_stopevent_reset('split_fail');")

		threads = []
		backends = []
		for i in range(4):
			c = self.createConnection()
			backends.append(c)
			t = ThreadQueryExecutor(
			    c, "INSERT INTO o_split VALUES "
			    "(repeat('x', 708) || '%d');" % (1645 + i))
			threads.append(t)

		for t in threads:
			t.start()
		for t in threads:
			t.join()
		for c in backends:
			c.commit()

		final = node.execute("SELECT COUNT(*) FROM o_split;")[0][0]
		self.assertEqual(final, initial + 4)
		self.assertEqual(
		    node.execute("SELECT orioledb_tbl_check('o_split'::regclass)")[0]
		    [0], True)

		self.stopAll()

	def test_multiple_concurrent_leaf_fixers(self):
		"""
		Stress test: multiple backends concurrently access a table with
		a leaf-level BROKEN_SPLIT. Each backend performs a DELETE that
		traverses into the BROKEN_SPLIT page range, triggering the fix
		path in o_btree_fix_page_split. Only one should actually fix
		the split; the others must detect the already-cleared
		BROKEN_SPLIT and bail out gracefully.

		Exercises btree_split_mark_finished idempotency.
		"""
		node = self.node
		con_setup = self.createConnection()

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		self.insertToSplitTable(node, 10, 50, 4)
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 11)

		con_setup.execute("SELECT pg_stopevent_set('split_fail', 'true');")
		con1.begin()
		try:
			con1.execute("INSERT INTO o_split VALUES "
			             "(repeat('x', 708) || '32');")
			self.assertTrue(False)
		except AssertionError:
			raise
		except Exception:
			pass
		finally:
			con1.commit()
		con_setup.execute("SELECT pg_stopevent_reset('split_fail');")

		threads = []
		backends = []
		for key in ['34', '38', '42', '30']:
			c = self.createConnection()
			c.execute("SET enable_seqscan = off;")
			backends.append(c)
			t = ThreadQueryExecutor(
			    c, "DELETE FROM o_split "
			    "WHERE id = repeat('x', 708) || '%s';" % key)
			threads.append(t)

		for t in threads:
			t.start()
		for t in threads:
			t.join()
		for c in backends:
			c.commit()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 7)
		self.assertEqual(
		    node.execute("SELECT orioledb_tbl_check('o_split'::regclass)")[0]
		    [0], True)

		self.stopAll()

	def createConnection(self):
		connection = self.node.connect()
		self.connections.append(connection)
		return connection

	def stopAll(self):
		for con in self.connections:
			con.close()
		self.connections = []
		self.node.stop()

	def insertToSplitTable(self, node_or_con, insert_from, insert_to, step):
		if isinstance(node_or_con, NodeConnection):
			node_or_con.begin()
		try:
			node_or_con.execute("INSERT INTO o_split"
			                    "       (SELECT repeat('x', 708) || id\n"
			                    "FROM generate_series(%d, %d, %d) id);" %
			                    (insert_from, insert_to, step))
		finally:
			if isinstance(node_or_con, NodeConnection):
				node_or_con.commit()

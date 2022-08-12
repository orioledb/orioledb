#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from testgres.enums import NodeStatus
from testgres.connection import NodeConnection
from .base_test import wait_stopevent

import string
import random

class SplitTest(BaseTest):
	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
							  "orioledb.main_buffers = 8MB\n"
							  "log_min_messages = notice\n")

		self.node.start() # start PostgreSQL
		self.node.safe_psql('postgres',
							"""
							CREATE EXTENSION IF NOT EXISTS orioledb;
							CREATE TABLE IF NOT EXISTS o_split (
							     id text NOT NULL,
							     PRIMARY KEY (id)
							) USING orioledb;
							TRUNCATE o_split;
							""")
		self.connections = []

	def tearDown(self):
		# stops node if tests fails
		if self.node.status() == NodeStatus.Running:
			self.stopAll() # just comment it if node should not stops on fails
			pass
		super().tearDown()

	def test_incomplete_leaf_insert_fix(self):
		node = self.node
		self.insertToSplitTable(node, 10, 50, 4)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 11)

		self.failedInsertToSplitTable(con1, 51, 90, 4)

		# insert fails
		# BTree has incomplete split pages
		self.checkSplitTable(11, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		self.insertToSplitTable(con2, 51, 90, 4)

		# second insert is successfully
		# BTree has not incomplete split pages
		self.checkSplitTable(21, True)

		self.stopAll()

	def test_incomplete_leaf_insert_checkpoint_fix_middle(self):
		node = self.node
		self.insertToSplitTable(node, 1, 100, 1)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 100)

		self.failedInsertToSplitTable(con1, 510, 515, 1)

		# insert fails
		# BTree has incomplete split pages
		self.checkSplitTable(100, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		con2.execute("CHECKPOINT;")
		# checkpoint fixes split and has autonomous page writes
		# btree_check() will fail
		self.checkSplitTable(100, False)
		con2.execute("CHECKPOINT;")

		# After second checkpoint there is no incomplete split pages and lost blocks in BTree
		self.checkSplitTable(100, True)

		self.stopAll()

	def test_incomplete_leaf_insert_checkpoint_fix_end(self):
		node = self.node
		self.insertToSplitTable(node, 1, 58, 1)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 58)

		self.failedInsertToSplitTable(con1, 91, 99, 1)

		# insert fails
		# BTree has incomplete split pages
		self.checkSplitTable(58, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		con2.execute("CHECKPOINT;")
		# checkpoint fixes split and has autonomous page writes
		# btree_check() will fail
		self.checkSplitTable(58, False)
		con2.execute("CHECKPOINT;")

		# After second checkpoint there is no incomplete split pages and lost blocks in BTree
		self.checkSplitTable(58, True)

		self.stopAll()

	def test_incomplete_leaf_checkpoint_fix(self):
		node = self.node
		self.insertToSplitTable(node, 10, 50, 4)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 11)

		self.failedInsertToSplitTable(con1, 51, 90, 4)

		# insert fails
		# BTree has incomplete split pages
		self.checkSplitTable(11, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")

		con2.begin()
		con2.execute("CHECKPOINT;")
		con2.commit()

		# checkpoint fixes split and has autonomous page writes
		# btree_check() will fail
		self.checkSplitTable(11, False)
		con2.execute("CHECKPOINT;")
		# After second checkpoint there is no incomplete split pages and lost blocks
		self.checkSplitTable(11, True)

		self.stopAll()

		node.start()
		self.checkSplitTable(11, True)

	def test_incomplete_node_insert_fix(self):
		node = self.node
		self.insertToSplitTable(node, 1, 400, 4)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', '$.level == 1');")

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 100)

		self.failedInsertToSplitTable(con1, 1650, 1685, 1)

		# insert fails
		# BTree has incomplete split pages
		self.checkSplitTable(100, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		self.insertToSplitTable(con2, 1645, 1654, 1)

		# second insert is successfully
		# BTree has not incomplete split pages
		self.checkSplitTable(110, True)

	def test_incomplete_node_checkpoint_fix(self):
		node = self.node
		self.insertToSplitTable(node, 1, 400, 4)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', '$.level == 1');")

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 100)

		self.failedInsertToSplitTable(con1, 1650, 1685, 1)

		# insert fails
		# BTree has incomplete split pages
		self.checkSplitTable(100, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		con2.begin()
		con2.execute("CHECKPOINT;")
		con2.commit()

		con2.execute("CHECKPOINT;")
		# After second checkpoint there is no incomplete split pages and lost blocks in BTree
		self.checkSplitTable(100, True)

		self.stopAll()

		node.start()
		self.checkSplitTable(100, True)

	def test_incomplete_root_fix(self):
		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.failedInsertToSplitTable(con1, 1, 20, 1)

		# BTree has no incomplete split pages (fixes by call of refind_page in undo_callback)
		self.checkSplitTable(0, True)
		self.insertToSplitTable(con2, 1, 20, 1)
		self.checkSplitTable(20, True)

	def test_incomplete_split_sequence(self):
		node = self.node

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")
		self.insertToSplitTable(con1, 1, 400, 4)
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_split;")[0][0], 100)

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', '$.level == 1');")

		self.failedInsertToSplitTable(con1, 1650, 1685, 1)

		# insert fails, now incomplete split is at a node
		# BTree has incomplete split pages
		self.checkSplitTable(100, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.failedInsertToSplitTable(con1, 1550, 1585, 1)

		# another insert fails now incomplete split is at a leaf and the node
		# BTree has incomplete split pages
		self.checkSplitTable(100, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		self.insertToSplitTable(con2, 1480, 1480, 1)

		# fixes all splits
		self.checkSplitTable(101, True)

	def test_incomplete_split_sequence2(self):
		node = self.node
		con1 = self.createConnection()
		con1_pid = con1.pid
		con1.execute("SET orioledb.enable_stopevents = true;")

		self.insertToSplitTable(con1, 1, 1000, 4);

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		# incomplete split on leaf
		self.failedInsertToSplitTable(con1, 10100, 10130, 1)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")

		con1.execute("SET orioledb.enable_stopevents = true;")
		con2.execute("SELECT pg_stopevent_set('page_read', '$.level == 0 && $pid == %d');" % (con1_pid,))

		# this thread fix incomplete split on leaf and than root
		# breakpoint helps us to avoid root split fix first (???)
		t1 = ThreadQueryExecutor(con1, "INSERT INTO o_split "
								 "(SELECT repeat('x', 708) || id FROM generate_series(10100, 10130, 1) id);")
		t1.start()

		wait_stopevent(node, con1_pid)

		con3 = self.createConnection()
		con3_pid = con3.pid

		# makes an incomplete split of root
		con3.execute("SET orioledb.enable_stopevents = true;")
		con2.execute("SELECT pg_stopevent_set('split_fail', '$.level == 2 && $pid == %d');" % (con3_pid,))
		# this breakpoint helps us to avoid root split fix on apply_undo()
		con2.execute("SELECT pg_stopevent_set('before_apply_undo', '$.commit == false');")

		t3 = ThreadQueryExecutor(con3, "INSERT INTO o_split "
								 "(SELECT repeat('x', 708) || id FROM generate_series(90000, 99999, 4) id);")
		t3.start()

		wait_stopevent(node, con3_pid)

		con2.execute("SELECT pg_stopevent_reset('page_read');")

		t1.join()
		con2.execute("SELECT pg_stopevent_reset('before_apply_undo');")
		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		try:
			t3.join()
			self.assertTrue(False)
		except:
			self.assertTrue(True)

	def test_rightlink_traverse(self):
		node = self.node
		self.insertToSplitTable(node, 5, 15, 1)

		con1 = self.createConnection()
		con1.execute("SET orioledb.enable_stopevents = true;")

		con2 = self.createConnection()
		con2.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		self.failedInsertToSplitTable(con1, 100, 105, 1)
		self.checkSplitTable(11, False)

		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		con1.rollback()

		con1.execute("SET orioledb.enable_stopevents = true;")
		con2.execute("SELECT pg_stopevent_set('page_read', '$.level == 0 && $pid == %d');" % (con1.pid,))
		t1 = ThreadQueryExecutor(con1,
								 "SELECT COUNT(*) FROM o_split "
								 "WHERE id > repeat('x', 708) || 14;")
		t1.start()

		self.insertToSplitTable(node, 100, 105, 1)

		node.safe_psql('postgres',
			"CREATE TABLE IF NOT EXISTS o_eviction (\n"
			"	key integer NOT NULL,\n"
			"	val integer NOT NULL,\n"
			"	PRIMARY KEY (key)\n"
			") USING orioledb;\n"
			"CREATE UNIQUE INDEX o_eviction_ix1 ON o_eviction (val);")

		n = 100000
		con3 = self.createConnection()
		con3.execute(
			"INSERT INTO o_eviction\n"
			"	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
			(str(n), str(1), str(n)))
		con3.commit()

		con2.execute("SELECT pg_stopevent_reset('page_read');")
		result = t1.join()

		self.assertEqual(result[0][0], 6)

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

	def checkSplitTable(self, expected_count, expected_check_result):
		con = self.node.connect()
		self.assertEqual(con.execute("SELECT COUNT(*) FROM o_split;")[0][0],
						 expected_count)
		self.assertEqual(con.execute("SELECT orioledb_tbl_check('o_split'::regclass)")[0][0],
						 expected_check_result)
		con.close()

	# insert data to the split table
	def insertToSplitTable(self, node_or_con, insert_from, insert_to, step):
		if isinstance(node_or_con, NodeConnection):
			node_or_con.begin()
		try:
			node_or_con.execute("INSERT INTO o_split"
								"       (SELECT repeat('x', 708) || id\n"
										"FROM generate_series(%d, %d, %d) id);"
								% (insert_from, insert_to, step))
		finally:
			if isinstance(node_or_con, NodeConnection):
				node_or_con.commit()

	# this insert must fails with exception
	def failedInsertToSplitTable(self, con, insert_from, insert_to, step):
		try:
			self.insertToSplitTable(con, insert_from, insert_to, step)
			self.assertTrue(False)
		except AssertionError:
			raise
		except Exception:
			self.assertTrue(True);

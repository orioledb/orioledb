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
		self.node.append_conf('postgresql.conf',
							  "log_min_messages = notice\n"
							  "orioledb.enable_stopevents = true\n")

		self.node.start() # start PostgreSQL
		self.node.safe_psql('postgres',
							"CREATE EXTENSION IF NOT EXISTS orioledb;"
							"CREATE TABLE IF NOT EXISTS o_merge ("
							"     id int NOT NULL,"
							"     PRIMARY KEY (id)"
							") USING orioledb;"
							"TRUNCATE o_merge;");

	def test_non_concurrent_merge_and_checkpoint(self):
		node = self.node
		node.execute("INSERT INTO o_merge"
					 "(SELECT id FROM generate_series(1, 10000, 1) id);")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])
		node.execute("CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])

		node.execute("DELETE FROM o_merge WHERE id <= 5000;")
		node.execute("CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])
		node.stop()

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 5000);
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])

	def test_non_concurrent_merge(self):
		node = self.node
		node.execute("INSERT INTO o_merge"
					 "(SELECT id FROM generate_series(1, 10000, 1) id);")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])
		node.execute("CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])

		node.execute("DELETE FROM o_merge WHERE id <= 5000;")
		node.stop()

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 5000);
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])

	def test_concurrent_checkpoint_begin(self):
		self.concurrent_checkpoint_base(0, 2600, 1)

	def test_concurrent_checkpoint_middle(self):
		self.concurrent_checkpoint_base(2500, 5000, 0)

	def test_concurrent_checkpoint_end(self):
		self.concurrent_checkpoint_base(4000, 800, 5)

	def concurrent_checkpoint_base(self, delete_offset, bp_value, checkpoint_after_count):
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
		con2.execute("DELETE FROM o_merge WHERE id > %d AND id <= %d;"
					 % (delete_offset, 5000 + delete_offset))

		t2 = ThreadQueryExecutor(con2, "COMMIT;");
		t2.start()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")

		t1.join()
		t2.join()

		for i in range(checkpoint_after_count):
			node.execute("CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])

		con1.close()
		con2.close()
		con3.close()
		node.stop()

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_merge;")[0][0], 5000);
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_merge'::regclass)")[0][0])

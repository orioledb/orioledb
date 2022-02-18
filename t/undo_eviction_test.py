#!/usr/bin/env python3
# coding: utf-8

import unittest
import os

from .base_test import BaseTest

class UndoEvictionTest(BaseTest):
	def setUp(self):
		super().setUp()
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 64MB\n")
		node.start()
		node.safe_psql('postgres',
					 """CREATE EXTENSION IF NOT EXISTS orioledb;
					    CREATE TABLE IF NOT EXISTS o_undo_evict (
							id integer NOT NULL,
							value integer NOT NULL,
							PRIMARY KEY (id)
						) USING orioledb;""")

	def get_undo_files_count(self):
		undoDir = self.node.data_dir + '/orioledb_undo'
		count = len([name for name in os.listdir(undoDir) if os.path.isfile(os.path.join(undoDir, name))])
		return count

	def test_undo_eviction_insert(self):
		node = self.node
		con1 = node.connect()
		con1.begin()
		con1.execute("INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);")
		self.assertGreaterEqual(self.get_undo_files_count(), 1)
		con1.rollback()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 0)

		con1.begin()
		con1.execute("INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);")
		self.assertGreaterEqual(self.get_undo_files_count(), 1)
		con1.commit()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 100000)
		con1.close()
		node.stop()

	def test_undo_eviction_update(self):
		node = self.node

		node.execute("INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);")

		con1 = node.connect()

		con1.begin()
		con1.execute("UPDATE o_undo_evict SET value = value + 1;")

		self.assertEqual(node.execute("SELECT SUM(value) FROM o_undo_evict;")[0][0], 5000050000)
		self.assertEqual(con1.execute("SELECT SUM(value) FROM o_undo_evict;")[0][0], 5000150000)
		self.assertGreaterEqual(self.get_undo_files_count(), 1)

		con1.rollback()

		con1.begin()
		con1.execute("UPDATE o_undo_evict SET value = value + 1;")
		con1.commit()

		self.assertEqual(node.execute("SELECT SUM(value) FROM o_undo_evict;")[0][0], 5000150000)

		con1.close()
		node.stop()

	def test_undo_eviction_delete(self):
		node = self.node

		node.execute("INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);")

		con1 = node.connect()

		con1.begin()
		con1.execute("DELETE FROM o_undo_evict;")

		self.assertEqual(con1.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 0)
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 100000)
		self.assertGreaterEqual(self.get_undo_files_count(), 1)

		con1.rollback()

		con1.begin()
		con1.execute("DELETE FROM o_undo_evict;")
		self.assertGreaterEqual(self.get_undo_files_count(), 1)
		con1.commit()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 0)

		con1.close()
		node.stop()

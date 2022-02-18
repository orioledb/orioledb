#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest

class EvictionBGWriterTest(BaseTest):
	def test_eviction_multiple_bgwriters(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n"
						 "orioledb.bgwriter_num_workers = 2\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE o_eviction (\n"
					   "	key integer NOT NULL,\n"
					   "	val integer NOT NULL,\n"
					   "	PRIMARY KEY(key)\n"
					   ") USING orioledb;\n\n")
		n = 200000
		node.safe_psql('postgres',
			"INSERT INTO o_eviction\n"
			"	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
			(str(n), str(1), str(n)))

		node.stop()

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_eviction;")[0][0], 200000)
		node.stop()

	def test_eviction_bgwriter_invalidation(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE o_eviction (\n"
					   "	key integer NOT NULL,\n"
					   "	val integer NOT NULL,\n"
					   "	PRIMARY KEY(key)\n"
					   ") USING orioledb;\n\n")
		n = 200000

		for i in range(1, 100):
			node.safe_psql('postgres', "CREATE TABLE o_tmp (\n"
						   "	key integer NOT NULL,\n"
						   "	PRIMARY KEY(key)\n"
						   ") USING orioledb;\n")
			node.safe_psql("DROP TABLE o_tmp;")

		node.safe_psql('postgres',
			"INSERT INTO o_eviction\n"
			"	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
			(str(n), str(1), str(n)))

		node.stop()

if __name__ == "__main__":
	unittest.main()

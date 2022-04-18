#!/usr/bin/env python3
# coding: utf-8

import random

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

import time
import os
class CheckpointEvictionTest(BaseTest):

	def test_checkpoint_compressed_indices(self):
		node = self.node
		node.append_conf('postgresql.conf',
					"""
						orioledb.debug_disable_pools_limit = true
						orioledb.main_buffers = 1MB
						orioledb.free_tree_buffers = 256kB
						orioledb.debug_disable_bgwriter = true
					""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
			key SERIAL NOT NULL,
			val int NOT NULL,
			PRIMARY KEY (key)
			) USING orioledb
			WITH (compress = 5, toast_compress = 10, primary_compress = 5);
			CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix5 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix6 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix7 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix8 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix9 ON o_test (key);
		""")
		n = 35000
		m = 5
		con1 = node.connect()
		for i in range(1, m):
			con1.execute("""
			INSERT INTO o_test (val)
			(SELECT val FROM generate_series(%s, %s, 1) val);
			""" % (str(1), str(n)))
			con1.commit()
		con1.close()
		node.safe_psql("CHECKPOINT;")

		n = 2000
		m = 5
		for i in range(1, m):
			node.safe_psql("""
				DELETE FROM o_test WHERE mod(key, %d) = 0;
			""" % n)
			if n > 100:
				n = n / 2
			else:
				n = n - 10
			node.safe_psql("CHECKPOINT;")

	def concurrent_eviction_base(self, compressed, bp_value):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n"
						 "orioledb.enable_stopevents = true\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);
			CREATE TABLE IF NOT EXISTS o_evicted (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb %s;"""
			% ("WITH (primary_compress)" if compressed else "")
		)
		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_evicted_pkey\" && "
											  "$.lokey.key > %s');" % bp_value)

		con1.execute("INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1, 1000, 1) val;\n")
		con1.commit()

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)


		con2.execute("INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1001, 15000, 1) val;\n")
		con2.commit()

		n = 100000
		con2.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con2.commit()

		con2.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_evicted;")[0][0], 15000);
		node.stop()

	def test_concurrent_eviction_10(self):
		self.concurrent_eviction_base(False, 900)

	def test_concurrent_eviction_9(self):
		self.concurrent_eviction_base(False, 600)

	def test_concurrent_eviction_rand(self):
		self.concurrent_eviction_base(False, random.randint(1, 8) * 100)

	def test_concurrent_compress_eviction_10(self):
		self.concurrent_eviction_base(True, 900)

	def test_concurrent_compress_eviction_9(self):
		self.concurrent_eviction_base(True, 600)

	def test_concurrent_compress_eviction_rand(self):
		self.concurrent_eviction_base(True, random.randint(1, 8) * 100)

#!/usr/bin/env python3
# coding: utf-8

import random

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

class CheckpointEvictionTest(BaseTest):
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

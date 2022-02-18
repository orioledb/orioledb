#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent
from .base_test import wait_bgwriter_stopevent

class EvictionTest(BaseTest):
	def test_eviction_txn(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "	id integer NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_eviction (\n"
					   "	id integer NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
					   "INSERT INTO o_test\n"
					   "    (SELECT id FROM generate_series(501, 1500, 1) id);")

		n = 30000
		node.safe_psql('postgres',
					   "INSERT INTO o_eviction\n"
					   "    (SELECT id FROM generate_series(%d, %d, 1) id);" % (1, n))

		con1 = node.connect()

		con1.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(con1.execute("SELECT COUNT(*) FROM o_test;")[0][0], 1000)

		con2 = node.connect()
		con2.begin()
		con2.execute("INSERT INTO o_test\n"
					 "    (SELECT id FROM generate_series(1, 500, 1) id);")
		con2.commit()
		con2.close()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_eviction;")[0][0], 30000)
		self.assertEqual(con1.execute("SELECT COUNT(*) FROM o_test;")[0][0], 1000)
		con1.commit()

		con1.close()
		node.stop()

	def test_eviction_tree(self):
		INDEX_NOT_LOADED = "Index o_evicted_pkey: not loaded"
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n"
						 "checkpoint_timeout = 86400\n"
						 "max_wal_size = 1GB\n"
						 "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	key SERIAL NOT NULL,\n"
			"	val int NOT NULL,\n"
			"	PRIMARY KEY (key)\n"
			") USING orioledb;\n"
			"CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);\n"
			"CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);\n"
			"CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);\n"
			"CREATE TABLE IF NOT EXISTS o_evicted (\n"
			"	key SERIAL NOT NULL,\n"
			"	val int NOT NULL,\n"
			"	PRIMARY KEY (key)\n"
			") USING orioledb;\n"
			"CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key);\n")
		con1 = node.connect()
		con1.execute("INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n")

		self.assertEqual(con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)

		n = 250000
		step = 1000
		for i in range(1, n, step):
			con1.execute("INSERT INTO o_test (val)\n"
						 "	(SELECT val FROM generate_series(%d, %d, 1) val);\n" %
				(i, i + step - 1))
			con1.commit()
		con1.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;")
		con1.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;")

		con2 = node.connect()
		try:
			self.assertEqual(
				con1.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)
			self.assertEqual(
				con2.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)

			self.assertEqual(con2.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
			con2.commit()
			self.assertEqual(con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
			con1.commit()
			con2.execute("INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1, 500, 1) val;")
			con2.commit()
			self.assertEqual(con2.execute("SELECT count(*) FROM o_evicted;")[0][0], 1000)
			self.assertEqual(con2.execute("SELECT val FROM o_evicted WHERE key = 500")[0][0], 1500)
			self.assertEqual(con2.execute("SELECT count(*) FROM o_evicted WHERE val > 1500 LIMIT 1;")[0][0], 0)
			con2.commit()

			self.assertNotEqual(
				con1.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)
			self.assertNotEqual(
				con2.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)

			con1.execute("INSERT INTO o_test (val)\n"
						 "	(SELECT val FROM generate_series(%d, %d, 1) val);\n" %
				(1, n))
			con1.commit()
			con1.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;")
			con1.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;")

			self.assertEqual(
				con1.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)
			self.assertEqual(
				con2.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)

			con3 = node.connect()
			self.assertEqual(
				con3.execute("SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');")[0][0].split('\n')[0],
				INDEX_NOT_LOADED)
			con3.close()
		finally:
			con1.close()
			con2.close()

	def eviction_after_checkpoint_base(self, compressed):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n")
		node.start()
		arg1 = "WITH (primary_compress)" if compressed else ""
		arg2 = "WITH (compress)" if compressed else ""
		node.safe_psql('postgres',
			"""
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
			) USING orioledb %s;
			CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key) %s;
			"""
			% (arg1, arg2)
		)
		con1 = node.connect()
		con1.execute("INSERT INTO  o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n")

		# different errors when CHECKPOINT called even times or odd
		node.safe_psql("CHECKPOINT;")
		con1.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
					 (1, 999))
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		con1.execute("SELECT * FROM o_evicted;")
		self.assertEqual(con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
		n = 20000
		con1.execute("INSERT INTO o_test (val)\n"
					 "	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con1.commit()
		con1.close()
		node.stop()

	def test_eviction_after_checkpoint(self):
		self.eviction_after_checkpoint_base(False)

	def test_eviction_compress_after_checkpoint(self):
		self.eviction_after_checkpoint_base(True)

	def test_eviction_after_checkpoint_con1(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "  key SERIAL NOT NULL,\n"
					   "  val int NOT NULL\n"
					   ") USING orioledb;\n")

		con1 = node.connect()
		# different errors when CHECKPOINT called even times or odd

		con1.execute("CHECKPOINT;")
		con1.begin()
		con1.execute("INSERT INTO o_test (val)\n"
					 "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
					 (1, 999))
		con1.commit()
		con1.execute("CHECKPOINT;")

		n = 20000
		con1.execute("INSERT INTO o_test (val)\n"
			"  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con1.commit()
		con1.close()
		node.stop()

	def test_eviction_concurrent_checkpoint_next_tbl(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n"
						 "bgwriter_delay = 400\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_first (\n"
					   "	id int NOT NULL,\n"
					   "	PRIMARY KEY (id)"
					   ") USING orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_second (\n"
					   "	id text  NOT NULL\n"
					   ") USING orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "	key SERIAL NOT NULL,\n"
					   "	val int NOT NULL\n"
					   ") USING orioledb;\n")

		con1 = node.connect()
		con2 = node.connect()

		con1.execute("CHECKPOINT;")
		con2.execute("SELECT pg_stopevent_set('checkpoint_table_start',\n"
											 "format(E'$.table.reloid == \\045s',\n"
													 "'o_second'::regclass::oid)::jsonpath);")
		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.execute("INSERT INTO o_first VALUES (0);")
		con2.execute("INSERT INTO o_second VALUES ('ajnslajslkdlaksjdlkajlsdkjlakjsdl')")
		con2.execute("SELECT * FROM o_first;")
		con2.execute("SELECT * FROM o_second;")

		n = 50000
		con2.execute("INSERT INTO o_test (val)\n"
					 "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
					 (str(1), str(n)))
		con2.commit()

		con2.execute("SELECT * FROM o_first;")
		con2.execute("SELECT pg_stopevent_reset('checkpoint_table_start')")
		t1.join()

		con1.execute("CHECKPOINT")
		con1.execute("CHECKPOINT")
		con1.close()
		con2.close()
		node.stop()

	def eviction_concurrent_checkpoint_base(self, compressed):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n"
						 "bgwriter_delay = 400\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_checkpoint (
					     id text NOT NULL,
						 PRIMARY KEY (id) %s
					   ) USING orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
					     key SERIAL NOT NULL,
					     val int NOT NULL
					   ) USING orioledb;
					   """
					   % ("WITH (compress)" if compressed else "")
		)

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con1.begin()
		con1.execute("INSERT INTO o_test (val)\n"
					 "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
					 (1, 7999))
		con1.commit()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											 "$.treeName == \"ctid_primary\" && "
											 "$.lokey.ctid[0] >= 2');")
		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		n = 20000
		con2.execute("INSERT INTO o_test (val)\n"
			"  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con2.commit()

		con2.execute("SELECT * FROM o_checkpoint;")
		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		con1.execute("CHECKPOINT;")
		con1.execute("CHECKPOINT;")
		con1.execute("CHECKPOINT;")
		con1.close()
		con2.close()
		con3.close()
		node.stop()

	def test_eviction_concurrent_checkpoint(self):
		self.eviction_concurrent_checkpoint_base(False)

	def test_eviction_compress_concurrent_checkpoint(self):
		self.eviction_concurrent_checkpoint_base(True)

	def test_eviction_concurrent_drop(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n"
						 "bgwriter_delay = 200\n"
						 "orioledb.enable_stopevents = true\n"
						 "checkpoint_timeout = 86400\n"
						 "max_wal_size = 1GB\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_evicted (\n"
					   "  id int8 NOT NULL,\n"
					   "  val int8 NOT NULL,\n"
					   "  PRIMARY KEY (id, val)\n"
					   ") USING orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "  id int8 NOT NULL,\n"
					   "  val int8 NOT NULL,\n"
					   "  PRIMARY KEY (id, val)\n"
					   ") USING orioledb;\n")

		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('after_write_page', '$backendType == \"orioledb background writer\"');")

		n = 150000
		con1.execute("INSERT INTO o_evicted (id, val)\n"
					 "  (SELECT id, id + 1 FROM generate_series(%s, %s, 1) id);\n" %
					 (str(1), str(n)))
		con1.commit()
		wait_bgwriter_stopevent(node)

		n = 150000
		con1.execute("INSERT INTO o_test (id, val)\n"
					 "  (SELECT id, id + 1 FROM generate_series(%s, %s, 1) id);\n" %
					 (str(1), str(n)))
		con1.commit()

		t1 = ThreadQueryExecutor(con1, "DROP TABLE o_evicted;")
		t1.start()

		self.assertTrue(con2.execute("SELECT pg_stopevent_reset('after_write_page');")[0][0])

		t1.join()
		con1.commit()

		con1.close()
		con2.close()
		node.stop()

	def test_eviction_and_change_main_buffers_size(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n"
						 "log_min_messages = notice\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE TABLE IF NOT EXISTS o_evicted (
				id int NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;""")
		con1 = node.connect()
		con2 = node.connect()

		node.execute("CHECKPOINT;")
		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(0, 75000, 5) id);\n")
		con1.commit()

		n = 200000
		con2.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con2.commit()

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(1, 15000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(2, 15000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(3, 15000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(4, 10000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")
		con1.close()
		con2.close()
		node.stop()

		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 10MB\n")
		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_evicted;")[0][0], 26001)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass)")[0][0])

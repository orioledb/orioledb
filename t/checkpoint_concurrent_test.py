#!/usr/bin/env python3
# coding: utf-8

import random

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent
from .base_test import wait_checkpointer_stopevent
from .base_test import wait_bgwriter_stopevent

class CheckpointConcurrentTest(BaseTest):
	def test_checkpoint_on_droped_table(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 250) FROM generate_series(1, 1000, 1) id);\n")
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.lokey.id > \"0500\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()

		t2 = ThreadQueryExecutor(con2, "DROP TABLE o_checkpoint;")
		t2.start()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		con3.execute("CREATE TABLE IF NOT EXISTS o_checkpoint2 (\n"
					 "	id text NOT NULL,\n"
					 "	PRIMARY KEY (id)\n"
					 ") USING orioledb;")
		con3.execute("INSERT INTO o_checkpoint2\n"
					 "	(SELECT to_char(id, 'fm0000') || repeat('x', 250) FROM generate_series(1, 500, 1) id);\n")
		con3.commit()

		t2.join()
		con2.commit()
		t1.join()

		con1.close()
		con2.close()
		con3.close()
		node.stop()

	def test_checkpoint_concurrent_to_truncate(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "checkpoint_flush_after = 0\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 1000) FROM generate_series(1, 1000, 1) id);\n")
		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('checkpoint_writeback',\n"
											 "'$.treeName == \"o_checkpoint_pkey\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		node.safe_psql("TRUNCATE o_checkpoint;")

		con2.execute("SELECT pg_stopevent_reset('checkpoint_writeback')")
		con2.close()

		t1.join()

		con1.close()
		node.stop()

	def test_checkpoint_load_btree_after_truncate(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n"
						 "orioledb.enable_stopevents = true\n"
						 "bgwriter_delay = 400\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_first (\n"
					   "	id int NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_second (\n"
					   "	id text  NOT NULL\n"
					   ") USING orioledb;\n")

		con1 = node.connect()
		con2 = node.connect()

		# changes table relnode
		con2.execute("TRUNCATE o_first;")
		con2.execute("SELECT pg_stopevent_set('checkpoint_table_start',\n"
											 "format(E'$.table.reloid == \\045s',\n"
													 "'o_second'::regclass::oid)::jsonpath);")
		con2.commit()

		# loads unused before o_second
		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.execute("SELECT pg_stopevent_reset('checkpoint_table_start')")
		t1.join()

		con1.execute("CHECKPOINT");
		con1.execute("CHECKPOINT");
		con1.execute("CHECKPOINT");

		con1.close()
		con2.close()
		node.stop()

	def test_checkpoint_create_table(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)"
					   ") USING orioledb;\n"
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 250) FROM generate_series(1, 1000, 1) id);\n")
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.lokey.id > \"0250\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("CREATE TABLE IF NOT EXISTS o_checkpoint2 (\n"
				   "	id text NOT NULL,\n"
				   "	PRIMARY KEY (id)\n"
				   ") USING orioledb;")
		con2.execute("SELECT * FROM o_checkpoint2;")
		con2.commit()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()
		con1.execute("CHECKPOINT;")
		con1.execute("CHECKPOINT;")
		con1.execute("CHECKPOINT;")

		con1.close()
		con2.close()
		con3.close()
		node.stop()

	def test_checkpoint_primary_insert_commit(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id int NOT NULL,\n"
					   "	secid int NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "CREATE UNIQUE INDEX o_checkpont_ix1 ON o_checkpoint (secid);\n")
		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('checkpoint_index_start',\n"
											 "'$.treeName == \"o_checkpont_ix1\"');")
		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()

		con2.begin()
		con2.execute("INSERT INTO o_checkpoint VALUES (1, 2);")
		con2.commit()

		con2.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")
		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		con1 = node.connect()
		self.assertEqual(con1.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 1)
		con1.close()

	def test_checkpoint_rightlink_ok(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10, 26, 4) id);\n"
		)
		con1 = node.connect()
		connection1_pid = con1.pid
		con2 = node.connect()
		con3 = node.connect()

		con1.execute("SET orioledb.enable_stopevents = true;")
		con2.execute("SELECT pg_stopevent_set('page_split', 'true');")

		t1 = ThreadQueryExecutor(con1, "INSERT INTO o_checkpoint\n"
								 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(27, 27, 1) id);")
		t1.start()
		wait_stopevent(node, connection1_pid)


		t2 = ThreadQueryExecutor(con3, "CHECKPOINT;")
		t2.start()

		con2.execute("SELECT pg_stopevent_reset('page_split');")
		t1.join()
		t2.join()
		con1.commit()

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT count(*) FROM o_checkpoint;")[0][0], 6)
		node.stop()

	def test_concurrent_compress_eviction_change_compressed_size(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 """
						 orioledb.debug_disable_bgwriter = true
						 orioledb.main_buffers = 8MB
						 log_min_messages = notice
						 orioledb.enable_stopevents = true
						 """)
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
			) USING orioledb WITH (compress = 10);""")
		con1 = node.connect()
		con2 = node.connect()

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(0, 150000, 5) id);\n")
		con1.commit()

		con1.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_evicted_pkey\" && "
											  "$.lokey.id > 100000');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		n = 200000
		con1.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con1.commit()

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(1, 100000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(2, 100000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(3, 100000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(4, 50000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 16MB\n")
		node.start()
		node.execute("CHECKPOINT;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_evicted;")[0][0], 100001)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass)")[0][0])

	def test_concurrent_compress_create_map_eviction(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 16MB\n"
						 "log_min_messages = notice\n"
						 "orioledb.enable_stopevents = true\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_evicted1 (
				id int NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb WITH (compress = 10);
			CREATE TABLE IF NOT EXISTS o_evicted2 (
				id int NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb WITH (compress = 10);
			CREATE INDEX o_evicted2_ix2 ON o_evicted2 (val);
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;""")
		con1 = node.connect()
		con2 = node.connect()

		con1.execute("INSERT INTO o_evicted1 (SELECT id, id + 1 FROM generate_series(0, 150000, 5) id);\n")
		con1.commit()
		con1.execute("INSERT INTO o_evicted2 (SELECT id, id + 1 FROM generate_series(0, 150000, 5) id);\n")
		con1.commit()

		n = 200000
		con1.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con1.commit()

		con1.execute("CHECKPOINT;")

		con1.execute("INSERT INTO o_evicted1 (SELECT id, id + 1 FROM generate_series(1, 100000, 5) id);\n")
		con1.commit()
		con1.execute("INSERT INTO o_evicted2 (SELECT id, id + 1 FROM generate_series(1, 100000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted1 (SELECT id, id + 1 FROM generate_series(2, 100000, 5) id);\n")
		con1.commit()
		con1.execute("INSERT INTO o_evicted2 (SELECT id, id + 1 FROM generate_series(2, 100000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("CHECKPOINT;")

		con1.execute("SELECT pg_stopevent_set('before_blkno_lock', 'true');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con1.execute("INSERT INTO o_evicted1 (SELECT id, id + 1 FROM generate_series(3, 100000, 5) id);\n")
		con1.commit()
		con1.execute("INSERT INTO o_evicted2 (SELECT id, id + 1 FROM generate_series(3, 100000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("INSERT INTO o_evicted1 (SELECT id, id + 1 FROM generate_series(4, 50000, 5) id);\n")
		con1.commit()
		con1.execute("INSERT INTO o_evicted2 (SELECT id, id + 1 FROM generate_series(4, 50000, 5) id);\n")
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute("SELECT pg_stopevent_reset('before_blkno_lock');")
		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 16MB\n")
		node.start()
		node.execute("CHECKPOINT;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_evicted1;")[0][0], 100001)
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_evicted2;")[0][0], 100001)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted1'::regclass)")[0][0])
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted2'::regclass)")[0][0])
		node.execute("CHECKPOINT;")
		node.execute("CHECKPOINT;")

	def test_checkpoint_concurrent_bruteforce_eviction_load(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		for t in range(0, 40):
			node.safe_psql('postgres',
				"CREATE TABLE IF NOT EXISTS t%d (\n"
				"	val text\n"
				") USING orioledb;" % t)
			node.safe_psql('postgres',
							"INSERT INTO t%d\n"
							"	(SELECT 'zzzzzzzzzzzzzzzz' || id FROM generate_series(1, 10000, 1) id);\n" %
						   (t))

		con1 = node.connect()

		for i in range(0, 10):
			t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
			t1.start()
			for j in range(0, 3):
				node.safe_psql('postgres',
								"SELECT * FROM t%d;" % random.randint(0, 9))
			t1.join()

		con1.close()
		node.stop()

	def test_checkpoint_autonomous_evict_random(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "log_min_messages = notice\n"
						 "orioledb.main_buffers = 48MB\n"
						 "orioledb.enable_stopevents = true\n"
						 "max_parallel_workers_per_gather = 0\n")
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql('postgres', """CREATE TABLE IF NOT EXISTS o_checkpoint (
										id text NOT NULL,
										PRIMARY KEY (id)
									) USING orioledb;""")
		node.safe_psql('postgres', "INSERT INTO o_checkpoint (SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(3500,7000,100) id);")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											  "'$.action == \"walkUpwards\" && "
											   "$.treeName == \"o_checkpoint_pkey\" && "
											   "($.nextKey.value.id > \"%d\" || $.nextKey.type == \"greatest\")');" % (random.randint(3500, 7000)))

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		for i in range(3500, 7200, 100):
			con2.execute("INSERT INTO o_checkpoint (SELECT to_char(id, 'fm0000') || repeat('y', 2500) FROM generate_series(%s, %s) id);" % (str(i), str(i + 99)))
		con2.commit()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.append_conf("postgresql.conf",
						 "orioledb.main_buffers = 64MB\n")
		node.start()
		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3736)
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_concurrent_io(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 """
							orioledb.debug_disable_bgwriter = true
							orioledb.enable_stopevents = true
						 """)

		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id text NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
		""")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con2_pid = con2.pid

		con1.execute("""
			INSERT INTO o_checkpoint (SELECT to_char(id, 'fm0000') ||
									  repeat('x', 2500)
									  FROM generate_series(1,50) id);
		""")
		con1.commit()
		con1.execute("""
			SELECT pg_stopevent_set('after_ionum_set',
									'$.treeName == "o_checkpoint_pkey" &&
									 $.level == 1 &&
									 $.hikey.id like_regex "^0007"');
		""")
		t2 = ThreadQueryExecutor(con2, """
			SELECT orioledb_write_pages('o_checkpoint'::regclass);
		""")
		t2.start()
		wait_stopevent(node, con2_pid)

		con1.execute("""
			SELECT pg_stopevent_set('checkpoint_step',
									'$.treeName == "o_checkpoint_pkey" &&
									 $.level == 2 &&
									 $.action == "walkDownwards" &&
									 $.lokey == null');
		""")

		t3 = ThreadQueryExecutor(con3, """
			CHECKPOINT;
		""")
		t3.start()

		wait_checkpointer_stopevent(node)
		con1.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		con1.execute("SELECT pg_stopevent_reset('after_ionum_set')")

		t2.join()
		t3.join()

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 50)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_concurrent_io_rightmost(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 """
							orioledb.debug_disable_bgwriter = true
							orioledb.enable_stopevents = true
						 """)

		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id text NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
		""")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con2_pid = con2.pid

		con1.execute("""
			INSERT INTO o_checkpoint (SELECT to_char(id, 'fm0000') ||
									  repeat('x', 2500)
									  FROM generate_series(1,50) id);
		""")
		con1.commit()
		con1.execute("""
			SELECT pg_stopevent_set('after_ionum_set',
									'$.treeName == "o_checkpoint_pkey" &&
									 $.level == 1 &&
									 $.hikey == null');
		""")

		t2 = ThreadQueryExecutor(con2, """
			SELECT orioledb_write_pages('o_checkpoint'::regclass);
		""")
		t2.start()
		wait_stopevent(node, con2_pid)

		con1.execute("""
			SELECT pg_stopevent_set('checkpoint_step',
									'$.treeName == "o_checkpoint_pkey" &&
									 $.level == 0 &&
									 $.action == "walkUpwards" &&
									 $.nextKey.type == "greatest"');
		""")
		t3 = ThreadQueryExecutor(con3, """
			CHECKPOINT;
		""")
		t3.start()

		wait_checkpointer_stopevent(node)
		con1.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		con1.execute("SELECT pg_stopevent_reset('after_ionum_set')")

		t3.join()

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 50)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

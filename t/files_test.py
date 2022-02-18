#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import re
import os
import glob
import sys

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent

from testgres.enums import NodeStatus

class FilesTest(BaseTest):
	IGNORED_FILES_PATTERN = "(1_[2-9].*)"
	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
							  "log_min_messages = notice\n")

	def test_map_files_sorted(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	key int NOT NULL,\n"
			"	value int NOT NULL\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT i, i + 1 FROM generate_series(1, 10000, 1) i);"
		)

		for i in range(10):
			node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
			node.safe_psql('postgres', "CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass, TRUE);")[0][0])
		node.stop(['-m', 'immediate'])

		node.start()
		for i in range(5):
			node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
			node.safe_psql('postgres', "CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass, TRUE);")[0][0])
		node.stop(['-m', 'immediate'])

		node.start()
		for i in range(5):
			node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
			node.safe_psql('postgres', "CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass, TRUE);")[0][0])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass, TRUE);")[0][0])
		self.assertEqual(node.execute('postgres', "SELECT count(*) FROM o_test;")[0][0], 10000)
		node.stop()

	def seq_scan_base(self, compressed):
		node = self.node
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	key int NOT NULL,\n"
			"	value int NOT NULL\n"
			") USING orioledb %s;\n"
			"INSERT INTO o_test\n"
			"	(SELECT i, i + 1 FROM generate_series(1, 10000, 1) i);"
			% ("WITH (compress)" if compressed else "")
		)
		node.safe_psql('postgres', "CHECKPOINT;")
		node.safe_psql('postgres', "SELECT orioledb_evict_pages('o_test'::regclass, 0);")

		con1 = node.connect()
		con2 = node.connect()
		con1_pid = con1.pid
		con2_pid = con2.pid
		con1.execute("SELECT pg_stopevent_set('scan_disk_page', 'true');")
		con2.execute("SET orioledb.enable_stopevents = true;")
		t1 = ThreadQueryExecutor(con2, "SELECT count(*) FROM o_test;")
		t1.start()

		wait_stopevent(node, con2_pid)

		for i in range(10):
			node.safe_psql('postgres',
						   "INSERT INTO o_test (SELECT i, i + 1 FROM generate_series(%d, %d, 1) i);"
						   % (10000 + i * 100, 10000 + i * 100 + 99))
			node.safe_psql('postgres', "CHECKPOINT;")
			node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")

		con1.execute("SELECT pg_stopevent_reset('scan_disk_page');")
		self.assertEqual(t1.join()[0][0], 10000)
		con2.close()
		if not compressed:
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass, TRUE);")[0][0])

		old_size = node.execute("SELECT orioledb_relation_size('o_test'::regclass)")[0][0]
		for i in range(10):
			node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
			node.safe_psql('postgres', "CHECKPOINT;")
		new_size = node.execute("SELECT orioledb_relation_size('o_test'::regclass)")[0][0]

		self.assertLessEqual(new_size, old_size)
		con1.close()

	def test_seq_scan_simple(self):
		self.seq_scan_base(False)

	def test_seq_scan_compressed(self):
		self.seq_scan_base(True)

	def test_check_if_tmp_not_exist(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql('postgres', "CHECKPOINT;")

		node.safe_psql('postgres',
					   "CREATE TABLE IF NOT EXISTS empty (\n"
					   "    id integer NOT NULL\n"
					   ") USING orioledb;\n")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('empty'::regclass, TRUE);")[0][0])
		node.stop()

	# checks orioledb.remove_old_checkpoint_files = true behavion (default)
	def test_tmp_map_cleanup(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.safe_psql("INSERT INTO o_test\n"
					   "	(SELECT id, id || 'val' FROM generate_series(1001, 2000, 1) id);\n")
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.stop(['-m', 'immediate'])

		orioledb_dir = node.data_dir + "/orioledb_data"
		map_files = ""
		tmp_files = ""

		for f in os.listdir(orioledb_dir):
			if re.match(".*\.map$", f):
				map_files = map_files + " " + f
			if re.match(".*\.tmp$", f):
				tmp_files = tmp_files + " " + f

		# this files should be deleted
		self.assertFalse(bool(re.match(".*-[1-3]\.map.*", map_files)))
		# this files should exists
		self.assertTrue(bool(re.match(".*-2\.tmp.*", tmp_files)))
		self.assertTrue(bool(re.match(".*-4\.map.*", map_files)))

		node.start()
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.stop(['-m', 'immediate'])

		orioledb_dir = node.data_dir + "/orioledb_data"
		map_files = ""
		tmp_files = ""

		for f in os.listdir(orioledb_dir):
			if re.match(".*\.map$", f):
				map_files = map_files + " " + f
			if re.match(".*\.tmp$", f):
				tmp_files = tmp_files + " " + f

		# this files should be deleted
		self.assertFalse(bool(re.match(".*-[0-35]\.map.*", map_files)))
		self.assertFalse(bool(re.match(".*-[0-4]\.tmp.*", tmp_files)))

		# this files should exists
		self.assertTrue(bool(re.match(".*-4\.map.*", map_files)))
		self.assertTrue(bool(re.match(".*-7\.map.*", map_files)))

	def test_tmp_map_cleanup_no_error(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")

		con = node.connect()
		for i in range(5):
			con.execute("CHECKPOINT;")
			con.begin()
			con.execute("CREATE TABLE IF NOT EXISTS o_test%d (\n"
						"	id integer NOT NULL,\n"
						"	val text NOT NULL\n"
						") USING orioledb;\n" % (i))
			con.execute("INSERT INTO o_test%d\n"
						"	(SELECT id, id || 'val'\n"
						"     FROM generate_series(1, 100, 1) id);\n" % (i))
			con.commit()
			con.execute("CHECKPOINT;")
		con.close()
		node.stop()

		node.start()
		con = node.connect()
		for i in range(5):
			self.assertEqual(con.execute("SELECT COUNT(*) FROM o_test%d;" % (i))[0][0],
							 100)
		con.close()
		node.stop()

	# checks orioledb.remove_old_checkpoint_files = false behavion
	def test_tmp_map_noncleanup(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.remove_old_checkpoint_files = false\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', 'CHECKPOINT;')
		node.safe_psql("INSERT INTO o_test\n"
					   "	(SELECT id, id || 'val' FROM generate_series(1001, 2000, 1) id);\n")
		node.stop()

		node.start()
		node.stop(['-m', 'immediate'])

		orioledb_dir = node.data_dir + "/orioledb_data"
		map_files = ""
		tmp_files = ""

		for f in os.listdir(orioledb_dir):
			if re.match(".*\.map$", f):
				map_files = map_files + " " + f
			if re.match(".*\.tmp$", f):
				tmp_files = tmp_files + " " + f

		# all files should exists
		self.assertTrue(bool(re.match(".*-0\.map.*", map_files)))
		self.assertTrue(bool(re.match(".*-1\.map.*", map_files)))
		self.assertTrue(bool(re.match(".*-2\.map.*", map_files)))
		self.assertTrue(bool(re.match(".*-2\.tmp.*", tmp_files)))

	def test_drop_table_cleanup(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', "DROP TABLE o_test;")
		node.stop()

		orioledb_dir = node.data_dir + "/orioledb_data"
		all_files = ""
		for f in os.listdir(orioledb_dir):
			if not re.match(self.IGNORED_FILES_PATTERN, f):
				# do not check files of trees duplicating syscach
				all_files = all_files + " " + f

		self.assertFalse(bool(re.match(".*\.map", all_files)))
		self.assertFalse(bool(re.match(".*\.tmp", all_files)))
		self.assertFalse(bool(re.match(".*evt", all_files)))
		self.assertFalse(bool(re.match("[0-9]*_[0-9]*_[0-9]*", all_files)))

	def test_drop_extension_cleanup(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', "DROP EXTENSION orioledb CASCADE;")
		node.stop()

		orioledb_dir = node.data_dir + "/orioledb_data"
		all_files = ""
		for f in os.listdir(orioledb_dir):
			if not re.match(self.IGNORED_FILES_PATTERN, f):
				# DROP EXTENSION saves o_tables BTree files, skip it
				all_files = all_files + " " + f

		self.assertFalse(bool(re.match(".*\.map", all_files)))
		self.assertFalse(bool(re.match(".*\.tmp", all_files)))
		self.assertFalse(bool(re.match(".*evt", all_files)))
		self.assertFalse(bool(re.match("[0-9]*_[0-9]*_[0-9]*", all_files)))

	def test_drop_database_cleanup(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres', "CREATE DATABASE t;")
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				id integer NOT NULL,
				val text
			) USING orioledb;""")
		node.safe_psql('t', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				id integer NOT NULL,
				val text
			) USING orioledb;
			CREATE TABLE IF NOT EXISTS o_test2 (
				id integer NOT NULL,
				val text
			) USING orioledb;
			INSERT INTO o_test
				(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);
			INSERT INTO o_test2
				(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);
			""")
		deleted = node.execute('t',
			"SELECT 'o_test'::regclass::oid, 'o_test2'::regclass::oid")[0]
		reloids = node.execute('postgres',
							   "SELECT * FROM orioledb_table_oids();")
		reloids = [item[1] for item in reloids if item[1] not in deleted]
		node.safe_psql('postgres', "DROP DATABASE t;")
		new_reloids = node.execute('postgres',
								   "SELECT * FROM orioledb_table_oids();")
		new_reloids = [item[1] for item in new_reloids]
		node.safe_psql('postgres', "DROP TABLE o_test;")
		node.stop()

		self.assertEqual(reloids, new_reloids)

		orioledb_dir = node.data_dir + "/orioledb_data"
		all_files = ""
		for f in os.listdir(orioledb_dir):
			if not re.match(self.IGNORED_FILES_PATTERN, f):
				# DROP EXTENSION saves o_tables BTree files, skip it
				all_files = all_files + " " + f

		self.assertFalse(bool(re.match(".*\.map", all_files)))
		self.assertFalse(bool(re.match(".*\.tmp", all_files)))
		self.assertFalse(bool(re.match(".*evt", all_files)))
		self.assertFalse(bool(re.match("[0-9]*_[0-9]*_[0-9]*", all_files)))

	def test_evt_cleanup(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n"
						 "orioledb.main_buffers = 8MB\n"
						 "log_min_messages = notice\n"
						 "checkpoint_timeout = 86400\n"
						 "max_wal_size = 5GB\n"
						 "orioledb.debug_disable_bgwriter = true\n")
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
			) USING orioledb;""")

		con1 = node.connect()
		con1.begin()
		con1.execute("INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n")
		con1.commit()
		self.assertEqual(con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)

		con1.execute("CHECKPOINT")

		con1.begin()
		con1.execute("""CREATE TABLE IF NOT EXISTS o_evicted_after_chkp (
							key SERIAL NOT NULL,
							val int NOT NULL,
							PRIMARY KEY (key)
					 ) USING orioledb;""")
		con1.execute("INSERT INTO o_evicted_after_chkp (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n")
		con1.commit()
		self.assertEqual(con1.execute("SELECT count(*) FROM o_evicted_after_chkp;")[0][0], 500)

		n = 200000
		con1.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con1.commit()

		evt_files = [f for f in glob.glob(node.data_dir + "/orioledb_data/*.evt")]
		self.assertNotEqual(len(evt_files), 0)

		con1.close()
		node.stop()

		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 100MB\n")
		node.start()
		node.safe_psql('postgres', 'CHECKPOINT;')
		evt_files = [f for f in glob.glob(node.data_dir + "/orioledb_data/*.evt")]
		self.assertEqual(len(evt_files), 0)

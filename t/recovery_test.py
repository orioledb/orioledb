#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import string
import random

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import generate_string
from .base_test import wait_stopevent
from .base_test import wait_checkpointer_stopevent

from testgres.enums import NodeStatus

class RecoveryTest(BaseTest):
	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
							  "log_min_messages = notice\n")

	def checkpoint_simple_base(self, compressed):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id) %s
			) USING orioledb;
			INSERT INTO o_test
				(SELECT id, id || 'val' FROM generate_series(1, 10000, 1) id);
			"""
			% ("WITH (compress)" if compressed else "")
		)
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass);")[0][0])
		node.safe_psql('postgres', "UPDATE o_test SET val = 'xxx' WHERE id % 1000 = 0;")
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass);")[0][0])
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_test'::regclass);")[0][0])
		node.stop() # stop PostgreSQL

	def test_checkpoint_simple(self):
		self.checkpoint_simple_base(False)

	def test_checkpoint_compress_simple(self):
		self.checkpoint_simple_base(True)

	def checkpoint_multiple_base(self, compressed):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key int NOT NULL,
				value int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb %s;
			INSERT INTO o_test
				(SELECT i, i + 1 FROM generate_series(1, 10000, 1) i);
			"""
			% ("WITH (primary_compress)" if compressed else "")
		)
		node.safe_psql('postgres', "CHECKPOINT;")
		node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
		node.stop(['-m', 'immediate'])

		node.start()
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertEqual(
			node.execute('postgres', "SELECT orioledb_tbl_check('o_test'::regclass);")[0][0],
			True)
		node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
		node.stop(['-m', 'immediate'])

		node.start()
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertEqual(
			node.execute('postgres', "SELECT orioledb_tbl_check('o_test'::regclass);")[0][0],
			True)
		node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
		self.assertEqual(
			node.execute('postgres', "SELECT value FROM o_test WHERE key = 10;")[0][0],
			14)
		self.assertEqual(
			node.execute('postgres', "SELECT orioledb_tbl_check('o_test'::regclass);")[0][0],
			True)
		node.safe_psql('postgres', "UPDATE o_test SET value = value + 1 WHERE key % 10 = 0;")
		node.safe_psql('postgres', "CHECKPOINT;")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			node.execute('postgres', "SELECT value FROM o_test WHERE key = 10;")[0][0],
			15)
		self.assertEqual(
			node.execute('postgres', "SELECT orioledb_tbl_check('o_test'::regclass);")[0][0],
			True)
		node.stop()

	def test_checkpoint_multiple(self):
		self.checkpoint_multiple_base(False)

	def test_checkpoint_compress_multiple(self):
		self.checkpoint_multiple_base(True)

	def test_checkpoint_simple_in_progress(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);\n"
		)
		con1 = node.connect()
		con1.execute("INSERT INTO o_test (SELECT id, id || 'val' FROM generate_series(101, 200, 1) id);")

		node.safe_psql('postgres', "CHECKPOINT;")

		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 100)
		node.stop()

	def test_primary_xip_secondary_tuples_insert(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
						   id integer NOT NULL,
						   id2 integer NOT NULL,
						   id3 integer NOT NULL,
						   PRIMARY KEY (id)
					   ) USING orioledb;
					   CREATE UNIQUE INDEX o_test_ix1 ON o_test (id2);
					   CREATE INDEX o_test_ix2 ON o_test (id3);
					   """)
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		con1.execute("INSERT INTO o_test (SELECT id, id + 1, id + 3 FROM generate_series(1, 100, 1) id);")

		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_ix1\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)
		con1.commit()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")

		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(100, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(100, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(100, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.execute("DELETE FROM o_test;")
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.stop()

	def test_primary_xip_secondary_tuples_delete(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
						   id integer NOT NULL,
						   id2 integer NOT NULL,
						   id3 integer NOT NULL,
						   PRIMARY KEY (id)
					   ) USING orioledb;
					   CREATE UNIQUE INDEX o_test_ix1 ON o_test (id2);
					   CREATE INDEX o_test_ix2 ON o_test (id3);
					   """)
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		con1.execute("INSERT INTO o_test (SELECT id, id + 1, id + 3 FROM generate_series(1, 100, 1) id);")
		con1.commit()
		con1.begin()
		con1.execute("DELETE FROM o_test WHERE mod(id, 5) = 0;")

		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_ix1\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)
		con1.commit()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")

		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(80, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(80, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(80, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.execute("DELETE FROM o_test;")
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.stop()

	def test_primary_xip_secondary_tuples_update(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
						   id integer NOT NULL,
						   id2 integer NOT NULL,
						   id3 integer NOT NULL,
						   PRIMARY KEY (id)
					   ) USING orioledb;
					   CREATE UNIQUE INDEX o_test_ix1 ON o_test (id2);
					   CREATE INDEX o_test_ix2 ON o_test (id3);
					   """)
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		con1.execute("INSERT INTO o_test (SELECT id, id + 1, id + 3 FROM generate_series(1, 100, 1) id);")
		con1.commit()
		con1.begin()
		con1.execute("UPDATE o_test SET id = id + 100 WHERE mod(id, 10) = 0;")
		con1.execute("UPDATE o_test SET id2 = id2 + 100 WHERE mod(id, 3) = 0;")
		con1.execute("UPDATE o_test SET id3 = id3 + 100 WHERE mod(id, 4) = 0;")
		con1.execute("UPDATE o_test SET id = id + 100, id2 = id2 + 100, id3 = id3 + 100 WHERE mod(id, 7) = 0;")
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_ix1\"');")

		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)
		con1.commit()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")

		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(100, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(100, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(100, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.execute("DELETE FROM o_test;")
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.stop()

	def test_primary_xip_secondary_tuples_mix(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
						   id integer NOT NULL,
						   id2 integer NOT NULL,
						   id3 integer NOT NULL,
						   PRIMARY KEY (id)
					   ) USING orioledb;
					   CREATE UNIQUE INDEX o_test_ix1 ON o_test (id2);
					   CREATE INDEX o_test_ix2 ON o_test (id3);
					   """)
		# insert-update-delete-commit
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		con1.execute("INSERT INTO o_test (SELECT id, id + 1, id + 3 FROM generate_series(1, 10, 1) id);")
		con1.execute("UPDATE o_test SET id3 = id3 + 1 WHERE id < 5")
		con1.execute("DELETE FROM o_test WHERE id > 5;")
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_ix1\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con1.commit()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")

		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual("[(1, 2, 5), (2, 3, 6), (3, 4, 7), (4, 5, 8), (5, 6, 8)]", str(node.execute("SELECT * FROM o_test;")))
		self.assertEqual(5, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(5, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(5, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.execute("DELETE FROM o_test;")
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.stop()

		node.start()
		# insert-update-delete-rollback
		con1 = node.connect()
		con2 = node.connect()
		con1.execute("INSERT INTO o_test (SELECT id, id + 1, id + 3 FROM generate_series(1, 10, 1) id);")
		con1.execute("UPDATE o_test SET id3 = id3 + 1 WHERE id < 5")
		con1.execute("DELETE FROM o_test WHERE id > 5;")
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_ix1\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con1.rollback()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")

		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.stop()

	def test_primary_empty_secondary_tuples(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
						   id integer NOT NULL,
						   id2 integer NOT NULL,
						   id3 integer NOT NULL,
						   PRIMARY KEY (id)
					   ) USING orioledb;
					   CREATE UNIQUE INDEX o_test_ix1 ON o_test (id2);
					   CREATE INDEX o_test_ix2 ON o_test (id3);
					   """)
		# insert-update-delete-commit
		con1 = node.connect()
		con2 = node.connect()
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_ix1\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con1.begin()
		con1.execute("INSERT INTO o_test (SELECT id, id + 1, id + 3 FROM generate_series(1, 10, 1) id);")
		con1.commit()
		con1.begin()
		con1.execute("UPDATE o_test SET id3 = id3 + 1 WHERE id < 5")
		con1.commit()
		con1.begin()
		con1.execute("DELETE FROM o_test WHERE id > 5;")
		con1.commit()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');");

		t1.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual("[(1, 2, 5), (2, 3, 6), (3, 4, 7), (4, 5, 8), (5, 6, 8)]", str(node.execute("SELECT * FROM o_test;")))
		self.assertEqual(5, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(5, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(5, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.execute("DELETE FROM o_test;")
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id2) t;")[0][0])
		self.assertEqual(0, node.execute("SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY id3) t;")[0][0])
		node.stop()

	def test_wal_truncate(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    id integer NOT NULL,\n"
					   "    val text,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);\n")
		node.safe_psql("TRUNCATE o_test;")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, id || 'val' FROM generate_series(101, 200, 1) id);\n")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(15050, node.execute("SELECT SUM(id) FROM o_test;")[0][0])
		node.stop()

	def test_wal_without_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    id integer NOT NULL,\n"
					   "    val text,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "TRUNCATE o_test;\n")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);\n")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			str(node.execute('postgres',
							 'SELECT * FROM o_test LIMIT 8;')),
			"[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val')]")

		self.assertEqual(
			str(node.execute('postgres',
							 'SELECT * FROM o_test WHERE id BETWEEN 1 and 8;')),
			"[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val')]")
		node.stop()

	def test_wal_simple(self):
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
		con1 = node.connect()
		con1.begin()
		con1.execute("UPDATE o_test SET val = 'xxx1' WHERE id = 1;")
		con1.execute("DELETE FROM o_test WHERE id = 2;")
		con1.execute("INSERT INTO o_test VALUES (1001, 'xxx2');")
		con2 = node.connect()
		con2.execute("UPDATE o_test SET val = 'yyy1' WHERE id = 3;")
		con2.execute("DELETE FROM o_test WHERE id = 4;")
		con2.execute("INSERT INTO o_test VALUES (1002, 'yyy2');")
		node.safe_psql('postgres', 'CHECKPOINT;')
		con1.execute("INSERT INTO o_test VALUES (1003, 'zzz1');")
		con2.execute("INSERT INTO o_test VALUES (1004, 'zzz2');")
		con1.commit()
		con2.rollback()
		con1.begin()
		con2.begin()
		con1.execute("UPDATE o_test SET val = 'xxx3' WHERE id = 5;")
		con2.execute("UPDATE o_test SET val = 'yyy3' WHERE id = 6;")
		con1.execute("DELETE FROM o_test WHERE id = 7;")
		con2.execute("DELETE FROM o_test WHERE id = 8;")
		con1.execute("INSERT INTO o_test VALUES (1005, 'zzz3');")
		con2.execute("INSERT INTO o_test VALUES (1006, 'zzz4');")
		con1.rollback()
		con2.commit()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 1 and 8;')),
			"[(1, 'xxx1'), (3, '3val'), (4, '4val'), (5, '5val'), (6, 'yyy3'), (7, '7val')]")
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id > 1000;')),
			"[(1001, 'xxx2'), (1003, 'zzz1'), (1006, 'zzz4')]")
		node.stop()  # stop PostgreSQL

	def test_wal_update_pk(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	secid integer NOT NULL,\n"
			"	PRIMARY KEY (id)\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id + 1 FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres',
			"CHECKPOINT;\n"
			"UPDATE o_test SET id = -1 WHERE id = 1;"
		)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			str(node.execute('postgres', 'SELECT count(*), sum(id) FROM o_test;')),
			"[(1000, 500498)]")
		node.stop()  # stop PostgreSQL

	def test_wal_update_sec_index(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	secid integer NOT NULL\n"
			") USING orioledb;\n"
			"CREATE INDEX o_test_ix1 ON o_test (secid);"
			"INSERT INTO o_test\n"
			"	(SELECT id, id + 1 FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', 'CHECKPOINT;')
		con1 = node.connect()
		con1.begin()
		con1.execute("UPDATE o_test SET id = i.newId\n"
					 "FROM (SELECT -1 * id as newId, secid  FROM o_test WHERE secid >= 995) i\n"
					 "WHERE i.secid = o_test.secid;\n")
		con1.commit()
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE secid >= 995;')),
			"[(-994, 995), (-995, 996), (-996, 997), (-997, 998), (-998, 999), (-999, 1000), (-1000, 1001)]")
		node.stop()

	def test_wal_update_unique_index(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	secid integer NOT NULL,\n"
			"	PRIMARY KEY (id)\n"
			") USING orioledb;\n"
			"CREATE UNIQUE INDEX o_test_ix1 ON o_test (secid);"
			"INSERT INTO o_test\n"
			"	(SELECT id, id + 1 FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', 'CHECKPOINT;')
		con1 = node.connect()
		con1.begin()
		con1.execute("UPDATE o_test SET id = i.newId\n"
					 "FROM (SELECT -1 * id as newId, secid  FROM o_test WHERE secid >= 995) i\n"
					 "WHERE i.secid = o_test.secid;\n")
		con1.commit()
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE secid >= 995 ORDER BY secid;')),
			"[(-994, 995), (-995, 996), (-996, 997), (-997, 998), (-998, 999), (-999, 1000), (-1000, 1001)]")
		node.stop()

	def test_wal_two_trans_sec_index(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	secid integer NOT NULL\n"
			") USING orioledb;\n"
			"CREATE INDEX o_test_ix1 ON o_test (secid);"
			"INSERT INTO o_test\n"
			"	(SELECT id, id + 1 FROM generate_series(1, 1000, 1) id);\n"
		)
		node.safe_psql('postgres', 'CHECKPOINT;')
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		con2.begin()
		con1.execute("UPDATE o_test SET id = i.newId\n"
					 "FROM (SELECT -1 * id as newId, secid  FROM o_test WHERE secid >= 995) i\n"
					 "WHERE i.secid = o_test.secid;\n")
		con1.execute("INSERT INTO o_test VALUES (1001, 1002);")
		con2.execute("INSERT INTO o_test VALUES (1002, 1003);")
		con1.execute("DELETE FROM o_test WHERE secid >= 990 and secid <= 992;")
		con1.execute("UPDATE o_test SET id = 1010 WHERE secid = 994;")
		con1.commit()
		con2.execute("DELETE FROM o_test WHERE secid >= 991 and secid <= 994;")
		con2.execute("UPDATE o_test SET id = 1020 WHERE secid = 995;")
		con2.rollback()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE secid >= 990;')),
			"[(992, 993), (1010, 994), (-994, 995), (-995, 996), (-996, 997), (-997, 998), (-998, 999), (-999, 1000), (-1000, 1001), (1001, 1002)]")
		node.stop()

	def test_wal_joint_commit_flush(self):
		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 int UNIQUE,
				filler char(110)
			) USING orioledb;

			CREATE TABLE o_test_2(
				val_1 int UNIQUE
			) USING orioledb;

			BEGIN;
			INSERT INTO o_test_1(val_1, filler)
				(SELECT val_1, '' FROM generate_series (1, 128) val_1);
			INSERT INTO o_test_2(val_1)
				(SELECT * FROM generate_series (1, 13));
			ALTER TABLE o_test_1 DROP COLUMN filler;
			COMMIT;
		""")

	def test_subtrans(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id)
			) USING orioledb;
			CREATE INDEX o_test_ix1 ON o_test (val);
			INSERT INTO o_test
				(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);
			"""
		)
		con1 = node.connect()
		con1.begin()
		con1.execute("UPDATE o_test SET val = 'xxx1' WHERE id = 1;")
		con1.execute("SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = 'xxx2' WHERE id = 2;")
		con1.execute("DELETE FROM o_test WHERE id = 1000;")
		con1.execute("INSERT INTO o_test VALUES (1001, 'xxx1001');")

		node.safe_psql('postgres', 'CHECKPOINT;')

		con1.execute("UPDATE o_test SET val = 'xxx3' WHERE id = 3;")
		con1.execute("ROLLBACK TO SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = 'xxx4' WHERE id = 4;")
		con1.execute("DELETE FROM o_test WHERE id = 999;")
		con1.execute("INSERT INTO o_test VALUES (1002, 'xxx1002');")
		con1.commit()
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start() # start PostgreSQL

		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 1 and 4;')),
			"[(1, 'xxx1'), (2, '2val'), (3, '3val'), (4, 'xxx4')]")
		self.assertEqual(node.execute('postgres', "SELECT id FROM o_test WHERE val = 'xxx4'")[0][0], 4)
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 999 and 1002;')),
			"[(1000, '1000val'), (1002, 'xxx1002')]")
		self.assertEqual(node.execute('postgres', "SELECT id FROM o_test WHERE val = 'xxx1002'")[0][0], 1002)

		node.stop()  # stop PostgreSQL

	def test_subtrans_from_begin(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text,\n"
			"	PRIMARY KEY (id)"
			") USING orioledb;\n"
			"CREATE INDEX o_test_ix1 ON o_test (val);"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n"
		)
		con1 = node.connect()
		con1.begin()
		con1.execute("SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = 'xxx1' WHERE id = 1;")
		con1.execute("UPDATE o_test SET val = 'xxx2' WHERE id = 2;")
		con1.execute("DELETE FROM o_test WHERE id = 1000;")
		con1.execute("INSERT INTO o_test VALUES (1001, 'xxx1001');")

		node.safe_psql('postgres', 'CHECKPOINT;')

		con1.execute("UPDATE o_test SET val = 'xxx3' WHERE id = 3;")
		con1.execute("ROLLBACK TO SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = 'xxx4' WHERE id = 4;")
		con1.execute("DELETE FROM o_test WHERE id = 999;")
		con1.execute("INSERT INTO o_test VALUES (1002, 'xxx1002');")
		con1.commit()
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start() # start PostgreSQL

		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 1 and 4;')),
			"[(1, '1val'), (2, '2val'), (3, '3val'), (4, 'xxx4')]")
		self.assertEqual(node.execute('postgres', "SELECT id FROM o_test WHERE val = 'xxx4'")[0][0], 4)
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 999 and 1002;')),
			"[(1000, '1000val'), (1002, 'xxx1002')]")
		self.assertEqual(node.execute('postgres', "SELECT id FROM o_test WHERE val = 'xxx1002'")[0][0], 1002)

		node.stop()  # stop PostgreSQL

	def test_wal_compression_simple(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
		)

		initial = 'x' * 500
		updated = 'y' * 700;
		inserted = 'z' * 500;

		node.safe_psql('postgres', "INSERT INTO o_test VALUES(0, '%s');" % (initial))
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(1, '%s');" % (initial))

		node.safe_psql('postgres', "CHECKPOINT;")

		node.safe_psql('postgres', "UPDATE o_test SET val = '%s' WHERE id = 1;" % (updated))
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(2, '%s');" % (inserted))
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(3, '%s');" % (inserted))
		node.safe_psql('postgres', "DELETE FROM o_test WHERE id = 3;")
		node.stop(['-m', 'immediate'])

		node.start(); # start PostgreSQL

		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 0;')[0][1], initial);

		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 0;')[0][1], initial);
		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 1;')[0][1], updated);
		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 2;')[0][1], inserted);

		# value with id = 3 was INSERT then DELETE
		self.assertEqual(
			len(node.execute('postgres', 'SELECT * FROM o_test WHERE id = 3;')), 0);
		# only three values in the table
		self.assertEqual(len(node.execute('postgres', 'SELECT * FROM o_test;')), 3);

		node.stop(); # stop PostgreSQL

	def test_compression_subtrans(self):
		node = self.node
		self.maxDiff = None
		node.start() # start PostgreSQL

		initial = 'x' * 500
		update_before_savepoint = 'y' * 500
		tmp1 = 'z' * 400
		tmp2 = 'a' * 300
		tmp3 = 'q' * 500
		update_after_rollback = 'l' * 350
		insert_after_rollback = 'p' * 400

		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || '%s' FROM generate_series(1, 500, 1) id);\n" % (initial)
		)

		con1 = node.connect()
		con1.begin()

		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 1;" % (update_before_savepoint))
		con1.execute("SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 2;" % (tmp1))
		con1.execute("DELETE FROM o_test WHERE id = 500;")
		con1.execute("INSERT INTO o_test VALUES (501, '%s');" % (tmp2))

		node.safe_psql('postgres', 'CHECKPOINT;')

		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 3;" % (tmp3))
		con1.execute("ROLLBACK TO SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 4;" % (update_after_rollback))
		con1.execute("DELETE FROM o_test WHERE id = 499;")
		con1.execute("INSERT INTO o_test VALUES (502, '%s');" % (insert_after_rollback))

		con1.commit()
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start() # start PostgreSQL

		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 1 and 4;')),
			"[(1, '%s'), (2, '%s'), (3, '%s'), (4, '%s')]"
			% (update_before_savepoint, '2' + initial, '3' + initial, update_after_rollback))
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 499 and 502;')),
			"[(500, '%s'), (502, '%s')]"
			% ( '500' + initial, insert_after_rollback))
		node.stop()  # stop PostgreSQL

	def test_wal_toast_simple(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
		)

		random.seed(0)
		initial = generate_string(20000)
		updated = generate_string(30000)
		inserted = generate_string(20000)

		node.safe_psql('postgres', "INSERT INTO o_test VALUES(0, '%s');" % (initial))
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(1, '%s');" % (initial))

		node.safe_psql('postgres', "CHECKPOINT;")

		node.safe_psql('postgres', "UPDATE o_test SET val = '%s' WHERE id = 1;" % (updated))
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(2, '%s');" % (inserted))
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(3, '%s');" % (inserted))
		node.safe_psql('postgres', "DELETE FROM o_test WHERE id = 3;")

		node.stop(['-m', 'immediate'])
		node.start(); # start PostgreSQL

		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 0;')[0][1], initial)
		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 1;')[0][1], updated)
		self.assertEqual(
			node.execute('postgres', 'SELECT * FROM o_test WHERE id = 2;')[0][1], inserted)

		# value with id = 3 was INSERT then DELETE
		self.assertEqual(
			len(node.execute('postgres', 'SELECT * FROM o_test WHERE id = 3;')), 0)
		# only three values in the table
		self.assertEqual(len(node.execute('postgres', 'SELECT * FROM o_test;')), 3)

		node.stop(); # stop PostgreSQL

	def test_toast_subtrans(self):
		node = self.node
		self.maxDiff = None
		node.start() # start PostgreSQL

		random.seed(0)
		initial = generate_string(10000)
		update_before_savepoint = generate_string(10001)
		tmp1 = generate_string(8000)
		tmp2 = generate_string(7000)
		tmp3 = generate_string(5000)
		update_after_rollback = generate_string(7000)
		insert_after_rollback = generate_string(9000)

		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || '%s' FROM generate_series(1, 500, 1) id);\n" % (initial)
		)

		con1 = node.connect()
		con1.begin()

		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 1;" % (update_before_savepoint))
		con1.execute("SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 2;" % (tmp1))
		con1.execute("DELETE FROM o_test WHERE id = 500;")
		con1.execute("INSERT INTO o_test VALUES (501, '%s');" % (tmp2))

		node.safe_psql('postgres', 'CHECKPOINT;')

		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 3;" % (tmp3))
		con1.execute("ROLLBACK TO SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = '%s' WHERE id = 4;" % (update_after_rollback))
		con1.execute("DELETE FROM o_test WHERE id = 499;")
		con1.execute("INSERT INTO o_test VALUES (502, '%s');" % (insert_after_rollback))

		con1.commit()
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start() # start PostgreSQL

		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 1 and 4;')),
			"[(1, '%s'), (2, '%s'), (3, '%s'), (4, '%s')]"
			% (update_before_savepoint, '2' + initial, '3' + initial, update_after_rollback))
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 499 and 502;')),
			"[(500, '%s'), (502, '%s')]"
			% ( '500' + initial, insert_after_rollback))
		node.stop()  # stop PostgreSQL

	def number_to_ctid(self, num):
		return '(%s,%s)' % (str(num // 2047), str(num % 2047))

	def test_ctid_index(self):
		node = self.node
		node.start() # start PostgreSQL
		# insert 1..10000 before a checkpoint
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 10000, 1) id);\n"
		)
		node.safe_psql('postgres', "CHECKPOINT;")
		# insert 10001..20000 after the checkpoint
		node.safe_psql("INSERT INTO o_test\n"
					   "    (SELECT id, id || 'val' FROM generate_series(%d, %d, 1) id);" %
					   (10001, 20000))
		node.stop(['-m', 'immediate'])

		node.start()
		# insert 20001..70000 after recovery
		node.safe_psql("INSERT INTO o_test\n"
					   "	(SELECT id, id || 'val' FROM generate_series(20001, 70000, 1) id);")
		self.assertEqual(node.execute('postgres', "SELECT count(*) FROM o_test;")[0][0], 70000)
		self.assertEqual(node.execute('postgres', "SELECT ctid FROM o_test WHERE id = 30000;")[0][0], self.number_to_ctid(30000))
		self.assertEqual(node.execute('postgres', "SELECT ctid FROM o_test WHERE id = 70000;")[0][0], self.number_to_ctid(70000))

		# makes a new checkpoint
		node.stop()

		node.start()
		# insert 70001..80000 after the second checkpoint (1..70000 should be restored on startup)
		node.safe_psql("INSERT INTO o_test\n"
					   "	(SELECT id, id || 'val' FROM generate_series(70001, 80000, 1) id);")
		self.assertEqual(node.execute('postgres', "SELECT count(*) FROM o_test;")[0][0], 80000)
		self.assertEqual(node.execute('postgres', "SELECT ctid FROM o_test WHERE id = 30000;")[0][0], self.number_to_ctid(30000))
		self.assertEqual(node.execute('postgres', "SELECT ctid FROM o_test WHERE id = 70000;")[0][0], self.number_to_ctid(70000))
		self.assertEqual(node.execute('postgres', "SELECT ctid FROM o_test WHERE id = 80000;")[0][0], self.number_to_ctid(80000))
		node.stop()

	def test_wal_only_commit_or_rollback_container(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n")
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL\n"
			") USING orioledb;\n"
		)

		con1 = node.connect()
		con1.begin()
		con1.execute('INSERT INTO o_test VALUES (1);')
		con1.commit()

		con2 = node.connect()
		con2.begin()
		con2.execute("TRUNCATE o_test;") # TRUNCATE does not create a WAL record
		con2.commit()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.execute("CHECKPOINT;")
		node.stop(['-m', 'immediate'])

		node.start()
		con1 = node.connect()
		con1.begin()
		con1.execute('INSERT INTO o_test VALUES (2);')
		con1.commit()

		con2 = node.connect()
		con2.begin()
		con2.execute("TRUNCATE o_test;")
		con2.rollback()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_wal_overflow_on_invalidate(self):
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql("""
			CREATE TABLE IF NOT EXISTS o_test (
				key integer NOT NULL,
				val integer NOT NULL,
				PRIMARY KEY(key)
			) USING orioledb;
		""")

		with node.connect() as con1:
			with node.connect() as con2:
				con1.begin()
				con1.execute("""
					ALTER TABLE o_test ADD COLUMN val_2 integer NOT NULL;
					INSERT INTO o_test (key, val, val_2)
						(SELECT val, val * 100, val + 100
							FROM generate_series(1, 223) val);
					ALTER TABLE o_test DROP COLUMN val_2;
				""")

				con2.execute("""
					CHECKPOINT;
				""")
				con1.commit()
				con2.commit()

		node.stop(['-m', 'immediate'])
		node.start()

		node.stop()

	def test_tup_key_hash_with_nulls(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n")

		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "	id1 integer NOT NULL,\n"
					   "    id2 integer,\n"
					   "    id3 integer,\n"
					   "    PRIMARY KEY (id1)"
					   ") USING orioledb;\n"
					   "CREATE UNIQUE INDEX o_test_ix1 ON o_test (id2);"
					   "CREATE UNIQUE INDEX o_test_ix2 ON o_test (id3);"
		)

		node.execute("INSERT INTO o_test VALUES (1, NULL, 3);")
		node.execute("INSERT INTO o_test VALUES (2, 4, NULL);")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(2, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		node.stop()

	def test_recovery_rename_index(self):
		node = self.node

		node.start()
		node.safe_psql('postgres', """
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE IF NOT EXISTS o_test (
							key bigint NOT NULL,
							val int,
							val2 int NOT NULL,
							PRIMARY KEY (key)
						) USING orioledb;

						CREATE INDEX o_test_idx1 ON o_test (val);
						INSERT INTO o_test
							SELECT 1000 + i,
								   3000 + i,
								   3000 + i FROM
									generate_series(1, 500) AS i;""")
		node.safe_psql('postgres', "CREATE INDEX o_test_idx2 ON o_test (val2);")
		node.safe_psql('postgres', """
						BEGIN;
						ALTER INDEX o_test_idx2 RENAME TO
									o_test_idx2_renamed;
						ROLLBACK;""")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(500,
						 node.execute(
							"SELECT COUNT(val2) FROM o_test WHERE val2 > 0;"
						 )[0][0])
		explain = node.safe_psql("""
			SET enable_seqscan = off;
			EXPLAIN SELECT val2 FROM o_test
				WHERE val2 > 0 ORDER BY val2;""").decode('utf-8')
		self.assertNotEqual(explain.find('o_test_idx2'), -1)
		node.stop()

	def test_missingattr_recovery(self):
		node = self.node

		node.start()
		with node.connect('postgres') as con1:
			with node.connect('postgres') as con2:
				node.safe_psql("""
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test_missingattr
					(
						key int NOT NULL,
						val int NOT NULL DEFAULT 5
					) USING orioledb;

					INSERT INTO o_test_missingattr
						SELECT id FROM generate_series(1, 10) id;
				""")
				con1.execute("""ALTER TABLE o_test_missingattr
								ADD COLUMN val3 int NOT NULL DEFAULT 18;""")
				con1.execute("""ALTER TABLE o_test_missingattr
								ADD COLUMN val4 text DEFAULT 'abc';""")
				con1.commit()
				self.assertEqual((1, 5, 18, 'abc'),
						con1.execute("""SELECT key, val, val3, val4 FROM
							o_test_missingattr WHERE key = 1;"""
						)[0])
				self.assertEqual((1, 5, 18, 'abc'),
						con2.execute("""SELECT key, val, val3, val4 FROM
							o_test_missingattr WHERE key = 1;"""
						)[0])
				con1.execute("""UPDATE o_test_missingattr
								SET val3 = 33 WHERE key BETWEEN 6 AND 8;""")
				con1.commit()
				node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual((6, 5, 33, 'abc'),
				node.execute('postgres', """
					SELECT key, val, val3, val4 FROM
						o_test_missingattr WHERE key = 6;""")[0])

	def test_recovery_partial_index(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n")

		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key int8 NOT NULL PRIMARY KEY,
				value text
			) USING orioledb;
			CREATE INDEX o_test_ix_partial ON o_test (key, value) WHERE key > 10;
		""")

		node.execute("""INSERT INTO o_test
			(SELECT id, id || 'text' FROM generate_series(1, 20) as id);""")
		node.execute("""UPDATE o_test SET value = 'UPD' WHERE key IN (5, 15)""")
		node.execute("""DELETE FROM o_test WHERE key IN (5, 16)""")
		self.assertEqual(9,
						 node.execute(
							"SELECT COUNT(*) FROM o_test WHERE key > 10;"
						 )[0][0])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual((11, 2),
						 node.execute("""SELECT * FROM
							 orioledb_index_rows(
								 'o_test_ix_partial'::regclass);""")[0])
		self.assertEqual('UPD',
						 node.execute("""SELECT value FROM o_test
						 					WHERE key = 15
											ORDER BY key""")[0][0])
		node.stop()


	def test_recovery_deep_sql_function_predicate(self):
		node = self.node
		node.append_conf('orioledb.recovery_pool_size = 1')
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE FUNCTION my_cmp_sql(a int, b int) RETURNS int AS $$
				SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
								(b::bit(5) & X'A8'::bit(5))::int);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_cmp_sql_sql(a int, b int) RETURNS int
			AS $$
				SELECT my_cmp_sql(a, b);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_cmp_sql_sql_sql(a int, b int) RETURNS int
			AS $$
				SELECT my_cmp_sql_sql(a, b);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_eq_sql_sql_sql_sql(a int, b int)
				RETURNS bool
			AS $$
				SELECT my_cmp_sql_sql_sql(a, b) = 0;
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE TABLE IF NOT EXISTS o_test (
				val integer
			) USING orioledb;

			CREATE INDEX o_test_ix1 ON o_test (val)
					WHERE (my_eq_sql_sql_sql_sql(val, val * 11));
		""")
		node.safe_psql("""
			INSERT INTO o_test VALUES (1);
			INSERT INTO o_test VALUES (2);
			INSERT INTO o_test VALUES (3);
			INSERT INTO o_test VALUES (4);
			INSERT INTO o_test VALUES (5);
		""")

		self.assertEqual(node.execute("""
			SELECT * FROM o_test
				WHERE my_eq_sql_sql_sql_sql(val, val * 11);
		"""), [(1,),(3,),(4,)])

		node.stop(['-m','immediate'])

		node.start()

		self.assertEqual(node.execute("""
			SELECT * FROM o_test
				WHERE my_eq_sql_sql_sql_sql(val, val * 11);
		"""), [(1,),(3,),(4,)])

	def test_recovery_expression_index(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n")

		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key int8 NOT NULL PRIMARY KEY,
				value text
			) USING orioledb;
			CREATE INDEX o_test_ix_expression ON o_test ((key * 100),
														 (value || 'WOW'));
		""")

		node.execute("""INSERT INTO o_test
			(SELECT id, id || 'text' FROM generate_series(1, 10) as id);""")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(6,
						 node.execute("""
							WITH o_test_cte AS (
								SELECT * FROM o_test WHERE (key * 100)
									BETWEEN 300 AND 800
							) SELECT COUNT(*) FROM o_test_cte;
						""")[0][0])
		node.stop()

	def test_recovery_deep_sql_function_opclass_cmp(self):
		node = self.node
		node.append_conf('orioledb.recovery_pool_size = 1')
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE OR REPLACE FUNCTION my_cmp_sql(a int, b int) RETURNS int
			AS $$
				SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
								(b::bit(5) & X'A8'::bit(5))::int);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_cmp_s_s(a int, b int) RETURNS int
			AS $$
 				SELECT my_cmp_sql(a, b);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_cmp_s_s_s(a int, b int) RETURNS int
			AS $$
				SELECT my_cmp_s_s(a, b);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_cmp_s_s_s_s(a int, b int) RETURNS int
			AS $$
				SELECT my_cmp_s_s_s(a, b);
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE FUNCTION my_eq(a int, b int) RETURNS bool AS $$
				SELECT my_cmp_sql(a, b) = 0;
			$$ LANGUAGE SQL IMMUTABLE;

			CREATE OPERATOR =^ (
				LEFTARG = int4,
				RIGHTARG = int4,
				PROCEDURE = my_eq,
				COMMUTATOR = =^
			);
			CREATE OPERATOR <^ (
				LEFTARG = int4,
				RIGHTARG = int4,
				PROCEDURE = int4lt
			);
			CREATE OPERATOR >^ (
				LEFTARG = int4,
				RIGHTARG = int4,
				PROCEDURE = int4gt
			);

			CREATE OPERATOR CLASS my_op_class FOR TYPE int
				USING btree
				AS OPERATOR 1 <^, OPERATOR 3 =^, OPERATOR 5 >^,
				FUNCTION 1 my_cmp_s_s_s_s(int, int);

			CREATE TABLE IF NOT EXISTS o_test (
				val integer
			) USING orioledb;

			CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
		""")
		node.safe_psql("""
			INSERT INTO o_test VALUES (1);
			INSERT INTO o_test VALUES (2);
			INSERT INTO o_test VALUES (3);
			INSERT INTO o_test VALUES (4);
			INSERT INTO o_test VALUES (5);
		""")

		self.assertEqual(node.execute("""
			SELECT * FROM o_test WHERE my_eq(val, val * 11);
		"""), [(1,),(3,),(4,)])

		node.stop(['-m','immediate'])

		node.start()

		self.assertEqual(node.execute("""
			SELECT * FROM o_test WHERE my_eq(val, val * 11);
		"""), [(1,),(3,),(4,)])

	def test_checkpoint_concurrent_no_wal_undo(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    id integer NOT NULL,\n"
					   "    val text,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);\n")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_pkey\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con1.execute("INSERT INTO o_test VALUES(101, 'abcdef');")
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');");
		t1.join()
		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			node.execute('postgres',
						 'SELECT count(*) FROM o_test')[0][0],
						  100)
		node.stop()

	def test_checkpoint_concurrent_no_wal_undo_secondary(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    id integer NOT NULL,\n"
					   "    val text,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "CREATE INDEX o_test_val_idx ON o_test(val);\n")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, 'val' || id FROM generate_series(1, 100, 1) id);\n")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test_val_idx\"');")
		t1 = ThreadQueryExecutor(con2, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con1.execute("INSERT INTO o_test VALUES(101, 'abcdef');")
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');");
		t1.join()
		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			node.execute('postgres',
						 "SELECT count(*) FROM o_test WHERE val = 'abcdef';")[0][0],
						  0)
		node.stop()

	def test_apply_branches(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    id integer NOT NULL,\n"
					   "    val integer,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test2 (\n"
					   "    id integer NOT NULL,\n"
					   "    val integer,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "CREATE UNIQUE INDEX o_test_val_idx ON o_test(val);\n")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, id FROM generate_series(1, 100, 1) id);\n")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()
		con2_pid = con2.pid
		con3_pid = con3.pid

		con1.execute("SELECT pg_stopevent_set('index_insert', 'true');")
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_test2_pkey\"');")

		con2.begin()
		con2.execute("SET orioledb.debug_slot_number = 1;")
		t1 = ThreadQueryExecutor(con2, "INSERT INTO o_test VALUES (101, 1) ON CONFLICT DO NOTHING;")
		t1.start()
		wait_stopevent(node, con2_pid)

		t2 = ThreadQueryExecutor(con3, "CHECKPOINT;")
		t2.start()
		wait_checkpointer_stopevent(node)

		con1.execute("SELECT pg_stopevent_reset('index_insert');")
		t1.join()

		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")
		t2.join()
		con1.close()
		con2.close()
		con3.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			node.execute('postgres',
						 "SELECT count(*) FROM o_test;")[0][0],
						 100)
		node.stop()

	def test_recovery_subtrans_concurrent(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    id integer NOT NULL,\n"
					   "    val text,\n"
					   "    PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql("INSERT INTO o_test\n"
					   "(SELECT id, 'val' || id FROM generate_series(1, 2, 1) id);\n")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con1.execute("UPDATE o_test SET val = val || 'aaa' WHERE id = 1;")
		con1.execute("SAVEPOINT s1;")
		con1.execute("UPDATE o_test SET val = val || 'aaa' WHERE id = 2;")
		con1.execute("ROLLBACK TO SAVEPOINT s1;")
		con2.begin()
		con2.execute("UPDATE o_test SET val = val || 'bbb' WHERE id = 2;")
		con2.commit()
		con1.commit()

		self.assertEqual("[(1, 'val1aaa'), (2, 'val2bbb')]", str(node.execute("SELECT * FROM o_test;")))
		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual("[(1, 'val1aaa'), (2, 'val2bbb')]", str(node.execute("SELECT * FROM o_test;")))
		node.stop()

	def test_recovery_timestamp(self):
		node = self.node
		node.start()
		node.safe_psql("""

			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1 (
				val_1 timestamp DEFAULT timeofday()::timestamp
			) USING orioledb;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

		node.stop()

	def test_recovery_default_sql_func(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE FUNCTION foo(a INT) RETURNS TEXT
			AS $$ SELECT 'WOW'
			$$ LANGUAGE sql VOLATILE;
			CREATE TABLE o_test_plpgsql_default (
				pk INT NOT NULL PRIMARY KEY,
				c_int INT DEFAULT LENGTH(foo(6))
			) USING orioledb;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

		node.stop()

	def test_recovery_default_plpgsql_func(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE FUNCTION foo(a INT) RETURNS TEXT AS $$
			BEGIN
				RETURN 'WOW';
			END;
			$$ LANGUAGE plpgsql VOLATILE;
			CREATE TABLE o_test_plpgsql_default (
				pk INT NOT NULL PRIMARY KEY,
				c_int INT DEFAULT LENGTH(foo(6))
			) USING orioledb;
			INSERT INTO o_test_plpgsql_default (pk) VALUES (1), (3), (8);
		""")

		self.assertEqual([(1, 3), (3, 3), (8, 3)],
						 node.execute("""
							SELECT * FROM o_test_plpgsql_default
								ORDER BY pk
						 """))

		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual([(1, 3), (3, 3), (8, 3)],
						 node.execute("""
							SELECT * FROM o_test_plpgsql_default
								ORDER BY pk
						 """))

		node.stop()
	def test_recovery_truncate(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql("""
			CREATE TABLE o_test_1 (
				val_1 int PRIMARY KEY,
				val_2 int
			) USING orioledb;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 100 FROM generate_series(1, 5) val_1);

			TRUNCATE o_test_1;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

		node.safe_psql("""
			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 100 FROM generate_series(1, 5) val_1);
		""")

		node.stop()

	def test_recovery_partiton(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		with node.connect() as con:
			con.execute("""
				CREATE TABLE o_test_1 (
					val_1 int,
					val_2 text,
					PRIMARY KEY (val_1)
				) PARTITION BY RANGE (val_1);
			""")
			con.execute("""
				CREATE TABLE o_test_2 (
					val_3 int,
					like o_test_1
				) USING orioledb;
			""")
			con.execute("""
				ALTER TABLE o_test_2 DROP COLUMN val_3;
			""")
			con.execute("""
				ALTER TABLE o_test_1 ATTACH PARTITION o_test_2
					FOR VALUES FROM (1) to (10);
			""")
			con.execute("""
				INSERT INTO o_test_1 (val_2, val_1)
					VALUES ('abc', 2), ('qwe', 4);
			""")

		node.stop(['-m', 'immediate'])

		node.start()

		node.stop()


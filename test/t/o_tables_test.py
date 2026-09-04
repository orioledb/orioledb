#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

from testgres.connection import DatabaseError
from testgres.enums import NodeStatus


class OTablesTest(BaseTest):

	def assertTblCount(self, size):
		self.assertEqual(
		    size,
		    self.node.execute(
		        'postgres',
		        'SELECT count(*) FROM orioledb_table_oids();')[0][0])

	def test_reject_unterminated_predicate(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				key integer PRIMARY KEY,
				value text
			) USING orioledb;
		""")

		con = node.connect(autocommit=True)
		try:
			con.execute('SET orioledb.enable_stopevents = true;')
			con.execute("SELECT pg_stopevent_set("
			            "'corrupt_o_tables_predicate', 'true');")
			with self.assertRaises(DatabaseError) as e:
				con.execute("CREATE INDEX o_test_partial ON o_test (value) "
				            "WHERE key > 10;")
			self.assertIn('failed to deserialize OTable', str(e.exception))
			self.assertEqual(con.execute('SELECT 1;')[0][0], 1)
		finally:
			node.execute("SELECT pg_stopevent_reset("
			             "'corrupt_o_tables_predicate');")
			con.close()

		node.stop()

	def test_o_tables_wal_commit(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test(\n"
		    "   id integer NOT NULL,\n"
		    "   val text\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_test VALUES(1, 'test');")
		self.assertTblCount(1)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
		node.stop()

	def test_o_tables_wal_rollback(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		self.assertTblCount(0)

		con1 = node.connect()
		con1.begin()
		con1.execute("CREATE TABLE IF NOT EXISTS o_test(\n"
		             "   id integer NOT NULL,\n"
		             "   val text\n"
		             ") USING orioledb;\n")
		con1.execute("INSERT INTO o_test VALUES(1, 'test');")
		self.assertTblCount(1)
		con1.rollback()
		self.assertTblCount(0)
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_wal_drop_commit(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test(\n"
		    "   id integer NOT NULL,\n"
		    "   val text\n"
		    ") USING orioledb;\n")
		self.assertTblCount(1)
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(1, 'test');")
		node.safe_psql('postgres', "DROP TABLE o_test;")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_wal_drop_rollback(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test(\n"
		    "   id integer NOT NULL,\n"
		    "   val text\n"
		    ") USING orioledb;\n")
		self.assertTblCount(1)
		node.safe_psql('postgres', "INSERT INTO o_test VALUES(1, 'test');")

		con1 = node.connect()
		con1.begin()
		con1.execute("DROP TABLE o_test;")
		self.assertTblCount(1)
		con1.rollback()
		self.assertTblCount(1)
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
		node.stop()

	def test_o_tables_xip_commit(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		con1 = node.connect()
		con1.begin()
		con1.execute("CREATE TABLE IF NOT EXISTS o_test(\n"
		             " id integer NOT NULL\n"
		             ") USING orioledb;\n")
		con1.execute("INSERT INTO o_test VALUES(1);")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)
		node.stop()

	def test_o_tables_xip_rollback(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		con1 = node.connect()
		con1.begin()
		con1.execute("CREATE TABLE IF NOT EXISTS o_test(\n"
		             " id integer NOT NULL\n"
		             ") USING orioledb;\n")
		con1.execute("INSERT INTO o_test VALUES(1);")

		node.safe_psql("CHECKPOINT;")

		con1.rollback()
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_wal_drop_extension_commit(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test(\n"
		    "   id integer NOT NULL,\n"
		    "   val text\n"
		    ") USING orioledb;\n")
		self.assertTblCount(1)
		node.safe_psql('postgres', "DROP EXTENSION orioledb CASCADE;")

		node.safe_psql('postgres', "CREATE EXTENSION orioledb;")
		self.assertTblCount(0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_wal_drop_extension_rollback(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test(\n"
		    "   id integer NOT NULL,\n"
		    "   val text\n"
		    ") USING orioledb;\n")
		self.assertTblCount(1)
		con1 = node.connect()
		con1.begin()
		con1.execute("DROP EXTENSION orioledb CASCADE;")
		con1.rollback()
		con1.close()
		self.assertTblCount(1)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)
		node.stop()

	def test_o_tables_mix(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test(\n"
		    "   id integer NOT NULL,\n"
		    "   val text\n"
		    ") USING orioledb;\n")
		self.assertTblCount(1)
		node.safe_psql('postgres', "DROP TABLE o_test;")
		self.assertTblCount(0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(0)
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test1(\n"
		    "   id integer NOT NULL\n"
		    ") USING orioledb;\n")
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test2(\n"
		    "   id integer NOT NULL\n"
		    ") USING orioledb;\n")
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test3(\n"
		    "   id integer NOT NULL\n"
		    ") USING orioledb;\n")
		self.assertTblCount(3)
		node.safe_psql('postgres', "DROP TABLE o_test3;")
		self.assertTblCount(2)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(2)
		con1 = node.connect()
		con1.begin()
		con1.execute("CREATE TABLE IF NOT EXISTS o_test3(\n"
		             "   id integer NOT NULL\n"
		             ") USING orioledb;\n")
		con1.rollback()
		self.assertTblCount(2)
		con1.begin()
		con1.execute("CREATE TABLE IF NOT EXISTS o_test3(\n"
		             "   id integer NOT NULL\n"
		             ") USING orioledb;\n")
		con1.commit()
		con1.close()
		self.assertTblCount(3)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(3)
		node.safe_psql('postgres', "DROP EXTENSION orioledb CASCADE;\n")
		node.stop(['-m', 'immediate'])

		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION orioledb;\n")
		self.assertTblCount(0)
		node.stop()

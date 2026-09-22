#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

from testgres.enums import NodeStatus


class OTablesTest(BaseTest):

	def assertTblCount(self, size):
		self.assertEqual(
		    size,
		    self.node.execute(
		        'postgres',
		        'SELECT count(*) FROM orioledb_table_oids();')[0][0])

	def test_reject_malformed_serialized_string(self):
		"""
		o_deserialize_string_safe() has to reject a blob that is too short as
		well as one whose payload does not end in NUL.  The wrapper builds a
		well-formed blob and can be told to claim it is shorter than it is,
		which is the only way to reach the length checks: a blob of its own
		natural size never trips them.

		Layout is sizeof(size_t) for the length, then the payload, so
		'terminated\0' makes 8 + 11 = 19 bytes.
		"""
		node = self.node
		node.start()
		node.safe_psql('postgres', 'CREATE EXTENSION orioledb;')

		payload = "convert_to('terminated', 'UTF8') || decode('00', 'hex')"
		cases = [
		    ("%s" % payload, True, 'well-formed'),
		    ("%s, 19" % payload, True, 'claiming its exact size'),
		    ("convert_to('unterminated', 'UTF8')", False, 'no terminator'),
		    ("%s, 4" % payload, False, 'too short for the length prefix'),
		    ("%s, 8" % payload, False, 'length prefix only, no payload'),
		    ("%s, 18" % payload, False, 'payload one byte short'),
		    ("decode('', 'hex')", True, 'empty string is a NULL result'),
		]

		for expr, expected, what in cases:
			with self.subTest(what):
				self.assertEqual(
				    node.execute(
				        'postgres',
				        'SELECT orioledb_test_deserialize_string(%s);' % expr),
				    [(expected, )])

		node.stop()

	def test_reject_malformed_serialized_node(self):
		"""
		The same for o_deserialize_node_safe(), which needs it more: both of
		its readers run to a terminator rather than to the recorded length --
		stringToNode() parses until the string ends, o_node_str_is_empty()
		compares one -- so a blob without one is read past its end.

		Layout is int32 + sizeof(size_t) before the payload, so '<>\0' makes
		4 + 8 + 3 = 15 bytes.  The version stamp is deliberately an old major
		so that the blob is never handed to stringToNode(): that branch would
		elog on malformed input instead of returning, and it is the length
		handling under test here, not the parser.
		"""
		node = self.node
		node.start()
		node.safe_psql('postgres', 'CREATE EXTENSION orioledb;')

		empty_node = "convert_to('<>', 'UTF8') || decode('00', 'hex')"
		old_major = 90600

		cases = [
		    ("%s, %d" % (empty_node, old_major), True, 'well-formed'),
		    ("%s, %d, 15" % (empty_node, old_major), True, 'exact size'),
		    ("convert_to('<>', 'UTF8'), %d" % old_major, False,
		     'no terminator'),
		    ("%s, %d, 8" % (empty_node, old_major), False,
		     'too short for the header'),
		    ("%s, %d, 12" % (empty_node, old_major), False,
		     'header only, no payload'),
		    ("%s, %d, 14" % (empty_node, old_major), False,
		     'payload one byte short'),
		    ("decode('', 'hex'), %d" % old_major, False,
		     'a zero-length node string is never ours'),
		]

		for expr, expected, what in cases:
			with self.subTest(what):
				self.assertEqual(
				    node.execute(
				        'postgres',
				        'SELECT orioledb_test_deserialize_node(%s);' % expr),
				    [(expected, )])

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

#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import time
import re
import os
import sys

from .base_test import BaseTest
from .base_test import generate_string
from testgres.enums import NodeStatus
from testgres.exceptions import StartNodeException, QueryException

import string
import random
import tempfile


class RewindXidTest(BaseTest):

	# Small tests:
	# test_rewind_xid_oriole
	# test_rewind_xid_heap
	# test_rewind_xid_heap_subxids
	# test_rewind_xid               // oriole+heap
	# test_rewind_xid_subxids       // oriole+heap

	# DDL tests :
	# test_rewind_xid_ddl_create    // oriole+heap
	# test_rewind_xid_ddl_drop      // oriole+heap
	# test_rewind_xid_ddl_truncate  // oriole+heap
	# test_rewind_xid_ddl_rewrite   // oriole+heap

	# Small scale tests

	def test_rewind_xid_oriole(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		invalidxid = 0

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test;')),
		    "[(20,)]")

		node.safe_psql(
		    'postgres', "select orioledb_rewind_to_transaction(%d,%ld);\n" %
		    (invalidxid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		node.stop()

	def test_rewind_xid_heap(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)
		invalidoxid = 9223372036854775807

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_heap;')),
		    "[(20,)]")

		node.safe_psql(
		    'postgres', "select orioledb_rewind_to_transaction(%d,%ld);\n" %
		    (xid, invalidoxid))
		time.sleep(1)
		node.is_started = False
		node.start()

		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		node.stop()

	def test_rewind_xid_heap_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("BEGIN;\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("SAVEPOINT sp1;\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("SAVEPOINT sp2;\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("SAVEPOINT sp3;\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("COMMIT;\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("BEGIN;\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("SAVEPOINT sp1;\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("SAVEPOINT sp2;\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("SAVEPOINT sp3;\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("COMMIT;\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '6', '-j', '6', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)
		invalidoxid = 9223372036854775807

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '6', '-j', '6', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_heap;')),
		    "[(96,)]")

		node.safe_psql(
		    'postgres', "select orioledb_rewind_to_transaction(%d,%ld);\n" %
		    (xid, invalidoxid))
		time.sleep(1)
		node.is_started = False
		node.start()

		self.maxDiff = None
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!'), (6, 'oldval!'), (7, 'oldval!'), (8, 'oldval!'), (9, 'oldval!'), (10, 'oldval!'), (11, 'oldval!'), (12, 'oldval!'), (13, 'oldval!'), (14, 'oldval!'), (15, 'oldval!'), (16, 'oldval!'), (17, 'oldval!'), (18, 'oldval!'), (19, 'oldval!'), (20, 'oldval!'), (21, 'oldval!'), (22, 'oldval!'), (23, 'oldval!'), (24, 'oldval!')]"
		)
		node.stop()

	def test_rewind_xid(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test;')),
		    "[(20,)]")
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_heap;')),
		    "[(20,)]")

		node.safe_psql(
		    'postgres',
		    "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		node.stop()

	def test_rewind_xid_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("BEGIN;\n")
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("SAVEPOINT sp1;\n")
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("SAVEPOINT sp2;\n")
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("SAVEPOINT sp3;\n")
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("COMMIT;\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("BEGIN;\n")
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("SAVEPOINT sp1;\n")
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("SAVEPOINT sp2;\n")
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("SAVEPOINT sp3;\n")
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("COMMIT;\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '6', '-j', '6', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '6', '-j', '6', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test;')),
		    "[(96,)]")
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_heap;')),
		    "[(96,)]")

		node.safe_psql(
		    'postgres',
		    "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid, oxid))

		time.sleep(1)
		node.is_started = False
		node.start()

		self.maxDiff = None
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!'), (6, 'oldval!'), (7, 'oldval!'), (8, 'oldval!'), (9, 'oldval!'), (10, 'oldval!'), (11, 'oldval!'), (12, 'oldval!'), (13, 'oldval!'), (14, 'oldval!'), (15, 'oldval!'), (16, 'oldval!'), (17, 'oldval!'), (18, 'oldval!'), (19, 'oldval!'), (20, 'oldval!'), (21, 'oldval!'), (22, 'oldval!'), (23, 'oldval!'), (24, 'oldval!')]"
		)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!'), (6, 'oldval!'), (7, 'oldval!'), (8, 'oldval!'), (9, 'oldval!'), (10, 'oldval!'), (11, 'oldval!'), (12, 'oldval!'), (13, 'oldval!'), (14, 'oldval!'), (15, 'oldval!'), (16, 'oldval!'), (17, 'oldval!'), (18, 'oldval!'), (19, 'oldval!'), (20, 'oldval!'), (21, 'oldval!'), (22, 'oldval!'), (23, 'oldval!'), (24, 'oldval!')]"
		)
		node.stop()


# DDL tests

	def test_rewind_xid_ddl_create(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_ddl (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap_ddl (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)

		node.safe_psql(
		    'postgres', "CREATE TABLE o_test_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE o_test_heap_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test;')),
		    "[(20,)]")
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_heap;')),
		    "[(20,)]")
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_ddl;')),
		    "[(15,)]")
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT count(*) FROM o_test_heap_ddl;')),
		    "[(15,)]")

		node.safe_psql(
		    'postgres',
		    "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)

		with self.assertRaises(QueryException) as e:
			node.safe_psql('postgres', 'SELECT * FROM o_test_ddl ORDER BY id;')
		self.assertIn('ERROR:  relation "o_test_ddl" does not exist',
		              self.stripErrorMsg(e.exception.message).rstrip("\r\n"))
		with self.assertRaises(QueryException) as e:
			node.safe_psql('postgres',
			               'SELECT * FROM o_test_heap_ddl ORDER BY id;')
		self.assertIn('ERROR:  relation "o_test_heap_ddl" does not exist',
		              self.stripErrorMsg(e.exception.message).rstrip("\r\n"))
		node.stop()

	def test_rewind_xid_ddl_drop(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n"
		    "CREATE TABLE o_test_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE o_test_heap_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_ddl (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap_ddl (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_ddl (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap_ddl (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test;')),
		    "[(20,)]")
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_heap;')),
		    "[(20,)]")
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_ddl;')),
		    "[(20,)]")
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT count(*) FROM o_test_heap_ddl;')),
		    "[(20,)]")

		node.safe_psql('postgres', "DROP TABLE o_test_heap_ddl;\n")
		node.safe_psql('postgres', "DROP TABLE o_test_ddl;\n")

		node.safe_psql(
		    'postgres',
		    "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_ddl ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap_ddl ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)

		node.stop()

	def test_rewind_xid_ddl_truncate(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE o_test_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE o_test_heap_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test_ddl (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap_ddl (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test_ddl (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap_ddl (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_ddl;')),
		    "[(5,)]")
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT count(*) FROM o_test_heap_ddl;')),
		    "[(5,)]")

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)

		node.safe_psql(
		    'postgres',
		    "TRUNCATE TABLE o_test_ddl; TRUNCATE TABLE o_test_heap_ddl;")

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '3'
		],
		                       stderr=sys.stderr)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT count(*) FROM o_test_ddl;')),
		    "[(15,)]")
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT count(*) FROM o_test_heap_ddl;')),
		    "[(15,)]")

		node.safe_psql(
		    'postgres',
		    "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_ddl ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap_ddl ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)

		node.stop()

	def test_rewind_xid_ddl_rewrite(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_buffers = 6\n")
		node.start()

		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_test_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE o_test_heap_ddl (\n"
		    "	id serial NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		fp1 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp1.write("INSERT INTO o_test_ddl (val) VALUES ('oldval!');\n")
		fp1.write("INSERT INTO o_test_heap_ddl (val) VALUES ('oldval!');\n")
		fp1.close()
		fp2 = tempfile.NamedTemporaryFile(mode='wt', delete_on_close=False)
		fp2.write("INSERT INTO o_test_ddl (val) VALUES ('newval!');\n")
		fp2.write("INSERT INTO o_test_heap_ddl (val) VALUES ('newval!');\n")
		fp2.close()

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp1.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		a, *b = (node.execute('postgres',
		                      'select orioledb_get_current_oxid();\n'))[0]
		oxid = int(a)
		#		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		#		print(xid)

		node.safe_psql('postgres',
		               "ALTER TABLE o_test_ddl ALTER COLUMN id TYPE real;")
		node.safe_psql(
		    'postgres',
		    "ALTER TABLE o_test_heap_ddl ALTER COLUMN id TYPE real;")

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '1'
		],
		                       stderr=sys.stderr)

		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_ddl ORDER BY id;')),
		    "[(1.0, 'oldval!'), (2.0, 'oldval!'), (3.0, 'oldval!'), (4.0, 'oldval!'), (5.0, 'oldval!'), (6.0, 'newval!'), (7.0, 'newval!'), (8.0, 'newval!'), (9.0, 'newval!'), (10.0, 'newval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap_ddl ORDER BY id;')),
		    "[(1.0, 'oldval!'), (2.0, 'oldval!'), (3.0, 'oldval!'), (4.0, 'oldval!'), (5.0, 'oldval!'), (6.0, 'newval!'), (7.0, 'newval!'), (8.0, 'newval!'), (9.0, 'newval!'), (10.0, 'newval!')]"
		)

		node.pgbench_with_wait(options=[
		    '-M', 'prepared', '-f', fp2.name, '-n', '-c', '5', '-j', '5', '-t',
		    '2'
		],
		                       stderr=sys.stderr)

		node.safe_psql(
		    'postgres',
		    "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_ddl ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)
		self.assertEqual(
		    str(
		        node.execute('postgres',
		                     'SELECT * FROM o_test_heap_ddl ORDER BY id;')),
		    "[(1, 'oldval!'), (2, 'oldval!'), (3, 'oldval!'), (4, 'oldval!'), (5, 'oldval!')]"
		)

		node.stop()

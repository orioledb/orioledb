#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import time
import re
import os

from .base_test import BaseTest
from .base_test import generate_string
from testgres.enums import NodeStatus

import string
import random


class RewindXidTest(BaseTest):

	def test_rewind_xid_oriole(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n")
		node.start()

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n")

		for i in range(1, 6):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))

		a, *b = (node.execute('postgres', 'select orioledb_current_oxid();\n'))[0]
		oxid = int(a)
		print(oxid)
		time.sleep(1)
		invalidxid = 0

		for i in range(6, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))

		node.safe_psql('postgres',
		               "select orioledb_rewind_to_transaction(%d,%ld);\n" % (invalidxid, oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]")
		node.stop()

	def test_rewind_xid_heap(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n")
		node.start()

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		for i in range(1, 6):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))

		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		print(xid)
		invalidoxid = 9223372036854775807
		time.sleep(1)

		for i in range(6, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))

		node.safe_psql('postgres',
		               "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid,invalidoxid))
		time.sleep(1)
		node.is_started = False
		node.start()

		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test_heap;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]")
		node.stop()

	def test_rewind_xid_heap_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n")
		node.start()

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		for i in range(1, 25, 4):
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))

		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		print(xid)
		invalidoxid = 9223372036854775807
		time.sleep(1)

		i = 25
		node.execute(
		    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))

		for i in range(29, 80, 4):
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))

		node.safe_psql('postgres',
		               "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid,invalidoxid))
		time.sleep(10)
		node.is_started = False
		node.start()

		self.maxDiff = None
		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test_heap;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val'), (9, '9val'), (10, '10val'), (11, '11val'), (12, '12val'), (13, '13val'), (14, '14val'), (15, '15val'), (16, '16val'), (17, '17val'), (18, '18val'), (19, '19val'), (20, '20val'), (21, '21val'), (22, '22val'), (23, '23val'), (24, '24val')]")
		node.stop()

	def test_rewind_xid_oriole_heap(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n")
		node.start()

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		for i in range(1, 6):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))

		a, *b = (node.execute('postgres', 'select orioledb_current_oxid();\n'))[0]
		oxid = int(a)
		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		print(xid)
		time.sleep(1)

		for i in range(6, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))

		node.safe_psql('postgres',
		               "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid,oxid))
		time.sleep(1)

		node.is_started = False
		node.start()

		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]")
		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test_heap;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]")
		node.stop()

	def test_rewind_xid_oriole_heap_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 500\n"
		    "orioledb.enable_rewind = true\n")
		node.start()

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n")

		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING heap;\n")

		for i in range(1, 25, 4):
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i, i, i+1, i+1, i+1, i+1, i+2, i+2, i+2, i+2 ,i+3, i+3, i+3, i+3))

		a, *b = (node.execute('postgres', 'select orioledb_current_oxid();\n'))[0]
		oxid = int(a)
		print(oxid)
		a, *b = (node.execute('postgres', 'select pg_current_xact_id();\n'))[0]
		xid = int(a)
		print(xid)
		time.sleep(1)
		i = 25

		node.safe_psql(
		    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
		    (i, i, i, i, i+1, i+1, i+1, i+1, i+2, i+2, i+2, i+2 ,i+3, i+3, i+3, i+3))

		for i in range(29, 80, 4):
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i, i, i+1, i+1, i+1, i+1, i+2, i+2, i+2, i+2 ,i+3, i+3, i+3, i+3))

		node.safe_psql('postgres',
		               "select orioledb_rewind_to_transaction(%d,%ld);\n" % (xid,oxid))

		time.sleep(10)
		node.is_started = False
		node.start()

		self.maxDiff = None
		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test_heap;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val'), (9, '9val'), (10, '10val'), (11, '11val'), (12, '12val'), (13, '13val'), (14, '14val'), (15, '15val'), (16, '16val'), (17, '17val'), (18, '18val'), (19, '19val'), (20, '20val'), (21, '21val'), (22, '22val'), (23, '23val'), (24, '24val')]")
		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val'), (9, '9val'), (10, '10val'), (11, '11val'), (12, '12val'), (13, '13val'), (14, '14val'), (15, '15val'), (16, '16val'), (17, '17val'), (18, '18val'), (19, '19val'), (20, '20val'), (21, '21val'), (22, '22val'), (23, '23val'), (24, '24val')]")
		node.stop()

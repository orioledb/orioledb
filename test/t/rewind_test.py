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


class RewindTest(BaseTest):

	def test_rewind_oriole(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_period = 100\n"
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

		for i in range(1, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))
			time.sleep(1)

		node.safe_psql('postgres',
		               "select orioledb_rewind(20);\n")
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

	def test_rewind_heap(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_period = 100\n"
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

		for i in range(1, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))
			time.sleep(1)

		node.safe_psql('postgres',
		               "select orioledb_rewind(20);\n")

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

	def test_rewind_heap_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_period = 100\n"
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

		for i in range(1, 80, 4):
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))
			time.sleep(1)

		node.safe_psql('postgres',
		               "select orioledb_rewind(20);\n")

		time.sleep(1)
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

	def test_rewind_oriole_heap(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_period = 100\n"
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

		for i in range(1, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" %
			    (i, i))
			time.sleep(1)

		node.safe_psql('postgres',
		               "select orioledb_rewind(20);\n")
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

	def test_rewind_oriole_heap_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_period = 100\n"
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

		for i in range(1, 80, 4):
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))
			node.safe_psql(
			    'postgres', "BEGIN; INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))
			time.sleep(1)

		node.safe_psql('postgres',
		               "select orioledb_rewind(20);\n")

		time.sleep(1)
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

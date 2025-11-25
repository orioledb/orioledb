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

	def wait_shutdown_and_start(self, node):
		super().wait_shutdown_and_start(node)

	def wait_restart(self, node, previous_start_time):
		super().wait_restart(node, previous_start_time)

	def get_pg_start_time(self, node):
		return super().get_pg_start_time(node)

	def test_rewind_oriole(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 100\n"
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
			    "	VALUES (%d, %d || 'val');\n" % (i, i))

		# Store the current timestamp in a temp table to reuse it later
		node.safe_psql(
		    'postgres', "DROP TABLE IF EXISTS o_rewind;\n"
		    "CREATE TABLE o_rewind(ts, note) AS\n"
		    "	SELECT\n"
		    "		clock_timestamp() as ts,\n"
		    "		'before sleep' as note;")

		time.sleep(10)

		for i in range(6, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test\n"
			    "	VALUES (%d, %d || 'val');\n" % (i, i))

		previous_start_time = self.get_pg_start_time(node)

		# Rewind to the time we stored above + 5 seconds for safety
		node.safe_psql(
		    'postgres', "SELECT orioledb_rewind_by_time(i-5) "
		    "	FROM "
		    "		o_rewind as r, "
		    "		lateral (select floor(date_part('epoch', clock_timestamp()-r.ts))::int4) f(i) "
		    "	WHERE note = 'before sleep';")

		self.wait_restart(node, previous_start_time)

		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]"
		)
		node.stop()

	def test_rewind_heap(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 100\n"
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
			    "	VALUES (%d, %d || 'val');\n" % (i, i))

		# Store the current timestamp in a temp table to reuse it later
		node.safe_psql(
		    'postgres', "DROP TABLE IF EXISTS o_rewind;\n"
		    "CREATE TABLE o_rewind(ts, note) AS\n"
		    "	SELECT\n"
		    "		clock_timestamp() as ts,\n"
		    "		'before sleep' as note;")

		time.sleep(10)

		for i in range(6, 20):
			node.safe_psql(
			    'postgres', "INSERT INTO o_test_heap\n"
			    "	VALUES (%d, %d || 'val');\n" % (i, i))

		previous_start_time = self.get_pg_start_time(node)

		# Rewind to the time we stored above + 5 seconds for safety
		node.safe_psql(
		    'postgres', "SELECT orioledb_rewind_by_time(i-5) "
		    "	FROM "
		    "		o_rewind as r, "
		    "		lateral (select floor(date_part('epoch', clock_timestamp()-r.ts))::int4) f(i) "
		    "	WHERE note = 'before sleep';")

		self.wait_restart(node, previous_start_time)

		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test_heap;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]"
		)
		node.stop()


## Tests disabled for not to strain CI as they are much similar to neighbouring
#
#	def test_rewind_heap_subxids(self):
#		node = self.node
#		node.append_conf(
#		    'postgresql.conf', "orioledb.rewind_max_time = 100\n"
#		    "orioledb.enable_rewind = true\n")
#		node.start()
#
#		node.safe_psql('postgres',
#		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
#
#		node.safe_psql(
#		    'postgres', "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
#		    "	id integer NOT NULL,\n"
#		    "	val text,\n"
#		    "	PRIMARY KEY (id)\n"
#		    ") USING heap;\n")
#
#		for i in range(1, 25, 4):
#			node.safe_psql(
#			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
#			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
#			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
#			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
#			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))
#
#		i = 25
#		node.safe_psql(
#		    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
#		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
#		    "select pg_sleep(10);\n"
#		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
#		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
#			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))
#
#		for i in range(29, 80, 4):
#			node.safe_psql(
#			    'postgres', "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
#			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
#			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
#			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val'); COMMIT;\n" %
#			    (i, i, i+1, i+1, i+2, i+2, i+3, i+3))
#
#		node.safe_psql('postgres',
#		               "select orioledb_rewind_by_time(9);\n")
#
#		self.wait_shutdown_and_start(node)
#
#		self.maxDiff = None
#		self.assertEqual(
#		    str(
#		        node.execute(
#		            'postgres',
#		            'SELECT * FROM o_test_heap;')),
#		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val'), (9, '9val'), (10, '10val'), (11, '11val'), (12, '12val'), (13, '13val'), (14, '14val'), (15, '15val'), (16, '16val'), (17, '17val'), (18, '18val'), (19, '19val'), (20, '20val'), (21, '21val'), (22, '22val'), (23, '23val'), (24, '24val')]")
#		node.stop()
#
#	def test_rewind_oriole_heap(self):
#		node = self.node
#		node.append_conf(
#		    'postgresql.conf', "orioledb.rewind_max_time = 100\n"
#		    "orioledb.enable_rewind = true\n")
#		node.start()
#
#		node.safe_psql('postgres',
#		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
#
#		node.safe_psql(
#		    'postgres', "CREATE TABLE IF NOT EXISTS o_test (\n"
#		    "	id integer NOT NULL,\n"
#		    "	val text,\n"
#		    "	PRIMARY KEY (id)\n"
#		    ") USING orioledb;\n")
#
#		node.safe_psql(
#		    'postgres', "CREATE TABLE IF NOT EXISTS o_test_heap (\n"
#		    "	id integer NOT NULL,\n"
#		    "	val text,\n"
#		    "	PRIMARY KEY (id)\n"
#		    ") USING heap;\n")
#
#		for i in range(1, 6):
#			node.safe_psql(
#			    'postgres', "INSERT INTO o_test\n"
#			    "	VALUES (%d, %d || 'val');\n" %
#			    (i, i))
#			node.safe_psql(
#			    'postgres', "INSERT INTO o_test_heap\n"
#			    "	VALUES (%d, %d || 'val');\n" %
#			    (i, i))
#
#		time.sleep(10)
#
#		for i in range(6, 20):
#			node.safe_psql(
#			    'postgres', "INSERT INTO o_test\n"
#			    "	VALUES (%d, %d || 'val');\n" %
#			    (i, i))
#			node.safe_psql(
#			    'postgres', "INSERT INTO o_test_heap\n"
#			    "	VALUES (%d, %d || 'val');\n" %
#			    (i, i))
#
#		node.safe_psql('postgres',
#		               "select orioledb_rewind_by_time(9);\n")
#
#		self.wait_shutdown_and_start(node)
#
#		self.assertEqual(
#		    str(
#		        node.execute(
#		            'postgres',
#		            'SELECT * FROM o_test;')),
#		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]")
#		self.assertEqual(
#		    str(
#		        node.execute(
#		            'postgres',
#		            'SELECT * FROM o_test_heap;')),
#		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val')]")
#		node.stop()

	def test_rewind_subxids(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.rewind_max_time = 100\n"
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
			    'postgres',
			    "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i, i, i + 1, i + 1, i + 1, i + 1, i + 2, i + 2, i + 2,
			     i + 2, i + 3, i + 3, i + 3, i + 3))

		i = 25
		node.safe_psql(
		    'postgres',
		    "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
		    "DROP TABLE IF EXISTS o_rewind;"
		    "CREATE TABLE o_rewind(ts, note) AS\n"
		    "	SELECT\n"
		    "		clock_timestamp() as ts,\n"
		    "		'before sleep' as note;"
		    "SELECT pg_sleep(10);\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
		    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
		    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
		    (i, i, i, i, i + 1, i + 1, i + 1, i + 1, i + 2, i + 2, i + 2,
		     i + 2, i + 3, i + 3, i + 3, i + 3))

		for i in range(29, 80, 4):
			node.safe_psql(
			    'postgres',
			    "BEGIN; INSERT INTO o_test_heap VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp1;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp2;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); SAVEPOINT sp3;\n"
			    "INSERT INTO o_test_heap VALUES (%d, %d || 'val');\n"
			    "INSERT INTO o_test VALUES (%d, %d || 'val'); COMMIT;\n" %
			    (i, i, i, i, i + 1, i + 1, i + 1, i + 1, i + 2, i + 2, i + 2,
			     i + 2, i + 3, i + 3, i + 3, i + 3))

		previous_start_time = self.get_pg_start_time(node)
		node.safe_psql(
		    'postgres', "SELECT orioledb_rewind_by_time(i-5) "
		    "	FROM "
		    "		o_rewind as r, "
		    "		lateral (select floor(date_part('epoch', clock_timestamp()-r.ts))::int4) f(i) "
		    "	WHERE note = 'before sleep';")

		self.wait_restart(node, previous_start_time)

		self.maxDiff = None
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test_heap;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val'), (9, '9val'), (10, '10val'), (11, '11val'), (12, '12val'), (13, '13val'), (14, '14val'), (15, '15val'), (16, '16val'), (17, '17val'), (18, '18val'), (19, '19val'), (20, '20val'), (21, '21val'), (22, '22val'), (23, '23val'), (24, '24val')]"
		)
		self.assertEqual(
		    str(node.execute('postgres', 'SELECT * FROM o_test;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val'), (9, '9val'), (10, '10val'), (11, '11val'), (12, '12val'), (13, '13val'), (14, '14val'), (15, '15val'), (16, '16val'), (17, '17val'), (18, '18val'), (19, '19val'), (20, '20val'), (21, '21val'), (22, '22val'), (23, '23val'), (24, '24val')]"
		)
		node.stop()

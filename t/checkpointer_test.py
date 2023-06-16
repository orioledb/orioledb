#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import time
import re
import os

from .base_test import BaseTest
from .base_test import wait_checkpointer_stopevent
from .base_test import generate_string
from testgres.enums import NodeStatus

import string
import random

class CheckpointerTest(BaseTest):
	def test_checkpoint_by_time(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.debug_checkpoint_timeout = 2s\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		con1 = node.connect()

		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		node.safe_psql('postgres',
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "	id integer NOT NULL,\n"
					   "	val text,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		con1.execute("SELECT pg_stopevent_set('checkpoint_step', 'true');")

		node.safe_psql('postgres',
					   "INSERT INTO o_test\n"
					   "	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n")

		wait_checkpointer_stopevent(node)

		con1.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		con1.close()

		node.stop(['-m', 'immediate'])

		self.assertTrue(self.is_checkpoint_exist())

		node.start()
		self.assertEqual(
			str(node.execute('postgres',
							 'SELECT * FROM o_test WHERE id BETWEEN 1 and 8;')),
			"[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'),"
			" (6, '6val'), (7, '7val'), (8, '8val')]")
		node.stop()

	def test_checkpoint_by_wal_size(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 500MB\n"
						 "max_wal_size = 32MB\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		con1 = node.connect()
		node.safe_psql('postgres',
					   "CREATE TABLE IF NOT EXISTS o_test (\n"
					   "    val text\n"
					   ") USING orioledb;\n"
					   "TRUNCATE o_test;\n"
		)
		con1.execute("SELECT pg_stopevent_set('checkpoint_step', 'true');")

		con2 = node.connect()

		val = generate_string(2000)
		for _ in range(0, 10000):
			con2.execute("INSERT INTO o_test VALUES ('%s');" % (val))
		con2.commit()

		wait_checkpointer_stopevent(node)
		con1.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		self.assertTrue(self.is_checkpoint_exist())

		node.start()

		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 10000)
		node.stop()

	def is_checkpoint_exist(self):
		orioledb_dir = self.node.data_dir + "/orioledb_data"
		exist = False
		for f in os.listdir(orioledb_dir):
			dbDir = os.path.join(orioledb_dir, f)
			if os.path.isdir(dbDir):
				for ff in os.listdir(dbDir):
					if re.match(".*[1-9]\.map$", ff):
						exist = True
		return exist

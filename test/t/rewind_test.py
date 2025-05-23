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

	def test_rewind(self):
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

		for i in range(1, 10):
		    node.safe_psql(
		    'postgres', "INSERT INTO o_test\n"
		    "	VALUES (%d, %d || 'val');\n" %
		    (i, i))
		time.sleep(1)

		node.safe_psql('postgres',
		               "select orioledb_rewind(5);\n")
		time.sleep(3);

#		node.stop(['-m', 'immediate'])
		time.sleep(3);
		node.start()
		time.sleep(3);

		self.assertEqual(
		    str(
		        node.execute(
		            'postgres',
		            'SELECT * FROM o_test;')),
		    "[(1, '1val'), (2, '2val'), (3, '3val'), (4, '4val'), (5, '5val'),")
		node.stop()

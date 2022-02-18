#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import logging
import time

from .base_test import BaseTest

from testgres.enums import NodeStatus
from testgres.exceptions import QueryException

class RecoveryWorkerTest(BaseTest):
	def test_recovery_worker(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text,\n"
			"	PRIMARY KEY (id)"
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
		con1.commit()
		con2 = node.connect()
		con2.begin()
		con2.execute("UPDATE o_test SET val = 'yyy1' WHERE id = 3;")
		con2.execute("DELETE FROM o_test WHERE id = 4;")
		con2.execute("INSERT INTO o_test VALUES (1002, 'yyy2');")
		con2.commit()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
			str(node.execute('postgres', 'SELECT * FROM o_test WHERE id BETWEEN 1 and 8;')),
			"[(1, 'xxx1'), (3, 'yyy1'), (5, '5val'), (6, '6val'), (7, '7val'), (8, '8val')]")
		node.stop()

	def test_too_much_workers(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
			"INSERT INTO o_test\n"
			"	(SELECT id, id || 'val' FROM generate_series(1, 1000, 1) id);\n"
		)
		node.stop(['-m', 'immediate'])

		node.append_conf('postgresql.conf',
						 "max_worker_processes = 8\n"
						 "orioledb.recovery_pool_size = 12\n")

		try:
			node.start()
			self.assertFalse(True)
		except Exception:
			self.assertTrue(True)

	def wait_recovery_breakpoint(self, block_pid):
		recovery_pid = None
		while recovery_pid == None:
			recovery_pid = self.node.execute("SELECT pid FROM pg_stat_activity WHERE backend_type = 'orioledb recovery master';")
			if len(recovery_pid) > 0 and len(recovery_pid[0]) > 0:
				recovery_pid = recovery_pid[0][0]
			else:
				recovery_pid = None

		self.wait_breakpoint(recovery_pid, block_pid)

	def wait_breakpoint(self, blocked_pid, block_pid):
		while self.node.execute("SELECT pg_isolation_test_session_is_blocked(%d, '{%d}');"
								 % (blocked_pid, block_pid))[0][0] == False:
			continue

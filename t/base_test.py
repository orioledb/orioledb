#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import sys
import os
import random
import re
import string
import time
import hashlib
import base64
import inspect

from threading import Thread
from testgres.enums import NodeStatus
from testgres.consts import PG_CONF_FILE
from testgres.utils import get_pg_version

class BaseTest(unittest.TestCase):
	replica = None
	basePort = None

	def getTestNum(self):
		testFullName = inspect.getfile(self.__class__)
		names = []
		for entry in os.scandir(os.path.dirname(testFullName)):
			if entry.is_file() and entry.name.endswith('_test.py') and entry.name != 'base_test.py':
				names.append(entry.name)
		names.sort()
		return names.index(os.path.basename(testFullName))

	def getBasePort(self):
		if self.basePort is None:
			self.basePort = int(os.getenv('TESTGRES_BASE_PORT', '20000')) + self.getTestNum() * 2
		return self.basePort

	def getReplica(self) -> testgres.PostgresNode:
		if self.replica is None:
			replica = self.node.backup().spawn_replica('replica')
			replica.port = self.getBasePort() + 1
			replica.append_conf(filename=PG_CONF_FILE, line='\n')
			replica.append_conf(filename=PG_CONF_FILE, port=replica.port)
			self.replica = replica
		return self.replica

	def setUp(self):
		self.startTime = time.time()
		self.node = testgres.get_new_node('test', port = self.getBasePort())
		self.node.init()  # run initdb
		self.node.append_conf('postgresql.conf',
							  "shared_preload_libraries = orioledb\n")

	def list2reason(self, exc_list):
		if exc_list and exc_list[-1][0] is self:
			return exc_list[-1][1]

	def tearDown(self):
		if hasattr(self._outcome, 'errors'):
			# Python 3.4 - 3.10  (These two methods have no side effects)
			result = self.defaultTestResult()  # these 2 methods have no side effects
			self._feedErrorsToResult(result, self._outcome.errors)
		else:
			# Python 3.11+
			result = self._outcome.result
		error = self.list2reason(result.errors)
		failure = self.list2reason(result.failures)
		ok = not error and not failure
		if self.node.status() == NodeStatus.Running:
			self.node.stop()  # just comment it if node should not stops on fails
			pass
		if ok:
			self.node.cleanup()
		else:
			print("\nBase directory: " + self.node.base_dir)
		if self.replica:
			if self.replica.status() == NodeStatus.Running:
				self.replica.stop()  # just comment it if node should not stops on fails
				pass
			if ok:
				self.replica._custom_base_dir = None
				self.replica.cleanup()
			else:
				print("\nReplica base directory: " + self.replica.base_dir)
		t = time.time() - self.startTime
		sys.stderr.write('%.3f s ' % (t,))

	def genString(self, id, length):
		i = 0
		chunkLen = 21
		result = b''
		while i * chunkLen < length:
			m = hashlib.md5()
			m.update((str(id) + '-' + str(i)).encode('ascii'))
			result = result + base64.b64encode(m.digest())
			i = i + 1
		return result[0:length].decode('ascii')

	def assertErrorMessageEquals(self, e: Exception, err_msg: str,
								 second_msg: str = None,
								 second_title: str = 'HINT'):
		if (hasattr(e, 'exception')):
			e = e.exception

		if (hasattr(e, 'pgerror')) or (hasattr(e, 'message')):
			exp_msg = "ERROR:  %s\n" % (err_msg)
			if (second_msg != None):
				exp_msg += "%s:  %s\n" % (second_title, second_msg)

		if (hasattr(e, 'pgerror')):
			msg = e.pgerror
		elif (hasattr(e, 'message')):
			msg = e.message
		else:
			exp_msg = err_msg
			msg = e.args[0]['M']
		self.assertEqual(msg, exp_msg)

	@staticmethod
	def get_pg_version():
		return int(re.match(r'\d+', get_pg_version())[0])

	def catchup_orioledb(self, replica):
		# wait for synchronization
		replica.catchup()
		replica.poll_query_until("SELECT orioledb_recovery_synchronized();",
								 expected = True)

# execute SQL query Thread for PostgreSql node's connection
class ThreadQueryExecutor(Thread):
	def __init__(self, connection, sql_query):
		Thread.__init__(self, target=ThreadQueryExecutor.execute_con, args=(connection, sql_query))
		self._return = None

	def run(self):
		try:
			if self._target:
				self._return = self._target(*self._args)
		finally:
			del self._target, self._args

	def join(self,timeout=None):
		Thread.join(self,timeout)
		if isinstance(self._return, Exception):
			raise self._return
		return self._return

	@staticmethod
	def execute_con(connection, sql_query):
		try:
			return connection.execute(sql_query)
		except Exception as e:
			return e

def generate_string(size, seed = None):
	if seed:
		random.seed(seed)
	chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
	return ''.join(random.choice(chars) for _ in range(size))

def wait_stopevent(node, blocked_pid):
	while node.execute("""SELECT EXISTS(
							 SELECT se.*
							 FROM pg_stopevents() se
							 WHERE se.waiter_pids @> ARRAY[%d]
						  );""" % (blocked_pid, ))[0][0] == False:
		time.sleep(0.1)
		continue

# waits for blocking checkpointer process on breakpoint by process with pid = block_pid
def wait_checkpointer_stopevent(node):
	checkpointer_pid = None
	while checkpointer_pid == None:
		select_list = node.execute("SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer';")
		# checkpointer may not start yet, check list range
		if len(select_list) > 0 and len(select_list[0]) > 0:
			checkpointer_pid = select_list[0][0]

	wait_stopevent(node, checkpointer_pid)

# waits for blocking bgwriter process on breakpoint by process with pid = block_pid
def wait_bgwriter_stopevent(node):
	bgwriter_pid = None
	while bgwriter_pid == None:
		select_list = node.execute("SELECT pid FROM pg_stat_activity WHERE backend_type = 'orioledb background writer';")
		# checkpointer may not start yet, check list range
		if len(select_list) > 0 and len(select_list[0]) > 0:
			bgwriter_pid = select_list[0][0]

	wait_stopevent(node, bgwriter_pid)

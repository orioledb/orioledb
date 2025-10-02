#!/usr/bin/env python3
# coding: utf-8

import base64
import hashlib
import inspect
import os
import random
import re
import shutil
import socket
import string
import sys
import testgres
import time
import typing
import unittest

from threading import Thread
from testgres.enums import NodeStatus
from testgres.exceptions import PortForException
from testgres.node import PostgresNode
from testgres.operations.os_ops import OsOperations, ConnectionParams
from testgres.port_manager import PortManager__Generic
from testgres.utils import get_pg_version, get_pg_config
from typing import Any


class TestPortManager(PortManager__Generic):

	def __init__(self, os_ops: OsOperations, base_port: int):
		super().__init__(os_ops)
		self._available_ports: typing.Set[int] = set(
		    [base_port, base_port + 1])

	def is_port_free(self, port: int):
		port_free = port in self._available_ports

		if port_free:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				try:
					s.bind(("", port))
					return True
				except OSError:
					return False
		return False

	def reserve_port(self) -> int:
		assert self._guard is not None
		assert type(self._available_ports) == set  # noqa: E721t
		assert type(self._reserved_ports) == set  # noqa: E721

		with self._guard:
			t = tuple(self._available_ports)
			assert len(t) == len(self._available_ports)
			t = None

			for port in self._available_ports:
				assert not (port in self._reserved_ports)

				port_free = self.is_port_free(port)

				if not port_free:
					continue

				self._reserved_ports.add(port)
				self._available_ports.discard(port)
				assert port in self._reserved_ports
				assert not (port in self._available_ports)
				return port

		raise PortForException("Can't select a port.")


class BaseTest(unittest.TestCase):
	replica = None
	subscriber = None
	restoredNode = None
	basePort = None
	_myName = None

	def getTestNum(self):
		testFullName = inspect.getfile(self.__class__)
		names = []
		for entry in os.scandir(os.path.dirname(testFullName)):
			if entry.is_file() and entry.name.endswith(
			    '_test.py') and entry.name != 'base_test.py':
				names.append(entry.name)
		names.sort()
		return names.index(os.path.basename(testFullName))

	def getBasePort(self):
		if self.basePort is None:
			self.basePort = int(os.getenv('TESTGRES_BASE_PORT',
			                              '20000')) + self.getTestNum() * 2
		return self.basePort

	def getReplica(self) -> testgres.PostgresNode:
		if self.replica is None:
			(test_path, t) = os.path.split(
			    os.path.dirname(inspect.getfile(self.__class__)))
			baseDir = os.path.join(test_path, 'tmp_check_t',
			                       self.myName + '_tgsb')
			if os.path.exists(baseDir):
				shutil.rmtree(baseDir)
			replica = self.node.backup(
			    base_dir=baseDir).spawn_replica('replica')
			replica.append_conf(port=replica.port)
			self.replica = replica
		return self.replica

	def getSubsriber(self) -> testgres.PostgresNode:
		if self.subscriber is None:
			(test_path, t) = os.path.split(
			    os.path.dirname(inspect.getfile(self.__class__)))
			baseDir = os.path.join(test_path, 'tmp_check_t',
			                       self.myName + '_tgss')
			if os.path.exists(baseDir):
				shutil.rmtree(baseDir)

			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')
			self.subscriber = subscriber
		return self.subscriber

	def restoreNode(self, port: int, filename: str) -> testgres.PostgresNode:
		self.assertIsNone(self.restoredNode)

		restoredNode = self.initNode(port, "restored_tgsn")
		restoredNode.start()
		restoredNode.restore(filename)

		self.restoredNode = restoredNode

		return restoredNode

	def initNode(self, base_port: int, suffix='tgsn') -> testgres.PostgresNode:
		(test_path,
		 t) = os.path.split(os.path.dirname(inspect.getfile(self.__class__)))
		baseDir = os.path.join(test_path, 'tmp_check_t',
		                       self.myName + '_' + suffix)
		if os.path.exists(baseDir):
			shutil.rmtree(baseDir)
		port_manager = TestPortManager(
		    PostgresNode._get_os_ops(ConnectionParams()), base_port)
		node = testgres.get_new_node('test',
		                             base_dir=baseDir,
		                             port_manager=port_manager)
		node.init(["--no-locale", "--encoding=UTF8"])  # run initdb
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.use_sparse_files = true\n"
		    "restart_after_crash = false\n")
		return node

	@property
	def myName(self):
		if not self._myName:
			name = os.path.basename(inspect.getfile(self.__class__))
			if name.endswith('_test.py'):
				name = name[:-8]
			elif name.endswith('.py'):
				name = name[:-3]
			name = name + '-' + self.id().split('.')[-1].removeprefix('test_')
			self._myName = name
		return self._myName

	def setUp(self):
		self.startTime = time.time()
		self.node = self.initNode(self.getBasePort())

	def list2reason(self, exc_list):
		if exc_list and exc_list[-1][0] is self:
			return exc_list[-1][1]

	def tearDown(self):
		if hasattr(self._outcome, 'errors'):
			# Python 3.4 - 3.10  (These two methods have no side effects)
			result = self.defaultTestResult(
			)  # these 2 methods have no side effects
			self._feedErrorsToResult(result, self._outcome.errors)
		else:
			# Python 3.11+
			result = self._outcome.result
		error = self.list2reason(result.errors)
		failure = self.list2reason(result.failures)
		ok = not error and not failure
		if self.node.status() == NodeStatus.Running:
			self.node.stop(
			)  # just comment it if node should not stops on fails
			pass
		if ok:
			self.node.cleanup()
		else:
			print("\nBase directory: " + self.node.base_dir)
		if self.replica:
			if self.replica.status() == NodeStatus.Running:
				self.replica.stop(
				)  # just comment it if node should not stops on fails
				pass
			if ok:
				self.replica._custom_base_dir = None
				self.replica.cleanup()
			else:
				print("\nReplica base directory: " + self.replica.base_dir)
		if self.subscriber:
			if self.subscriber.status() == NodeStatus.Running:
				self.subscriber.stop(
				)  # just comment it if node should not stops on fails
				pass
			if ok:
				self.subscriber._custom_base_dir = None
				self.subscriber.cleanup()
			else:
				print("\nSubscriber base directory: " +
				      self.subscriber.base_dir)
		if self.restoredNode:
			if self.restoredNode.status() == NodeStatus.Running:
				self.restoredNode.stop(
				)  # just comment it if node should not stops on fails
				pass
			if ok:
				self.restoredNode.cleanup()
			else:
				print("\nRestored node base directory: " +
				      self.restoredNode.base_dir)
		t = time.time() - self.startTime
		sys.stderr.write('%.3f s ' % (t, ))

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

	def stripErrorMsg(self, msg):
		prefix = r'^Utility exited with non-zero code \(\d+\). Error: `'
		match = re.match(prefix, msg)
		if match:
			msg = msg[len(match[0]):]
		if msg.endswith('`'):
			msg = msg[:-1]
		return msg

	def assertErrorMessageEquals(self,
	                             e: Exception,
	                             err_msg: str,
	                             second_msg: str = None,
	                             second_title: str = 'HINT',
	                             third_msg: str = None,
	                             third_title: str = 'HINT'):
		if (hasattr(e, 'exception')):
			e = e.exception

		if (hasattr(e, 'pgerror')) or (hasattr(e, 'message')):
			exp_msg = "ERROR:  %s\n" % (err_msg)
			if (second_msg != None):
				exp_msg += "%s:  %s\n" % (second_title, second_msg)
			if (third_msg != None):
				exp_msg += "%s:  %s\n" % (third_title, third_msg)

		if (hasattr(e, 'pgerror')):
			msg = e.pgerror
		elif (hasattr(e, 'message')):
			msg = e.message
		else:
			exp_msg = err_msg
			msg = e.args[0]['M']

		msg = self.stripErrorMsg(msg)
		msg = msg.rstrip("\r\n")
		exp_msg = exp_msg.rstrip("\r\n")

		self.assertEqual(msg, exp_msg)

	@staticmethod
	def get_pg_version():
		return int(re.match(r'\d+', get_pg_version())[0])

	@staticmethod
	def pg_with_icu():
		with open(os.path.join(get_pg_config()["INCLUDEDIR"],
		                       'pg_config.h')) as file:
			for line in file:
				if re.match(r'#define USE_ICU 1.*', line):
					return True
		return False

	@staticmethod
	def extension_installed(name: str) -> bool:
		if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
			dlsuffix = 'dll'
		elif sys.platform.startswith("darwin"):
			dlsuffix = 'dylib'
		else:
			dlsuffix = 'so'
		pkg_lib_dir = get_pg_config()["PKGLIBDIR"]
		path = os.path.join(pkg_lib_dir, f'{name}.{dlsuffix}')
		return os.path.isfile(path)

	def catchup_orioledb(self, replica):
		# wait for synchronization
		replica.catchup()
		replica.poll_query_until("SELECT orioledb_recovery_synchronized();",
		                         expected=True)

	@staticmethod
	def sparse_files_supported():
		(test_path, t) = os.path.split(os.path.dirname(__file__))
		tmp_check_path = os.path.join(test_path, 'tmp_check_t')
		if not os.path.exists(tmp_check_path):
			os.makedirs(tmp_check_path)
		fname = os.path.join(tmp_check_path, 'sparse_file_test')
		fp = open(fname, 'wb')
		fp.truncate(1024 * 16)
		fp.close()
		stat = os.stat(fname)
		return (stat.st_blocks == 0)


# execute SQL query Thread for PostgreSql node's connection
class ThreadQueryExecutor(Thread):

	def __init__(self, connection, sql_query):
		Thread.__init__(self,
		                target=ThreadQueryExecutor.execute_con,
		                args=(connection, sql_query))
		self._return = None

	def run(self):
		try:
			if self._target:
				self._return = self._target(*self._args)
		finally:
			del self._target, self._args

	def join(self, timeout=None):
		Thread.join(self, timeout)
		if isinstance(self._return, Exception):
			raise self._return
		return self._return

	@staticmethod
	def execute_con(connection, sql_query):
		try:
			return connection.execute(sql_query)
		except Exception as e:
			return e


def generate_string(size, seed=None):
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
		select_list = node.execute(
		    "SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer';"
		)
		# checkpointer may not start yet, check list range
		if len(select_list) > 0 and len(select_list[0]) > 0:
			checkpointer_pid = select_list[0][0]

	wait_stopevent(node, checkpointer_pid)


# waits for blocking bgwriter process on breakpoint by process with pid = block_pid
def wait_bgwriter_stopevent(node):
	bgwriter_pid = None
	while bgwriter_pid == None:
		select_list = node.execute(
		    "SELECT pid FROM pg_stat_activity WHERE backend_type = 'orioledb background writer';"
		)
		# checkpointer may not start yet, check list range
		if len(select_list) > 0 and len(select_list[0]) > 0:
			bgwriter_pid = select_list[0][0]

	wait_stopevent(node, bgwriter_pid)


# workaround for testgres error messages on empty results
def new_execute(self, query, *args):
	self.cursor.execute(query, args)
	try:
		res = self.cursor.fetchall()
		# pg8000 might return tuples
		if isinstance(res, tuple):
			res = [tuple(t) for t in res]

		return res
	except Exception as e:
		return None


# Convert output of orioledb_tbl_structure/orioledb_idx_structure to json
def tbl_structure_to_json(structure: str) -> dict[str, dict[int, Any]]:
	res = dict()
	cur_index = None
	cur_page = None

	# TODO Currently limited information is parsed from the ouput of the
	# orioledb_tbl_structure() function. Add more information to the output
	# JSON object.

	index_pattern_str = r"Index (.+) contents"
	index_pattern = re.compile(index_pattern_str)

	page_pattern_str = r"Page (?P<page>\d+?): level = (?P<level>\d+?)(?:, \S+ = \d+)*(?P<sparse>, sparse)?"
	page_pattern = re.compile(page_pattern_str)

	hikey_pattern_str = r"\s+Hikey: offset = \d+, key = (.+)"
	hikey_pattern = re.compile(hikey_pattern_str)

	for line in structure.splitlines():
		line = line.rstrip()

		# Match an index
		m = index_pattern.search(line)
		if m is not None:
			if m.group(1) not in res:
				cur_index = dict()
				res[m.group(1)] = cur_index
			else:
				raise Exception(f"Duplicate index entry {m.group(1)}")

			continue

		if cur_index is None:
			raise Exception("Invalid table structure")

		# Match a page
		m = page_pattern.search(line)
		if m is not None:
			line_dict = m.groupdict()

			level = int(line_dict["level"])
			if level not in cur_index:
				cur_index[level] = dict()

			cur_level = cur_index[level]

			pagenum = int(line_dict["page"])
			if pagenum not in cur_level:
				cur_level[pagenum] = dict()

			cur_page = cur_level[pagenum]
			cur_page["is_sparse"] = line_dict["sparse"] is not None

			continue

		if cur_page is None:
			raise Exception("Invalid table structure")

		m = hikey_pattern.search(line)
		if m is not None:
			cur_page["hikey"] = m.group(1)

	return res


testgres.NodeConnection.execute = new_execute

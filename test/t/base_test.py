#!/usr/bin/env python3
# coding: utf-8

import base64
import hashlib
import inspect
import os
import random
import re
import shutil
import signal
import socket
import string
import subprocess
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

# When USE_DM_LOG_WRITES=1 is passed (via Makefile's USE_DM_LOG_WRITES flag),
# crash simulations use dm-log-writes to replay only the writes that actually
# reached the block device before the mark, discarding OS-buffered writes.
# Requires root and the dm-log-writes kernel module + replay-log tool.
DM_LOG_WRITES_ENABLED = os.environ.get('USE_DM_LOG_WRITES', '0') == '1'


class DmLogWritesContext:
	"""
	Manages a dm-log-writes device-mapper target for OS buffer loss testing.

	Sets up two loopback devices (data + write log), stacks a log-writes
	DM target on top, and formats ext4 on the result.  Named marks can be
	inserted into the write log at any point; replaying to a mark gives a
	crash-consistent disk image that contains only writes that reached the
	block device before that mark — simulating a hard power failure with no
	OS page-cache data surviving.

	Typical use::

	    ctx = DmLogWritesContext()
	    pgdata_dir = os.path.join(ctx.setup(), 'pgdata')
	    os.makedirs(pgdata_dir)
	    # ... run postgres on pgdata_dir ...
	    ctx.set_mark('before_crash')
	    # ... optional additional writes that will be "lost" ...
	    os.kill(postmaster_pid, signal.SIGKILL)
	    ctx.replay_to_mark('before_crash')   # remounts without lost writes
	    # ... start postgres for crash recovery ...
	    ctx.teardown()
	"""

	DM_NAME_PREFIX = 'orioledb_log_writes'

	def __init__(self, data_size_mb: int = None, log_size_mb: int = None):
		self.data_size_mb = data_size_mb or int(
		    os.environ.get('DM_LOG_WRITES_DATA_MB', '2048'))
		self.log_size_mb = log_size_mb or int(
		    os.environ.get('DM_LOG_WRITES_LOG_MB', '1024'))
		self._tmpdir = None
		self._data_img = None
		self._log_img = None
		self._data_loop = None
		self._log_loop = None
		self._dm_name = None
		self._mount_point = None
		self._mark_counter = 0

	@staticmethod
	def is_available() -> bool:
		"""Return True when the host can run dm-log-writes tests."""
		if not sys.platform.startswith('linux'):
			return False
		for cmd in ('dmsetup', 'losetup', 'replay-log'):
			if shutil.which(cmd) is None:
				return False
		return True

	def _run(self, cmd: list):
		subprocess.check_call(['sudo'] + cmd)

	def _run_output(self, cmd: list) -> str:
		return subprocess.check_output(['sudo'] + cmd).decode().strip()

	def setup(self) -> str:
		"""
		Create the loopback devices, log-writes DM target and ext4
		filesystem.  Returns the path of the mounted filesystem root.
		"""
		import tempfile
		self._tmpdir = tempfile.mkdtemp(prefix='orioledb_dmlog_')

		self._data_img = os.path.join(self._tmpdir, 'data.img')
		self._log_img = os.path.join(self._tmpdir, 'log.img')

		# Sparse files — fast to create, only consume space on write.
		self._run(['truncate', '-s', f'{self.data_size_mb}M', self._data_img])
		self._run(['truncate', '-s', f'{self.log_size_mb}M', self._log_img])

		# Attach loop devices.
		self._data_loop = self._run_output(
		    ['losetup', '--find', '--show', self._data_img])
		self._log_loop = self._run_output(
		    ['losetup', '--find', '--show', self._log_img])

		# Create the log-writes DM target.
		data_sectors = int(
		    self._run_output(['blockdev', '--getsz', self._data_loop]))
		import uuid
		self._dm_name = f'{self.DM_NAME_PREFIX}_{os.getpid()}_{uuid.uuid4().hex[:8]}'
		dm_dev = f'/dev/mapper/{self._dm_name}'
		self._run([
		    'dmsetup', 'create', self._dm_name, '--table',
		    f'0 {data_sectors} log-writes {self._data_loop} {self._log_loop}'
		])

		# Format and mount.
		self._run(['mkfs.ext4', '-F', '-q', dm_dev])
		self._mount_point = os.path.join(self._tmpdir, 'mnt')
		os.makedirs(self._mount_point)
		self._run(['mount', dm_dev, self._mount_point])
		# Give the current user ownership so unprivileged code can create
		# files and directories on the mounted filesystem.
		self._run(['chown', f'{os.getuid()}:{os.getgid()}', self._mount_point])

		return self._mount_point

	def set_mark(self, name: str = None) -> str:
		"""
		Insert a named mark into the write log.  Returns the mark name.
		All writes that reach the block device before this call will be
		visible after a replay_to_mark(name); later writes will not.
		"""
		if name is None:
			self._mark_counter += 1
			name = f'mark_{self._mark_counter}'
		self._run(['dmsetup', 'message', self._dm_name, '0', f'mark {name}'])
		return name

	def replay_to_mark(self, mark_name: str):
		"""
		Unmount the filesystem, tear down the log-writes DM target, replay
		the write log up to *mark_name* onto the data loop device, run
		e2fsck to fix any partial journal entries, then remount.

		After this call the mount-point contains an ext4 filesystem
		consistent with the state at the named mark, with no OS-buffered
		writes visible — exactly as if power were cut at that instant.
		"""
		# Unmount so all VFS state is flushed before we remove the target.
		self._run(['umount', self._mount_point])

		# Tear down the log-writes target (must precede replay-log).
		self._run(['dmsetup', 'remove', self._dm_name])
		self._dm_name = None

		# Replay writes up to the mark onto the raw data loop device.
		self._run([
		    'replay-log',
		    '--log',
		    self._log_loop,
		    '--replay',
		    self._data_loop,
		    '--end-mark',
		    mark_name,
		])

		# Fix any incomplete journal entries left by the simulated crash.
		# Exit code 1 means "corrections were made" — that is expected here.
		rc = subprocess.call(['sudo', 'e2fsck', '-fp', self._data_loop],
		                     stdout=subprocess.DEVNULL,
		                     stderr=subprocess.DEVNULL)
		if rc not in (0, 1):
			raise RuntimeError(
			    f'e2fsck returned unexpected exit code {rc} '
			    f'after dm-log-writes replay to mark {mark_name!r}')

		# Remount the data device directly (no further write logging).
		self._run(['mount', self._data_loop, self._mount_point])

	def teardown(self):
		"""Release all resources unconditionally (safe to call multiple times)."""
		if self._mount_point and os.path.ismount(self._mount_point):
			subprocess.call(['sudo', 'umount', '-f', self._mount_point])

		if self._dm_name:
			subprocess.call(['sudo', 'dmsetup', 'remove', self._dm_name])
			self._dm_name = None

		for loop in (self._data_loop, self._log_loop):
			if loop:
				subprocess.call(['sudo', 'losetup', '-d', loop])
		self._data_loop = None
		self._log_loop = None

		if self._tmpdir and os.path.exists(self._tmpdir):
			shutil.rmtree(self._tmpdir, ignore_errors=True)
		self._tmpdir = None


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
	_dm_ctx = None

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

	def getReplica(self, has_restoring: bool = False) -> testgres.PostgresNode:
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

			if has_restoring:
				self.enableRestoring(
				    self.replica, os.path.join(self.node.base_dir, "archives"))

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

	def initNode(self,
	             base_port: int,
	             suffix='tgsn',
	             has_archiving: bool = False,
	             allows_streaming: bool = False,
	             override_base_dir: str = None) -> testgres.PostgresNode:
		(test_path,
		 t) = os.path.split(os.path.dirname(inspect.getfile(self.__class__)))
		if override_base_dir is not None:
			baseDir = override_base_dir
		else:
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

		if has_archiving:
			self.enableArchiving(node)
		if allows_streaming:
			self.enableStreaming(node)

		return node

	def enableArchiving(self, node: testgres.PostgresNode):
		path = os.path.join(node.base_dir, "archives")
		node.append_conf(f"archive_mode = on\n"
		                 f"archive_command = 'cp \"%p\" \"{path}/%f\"'")

	def enableStreaming(self, node: testgres.PostgresNode):
		node.append_conf("wal_level = replica\n"
		                 "max_wal_senders = 3\n"
		                 "max_replication_slots = 3")

	def enableRestoring(self, node: testgres.PostgresNode, path: str):
		node.append_conf(f"restore_command = 'cp \"{path}/%f\" \"%p\"'")
		with open(os.path.join(node.data_dir, "recovery.signal"), mode='a'):
			pass

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
		self._dm_ctx = None
		if DM_LOG_WRITES_ENABLED:
			self._dm_ctx = DmLogWritesContext()
			try:
				mount_dir = self._dm_ctx.setup()
				node_dir = os.path.join(mount_dir, 'pgdata')
				os.makedirs(node_dir)
				self.node = self.initNode(self.getBasePort(),
				                          override_base_dir=node_dir)
				# Make sure all the FS infrastructure (mount root chown,
				# pgdata directory, initdb files) is actually on the block
				# device before any test code runs, so a later crash can't
				# lose it.
				os.sync()
			except Exception:
				# tearDown is not called by unittest when setUp raises, so
				# release the DM device and loop devices here to avoid leaking
				# them into the next test.
				self._dm_ctx.teardown()
				self._dm_ctx = None
				raise
		else:
			self.node = self.initNode(self.getBasePort())

	def crash_with_os_buffer_loss(self, mark_name: str = 'crash_point'):
		"""
		Simulate a hard crash with OS buffer loss.

		When USE_DM_LOG_WRITES=1 (and setUp placed the data directory on a
		dm-log-writes device), this method:

		1. Sets a named mark in the write log — capturing all writes that
		   have reached the block device up to this point.
		2. Sends SIGKILL to the postmaster, bypassing any graceful shutdown.
		3. Replays the write log only up to the mark, so writes that were
		   still in OS page-cache buffers at the time of the kill are
		   discarded — exactly as after a power failure.

		Without USE_DM_LOG_WRITES, falls back to ``node.stop(['-m',
		'immediate'])`` so that existing tests continue to work unchanged.

		Call ``node.start()`` after this to trigger orioledb crash recovery.
		"""
		if self._dm_ctx is None or self._dm_ctx._dm_name is None:
			# No dm-log-writes device active: either disabled or already
			# consumed by a prior replay in this test.  Fall back to a
			# regular immediate stop so multi-crash tests still work.
			self.node.stop(['-m', 'immediate'])
			return

		# Flush just the test-harness-written config files to the block
		# device before the mark, so they survive replay-to-mark.  We must
		# NOT os.sync() here — tests rely on unsync'd postgres data pages
		# being lost on replay to exercise crash recovery (e.g. OrioleDB
		# copy-on-write checkpoints).
		conf_path = os.path.join(self.node.data_dir, 'postgresql.conf')
		fd = os.open(conf_path, os.O_RDONLY)
		try:
			os.fsync(fd)
		finally:
			os.close(fd)

		# 1. Mark the point we want to replay to.
		self._dm_ctx.set_mark(mark_name)

		# 2. Kill the postmaster without giving it a chance to flush anything.
		pid_file = os.path.join(self.node.data_dir, 'postmaster.pid')
		with open(pid_file) as f:
			pid = int(f.readline().strip())
		os.kill(pid, signal.SIGKILL)

		# Wait for the postmaster to actually disappear (up to 30 s).
		deadline = time.time() + 30
		while time.time() < deadline:
			try:
				os.kill(pid, 0)
				time.sleep(0.05)
			except ProcessLookupError:
				break

		# 3. Replay the write log to the mark and remount.
		self._dm_ctx.replay_to_mark(mark_name)

		# 4. Let testgres know the node is no longer running so start() works.
		self.node.is_started = False

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
			if self._dm_ctx is not None:
				# Teardown unmounts and deletes the tmpdir that contains
				# base_dir, so node.cleanup() sees a missing directory and
				# skips the rmtree — that is fine.
				self._dm_ctx.teardown()
				self._dm_ctx = None
			self.node.cleanup()
		else:
			if self._dm_ctx is not None:
				print("\nBase directory (dm-log-writes, image files in " +
				      str(self._dm_ctx._tmpdir) + "): " + self.node.base_dir)
				self._dm_ctx.teardown()
				self._dm_ctx = None
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

	@staticmethod
	def sparse_files_supported():
		(test_path, t) = os.path.split(os.path.dirname(__file__))
		tmp_check_path = os.path.join(test_path, 'tmp_check_t')
		if not os.path.exists(tmp_check_path):
			os.makedirs(tmp_check_path, exist_ok=True)
		fname = os.path.join(tmp_check_path, 'sparse_file_test')
		fp = open(fname, 'wb')
		fp.truncate(1024 * 16)
		fp.close()
		stat = os.stat(fname)
		return (stat.st_blocks == 0)

	def wait_shutdown_and_start(self, node):
		while node.status() == NodeStatus.Running:
			time.sleep(0.1)
		node.is_started = False
		node.start()

	def get_pg_start_time(self, node):
		result = node.execute("select pg_postmaster_start_time()")
		start_time = result[0][0]
		return start_time

	def wait_restart(self, node, previous_start_time, timeout_s=900):
		node.poll_query_until(
		    f"select pg_postmaster_start_time() != '{previous_start_time}'",
		    expected=True,
		    sleep_time=1,
		    max_attempts=timeout_s,
		    suppress=[Exception])


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

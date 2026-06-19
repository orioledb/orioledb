#!/usr/bin/env python3
# coding: utf-8

import inspect
import json
import os
import shutil
import subprocess
import time
import unittest

import testgres
from testgres.enums import NodeStatus
from testgres.node import PostgresNode
from testgres.operations.os_ops import ConnectionParams

from .base_test import BaseTest, TestPortManager


class PgBackRestTest(BaseTest):
	STANZA = 'test'

	# ------------------------------------------------------------------
	# Setup / teardown
	# ------------------------------------------------------------------

	def setUp(self):
		self.startTime = time.time()
		self._dm_ctx = None
		self.replica = None
		self.subscriber = None
		self.restoredNode = None

		initdb_args = None
		if self.pg_with_icu():
			initdb_args = [
			    '--locale-provider=icu', '--icu-locale=und', '--encoding=UTF8'
			]

		self.node = self.initNode(self.getBasePort(), initdb_args=initdb_args)

		# Derive paths from the directory that initNode chose
		node_base = self.node.base_dir
		self._repo_path = os.path.join(node_base, 'repo')
		self._log_path = os.path.join(node_base, 'log')
		self._conf_path = os.path.join(node_base, 'pgbackrest.conf')

		os.makedirs(self._repo_path, exist_ok=True)
		os.makedirs(self._log_path, exist_ok=True)

		self._write_pgbackrest_conf(self.node.data_dir)

		self.node.append_conf(
		    'postgresql.conf',
		    f"default_table_access_method = 'orioledb'\n"
		    f"wal_level = replica\n"
		    f"archive_mode = on\n"
		    f"archive_command = '{self._pgbackrest_bin()} --config={self._conf_path}"
		    f" --stanza={self.STANZA} archive-push %p'\n"
		    f"restore_command = '{self._pgbackrest_bin()} --config={self._conf_path}"
		    f" --stanza={self.STANZA} archive-get %f %p'\n"
		    f"max_wal_senders = 3\n"
		    f"max_replication_slots = 3\n")

	def tearDown(self):
		# replica is set to the standby node when created; BaseTest.tearDown
		# handles stopping and cleanup for self.node and self.replica.
		super().tearDown()

	# ------------------------------------------------------------------
	# Helpers
	# ------------------------------------------------------------------

	@staticmethod
	def _pgbackrest_bin():
		return shutil.which('pgbackrest') or 'pgbackrest'

	def _write_pgbackrest_conf(self, pg_path):
		conf = (f"[{self.STANZA}]\n"
		        f"pg1-path={pg_path}\n"
		        f"\n"
		        f"[global]\n"
		        f"repo1-path={self._repo_path}\n"
		        f"log-level-console=warn\n"
		        f"log-level-file=detail\n"
		        f"log-path={self._log_path}\n")
		with open(self._conf_path, 'w') as f:
			f.write(conf)

	def _pgbackrest(self, *args, expect_error=False):
		cmd = [
		    self._pgbackrest_bin(),
		    f'--config={self._conf_path}',
		    f'--stanza={self.STANZA}',
		] + list(args)
		result = subprocess.run(cmd, capture_output=True, text=True)
		if not expect_error and result.returncode != 0:
			raise AssertionError(
			    f"pgbackrest {' '.join(args)} failed (exit {result.returncode}):\n"
			    f"stdout: {result.stdout}\nstderr: {result.stderr}")
		if expect_error and result.returncode == 0:
			raise AssertionError(
			    f"pgbackrest {' '.join(args)} expected to fail but succeeded\n"
			    f"stdout: {result.stdout}")
		return result

	def _backup_count(self):
		result = self._pgbackrest('info', '--output=json')
		info = json.loads(result.stdout)
		return len(info[0]['backup']) if info else 0

	def _restore_primary(self, *extra_opts):
		"""Restore into the primary data dir (uses conf pg1-path)."""
		self._pgbackrest('restore', '--delta', *extra_opts)

	def _create_standby(self, *extra_opts):
		"""
		Restore from pgbackrest into a fresh data dir and return a configured
		testgres node ready to start.  Stored in self.replica so tearDown
		cleans it up.
		"""
		(test_path, _) = os.path.split(
		    os.path.dirname(inspect.getfile(self.__class__)))
		base_dir = os.path.join(test_path, 'tmp_check_t',
		                        self.myName + '_tgsb')
		if os.path.exists(base_dir):
			shutil.rmtree(base_dir)

		port = self.getBasePort() + 1
		pm = TestPortManager(PostgresNode._get_os_ops(ConnectionParams()), port)
		standby = testgres.get_new_node('standby',
		                                base_dir=base_dir,
		                                port_manager=pm)
		os.makedirs(standby.data_dir, exist_ok=True)

		# pgbackrest restore writes standby.signal and restore_command into
		# postgresql.auto.conf, so no manual signal file is needed.
		self._pgbackrest('restore', f'--pg1-path={standby.data_dir}',
		                 '--type=standby', *extra_opts)

		# The restored postgresql.conf has the primary's port; override it.
		standby.append_conf('postgresql.conf', f"port = {standby.port}\n")

		self.replica = standby
		return standby

	def _repromote_standby(self, standby, *extra_opts):
		"""
		Wipe the standby data dir and restore again.  Re-appends the port
		override after restore (pgbackrest rewrites auto.conf).
		"""
		shutil.rmtree(standby.data_dir)
		os.makedirs(standby.data_dir, exist_ok=True)
		self._pgbackrest('restore', f'--pg1-path={standby.data_dir}',
		                 '--type=standby', *extra_opts)
		standby.append_conf('postgresql.conf', f"port = {standby.port}\n")

	def _wait_replication(self, standby, timeout=30):
		"""Poll until the standby's pg_last_wal_receive_lsn stops advancing."""
		standby.poll_query_until(
		    "SELECT pg_last_wal_replay_lsn() IS NOT NULL",
		    expected=True,
		    sleep_time=0.5,
		    max_attempts=timeout * 2,
		    suppress=[Exception])

	# ------------------------------------------------------------------
	# Test
	# ------------------------------------------------------------------

	@unittest.skipUnless(
	    shutil.which('pgbackrest') is not None,
	    'pgbackrest binary not found in PATH')
	def test_integration(self):
		node = self.node
		node.start()

		node.safe_psql(
		    "CREATE EXTENSION IF NOT EXISTS orioledb;"
		    "CREATE TABLE status (message text NOT NULL);")
		node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Stanza create and check
		# ------------------------------------------------------------------
		self._pgbackrest('stanza-create')
		self._pgbackrest('check')

		# ------------------------------------------------------------------
		# Primary full backup
		# ------------------------------------------------------------------
		node.safe_psql("INSERT INTO status VALUES ('full')")
		node.safe_psql("SELECT pg_switch_wal()")
		self._pgbackrest('backup', '--type=full')
		self.assertEqual(self._backup_count(), 1)

		# ------------------------------------------------------------------
		# Standby restore
		# ------------------------------------------------------------------
		standby = self._create_standby()
		standby.start()

		self._wait_replication(standby)
		self.assertEqual(
		    standby.execute('postgres', 'SELECT message FROM status')[0][0],
		    'full')

		# Update primary status; standby should receive it via streaming/log shipping
		node.safe_psql("UPDATE status SET message = 'standby'")
		node.safe_psql("SELECT pg_switch_wal()")
		standby.poll_query_until(
		    "SELECT message = 'standby' FROM status",
		    expected=True,
		    sleep_time=0.5,
		    max_attempts=60,
		    suppress=[Exception])

		# Promote the standby to create timeline 2, then restore it again on
		# current timeline so it tracks the primary.
		standby.safe_psql('SELECT pg_promote()')
		time.sleep(3)  # Allow history file to be archived
		standby.stop()

		self._repromote_standby(standby, '--target-timeline=current')
		standby.start()
		self._wait_replication(standby)
		standby.stop()

		# ------------------------------------------------------------------
		# Second full backup + expire
		# ------------------------------------------------------------------
		self._pgbackrest('backup', '--type=full')
		self.assertEqual(self._backup_count(), 2)

		self._pgbackrest('expire', '--repo1-retention-full=1')
		self.assertEqual(self._backup_count(), 1)

		# ------------------------------------------------------------------
		# Async archiving exercise
		# ------------------------------------------------------------------
		for i in range(1, 6):
			node.safe_psql(f"INSERT INTO status VALUES ('wal_{i}')")
			node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Time target setup
		# ------------------------------------------------------------------
		time.sleep(1)  # Guarantee the timestamp is after the last backup
		node.safe_psql("UPDATE status SET message = 'time'")
		node.safe_psql("SELECT pg_switch_wal()")
		target_time = node.execute(
		    'postgres', 'SELECT current_timestamp::text')[0][0]

		# ------------------------------------------------------------------
		# Incremental backup
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE status SET message = 'incr'")
		self._pgbackrest('backup', '--type=incr')

		# ------------------------------------------------------------------
		# XID target setup
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE status SET message = 'xid'")
		node.safe_psql("SELECT pg_switch_wal()")
		target_xid = node.execute('postgres',
		                          'SELECT txid_current()::text')[0][0]
		node.safe_psql("COMMIT")
		node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Name target setup
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE status SET message = 'name'")
		node.safe_psql("SELECT pg_switch_wal()")
		node.safe_psql("SELECT pg_create_restore_point('pgbackrest')")
		node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Restore: default target (recovers to named restore point)
		# ------------------------------------------------------------------
		node.stop()
		# Restore on the current timeline to avoid the timeline 2 mismatch
		# created by the standby promotion above.
		self._restore_primary('--target-timeline=current', '--force')
		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT message FROM status')[0][0],
		    'name')

		# ------------------------------------------------------------------
		# Restore: immediate target (recovers to end of first backup)
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=immediate', '--target-action=promote')
		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT message FROM status')[0][0],
		    'incr')

		# ------------------------------------------------------------------
		# Restore: time target
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary(f'--type=time', f'--target={target_time}')
		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT message FROM status')[0][0],
		    'time')

		# ------------------------------------------------------------------
		# Restore: XID target (inclusive)
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=xid', f'--target={target_xid}',
		                       '--target-action=promote')
		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT message FROM status')[0][0],
		    'xid')

		# ------------------------------------------------------------------
		# Restore: XID target (exclusive)
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=xid', f'--target={target_xid}',
		                       '--target-exclusive', '--target-action=promote')
		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT message FROM status')[0][0],
		    'incr')

		# ------------------------------------------------------------------
		# Restore: name target
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=name', '--target=pgbackrest')
		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT message FROM status')[0][0],
		    'name')

		# ------------------------------------------------------------------
		# Stanza delete
		# ------------------------------------------------------------------
		node.stop()
		self._pgbackrest('stop')
		self._pgbackrest('stanza-delete', '--force')

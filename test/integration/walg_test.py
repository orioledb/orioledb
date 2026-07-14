#!/usr/bin/env python3
# coding: utf-8
"""
wal-g v2.0.1 integration test using orioledb as the default table access
method: full/incremental backups, standby restore/promotion, and PITR by
time and by named restore point.
"""

import json
import os
import shutil
import subprocess
import time

from threading import Thread

from test.t.base_backup_test import BaseBackupTest
from test.t.base_test import wait_checkpointer_stopevent


class WalGTest(BaseBackupTest):

	# ------------------------------------------------------------------
	# Setup / teardown
	# ------------------------------------------------------------------

	def setUp(self):
		super().setUp()

		node_base = self.node.base_dir
		self._repo_path = os.path.join(node_base, 'repo')

		os.makedirs(self._repo_path, exist_ok=True)

		# wal-g requires the "localhost" host segment even for local paths;
		# file://<path> without it fails to resolve the folder.
		self._repo_prefix = f'file://localhost{self._repo_path}'

		self.node.append_conf(
		    'postgresql.conf', f"default_table_access_method = 'orioledb'\n"
		    f"orioledb.enable_stopevents = true\n"
		    f"wal_level = replica\n"
		    f"archive_mode = on\n"
		    f"archive_command = '{self._walg_env_prefix(self._repo_prefix)}"
		    f"{self._walg_bin()} wal-push \"%p\"'\n"
		    f"restore_command = '{self._walg_env_prefix(self._repo_prefix)}"
		    f"{self._walg_bin()} wal-fetch \"%f\" \"%p\"'\n"
		    f"max_wal_senders = 3\n"
		    f"max_replication_slots = 3\n")

	def tearDown(self):
		super().tearDown()

	# ------------------------------------------------------------------
	# Helpers
	# ------------------------------------------------------------------

	@staticmethod
	def _walg_bin():
		return shutil.which('wal-g') or 'wal-g'

	@staticmethod
	def _walg_env_prefix(repo_prefix, extra=''):
		"""
		wal-g has no config file; archive/restore_command must inline
		its env vars for the postgres-spawned subprocess to see them.
		"""
		return (f"WALG_FILE_PREFIX={repo_prefix} "
		        f"WALG_DELTA_MAX_STEPS=20 "
		        f"WALG_COMPRESSION_METHOD=lz4 {extra}")

	def _walg_env(self, host, port):
		env = dict(os.environ)
		env['WALG_FILE_PREFIX'] = self._repo_prefix
		env['WALG_DELTA_MAX_STEPS'] = '20'
		env['WALG_COMPRESSION_METHOD'] = 'lz4'
		env['PGHOST'] = host
		env['PGPORT'] = str(port)
		env['PGDATABASE'] = 'postgres'
		return env

	def _walg(self, *args, env=None):
		cmd = [self._walg_bin()] + list(args)
		result = subprocess.run(
		    cmd,
		    capture_output=True,
		    text=True,
		    env=env or self._walg_env(self.node.host, self.node.port))
		if result.returncode != 0:
			raise AssertionError(
			    f"wal-g {' '.join(args)} failed (exit {result.returncode}):\n"
			    f"stdout: {result.stdout}\nstderr: {result.stderr}")
		return result

	def _backup_list(self, env=None):
		result = self._walg('backup-list', '--json', '--detail', env=env)
		return json.loads(result.stdout) if result.stdout.strip() else []

	def _backup_count(self, env=None):
		return len(self._backup_list(env=env))

	@staticmethod
	def _is_delta_backup(entry):
		"""
		wal-g has no explicit type field; delta backups are named
		base_<wal>_D_<parent_wal>, full backups just base_<wal>.
		"""
		return '_D_' in entry['backup_name']

	@staticmethod
	def _file_sizes(path):
		sizes = {}
		for root, _, files in os.walk(path):
			for name in files:
				file_path = os.path.join(root, name)
				if not os.path.islink(file_path):
					sizes[file_path] = os.path.getsize(file_path)
		return sizes

	def _assert_is_incremental(self, entry, full_backup_size):
		"""
		Asserts entry is a delta backup whose reported size is
		meaningfully smaller than full_backup_size (the uncompressed_size
		of its parent full backup), i.e. it only shipped changed pages
		rather than silently falling back to a full backup. Comparing
		against another backup's reported size (rather than an on-disk
		directory snapshot) keeps both sides on the same basis.
		"""
		self.assertTrue(self._is_delta_backup(entry))
		self.assertLess(entry['uncompressed_size'], 0.5 * full_backup_size)

	def _create_standby(self):
		standby = self.createStandby()
		self._walg('backup-fetch', standby.data_dir, 'LATEST')
		self._write_standby_conf(standby)

		return standby

	def _repromote_standby(self, standby):
		"""
		Wipe and re-fetch so the standby tracks self.node's current
		timeline after a promotion.
		"""
		shutil.rmtree(standby.data_dir)
		os.makedirs(standby.data_dir, exist_ok=True)

		self._walg('backup-fetch', standby.data_dir, 'LATEST')
		self._write_standby_conf(standby)

	def _write_standby_conf(self, standby):
		with open(os.path.join(standby.data_dir, 'standby.signal'), mode='a'):
			pass
		standby.append_conf(
		    'postgresql.conf', f"primary_conninfo = 'host={self.node.host} "
		    f"port={self.node.port} application_name=standby'\n"
		    f"restore_command = '{self._walg_env_prefix(self._repo_prefix)}"
		    f"{self._walg_bin()} wal-fetch \"%f\" \"%p\"'\n"
		    f"port = {standby.port}\n")

	def _history_archive_pattern(self, timeline):
		return os.path.join(self._repo_path, 'wal_005',
		                    f'{timeline:08X}.history*')

	def _wait_for_history_archive(self, standby, timeout=30):
		"""
		After pg_promote() the standby is already on its new timeline,
		which is exactly what the archived .history file is named for.
		"""
		return self.waitForHistoryArchive(standby,
		                                  self._history_archive_pattern,
		                                  timeout=timeout)

	def _restore_to_new_node(self,
	                         node_name,
	                         recovery_lines='',
	                         backup_name='LATEST',
	                         restore_count=1):
		"""
		recovery_lines is appended verbatim to postgresql.conf (e.g.
		recovery_target_time). restore_count > 1 re-runs backup-fetch
		that many times before starting, to check repeat fetches are
		idempotent.
		"""
		scratch = self.restoreToNewNode(node_name)

		for _ in range(restore_count):
			# backup-fetch refuses to unpack a delta backup into a
			# non-empty directory, so each repeated fetch needs a clean
			# directory first.
			if os.path.exists(scratch.data_dir):
				shutil.rmtree(scratch.data_dir)
			os.makedirs(scratch.data_dir, exist_ok=True)
			self._walg('backup-fetch', scratch.data_dir, backup_name)

		with open(os.path.join(scratch.data_dir, 'recovery.signal'), mode='a'):
			pass

		# 'current' avoids following timeline 2, created by the earlier
		# standby promotion, which diverges before these later backups.
		# The fetched postgresql.conf may carry recovery_target_*
		# settings baked in from an earlier in-place PITR restore on
		# the primary (they're never cleared, so they ride along in
		# later backups); reset them ('' is the documented way) before
		# recovery_lines can reapply a real target.
		scratch.append_conf(
		    'postgresql.conf', f"archive_mode = off\n"
		    f"restore_command = '{self._walg_env_prefix(self._repo_prefix)}"
		    f"{self._walg_bin()} wal-fetch \"%f\" \"%p\"'\n"
		    f"recovery_target_timeline = 'current'\n"
		    f"recovery_target_time = ''\n"
		    f"recovery_target_name = ''\n"
		    f"recovery_target_lsn = ''\n"
		    f"recovery_target_xid = ''\n"
		    f"port = {scratch.port}\n{recovery_lines}")

		scratch.start()
		return scratch

	def _restore_primary_in_place(self, recovery_lines):
		"""
		Wipe self.node's data dir, re-fetch LATEST, and restart with the
		given recovery_target_* lines appended.  'current' pins the
		timeline to avoid following timeline 2 from the earlier standby
		promotion.
		"""
		node = self.node
		node.stop()
		shutil.rmtree(node.data_dir)
		os.makedirs(node.data_dir, exist_ok=True)
		self._walg('backup-fetch', node.data_dir, 'LATEST')
		with open(os.path.join(node.data_dir, 'recovery.signal'), mode='a'):
			pass
		node.append_conf(
		    'postgresql.conf', f"recovery_target_timeline = 'current'\n"
		    f"recovery_target_time = ''\n"
		    f"recovery_target_name = ''\n"
		    f"recovery_target_lsn = ''\n"
		    f"recovery_target_xid = ''\n"
		    f"{recovery_lines}")
		node.start()

	# ------------------------------------------------------------------
	# Test
	# ------------------------------------------------------------------

	def test_integration(self):
		node = self.node
		node.start()

		# wal-g has no stanza-create/check equivalent; an empty backup
		# list is our connectivity check.
		self.assertEqual(self._backup_count(), 0)

		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("CREATE TABLE status (message text NOT NULL);")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Primary full backup
		# ------------------------------------------------------------------
		node.safe_psql("INSERT INTO status VALUES ('full')")

		# Bulk, never-modified-again table so the full backups are
		# significantly bigger than any later delta backup, making the
		# incremental-vs-full size comparison below unambiguous.
		node.safe_psql("CREATE TABLE bulk_tbl (id int PRIMARY KEY, val text)")
		node.safe_psql("INSERT INTO bulk_tbl SELECT i, repeat('a', 800) FROM "
		               "generate_series(1, 40000) i")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("SELECT pg_switch_wal()")

		self._walg('backup-push', node.data_dir, '--full')
		self.assertEqual(self._backup_count(), 1)
		first_backup = self._backup_list()[0]
		self.assertFalse(self._is_delta_backup(first_backup))

		# ------------------------------------------------------------------
		# Standby restore
		# ------------------------------------------------------------------
		standby = self._create_standby()
		standby.start()

		standby.poll_query_until("SELECT pg_last_wal_replay_lsn() IS NOT NULL",
		                         sleep_time=0.5,
		                         max_attempts=60)

		self.assertEqual(
		    standby.execute('postgres', 'SELECT message FROM status')[0][0],
		    'full')

		node.safe_psql("UPDATE status SET message = 'standby'")
		node.safe_psql("SELECT pg_switch_wal()")
		standby.poll_query_until("SELECT message = 'standby' FROM status",
		                         sleep_time=0.5,
		                         max_attempts=60)

		standby.safe_psql('SELECT pg_promote()')
		standby.safe_psql('SELECT pg_switch_wal()')
		self._wait_for_history_archive(standby)
		standby.stop()

		self._repromote_standby(standby)
		standby.start()
		standby.poll_query_until("SELECT pg_last_wal_replay_lsn() IS NOT NULL",
		                         sleep_time=0.5,
		                         max_attempts=60)
		standby.stop()

		# ------------------------------------------------------------------
		# Second full backup (from primary) + expire
		# ------------------------------------------------------------------
		self._walg('backup-push', node.data_dir, '--full')
		self.assertEqual(self._backup_count(), 2)
		second_backup = self._backup_list()[-1]
		self.assertFalse(self._is_delta_backup(second_backup))
		full_backup_size = second_backup['uncompressed_size']

		self._walg('delete', 'retain', 'FULL', '1', '--confirm')
		self.assertEqual(self._backup_count(), 1)

		# ------------------------------------------------------------------
		# Incremental (delta) backup with real page churn
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE status SET message = 'incr'")

		node.safe_psql("CREATE TABLE churn_tbl (id int PRIMARY KEY, "
		               "val text)")
		node.safe_psql("INSERT INTO churn_tbl "
		               "SELECT i, repeat('x', 200) FROM "
		               "generate_series(1, 5000) i")
		node.safe_psql("UPDATE churn_tbl SET val = repeat('y', 200) "
		               "WHERE id % 10 = 0")
		node.safe_psql("CHECKPOINT")

		churn_fingerprint_1 = node.execute(
		    'postgres',
		    "SELECT md5(string_agg(val, '' ORDER BY id)) FROM churn_tbl")[0][0]

		self._walg('backup-push', node.data_dir)

		info = self._backup_list()
		incr_backup_1 = info[-1]
		self._assert_is_incremental(incr_backup_1, full_backup_size)

		scratch = self._restore_to_new_node(
		    'deltaincr', backup_name=incr_backup_1['backup_name'])
		try:
			churn_fingerprint_restored = scratch.execute(
			    'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			    "FROM churn_tbl")[0][0]
			self.assertEqual(churn_fingerprint_restored, churn_fingerprint_1)
			scratch.stop()
		finally:
			scratch.cleanup()

		# ------------------------------------------------------------------
		# Second incremental backup: 2-deep chain (full -> delta -> delta)
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE churn_tbl SET val = repeat('z', 200) "
		               "WHERE id % 13 = 0")
		node.safe_psql("UPDATE status SET message = 'incr2'")

		churn_fingerprint_2 = node.execute(
		    'postgres',
		    "SELECT md5(string_agg(val, '' ORDER BY id)) FROM churn_tbl")[0][0]

		self._walg('backup-push', node.data_dir)

		info = self._backup_list()
		incr_backup_2 = info[-1]
		self._assert_is_incremental(incr_backup_2, full_backup_size)

		scratch = self._restore_to_new_node('deltachain')
		try:
			self.assertEqual(
			    scratch.execute('postgres',
			                    'SELECT message FROM status')[0][0], 'incr2')
			churn_fingerprint_chain = scratch.execute(
			    'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			    "FROM churn_tbl")[0][0]
			self.assertEqual(churn_fingerprint_chain, churn_fingerprint_2)
			scratch.stop()
		finally:
			scratch.cleanup()

		# ------------------------------------------------------------------
		# Fetch repeatability: wal-g backup-fetch is always a fresh full
		# unpack (not an incremental restore), so unlike pgBackRest's
		# --delta restore idempotency this checks repeat-fetch stability
		# instead.
		# ------------------------------------------------------------------
		scratch_twice = self._restore_to_new_node(
		    'fetchtwice',
		    backup_name=incr_backup_2['backup_name'],
		    restore_count=2)
		try:
			self.assertEqual(
			    scratch_twice.execute('postgres',
			                          'SELECT message FROM status')[0][0],
			    'incr2')
			self.assertEqual(
			    scratch_twice.execute(
			        'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			        "FROM churn_tbl")[0][0], churn_fingerprint_2)
			scratch_twice.stop()
		finally:
			scratch_twice.cleanup()

		scratch_once = self._restore_to_new_node(
		    'fetchonce', backup_name=incr_backup_2['backup_name'])
		try:
			self.assertEqual(
			    scratch_once.execute(
			        'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			        "FROM churn_tbl")[0][0], churn_fingerprint_2)
			scratch_once.stop()
		finally:
			scratch_once.cleanup()

		# ------------------------------------------------------------------
		# Time target setup
		# ------------------------------------------------------------------
		time.sleep(1)  # Guarantee the timestamp is after the last backup
		node.safe_psql("UPDATE status SET message = 'time'")
		node.safe_psql("SELECT pg_switch_wal()")
		target_time = node.execute('postgres',
		                           'SELECT current_timestamp::text')[0][0]

		# Mutate further so the target and latest states differ.
		node.safe_psql("UPDATE status SET message = 'after_time'")
		node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Name target setup
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE status SET message = 'name'")
		node.safe_psql("SELECT pg_switch_wal()")
		node.safe_psql("SELECT pg_create_restore_point('walg')")
		node.safe_psql("SELECT pg_switch_wal()")

		# ------------------------------------------------------------------
		# Restore: time target
		# ------------------------------------------------------------------
		self._restore_primary_in_place(
		    f"recovery_target_time = '{target_time}'\n"
		    f"recovery_target_action = 'promote'\n")
		node.poll_query_until("SELECT message = 'time' FROM status",
		                      sleep_time=0.5,
		                      max_attempts=60)
		node.poll_query_until("SELECT pg_is_in_recovery()",
		                      expected=False,
		                      sleep_time=0.5,
		                      max_attempts=60)

		# ------------------------------------------------------------------
		# Restore: name target
		# ------------------------------------------------------------------
		self._restore_primary_in_place("recovery_target_name = 'walg'\n"
		                               "recovery_target_action = 'promote'\n")
		node.poll_query_until("SELECT message = 'name' FROM status",
		                      sleep_time=0.5,
		                      max_attempts=60)
		node.poll_query_until("SELECT pg_is_in_recovery()",
		                      expected=False,
		                      sleep_time=0.5,
		                      max_attempts=60)

		# ------------------------------------------------------------------
		# Backup concurrent with an in-flight OrioleDB checkpoint
		# ------------------------------------------------------------------
		node.safe_psql("CREATE TABLE concurrent_tbl (id int PRIMARY KEY, "
		               "val text)")
		node.safe_psql("INSERT INTO concurrent_tbl "
		               "SELECT i, repeat('z', 200) FROM "
		               "generate_series(1, 5000) i")

		con_ctrl = node.connect()
		con_ctrl.execute(
		    "SELECT pg_stopevent_set('checkpoint_writeback', 'true')")

		backup_errors = []

		def _run_backup():
			try:
				self._walg('backup-push', node.data_dir)
			except Exception as e:
				backup_errors.append(e)

		backup_thread = Thread(target=_run_backup)
		backup_thread.start()

		try:
			wait_checkpointer_stopevent(node)

			node.safe_psql("UPDATE concurrent_tbl SET val = repeat('w', 200) "
			               "WHERE id % 7 = 0")
			concurrent_fingerprint = node.execute(
			    'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			    "FROM concurrent_tbl")[0][0]
		finally:
			con_ctrl.execute(
			    "SELECT pg_stopevent_reset('checkpoint_writeback')")
			con_ctrl.close()

		backup_thread.join()
		if backup_errors:
			raise backup_errors[0]

		scratch = self._restore_to_new_node('chkptrace')
		try:
			restored_fingerprint = scratch.execute(
			    'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			    "FROM concurrent_tbl")[0][0]
			self.assertEqual(restored_fingerprint, concurrent_fingerprint)
			scratch.stop()
		finally:
			scratch.cleanup()

		# ------------------------------------------------------------------
		# Cleanup
		# ------------------------------------------------------------------
		node.stop()
		self._walg('delete', 'everything', 'FORCE', '--confirm')

	def test_delta_backup_ships_whole_orioledb_file(self):
		# wal-g's page-level delta diffing (isPagedFile() in
		# internal/databases/postgres/pagefile.go) only applies to files
		# whose path contains "base/" or "pg_tblspc/". OrioleDB's files
		# live under orioledb_data/ instead, so they never qualify: a
		# changed OrioleDB file is re-shipped in full by the delta backup,
		# rather than just its changed pages. This pins that behavior down
		# by touching a tiny fraction of a large single-file OrioleDB
		# table and checking the delta backup's size tracks the whole
		# file, not the tiny changed fraction.
		node = self.node
		node.start()

		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		orioledb_data_dir = os.path.join(node.data_dir, 'orioledb_data')
		before_files = self._file_sizes(orioledb_data_dir)

		node.safe_psql("CREATE TABLE big_tbl (id int PRIMARY KEY, "
		               "val text) USING orioledb")
		node.safe_psql("INSERT INTO big_tbl SELECT i, repeat('a', 800) "
		               "FROM generate_series(1, 20000) i")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("SELECT pg_switch_wal()")

		after_files = self._file_sizes(orioledb_data_dir)
		new_files = {
		    path: size
		    for path, size in after_files.items()
		    if before_files.get(path) != size
		}
		self.assertTrue(new_files, "no new orioledb_data file for big_tbl")
		data_file_size = max(new_files.values())

		self._walg('backup-push', node.data_dir, '--full')
		self.assertEqual(self._backup_count(), 1)

		# Touch a tiny fraction of rows: a true page-level diff would
		# ship only ~1/500th of the file's pages.
		node.safe_psql("UPDATE big_tbl SET val = repeat('b', 800) "
		               "WHERE id % 500 = 0")
		node.safe_psql("CHECKPOINT")

		self._walg('backup-push', node.data_dir)
		delta_backup = self._backup_list()[-1]
		self.assertTrue(self._is_delta_backup(delta_backup))

		# If wal-g truly diffed pages, the delta would be a tiny
		# fraction of data_file_size. Instead it re-ships (close to)
		# the whole file, because isPagedFile() never matches
		# orioledb_data/ paths.
		self.assertGreater(delta_backup['uncompressed_size'],
		                   0.7 * data_file_size)

		node.stop()
		self._walg('delete', 'everything', 'FORCE', '--confirm')

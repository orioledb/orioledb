#!/usr/bin/env python3
# coding: utf-8
"""
Python/testgres migration of pgBackRest's integration/allTest.c.

This test exercises the core pgBackRest integration flow using orioledb as
the default table access method: stanza creation, full/incremental backups,
standby restore and promotion, async archiving, and point-in-time recovery
by time and named restore point.
"""

import json
import os
import shutil
import subprocess
import time

from threading import Thread

from test.t.base_backup_test import BaseBackupTest
from test.t.base_test import wait_checkpointer_stopevent


class PgBackRestTest(BaseBackupTest):
	STANZA = 'test'

	# ------------------------------------------------------------------
	# Setup / teardown
	# ------------------------------------------------------------------

	def setUp(self):
		super().setUp()

		# Derive paths from the directory that initNode chose
		node_base = self.node.base_dir

		self._repo_path = os.path.join(node_base, 'repo')
		self._log_path = os.path.join(self._repo_path, 'logs')
		self._conf_path = os.path.join(node_base, 'pgbackrest.conf')
		self._standby_conf_path = os.path.join(node_base,
		                                       'pgbackrest_standby.conf')

		spool_path = os.path.join(node_base, 'spool')

		os.makedirs(self._repo_path, exist_ok=True)
		os.makedirs(self._log_path, exist_ok=True)
		os.makedirs(spool_path, exist_ok=True)

		self._write_pgbackrest_conf(self._conf_path, self.node.data_dir,
		                            self.node.port, spool_path)

		self.node.append_conf(
		    'postgresql.conf', f"default_table_access_method = 'orioledb'\n"
		    f"orioledb.enable_stopevents = true\n"
		    f"wal_level = replica\n"
		    f"archive_mode = on\n"
		    f"archive_command = '{self._pgbackrest_bin()} --config=\"{self._conf_path}\""
		    f" --stanza={self.STANZA} archive-push \"%p\"'\n"
		    f"restore_command = '{self._pgbackrest_bin()} --config=\"{self._conf_path}\""
		    f" --stanza={self.STANZA} archive-get \"%f\" \"%p\"'\n"
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

	def _write_pgbackrest_conf(self, conf_path, pg_path, pg_port, spool_path):
		conf = (f"[{self.STANZA}]\n"
		        f"pg1-path={pg_path}\n"
		        f"pg1-port={pg_port}\n"
		        f"pg1-socket-path=/tmp\n"
		        f"\n"
		        f"[global]\n"
		        f"repo1-path={self._repo_path}\n"
		        f"archive-async=y\n"
		        f"spool-path={spool_path}\n"
		        f"log-level-console=warn\n"
		        f"log-level-file=detail\n"
		        f"log-path={self._log_path}\n")
		with open(conf_path, 'w') as f:
			f.write(conf)

	def _pgbackrest(self, *args):
		return self._pgbackrest_with_conf(self._conf_path, *args)

	def _pgbackrest_with_conf(self, conf_path, *args):
		cmd = [
		    self._pgbackrest_bin(),
		    f'--config={conf_path}',
		    f'--stanza={self.STANZA}',
		] + list(args)
		result = subprocess.run(cmd, capture_output=True, text=True)
		if result.returncode != 0:
			raise AssertionError(
			    f"pgbackrest {' '.join(args)} failed (exit {result.returncode}):\n"
			    f"stdout: {result.stdout}\nstderr: {result.stderr}")
		return result

	def _backup_count(self):
		result = self._pgbackrest('info', '--output=json')
		info = json.loads(result.stdout)
		return len(info[0]['backup']) if info and info[0].get('backup') else 0

	def _restore_primary(self, *extra_opts):
		"""Restore into the primary data dir (uses conf pg1-path)."""
		self._pgbackrest('restore', '--delta', *extra_opts)

	def _create_standby(self):
		standby = self.createStandby()

		standby_spool_path = os.path.join(standby.base_dir, 'spool')
		os.makedirs(standby_spool_path, exist_ok=True)
		self._write_pgbackrest_conf(self._standby_conf_path, standby.data_dir,
		                            standby.port, standby_spool_path)

		# pgbackrest restore writes standby.signal and restore_command into
		# postgresql.auto.conf, so no manual signal file is needed.
		self._pgbackrest_with_conf(self._standby_conf_path, 'restore',
		                           f'--pg1-path={standby.data_dir}',
		                           '--type=standby')

		# The restored postgresql.conf has the primary's archive_command and
		# port.  Override both so the standby uses its own pgbackrest config.
		standby.append_conf(
		    'postgresql.conf',
		    f"archive_command = '{self._pgbackrest_bin()} --config=\"{self._standby_conf_path}\""
		    f" --stanza={self.STANZA} archive-push \"%p\"'\n"
		    f"port = {standby.port}\n")

		self.replica = standby
		return standby

	def _repromote_standby(self, standby):
		"""
		Wipe the standby data dir and restore again.  Re-appends the port
		override after restore (pgbackrest rewrites auto.conf).
		"""
		shutil.rmtree(standby.data_dir)
		os.makedirs(standby.data_dir, exist_ok=True)

		standby_spool_path = os.path.join(standby.base_dir, 'spool')
		os.makedirs(standby_spool_path, exist_ok=True)
		self._write_pgbackrest_conf(self._standby_conf_path, standby.data_dir,
		                            standby.port, standby_spool_path)

		self._pgbackrest_with_conf(self._standby_conf_path, 'restore',
		                           f'--pg1-path={standby.data_dir}',
		                           '--type=standby',
		                           '--target-timeline=current')
		standby.append_conf(
		    'postgresql.conf',
		    f"archive_command = '{self._pgbackrest_bin()} --config=\"{self._standby_conf_path}\""
		    f" --stanza={self.STANZA} archive-push \"%p\"'\n"
		    f"port = {standby.port}\n")

	def _history_archive_pattern(self, timeline):
		return os.path.join(self._repo_path, 'archive', self.STANZA, '*',
		                    f'{timeline:08X}.history*')

	def _wait_for_history_archive(self, standby, timeout=30):
		return self.waitForHistoryArchive(standby,
		                                  self._history_archive_pattern,
		                                  timeout=timeout)

	def _restore_to_new_node(self, node_name, restore_opts, restore_count=1):
		# restore_count > 1 runs the same restore command that many times
		# in a row against the same (increasingly populated) directory --
		# e.g. to check that pgBackRest's --delta comparison is idempotent.
		scratch = self.restoreToNewNode(node_name)

		for _ in range(restore_count):
			conf_path = os.path.join(scratch.base_dir,
			                         f'pgbackrest_{node_name}.conf')
			spool_path = os.path.join(scratch.base_dir, 'spool')
			os.makedirs(spool_path, exist_ok=True)
			self._write_pgbackrest_conf(conf_path, scratch.data_dir,
			                            scratch.port, spool_path)
			self._pgbackrest_with_conf(conf_path, 'restore',
			                           f'--pg1-path={scratch.data_dir}',
			                           *restore_opts)

		# The restored postgresql.conf carries the primary's archive
		# settings; disable archiving on this scratch node since it is
		# only used to verify already-archived data, not to produce more.
		scratch.append_conf('postgresql.conf', f"archive_mode = off\n"
		                    f"port = {scratch.port}\n")

		scratch.start()
		return scratch

	# ------------------------------------------------------------------
	# Test
	# ------------------------------------------------------------------

	def test_integration(self):
		node = self.node
		node.start()

		# ------------------------------------------------------------------
		# Stanza create and check
		# ------------------------------------------------------------------
		self._pgbackrest('stanza-create')
		self._pgbackrest('check')

		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("CREATE TABLE status (message text NOT NULL);")
		node.safe_psql("SELECT pg_switch_wal()")

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

		# Poll until the standby has replayed WAL and caught up enough to
		# see the status table.
		standby.poll_query_until("SELECT pg_last_wal_replay_lsn() IS NOT NULL",
		                         sleep_time=0.5,
		                         max_attempts=60)

		self.assertEqual(
		    standby.execute('postgres', 'SELECT message FROM status')[0][0],
		    'full')

		# Update primary status; standby should receive it via streaming.
		node.safe_psql("UPDATE status SET message = 'standby'")
		node.safe_psql("SELECT pg_switch_wal()")
		standby.poll_query_until("SELECT message = 'standby' FROM status",
		                         sleep_time=0.5,
		                         max_attempts=60)

		# Promote the standby to create timeline 2, then restore it again on
		# the current timeline so it tracks the primary.
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
		self._pgbackrest('backup', '--type=full')
		self.assertEqual(self._backup_count(), 2)

		self._pgbackrest('expire', '--repo1-retention-full=1')
		self.assertEqual(self._backup_count(), 1)

		# ------------------------------------------------------------------
		# Async archiving exercise
		# ------------------------------------------------------------------
		node.safe_psql("CREATE TABLE wal_activity (id int)")
		for i in range(1, 6):
			node.safe_psql(f"INSERT INTO wal_activity VALUES ({i})")
			node.safe_psql("SELECT pg_switch_wal()")

		# Verify the async archive-push log was created.
		async_log = os.path.join(self._log_path,
		                         f'{self.STANZA}-archive-push-async.log')
		deadline = time.time() + 10
		while time.time() < deadline:
			if os.path.exists(async_log):
				break
			time.sleep(0.5)
		self.assertTrue(os.path.exists(async_log),
		                "async archive-push log was not created")

		# ------------------------------------------------------------------
		# Time target setup
		# ------------------------------------------------------------------
		time.sleep(1)  # Guarantee the timestamp is after the last backup
		node.safe_psql("UPDATE status SET message = 'time'")
		node.safe_psql("SELECT pg_switch_wal()")
		target_time = node.execute('postgres',
		                           'SELECT current_timestamp::text')[0][0]

		# ------------------------------------------------------------------
		# Incremental backup with real page churn + block-incremental
		# ------------------------------------------------------------------
		# Generate enough page churn that OrioleDB actually rewrites data
		# during a checkpoint, rather than just a metadata-only change, so
		# the backup below exercises pgBackRest's block-incremental
		# diffing against real OrioleDB page rewrites.
		node.safe_psql("CREATE DATABASE exclude_me")
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

		self._pgbackrest('backup', '--type=incr', '--repo1-block',
		                 '--repo1-bundle')

		# Regression guard: pgBackRest must still report this as an
		# incremental backup for OrioleDB data, not silently fall back to
		# a full copy.
		info = json.loads(self._pgbackrest('info', '--output=json').stdout)
		incr_backup_1 = info[0]['backup'][-1]
		self.assertEqual(incr_backup_1['type'], 'incr')

		# Correctness check: restore this specific block-incremental
		# backup into a standalone scratch node and confirm OrioleDB data
		# survived the block-level diffing intact.
		scratch = self._restore_to_new_node(
		    'blkincr',
		    [f'--set={incr_backup_1["label"]}', '--target-timeline=current'])
		try:
			churn_fingerprint_restored = scratch.execute(
			    'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			    "FROM churn_tbl")[0][0]
			self.assertEqual(churn_fingerprint_restored, churn_fingerprint_1)
			scratch.stop()
		finally:
			scratch.cleanup()

		# ------------------------------------------------------------------
		# Second incremental backup: 2-deep chain (full -> incr -> incr)
		# ------------------------------------------------------------------
		node.safe_psql("UPDATE churn_tbl SET val = repeat('z', 200) "
		               "WHERE id % 13 = 0")
		node.safe_psql("UPDATE status SET message = 'incr2'")

		churn_fingerprint_2 = node.execute(
		    'postgres',
		    "SELECT md5(string_agg(val, '' ORDER BY id)) FROM churn_tbl")[0][0]

		self._pgbackrest('backup', '--type=incr', '--repo1-block',
		                 '--repo1-bundle')

		info = json.loads(self._pgbackrest('info', '--output=json').stdout)
		incr_backup_2 = info[0]['backup'][-1]
		self.assertEqual(incr_backup_2['type'], 'incr')

		# Restoring the latest backup with no --set must resolve the full
		# 2-deep incremental chain correctly against OrioleDB data.
		scratch = self._restore_to_new_node('blkincr2',
		                                    ['--target-timeline=current'])
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
		# Restore --delta applied twice onto the same populated directory
		# ------------------------------------------------------------------
		scratch_delta = self._restore_to_new_node('deltatwice', [
		    '--delta', f'--set={incr_backup_2["label"]}', '--type=immediate',
		    '--target-action=promote'
		],
		                                          restore_count=2)
		try:
			self.assertEqual(
			    scratch_delta.execute('postgres',
			                          'SELECT message FROM status')[0][0],
			    'incr2')
			self.assertEqual(
			    scratch_delta.execute(
			        'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			        "FROM churn_tbl")[0][0], churn_fingerprint_2)
			scratch_delta.stop()
		finally:
			scratch_delta.cleanup()

		# Known-good baseline: a single fresh restore of the same backup
		# into a separate directory, to compare against the twice-applied
		# delta restore above.
		scratch_baseline = self._restore_to_new_node('deltabaseline', [
		    f'--set={incr_backup_2["label"]}', '--type=immediate',
		    '--target-action=promote'
		])
		try:
			self.assertEqual(
			    scratch_baseline.execute(
			        'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			        "FROM churn_tbl")[0][0], churn_fingerprint_2)
			scratch_baseline.stop()
		finally:
			scratch_baseline.cleanup()

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
		node.poll_query_until("SELECT message = 'name' FROM status",
		                      sleep_time=0.5,
		                      max_attempts=60)

		# ------------------------------------------------------------------
		# Restore: immediate target (recovers to end of latest backup,
		# which is now the second incremental backup added above)
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=immediate', '--target-action=promote',
		                      '--db-exclude=exclude_me')
		node.start()
		node.poll_query_until("SELECT message = 'incr2' FROM status",
		                      sleep_time=0.5,
		                      max_attempts=60)

		# Wait for promotion to finish so the node is no longer read-only.
		node.poll_query_until("SELECT pg_is_in_recovery()",
		                      expected=False,
		                      sleep_time=0.5,
		                      max_attempts=60)

		# The excluded database should be droppable even though it was not
		# fully restored.
		node.safe_psql("DROP DATABASE exclude_me")

		# ------------------------------------------------------------------
		# Restore: time target
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=time', f'--target={target_time}',
		                      '--target-timeline=current')
		node.start()
		node.poll_query_until("SELECT message = 'time' FROM status",
		                      sleep_time=0.5,
		                      max_attempts=60)

		# ------------------------------------------------------------------
		# Restore: name target
		# ------------------------------------------------------------------
		node.stop()
		self._restore_primary('--type=name', '--target=pgbackrest',
		                      '--target-action=promote',
		                      '--target-timeline=current')
		node.start()
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
				self._pgbackrest('backup', '--type=incr', '--start-fast')
			except Exception as e:
				backup_errors.append(e)

		backup_thread = Thread(target=_run_backup)
		backup_thread.start()

		try:
			# Wait for the backup's own startup checkpoint to be parked
			# mid-writeback before mutating more data, so the mutation
			# genuinely happens while the checkpoint that establishes
			# this backup's consistency point is still in flight.
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

		scratch = self._restore_to_new_node('chkptrace',
		                                    ['--target-timeline=current'])
		try:
			restored_fingerprint = scratch.execute(
			    'postgres', "SELECT md5(string_agg(val, '' ORDER BY id)) "
			    "FROM concurrent_tbl")[0][0]
			self.assertEqual(restored_fingerprint, concurrent_fingerprint)
			scratch.stop()
		finally:
			scratch.cleanup()

		# ------------------------------------------------------------------
		# Stanza delete
		# ------------------------------------------------------------------
		node.stop()
		self._pgbackrest('stop')
		self._pgbackrest('stanza-delete', '--force')

	def test_incr_backup_block_diffs_orioledb_file(self):
		"""
		Unlike wal-g (see test_delta_backup_ships_whole_orioledb_file in
		walg_test.py), pgBackRest's block-incremental eligibility
		(manifestBuildBlockIncrSize() in
		src/info/manifest/build.c.inc) is decided purely from a file's
		size and age, with no check that it lives under base/ or
		pg_tblspc/ -- so it should genuinely block-diff OrioleDB files
		too. This pins that down empirically: touching a tiny fraction
		of a large single-file OrioleDB table should still produce a
		small block-incremental backup, rather than re-storing the
		whole file.

		The comparison is against the repo (stored, compressed) size of
		the earlier full backup rather than the file's raw on-disk
		size, so both sides went through the same compression and the
		result isolates block-level dedup rather than conflating it
		with compression ratio.
		"""
		node = self.node
		node.start()

		self._pgbackrest('stanza-create')
		self._pgbackrest('check')

		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		# Single relation, single file, with quasi-random (not
		# trivially compressible) content so the comparison below
		# isn't swamped by how well repetitive filler compresses.
		node.safe_psql("CREATE TABLE big_tbl (id int PRIMARY KEY, "
		               "val text) USING orioledb")
		node.safe_psql(
		    "INSERT INTO big_tbl SELECT i, "
		    "(SELECT string_agg(md5(i::text || ':' || g::text), '') "
		    "FROM generate_series(1, 25) g) FROM generate_series(1, 20000) i")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("SELECT pg_switch_wal()")

		self._pgbackrest('backup', '--type=full', '--repo1-block',
		                 '--repo1-bundle')
		info = json.loads(self._pgbackrest('info', '--output=json').stdout)
		full_backup = info[0]['backup'][-1]
		self.assertEqual(full_backup['type'], 'full')
		full_repo_size = full_backup['info']['repository']['delta']

		# Touch a tiny fraction of rows: a real block-level diff would
		# ship only ~1/500th of the file's blocks.
		node.safe_psql(
		    "UPDATE big_tbl SET val = "
		    "(SELECT string_agg(md5(id::text || '!' || g::text), '') "
		    "FROM generate_series(1, 25) g) WHERE id % 500 = 0")
		node.safe_psql("CHECKPOINT")

		self._pgbackrest('backup', '--type=incr', '--repo1-block',
		                 '--repo1-bundle')
		info = json.loads(self._pgbackrest('info', '--output=json').stdout)
		incr_backup = info[0]['backup'][-1]
		self.assertEqual(incr_backup['type'], 'incr')
		incr_repo_delta = incr_backup['info']['repository']['delta']

		# If pgBackRest's block-incremental had the same base/pg_tblspc-
		# only restriction as wal-g, this would land close to
		# full_repo_size (a whole-file re-store of big_tbl). Instead it
		# should be a small fraction of it.
		self.assertLess(incr_repo_delta, 0.3 * full_repo_size)

		node.stop()
		self._pgbackrest('stop')
		self._pgbackrest('stanza-delete', '--force')

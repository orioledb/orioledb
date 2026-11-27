#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class RecoveryLegacyUndoTest(BaseTest):
	"""Recovery of legacy pre-V2 undo records from old clusters."""

	def test_legacy_undo_recovery(self):
		"""Crash recovery replays committed legacy modify undo records."""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_legacy_undo (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id)
			) USING orioledb;
		""")

		node.safe_psql(
		    'postgres', """
			SET orioledb.debug_write_legacy_modify_undo = true;
			INSERT INTO o_test_legacy_undo
				(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);
			UPDATE o_test_legacy_undo SET val = val || '_upd'
				WHERE id <= 50;
		""")

		self.crash_with_os_buffer_loss()

		node.start()
		self.assertEqual(
		    node.execute(
		        'postgres',
		        "SELECT orioledb_tbl_check('o_test_legacy_undo'::regclass);")
		    [0][0], True)
		result = node.execute('postgres',
		                      "SELECT count(*) FROM o_test_legacy_undo;")[0][0]
		self.assertEqual(result, 100)

	def test_legacy_undo_rollback(self):
		"""Crash recovery rolls back uncommitted legacy modify undo records."""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_legacy_rb (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_test_legacy_rb
				(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);
		""")
		node.safe_psql('postgres', "CHECKPOINT;")

		con1 = node.connect('postgres')
		con1.begin()
		con1.execute("SET orioledb.debug_write_legacy_modify_undo = true;")
		con1.execute(
		    "UPDATE o_test_legacy_rb SET val = val || '_upd' WHERE id <= 50;")

		con1.close()
		self.crash_with_os_buffer_loss()

		node.start()
		self.assertEqual(
		    node.execute(
		        'postgres',
		        "SELECT orioledb_tbl_check('o_test_legacy_rb'::regclass);")[0]
		    [0], True)
		result = node.execute('postgres',
		                      "SELECT count(*) FROM o_test_legacy_rb;")[0][0]
		self.assertEqual(result, 100)
		vals = node.execute(
		    'postgres', "SELECT val FROM o_test_legacy_rb WHERE id = 1;")[0][0]
		self.assertEqual(vals, '1val')

	def test_legacy_undo_row_lock(self):
		"""Crash recovery handles legacy row-lock undo records."""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_legacy_lock (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_test_legacy_lock
				(SELECT id, id || 'val' FROM generate_series(1, 20, 1) id);
		""")
		node.safe_psql('postgres', "CHECKPOINT;")

		node.safe_psql(
		    'postgres', """
			SET orioledb.debug_write_legacy_modify_undo = true;
			BEGIN;
			SELECT val FROM o_test_legacy_lock WHERE id <= 10 FOR UPDATE;
			COMMIT;
		""")

		self.crash_with_os_buffer_loss()

		node.start()
		self.assertEqual(
		    node.execute(
		        'postgres',
		        "SELECT orioledb_tbl_check('o_test_legacy_lock'::regclass);")
		    [0][0], True)
		result = node.execute('postgres',
		                      "SELECT count(*) FROM o_test_legacy_lock;")[0][0]
		self.assertEqual(result, 20)

	def test_legacy_undo_update_diff(self):
		"""Crash recovery handles legacy update-diff undo records."""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_legacy_diff (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_test_legacy_diff
				(SELECT id, repeat('x', 200) FROM generate_series(1, 50, 1) id);
		""")
		node.safe_psql('postgres', "CHECKPOINT;")

		node.safe_psql(
		    'postgres', """
			SET orioledb.debug_write_legacy_modify_undo = true;
			UPDATE o_test_legacy_diff SET val = 'y' || substring(val, 2)
				WHERE id <= 30;
		""")

		self.crash_with_os_buffer_loss()

		node.start()
		self.assertEqual(
		    node.execute(
		        'postgres',
		        "SELECT orioledb_tbl_check('o_test_legacy_diff'::regclass);")
		    [0][0], True)
		result = node.execute('postgres',
		                      "SELECT count(*) FROM o_test_legacy_diff;")[0][0]
		self.assertEqual(result, 50)
		val = node.execute(
		    'postgres',
		    "SELECT val FROM o_test_legacy_diff WHERE id = 1;")[0][0]
		self.assertEqual(val, 'y' + 'x' * 199)

	def test_legacy_undo_lock_dispatch(self):
		"""Crash recovery handles legacy lock-dispatch undo records."""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_legacy_dispatch (
				id integer NOT NULL,
				val text,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_test_legacy_dispatch VALUES (1, 'original');
		""")
		node.safe_psql('postgres', "CHECKPOINT;")

		con1 = node.connect('postgres')
		con1.begin()
		con1.execute("SET orioledb.debug_write_legacy_modify_undo = true;")
		con1.execute(
		    "UPDATE o_test_legacy_dispatch SET val = 'updated' WHERE id = 1;")

		con2 = node.connect('postgres')
		t2 = ThreadQueryExecutor(
		    con2, "SET orioledb.debug_write_legacy_modify_undo = true; "
		    "SELECT val FROM o_test_legacy_dispatch WHERE id = 1 FOR UPDATE;")
		t2.start()

		for _ in range(100):
			time.sleep(0.1)
			waiters = node.execute(
			    'postgres', "SELECT count(*) FROM pg_stat_activity "
			    "WHERE wait_event_type = 'Lock';")[0][0]
			if waiters > 0:
				break

		con1.rollback()
		t2.join()
		con2.close()
		con1.close()

		self.crash_with_os_buffer_loss()

		node.start()
		self.assertEqual(
		    node.execute(
		        'postgres',
		        "SELECT orioledb_tbl_check('o_test_legacy_dispatch'::regclass);"
		    )[0][0], True)
		val = node.execute(
		    'postgres',
		    "SELECT val FROM o_test_legacy_dispatch WHERE id = 1;")[0][0]
		self.assertEqual(val, 'original')

#!/usr/bin/env python3
# coding: utf-8

import unittest
import os

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent


class UndoEvictionTest(BaseTest):

	def setUp(self):
		super().setUp()
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 64MB\n")
		node.start()
		node.safe_psql(
		    'postgres', """CREATE EXTENSION IF NOT EXISTS orioledb;
					    CREATE TABLE IF NOT EXISTS o_undo_evict (
							id integer NOT NULL,
							value integer NOT NULL,
							PRIMARY KEY (id)
						) USING orioledb;""")

	def get_undo_files_count(self):
		undoDir = self.node.data_dir + '/orioledb_undo'
		count = len([
		    name for name in os.listdir(undoDir)
		    if os.path.isfile(os.path.join(undoDir, name))
		])
		return count

	def test_row_lock_conflicts_terminates_on_broken_retain(self):
		"""row_lock_conflicts() must not spin when the retain invariant is broken.

		A backend's snapshotRetainUndoLocation is supposed to sit at or above
		minProcRetainLocation -- a live snapshot pins the horizon it claims.
		The field report in orioledb#1072 caught it below, and in the window
		"retained <= undoLocation < minProcRetain" the delete_record branch
		asks for undo that no longer exists while the only loop exit
		(undoLocation < retained) is unsatisfiable, so the loop re-evaluates
		identical state forever: 100% CPU, no wait event.

		No legitimate path produces that state, so the test parks the backend
		at the top of row_lock_conflicts() and writes the broken value into
		its slot directly, which is what the GDB snapshot in the report shows.
		"""
		node = self.node

		node.safe_psql(
		    'postgres', """
			CREATE TABLE o_rlc (id int PRIMARY KEY, v int) USING orioledb;
			INSERT INTO o_rlc SELECT i, i FROM generate_series(1, 100) i;
			UPDATE o_rlc SET v = v + 1 WHERE id = 1;
		""")

		# Leave a lock-only undo record from a committed transaction on row 1:
		# row_lock_conflicts() wants to delete it, which is the branch at issue.
		locker = node.connect()
		locker.execute("SELECT * FROM o_rlc WHERE id = 1 FOR KEY SHARE")
		locker.commit()
		locker.close()

		ctrl = node.connect()
		meta = ("SELECT minprocretainlocation, lastusedlocation "
		        "FROM orioledb_get_undo_meta() WHERE undo_type = 'row'")

		# Everything row 1's chain points at was written below this mark.
		base = ctrl.execute(meta)[0][1]

		# Push minProcRetainLocation past it, so that undo is really gone.
		# orioledb_get_undo_meta() runs update_min_undo_locations() itself.
		churn = node.connect()
		for _ in range(60):
			churn.execute("UPDATE o_rlc SET v = v + 1 WHERE id > 50")
			churn.commit()
			if ctrl.execute(meta)[0][0] > base:
				break
		churn.close()

		min_retain = ctrl.execute(meta)[0][0]
		self.assertGreater(
		    min_retain, base,
		    "could not advance minProcRetainLocation past row 1's undo")

		victim = node.connect()
		victim.connection.autocommit = True
		victim.execute("SET orioledb.enable_stopevents = true")
		victim_pid = victim.execute("SELECT pg_backend_pid()")[0][0]
		victim.connection.autocommit = False

		ctrl.execute(
		    "SELECT pg_stopevent_set('row_lock_conflicts_start', 'true')")
		t = ThreadQueryExecutor(
		    victim, "SELECT * FROM o_rlc WHERE id = 1 FOR KEY SHARE")
		t.start()
		wait_stopevent(node, victim_pid)

		# Drop this backend's snapshot retain below minProcRetainLocation.
		self.assertTrue(
		    ctrl.execute(
		        "SELECT orioledb_poke_proc_snapshot_retain_location(%d, 'row', 0)"
		        % victim_pid)[0][0])

		ctrl.execute("SELECT pg_stopevent_reset('row_lock_conflicts_start')")

		# Without the fix the backend never leaves the loop.
		t.join(timeout=30)
		spinning = t.is_alive()
		if spinning:
			node.stop(['-m', 'immediate'])
		self.assertFalse(spinning, "row_lock_conflicts() did not terminate")
		self.assertEqual(t._return, [(1, 2)])

		ctrl.close()
		victim.close()

	def test_undo_eviction_insert(self):
		node = self.node
		con1 = node.connect()
		con1.begin()
		con1.execute(
		    "INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);"
		)
		self.assertGreaterEqual(self.get_undo_files_count(), 1)
		con1.rollback()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 0)

		con1.begin()
		con1.execute(
		    "INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);"
		)
		self.assertGreaterEqual(self.get_undo_files_count(), 1)
		con1.commit()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 100000)
		con1.close()
		node.stop()

	def test_undo_eviction_update(self):
		node = self.node

		node.execute(
		    "INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);"
		)

		con1 = node.connect()

		con1.begin()
		con1.execute("UPDATE o_undo_evict SET value = value + 1;")

		self.assertEqual(
		    node.execute("SELECT SUM(value) FROM o_undo_evict;")[0][0],
		    5000050000)
		self.assertEqual(
		    con1.execute("SELECT SUM(value) FROM o_undo_evict;")[0][0],
		    5000150000)
		self.assertGreaterEqual(self.get_undo_files_count(), 1)

		con1.rollback()

		con1.begin()
		con1.execute("UPDATE o_undo_evict SET value = value + 1;")
		con1.commit()

		self.assertEqual(
		    node.execute("SELECT SUM(value) FROM o_undo_evict;")[0][0],
		    5000150000)

		con1.close()
		node.stop()

	def test_undo_eviction_delete(self):
		node = self.node

		node.execute(
		    "INSERT INTO o_undo_evict (SELECT i, i FROM generate_series(1, 100000) i);"
		)

		con1 = node.connect()

		con1.begin()
		con1.execute("DELETE FROM o_undo_evict;")

		self.assertEqual(
		    con1.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 0)
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 100000)
		self.assertGreaterEqual(self.get_undo_files_count(), 1)

		con1.rollback()

		con1.begin()
		con1.execute("DELETE FROM o_undo_evict;")
		self.assertGreaterEqual(self.get_undo_files_count(), 1)
		con1.commit()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_undo_evict;")[0][0], 0)

		con1.close()
		node.stop()

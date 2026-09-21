#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest

# Rows per transaction.  Enough that a single commit spreads many messages
# across the recovery pool, so a stop in the middle of one lands with some
# of its work applied and some not.
ROWS = 5000


class RecoveryStopMidRecordTest(BaseTest):
	"""
	Stopping recovery part-way through a WAL record must not lose data.

	The startup process acts on a shutdown request from
	ProcessStartupProcInterrupts(), which proc_exit(1)s where it stands.  In
	orioledb that is not only between records: worker_send_modify() calls
	o_worker_handle_interrupts() for every message it spreads, so a standby
	can stop with one worker's share of a record applied and another's not.

	What makes that safe is supposed to be replay: a restartpoint's redo
	position is the last checkpoint record in the stream, not the current
	replay position, so the partly-applied record is read again from the
	start after the restart.  These tests check that it really is.
	"""

	def wait_parked(self, replica, event):
		"""Wait until somebody sits on the given stop event."""
		deadline = time.time() + 60
		while time.time() < deadline:
			parked = replica.execute("""
				SELECT EXISTS(
					SELECT 1 FROM pg_stopevents() se
					WHERE se.stopevent = '%s'
					  AND array_length(se.waiter_pids, 1) > 0);
			""" % event)[0][0]
			if parked:
				return True
			time.sleep(0.1)
		return False

	def rows(self, node):
		return node.execute("SELECT count(*) FROM o_stopmid;")[0][0]

	def assert_behind(self, node, replica):
		"""
		The scenario only means something if replay really is held up: a
		standby that had already caught up would make the comparison below
		pass without ever having stopped mid-record.
		"""
		ahead = self.rows(node)
		behind = self.rows(replica)
		self.assertLess(
		    behind, ahead,
		    "standby was already caught up (%d rows); the stop event did not "
		    "hold replay, so this run proves nothing" % behind)

	def fingerprint(self, node):
		"""
		Everything a lost or duplicated row would move.  The secondary index
		is counted separately from the primary, so a row that reached one
		tree and not the other shows up as a mismatch rather than cancelling
		out.
		"""
		primary = node.execute("""
			SET enable_seqscan = on;
			SET enable_indexscan = off;
			SET enable_bitmapscan = off;
			SELECT count(*), coalesce(sum(id), 0), coalesce(sum(hashtext(val)::bigint), 0)
				FROM o_stopmid;
		""")[0]
		secondary = node.execute("""
			SET enable_seqscan = off;
			SET enable_indexscan = on;
			SELECT count(*), coalesce(sum(id), 0)
				FROM o_stopmid WHERE val > '';
		""")[0]
		return (tuple(primary), tuple(secondary))

	def setup_pair(self, replica_conf=''):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.enable_stopevents = true\n"
		    "checkpoint_timeout = 1d\n"
		    "max_wal_size = 4GB\n")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_stopmid (
				id int NOT NULL,
				val text NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
			CREATE INDEX o_stopmid_val ON o_stopmid (val);
		""")
		return node

	def check_recovered(self, node, replica, expected):
		"""Bring the standby back up, let it catch up, and compare."""
		replica.start()
		self.catchup_orioledb(replica)

		self.assertEqual(
		    expected, self.fingerprint(replica),
		    "standby diverged from the primary after a mid-record stop")
		self.assertEqual(expected, self.fingerprint(node),
		                 "primary changed under the test")

		# orioledb_tbl_check() would be the natural cross-tree check, but it
		# wants an AccessExclusiveLock and a standby cannot give it one.  The
		# two halves of the fingerprint stand in: they read the primary and
		# the secondary separately, so a row that reached one tree and not
		# the other shows up as a mismatch.

	def test_stop_while_replay_is_parked_mid_record(self):
		"""
		Park the recovery leader inside a record -- replay_on_record fires
		per orioledb sub-record, so earlier ones of the same commit are
		already applied -- then stop the standby where it stands.
		"""
		node = self.setup_pair()

		with self.getReplica().start() as replica:
			replica.append_conf('postgresql.conf',
			                    "orioledb.enable_stopevents = true\n")
			replica.restart()
			self.catchup_orioledb(replica)

			node.safe_psql("INSERT INTO o_stopmid SELECT i, 'seed' || i "
			               "FROM generate_series(1, %d) i;" % ROWS)
			self.catchup_orioledb(replica)

			replica.safe_psql(
			    "SELECT pg_stopevent_set('replay_on_record', 'true');")

			# A second commit, replayed while the leader is parked in the
			# middle of it.
			node.safe_psql("INSERT INTO o_stopmid SELECT i, 'more' || i "
			               "FROM generate_series(%d, %d) i;" %
			               (ROWS + 1, 2 * ROWS))

			self.assertTrue(self.wait_parked(replica, 'replay_on_record'),
			                "replay never reached replay_on_record")

			self.assert_behind(node, replica)
			expected = self.fingerprint(node)

			# The stop request reaches the parked leader, which exits from
			# wherever inside the record it happens to be.
			replica.stop()

			self.check_recovered(node, replica, expected)

	def test_stop_while_leader_waits_on_a_full_queue(self):
		"""
		The same, but with the leader blocked handing work to a worker rather
		than parked between messages.

		A worker parked in modify_start stops draining its queue; the leader
		fills it and waits in worker_queue_flush().  That wait is where
		issue #1197 hung, and where the wait now polls -- so this is the
		shape a stop has to survive for the change in that area to be safe.
		"""
		node = self.setup_pair()

		with self.getReplica().start() as replica:
			replica.append_conf('postgresql.conf',
			                    "orioledb.enable_stopevents = true\n")
			replica.restart()
			self.catchup_orioledb(replica)

			node.safe_psql("INSERT INTO o_stopmid SELECT i, 'seed' || i "
			               "FROM generate_series(1, %d) i;" % ROWS)
			self.catchup_orioledb(replica)

			replica.safe_psql(
			    "SELECT pg_stopevent_set('modify_start', 'true');")

			node.safe_psql("INSERT INTO o_stopmid SELECT i, 'more' || i "
			               "FROM generate_series(%d, %d) i;" %
			               (ROWS + 1, 2 * ROWS))

			self.assertTrue(self.wait_parked(replica, 'modify_start'),
			                "no recovery worker reached modify_start")

			# Give the leader time to fill that worker's queue and settle
			# into the wait; the point of the test is to stop it there.
			time.sleep(2)

			self.assert_behind(node, replica)
			expected = self.fingerprint(node)
			replica.stop()

			self.check_recovered(node, replica, expected)

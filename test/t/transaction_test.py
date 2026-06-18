#!/usr/bin/env python3
# coding: utf-8

from collections import Counter
import time

from .base_test import BaseTest, ThreadQueryExecutor, wait_stopevent, wait_for_wait_event


class TransactionTest(BaseTest):

	def test_waiter_select_after_self_insert(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE t (k int PRIMARY KEY, v text) USING orioledb;

			-- Insert a few rows so the leaf has some content but is
			-- far from full.
			INSERT INTO t SELECT g, 'seed' FROM generate_series(1, 10) g;
			CHECKPOINT;
		""")

		waiter = node.connect()
		holder = node.connect(autocommit=True)
		ctl = node.connect()

		try:
			# Make commandInfos[] non-empty.
			waiter.execute("INSERT INTO t VALUES (100, 'pre');")

			ctl.execute(
			    "SELECT pg_stopevent_set('before_get_waiters_with_tuples', 'true');"
			)

			# Holder takes the leaf lock
			holder_pid = holder.pid
			t_holder = ThreadQueryExecutor(
			    holder, "INSERT INTO t VALUES (200, 'holder');")
			t_holder.start()
			wait_stopevent(node, holder_pid)

			# Now insert at the same leaf from waiter;  buggy behaviour
			# is tuphdr.undoLocation = InvalidUndoLocation and assertion
			waiter_pid = waiter.pid
			t_waiter = ThreadQueryExecutor(
			    waiter, "INSERT INTO t VALUES (300, 'waiter');")
			t_waiter.start()

			# Now wait until the waiter backend is actually blocked on the page lock.
			wait_for_wait_event(node, waiter_pid, 'BufferContent')
			ctl.execute(
			    "SELECT pg_stopevent_reset('before_get_waiters_with_tuples');")

			t_holder.join()
			t_waiter.join()

			# At buggy build we get the Assert(lo >= 0 && lo < commandIndex) triggered here
			rows = waiter.execute("SELECT k, v FROM t WHERE k = 300;")
			self.assertEqual(rows, [(300, 'waiter')])

			waiter.commit()

		finally:
			for c in (waiter, holder, ctl):
				try:
					c.close()
				except Exception:
					pass
			node.stop()

	def test_waiter_select_after_self_insert_no_prior_undo(self):
		"""
		Same race as test_waiter_select_after_self_insert, but the waiter
		makes its FIRST undo allocation only AFTER the holder has inserted
		the waiter's tuple on its behalf.  That puts the holder's
		make_waiter_undo_record() location below every commandInfos[]
		entry the waiter eventually creates.

		Without lock_page_with_tuple() registering the holder-allocated
		location in commandInfos[] before returning
		OLockPageWithTupleResultInserted, the later SELECT calls
		undo_location_get_command() with a location below
		commandInfos[0].undoLocation, the binary search lands at lo=-1
		and tripsthe Assert(lo >= 0 && lo <= commandIndex) in
		undo_location_get_command().  In non-assert builds a bogus cid
		is returned and the row may be invisible.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE t (k int PRIMARY KEY, v text) USING orioledb;
			INSERT INTO t SELECT g, 'seed' FROM generate_series(1, 10) g;
			CHECKPOINT;
		""")

		waiter = node.connect()
		holder = node.connect(autocommit=True)
		ctl = node.connect()

		try:
			# Start the waiter transaction, but DO NOT yet do anything
			# that allocates an undo record in this command -- we want
			# the holder's make_waiter_undo_record() to be the first
			# UndoLogRegular allocation tied to this command.
			waiter.execute("BEGIN;")

			ctl.execute(
			    "SELECT pg_stopevent_set('before_get_waiters_with_tuples', 'true');"
			)

			# Holder takes the leaf lock and parks at the stopevent.
			holder_pid = holder.pid
			t_holder = ThreadQueryExecutor(
			    holder, "INSERT INTO t VALUES (200, 'holder');")
			t_holder.start()
			wait_stopevent(node, holder_pid)

			# Waiter queues on the same leaf -- this is its first
			# undo-allocating attempt in the current command.  Since
			# the holder will perform the insertion on the waiter's
			# behalf, the waiter itself never reaches the
			# current_command_get_undo_location() call site.
			waiter_pid = waiter.pid
			t_waiter = ThreadQueryExecutor(
			    waiter, "INSERT INTO t VALUES (300, 'waiter');")
			t_waiter.start()
			wait_for_wait_event(node, waiter_pid, 'BufferContent')
			ctl.execute(
			    "SELECT pg_stopevent_reset('before_get_waiters_with_tuples');")

			t_holder.join()
			t_waiter.join()

			# Force the waiter to allocate undo *after* the holder's
			# make_waiter_undo_record() location -- this is what makes
			# commandInfos[0].undoLocation strictly greater than the
			# holder-allocated location stamped into the inserted
			# tuphdr.
			waiter.execute("INSERT INTO t VALUES (400, 'after');")

			# The SELECT reads the holder-inserted tuple under an
			# in-progress snapshot, which routes through
			# undo_location_get_command() on the inserted tuphdr's
			# undoLocation.  Pre-fix builds abort here.
			rows = waiter.execute(
			    "SELECT k, v FROM t WHERE k IN (300, 400) ORDER BY k;")
			self.assertEqual(rows, [(300, 'waiter'), (400, 'after')])

			waiter.commit()
		finally:
			for c in (waiter, holder, ctl):
				try:
					c.close()
				except Exception:
					pass
			node.stop()

	def _live_waiter_split_seq_scan_reads_adjacent_undo_leaves(
	        self, evict_before_select=False):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE t (
				k text NOT NULL,
				v text NOT NULL,
				PRIMARY KEY (k)
			) USING orioledb;

			INSERT INTO t (k, v)
			SELECT 'k' || to_char(g, 'fm0000'), repeat('a', 600)
			FROM generate_series(1, 12) g;
			CHECKPOINT;
		""")

		holder = node.connect()
		waiter = node.connect()
		reader = node.connect()
		ctl = node.connect()

		try:
			ctl.execute(
			    "SELECT pg_stopevent_set('before_get_waiters_with_tuples', "
			    "'true');")

			holder_pid = holder.pid
			t_holder = ThreadQueryExecutor(
			    holder, "INSERT INTO t (k, v) VALUES "
			    "('k0013', repeat('h', 600));")
			t_holder.start()
			wait_stopevent(node, holder_pid)

			waiter_pid = waiter.pid
			t_waiter = ThreadQueryExecutor(
			    waiter, "INSERT INTO t (k, v) VALUES "
			    "('k0014', repeat('w', 600));")
			t_waiter.start()
			wait_for_wait_event(node, waiter_pid, 'BufferContent')

			ctl.execute(
			    "SELECT pg_stopevent_reset('before_get_waiters_with_tuples');")

			t_holder.join()
			t_waiter.join()

			if evict_before_select:
				ctl.execute("SELECT orioledb_evict_pages('t'::regclass, 0);")

			rows = reader.execute("SELECT k FROM t;")
			keys = [row[0] for row in rows]
			counts = Counter(keys)
			duplicated_keys = sorted(k for k, count in counts.items()
			                         if count > 1)

			self.assertEqual(
			    duplicated_keys, [],
			    "SELECT returned duplicate keys: %s; full result: %s" %
			    (duplicated_keys, keys))
			self.assertEqual(sorted(keys), ['k%04d' % i for i in range(1, 13)])
		finally:
			for c in (holder, waiter, reader, ctl):
				try:
					c.execute("ROLLBACK")
				except Exception:
					pass
				try:
					c.close()
				except Exception:
					pass
			node.stop()

	def test_live_waiter_split_seq_scan_reads_adjacent_undo_leaves(self):
		self._live_waiter_split_seq_scan_reads_adjacent_undo_leaves()

	def test_live_waiter_split_seq_scan_reads_adjacent_disk_undo_leaves(self):
		self._live_waiter_split_seq_scan_reads_adjacent_undo_leaves(
		    evict_before_select=True)

	def _seq_scan_internal_page_read_before_split_downlink(
	        self, evict_before_select=False):
		"""
		Reader copies the parent internal page BEFORE the splitter has
		installed the new sibling downlink, but AFTER the leaf split
		has physically separated L into L1 (in-place) and a new L2.

		Sequence:
		  - holder INSERT triggers a leaf split and halts at
		    STOPEVENT(page_split), i.e. after the leaf is split but
		    before the parent gets a downlink for L2.
		  - reader copies the parent I; its copy holds a single downlink
		    to L (block# of L1 after in-place modification), hikey
		    spans the pre-split range.
		  - reader follows that downlink: hits L1 whose csn equals the
		    split csn (>= imgReadCsn), so the page is reconstructed from
		    the leaf-level undo chain.
		  - reader is NOT supposed to visit L2: the L2 downlink is not
		    in the reader's copy of I, and pre-split L (returned via
		    undo) already contains every tuple visible at imgReadCsn.

		With evict_before_select=True, the leaf is pushed to disk
		between the holder halt and the reader's scan, so the scan
		goes through load_next_disk_leaf_page() rather than the
		in-memory iterate_internal_page() path.

		Verifies: no tuple is dropped and no duplicate appears.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE t (
				k text NOT NULL,
				v text NOT NULL,
				PRIMARY KEY (k)
			) USING orioledb;

			INSERT INTO t (k, v)
			SELECT 'k' || to_char(g, 'fm0000'), repeat('a', 600)
			FROM generate_series(1, 12) g;
			CHECKPOINT;
		""")

		holder = node.connect()
		reader = node.connect()
		ctl = node.connect()

		try:
			ctl.execute("SELECT pg_stopevent_set('page_split', 'true');")

			holder_pid = holder.pid
			t_holder = ThreadQueryExecutor(
			    holder, "INSERT INTO t (k, v) VALUES "
			    "('k0013', repeat('h', 600));")
			t_holder.start()
			wait_stopevent(node, holder_pid)

			if evict_before_select:
				ctl.execute("SELECT orioledb_evict_pages('t'::regclass, 0);")

			# Holder is paused right after the leaf has been split but
			# before the parent has been given the new sibling's
			# downlink.  Run the scan now — its copy of the parent will
			# carry the pre-split set of downlinks.
			rows = reader.execute("SELECT k FROM t;")
			keys = [row[0] for row in rows]
			counts = Counter(keys)
			duplicated_keys = sorted(k for k, count in counts.items()
			                         if count > 1)

			# Holder's INSERT has not committed yet, so k0013 must not
			# be visible to the reader.
			self.assertEqual(
			    duplicated_keys, [],
			    "SELECT returned duplicate keys: %s; full result: %s" %
			    (duplicated_keys, keys))
			self.assertEqual(
			    sorted(keys), ['k%04d' % i for i in range(1, 13)],
			    "expected 12 unique pre-split keys, got: %s" % keys)

			ctl.execute("SELECT pg_stopevent_reset('page_split');")
			t_holder.join()
		finally:
			for c in (holder, reader, ctl):
				try:
					c.execute("ROLLBACK")
				except Exception:
					pass
				try:
					c.close()
				except Exception:
					pass
			node.stop()

	def test_seq_scan_internal_page_read_before_split_downlink(self):
		self._seq_scan_internal_page_read_before_split_downlink()

	def test_seq_scan_internal_page_read_before_split_downlink_evicted(self):
		self._seq_scan_internal_page_read_before_split_downlink(
		    evict_before_select=True)

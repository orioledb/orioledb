#!/usr/bin/env python3
# coding: utf-8

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

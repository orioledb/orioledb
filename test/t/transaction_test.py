#!/usr/bin/env python3
# coding: utf-8
"""
Regression test for the deadlock between an aborter that has stamped
the COMMITSEQNO_STATUS_CSN_COMMITTING bit on its oxid CSN and another
backend that is updating the same row, holding the leaf-page-content
lock and busy-spinning on that bit inside oxid_get_csn().

Bug:

  * backend B reaches XACT_EVENT_COMMIT and calls
    current_oxid_precommit(), which stamps
    COMMITSEQNO_STATUS_CSN_COMMITTING on B's oxid CSN slot;
  * an error is raised AFTER that point but before
    current_oxid_commit() writes the real CSN, so B falls into
    XACT_EVENT_ABORT with the COMMITTING bit still set;
  * the old abort sequence ran wal_rollback() and then
    apply_undo_stack() while the COMMITTING bit was still set;
  * concurrently, backend A is updating the same row, holding the
    leaf-page-content lock, and spinning in oxid_get_csn() (with
    perform_spin_delay) waiting for the bit to clear;
  * apply_undo_stack() in B needs that page-content lock — A has it,
    A is spinning on B, B waits on A; once A burns NUM_DELAYS the
    cluster PANICs at "stuck spinlock detected at oxid_get_csn".

Fix:

  current_oxid_clear_committing() reverts the COMMITTING bit back to
  IN_PROGRESS at the very top of the abort handler, before
  apply_undo_stack() runs.  Spinners then make forward progress, A
  releases the page-content lock, and B's apply_undo_stack() and
  current_oxid_abort() complete without deadlock.

Test orchestration:

  * before_modify_oxid_get_csn parks A inside
    o_btree_modify_handle_conflicts() — A has already locked the leaf
    page and is one instruction away from oxid_get_csn();
  * after_csn_precommit parks B between current_oxid_precommit() and
    current_oxid_commit() (XACT_EVENT_COMMIT runs under
    HOLD_INTERRUPTS, so the callback briefly RESUME_INTERRUPTS()s
    around the wait when STOPEVENTS_ENABLED so a query cancel can
    fire and drive the precommit→abort transition);
  * pg_cancel_backend() then pg_stopevent_reset(after_csn_precommit)
    pushes B into XACT_EVENT_ABORT;
  * pg_stopevent_reset(before_modify_oxid_get_csn) wakes A; A calls
    oxid_get_csn(B.oxid) and starts spinning on the COMMITTING bit
    while still holding the leaf-page-content lock;
  * with the fix in place, B's abort handler runs
    current_oxid_clear_committing() before apply_undo_stack(), the
    spin breaks, A releases the page lock and B's apply_undo_stack()
    proceeds.  Without the fix B's apply_undo_stack() blocks on A's
    page-content lock while A keeps spinning, NUM_DELAYS is exhausted
    and the cluster PANICs.
"""

import time

from .base_test import BaseTest, ThreadQueryExecutor, wait_stopevent


class TransactionTest(BaseTest):

	def test_committing_bit_cleared_on_abort(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			INSERT INTO o_t VALUES (1, 'initial');
		""")

		b_conn = node.connect()
		a_conn = node.connect()
		a_pid = a_conn.pid
		ctl = node.connect()

		try:
			# B opens a transaction and stamps an undo record on row
			# k=1.  No COMMITTING bit yet.
			b_conn.execute("BEGIN")
			b_conn.execute("UPDATE o_t SET v = 'b-update' WHERE k = 1")
			b_pid = b_conn.pid

			# Park A inside o_btree_modify_handle_conflicts() with the
			# leaf-page-content lock held, just before its
			# oxid_get_csn() call.
			ctl.execute("SELECT pg_stopevent_set("
			            "'before_modify_oxid_get_csn', 'true')")

			t_a = ThreadQueryExecutor(
			    a_conn, "UPDATE o_t SET v = 'a-update' WHERE k = 1")
			t_a.start()
			wait_stopevent(node, a_pid)

			# Now park B between current_oxid_precommit() (which
			# stamps the COMMITTING bit) and current_oxid_commit()
			# (which would write the real CSN).
			ctl.execute(
			    "SELECT pg_stopevent_set('after_csn_precommit', 'true')")

			t_b = ThreadQueryExecutor(b_conn, "COMMIT")
			t_b.start()
			wait_stopevent(node, b_pid)

			# B is parked with the COMMITTING bit set; A is parked
			# holding the page-content lock.  Cancel B and let it
			# wake — XACT_EVENT_COMMIT lifted HOLD_INTERRUPTS for the
			# wait, so the cancel raises out of the COMMIT callback
			# and PG runs the abort path.
			ctl.execute("SELECT pg_cancel_backend(%d)" % b_pid)

			# Release A first so it starts spinning on the COMMITTING
			# bit while still holding the page-content lock; then
			# release B so its abort path runs concurrently.  Without
			# the fix, B's apply_undo_stack() blocks on A's page lock
			# while A spins on B's bit and the cluster PANICs.
			ctl.execute("SELECT pg_stopevent_reset("
			            "'before_modify_oxid_get_csn')")
			ctl.execute("SELECT pg_stopevent_reset('after_csn_precommit')")

			# B's COMMIT must report cancellation.
			b_raised = False
			try:
				t_b.join()
			except Exception:
				b_raised = True
			self.assertTrue(b_raised, "B's COMMIT should have been cancelled")

			# A finishes once the bit is cleared and B's undo is
			# applied.  Without the fix, this never returns.
			t_a.join()
			a_conn.execute("COMMIT")

			# B's update was rolled back; A's update is the surviving
			# version of the row.  The b-tree must remain
			# structurally sound.
			v = node.execute("SELECT v FROM o_t WHERE k = 1")[0][0]
			self.assertEqual(v, 'a-update')
			self.assertTrue(
			    node.execute("SELECT orioledb_tbl_check('o_t'::regclass)")[0]
			    [0])
		finally:
			node.stop()

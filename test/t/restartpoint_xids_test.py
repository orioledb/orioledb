#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import struct

from .base_test import BaseTest

# uint32 count, then that many XidFileRec: OXid, XidRecKind (+pad),
# UndoStackLocations (4 x UndoLocation), UndoLocation.
XID_REC_SIZE = 8 + 8 + 4 * 8 + 8
XID_HEADER_SIZE = 4


class RestartpointXidsTest(BaseTest):

	def xid_file(self, node):
		"""The newest xid file of a node, as (count, [oxid, ...], size)."""
		files = glob.glob(os.path.join(node.data_dir, 'orioledb_data',
		                               '*.xid'))
		self.assertTrue(files, "no xid file at all under %s" % node.data_dir)
		newest = max(files, key=lambda f: int(os.path.basename(f)[:-4]))
		with open(newest, 'rb') as f:
			blob = f.read()
		count = struct.unpack('<I', blob[:XID_HEADER_SIZE])[0]
		oxids = []
		for i in range(count):
			off = XID_HEADER_SIZE + i * XID_REC_SIZE
			if off + 8 > len(blob):
				break
			oxids.append(struct.unpack('<Q', blob[off:off + 8])[0])
		return os.path.basename(newest), count, oxids, len(blob)

	def test_restartpoint_records_in_progress_oxids(self):
		"""A standby's restartpoint must write down the oxids still open.

		The xid file is how a restarted standby learns which transactions
		were in progress: read_xids() turns each record into a
		COMMITSEQNO_INPROGRESS state with the undo stack to roll back.  On
		the primary the checkpointer snapshots its own backends in
		finish_write_xids(); on a standby the same information lives in the
		startup process and the recovery workers, which write it only when
		asked -- and the request is the last thing finish_write_xids() does,
		with close_xids_file() running straight after it.
		"""
		with self.node as master:
			master.start()
			with self.getReplica() as replica:
				replica.start()
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text
					) USING orioledb;
					INSERT INTO o_test SELECT id, id || 'committed'
						FROM generate_series(1, 100) id;
				""")
				self.catchup_orioledb(replica)

				con1 = master.connect()
				con1.begin()
				con1.execute("INSERT INTO o_test SELECT id, id || 'open'"
				             " FROM generate_series(1000, 11000) id;")
				self.catchup_orioledb(replica)

				# Both sides checkpoint the same open transaction.
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)
				replica.safe_psql("CHECKPOINT;")

				mname, mcount, moxids, msize = self.xid_file(master)
				rname, rcount, roxids, rsize = self.xid_file(replica)
				print("# primary %s: count=%d oxids=%s size=%d" %
				      (mname, mcount, sorted(set(moxids)), msize))
				print("# standby %s: count=%d oxids=%s size=%d" %
				      (rname, rcount, sorted(set(roxids)), rsize))

				# The primary writes the open transaction down; the standby
				# has to as well, or a restart from this file has no record
				# of it.
				self.assertGreater(
				    mcount, 0,
				    "the primary's own checkpoint recorded no in-progress"
				    " transaction")
				self.assertGreater(
				    rcount, 0,
				    "the standby's restartpoint recorded no in-progress"
				    " transaction (primary recorded %d)" % mcount)
				self.assertEqual(
				    sorted(set(roxids)), sorted(set(moxids)),
				    "standby and primary disagree on which oxids were open")

				con1.rollback()
				con1.close()

	def test_rollback_after_immediate_restart_across_restartpoints(self):
		"""The harsher shape: several restartpoints, then a crash restart.

		A clean restart writes a shutdown restartpoint and hands the recovery
		workers' state over through their temp files, which hides whether the
		restartpoint file carried anything.  Killing the standby leaves it to
		come back from the last restartpoint alone -- and the retain location
		of the open transaction is one of the things those records carry, so
		if they are missing the undo it needs may already be gone.
		"""
		with self.node as master:
			master.start()
			with self.getReplica() as replica:
				replica.start()
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text
					) USING orioledb;""")
				self.catchup_orioledb(replica)

				con1 = master.connect()
				con1.begin()
				con1.execute("INSERT INTO o_test SELECT id, id || 'val'"
				             " FROM generate_series(1, 10000) id;")
				self.catchup_orioledb(replica)

				for _ in range(3):
					master.safe_psql("CHECKPOINT;")
					replica.safe_psql("CHECKPOINT;")
					self.catchup_orioledb(replica)

				replica.stop(['-m', 'immediate'])
				replica.start()
				self.catchup_orioledb(replica)

				self.assertEqual(
				    replica.execute("SELECT count(*) FROM o_test;")[0][0], 0,
				    "the standby shows rows of a transaction still open on"
				    " the primary")

				con1.rollback()
				con1.close()
				self.catchup_orioledb(replica)

				self.assertEqual(
				    master.execute("SELECT count(*) FROM o_test;")[0][0], 0)
				self.assertEqual(
				    replica.execute("SELECT count(*) FROM o_test;")[0][0], 0,
				    "rolled-back rows are visible on the standby")

	def test_promote_with_a_transaction_open_across_restartpoints(self):
		"""Promotion is where the standby has to roll the transaction back.

		While it is a standby someone else owns the outcome; once promoted,
		the open transaction is its own to abort, and the undo stack and
		retain location for that are what the restartpoint file carries.
		"""
		with self.node as master:
			master.start()
			with self.getReplica() as replica:
				replica.start()
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text
					) USING orioledb;
					INSERT INTO o_test SELECT id, id || 'committed'
						FROM generate_series(1, 100) id;""")
				self.catchup_orioledb(replica)

				con1 = master.connect()
				con1.begin()
				con1.execute("INSERT INTO o_test SELECT id, id || 'open'"
				             " FROM generate_series(1000, 11000) id;")
				self.catchup_orioledb(replica)

				for _ in range(3):
					master.safe_psql("CHECKPOINT;")
					replica.safe_psql("CHECKPOINT;")
					self.catchup_orioledb(replica)

				replica.stop(['-m', 'immediate'])
				replica.start()
				self.catchup_orioledb(replica)

				master.stop(['-m', 'immediate'])
				replica.promote()
				replica.poll_query_until("SELECT NOT pg_is_in_recovery();",
				                         expected=True)

				self.assertEqual(
				    replica.execute("SELECT count(*) FROM o_test;")[0][0], 100,
				    "the promoted node kept rows of a transaction that never"
				    " committed")
				replica.safe_psql("CHECKPOINT;")
				replica.restart()
				self.assertEqual(
				    replica.execute("SELECT count(*) FROM o_test;")[0][0], 100,
				    "uncommitted rows came back after a restart of the"
				    " promoted node")

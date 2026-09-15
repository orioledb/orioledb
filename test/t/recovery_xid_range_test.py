#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import struct
import time

from testgres.enums import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class RecoveryXidRangeTest(BaseTest):
	"""A corrupt transaction id in a recovery artifact must not hang startup.

	Recovery hands the oxids it reads -- from the checkpoint's `<num>.xid`
	file in read_xids(), and from WAL_REC_XID in replay_on_record() -- to
	advance_oxids(), which fills one map entry per oxid between nextXid and
	the value read.  Neither artifact carries a checksum, so a value from a
	tampered-with or damaged backup arrives unchecked.

	UINT64_MAX made that loop endless: `Min(new_xid + 1, ...)` wrapped to
	zero, nextXid was set *backwards* to zero, and the loop then waited for
	nextXid to reach UINT64_MAX, which it never could.  The cluster stayed in
	startup recovery for ever -- no error, no start, nothing to act on.  A
	merely enormous value was no better: the extension is linear in the gap,
	so it is a hang in all but name.

	Both now fail recovery with a message that says which id and how far.
	"""

	# Offset of the first XidFileRec in the file: it opens with a uint32
	# count, and the record opens with its OXid.
	FIRST_OXID_OFFSET = 4

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "checkpoint_timeout = 1h\n"
		    "max_wal_size = 1GB\n")

	def prepare_xid_file(self):
		"""Leave a checkpoint xid file with an in-progress transaction in it.

		The file only lists transactions that were still open when the
		checkpoint ran, so one has to be held open across it.
		"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_xid (\n"
		    "  id int NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_xid SELECT g, g FROM generate_series(1, 100) g;\n")

		con = node.connect()
		con.begin()
		con.execute("UPDATE o_xid SET v = v + 1 WHERE id = 1;")
		node.safe_psql('postgres', "CHECKPOINT;")
		con.close()

		node.stop(['-m', 'immediate'])

		pattern = os.path.join(node.data_dir, 'orioledb_data', '*.xid')
		xid_files = sorted(glob.glob(pattern))
		self.assertTrue(xid_files, f"no xid files matched {pattern}")
		path = xid_files[-1]

		with open(path, 'rb') as f:
			original = f.read()
		count = struct.unpack('<I', original[:4])[0]
		self.assertGreater(count, 0,
		                   f"{path} lists no in-progress transactions")
		return path, original

	def poison_oxid(self, path, value):
		with open(path, 'r+b') as f:
			f.seek(self.FIRST_OXID_OFFSET)
			f.write(struct.pack('<Q', value))

	def start_expecting_failure(self, expected):
		"""Start the node, requiring it to fail recovery -- and promptly.

		A hang is what this test is about, so the start is given a deadline
		of its own rather than left to sit out pg_ctl's.
		"""
		node = self.node
		started = time.time()
		try:
			node.start(params=['-t', '30'])
		except Exception:
			pass
		elapsed = time.time() - started
		node.is_started = False

		with open(node.pg_log_file) as f:
			log = f.read()

		if node.status() == NodeStatus.Running:
			# The postmaster is up, so either recovery accepted the value or
			# it is still going -- which is the hang this test is about.  Tell
			# them apart, and take the cluster down either way so tearDown()
			# is not left with a half-known state (a fast shutdown does not
			# complete while recovery is spinning).
			node.is_started = True
			try:
				node.execute("SELECT 1;")
				reason = "recovery accepted a corrupt transaction id"
			except Exception:
				reason = ("recovery neither finished nor failed in %.0f s: "
				          "it is still extending the transaction id map" %
				          elapsed)
			node.stop(['-m', 'immediate'])
			self.fail(reason)

		self.assertIn(expected, log)
		self.assertLess(
		    elapsed, 60,
		    "recovery did not fail promptly: it took %.0f s" % elapsed)

	def restore_and_check(self, path, original):
		"""The artifact is fine again once the bytes are: this proves the
		poisoned field was the only thing standing between the cluster and a
		clean recovery."""
		node = self.node
		with open(path, 'r+b') as f:
			f.write(original)

		node.start()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_xid;")[0][0], 100)
		# The transaction that was open at checkpoint time never committed.
		self.assertEqual(
		    node.execute("SELECT v FROM o_xid WHERE id = 1;")[0][0], 1)
		node.stop()

	def test_wrapping_xid_in_checkpoint_file(self):
		"""UINT64_MAX: the value that made advance_oxids() loop for ever."""
		path, original = self.prepare_xid_file()
		self.poison_oxid(path, 0xFFFFFFFFFFFFFFFF)
		self.start_expecting_failure("is not a valid one")
		self.restore_and_check(path, original)

	def test_distant_xid_in_checkpoint_file(self):
		"""A value that is a legal oxid but is 2^40 transactions ahead: the
		map extension is linear in the gap, so this has to be refused too."""
		path, original = self.prepare_xid_file()
		self.poison_oxid(path, 1 << 40)
		self.start_expecting_failure("is too far ahead")
		self.restore_and_check(path, original)

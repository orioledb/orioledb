#!/usr/bin/env python3
# coding: utf-8

import time
import unittest

from threading import Thread

from testgres.enums import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

INVALID_UNDO_LOCATION = 1 << 61
SLOT = 'regression_slot'

# A small system undo log, so that a moderate amount of DDL is enough to push
# the retain floor forward.
SMALL_SYSTEM_UNDO_CONF = """
wal_level = logical
max_wal_senders = 4
max_replication_slots = 4
orioledb.main_buffers = 8MB
orioledb.undo_buffers = 128
orioledb.system_undo_circular_buffer_fraction = 0.05
"""


class LogicalUndoRetainTest(BaseTest):
	"""
	Tests for the system undo log retained for logical decoding: the
	SYS_TREES_CATALOG_XID_UNDO_LOCATION mapping that says how far back it has
	to be kept, and the places that have to honour it.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', SMALL_SYSTEM_UNDO_CONF)

	def start_node(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_decoded (id int PRIMARY KEY, v text) USING orioledb;
		""")
		return node

	def churn(self, con, tag, count, cols=8):
		"""Write system undo: every CREATE TABLE updates o_tables."""
		body = ", ".join(f"c{j} text" for j in range(cols))
		for i in range(count):
			con.execute(f"CREATE TABLE churn_{tag}_{i} "
			            f"(id int PRIMARY KEY, {body}) USING orioledb;")

	def create_slot(self, node):
		node.safe_psql(
		    'postgres', f"SELECT pg_create_logical_replication_slot("
		    f"'{SLOT}', 'test_decoding');")

	def decoded(self, node):
		return [
		    row[0] for row in node.execute(
		        f"SELECT data FROM pg_logical_slot_get_changes("
		        f"'{SLOT}', NULL, NULL);")
		]

	def system_undo(self, con):
		return con.execute("SELECT lastusedlocation, minprocretainlocation "
		                   "FROM orioledb_get_undo_meta() "
		                   "WHERE undo_type = 'system';")[0]

	def start_slot_creation(self, node):
		thread = ThreadQueryExecutor(
		    node.connect(autocommit=True),
		    f"SELECT pg_create_logical_replication_slot('{SLOT}', "
		    f"'test_decoding');")
		thread.start()
		# restart_lsn is set by CreateInitDecodingContext(), before the
		# startpoint search begins, so once it is visible everything the
		# caller does next lands inside the decoded range.
		node.poll_query_until(
		    f"SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
		    f"WHERE slot_name = '{SLOT}' AND restart_lsn IS NOT NULL);",
		    sleep_time=0.1,
		    max_attempts=600)
		return thread

	def wait_stopped(self, node, timeout=30):
		deadline = time.time() + timeout
		while time.time() < deadline:
			if node.status() != NodeStatus.Running:
				node.is_started = False
				return
			time.sleep(0.1)

	def finish_and_check_slot(self, node, thread, old_con):
		try:
			old_con.commit()
		except Exception:
			# If the decoding backend already crashed, the postmaster took
			# this connection down with it.  The check below reports it.
			pass
		try:
			thread.join()
		except Exception as e:
			# Either the decoding backend crashed (restart_after_crash is off,
			# so the whole node goes down with it) or it raised.  Neither is
			# acceptable: the slot has to be creatable.
			self.wait_stopped(node)
			self.fail(f"CREATE_REPLICATION_SLOT failed while decoding a "
			          f"record whose system undo was released: {e}")

		self.assertEqual(node.status(), NodeStatus.Running)
		self.assertEqual(
		    node.execute(f"SELECT count(*) FROM pg_replication_slots "
		                 f"WHERE slot_name = '{SLOT}';")[0][0], 1,
		    "slot was not created")

		# The transaction we left open was already running when the slot was
		# created, so the snapshot builder skips its changes; what matters is
		# that decoding got through them.  Check the slot decodes for real by
		# running a fresh transaction through it.
		node.safe_psql('postgres',
		               "INSERT INTO o_decoded VALUES (2, 'after-the-slot');")
		self.assertIn(
		    "table public.o_decoded: INSERT: id[integer]:2 v[text]:'after-the-slot'",
		    self.decoded(node))

	def test_no_slot_lets_the_retain_floor_run_away(self):
		node = self.start_node()

		# The transaction whose WAL_REC_RELATION we want decoded.  Read
		# committed, so its snapshot -- and with it its hold on the system
		# undo log -- is gone as soon as the statement ends, while the record
		# it built stays buffered carrying the CSN of this moment.
		old_con = node.connect()
		old_con.execute("INSERT INTO o_decoded VALUES (1, 'old-csn');")

		# No slot exists yet, so nothing else pegs catalog_xmin.  Churn until
		# the floor gets past the first batch -- which is what decoding has to
		# undo -- or until we have clearly given it enough chances to.
		churn_con = node.connect(autocommit=True)
		self.churn(churn_con, 'a', 40)
		target = self.system_undo(churn_con)[0]
		for round in range(6):
			if self.system_undo(churn_con)[1] > target:
				break
			self.churn(churn_con, f'b{round}', 40)

		thread = self.start_slot_creation(node)
		self.finish_and_check_slot(node, thread, old_con)

		old_con.close()
		churn_con.close()

	def read_mapping(self, node, boundary):
		return node.execute(f"SELECT orioledb_read_sys_xid_undo_location("
		                    f"'{boundary}'::pg_lsn);")[0][0]

	def test_mapping_scan_terminates(self):
		"""
		The scan has to walk the mapping page by page.  Restarting it from the
		leftmost leaf, as it used to, never gets past the first page once that
		page has been drained.
		"""
		node = self.start_node()
		node.safe_psql(
		    'postgres', "SELECT orioledb_insert_sys_xid_undo_location("
		    "i, i * 1000, ('F000/' || to_hex(i * 16))::pg_lsn) "
		    "FROM generate_series(1, 5000) i;")

		# The least location sits on the last page, so answering correctly
		# takes walking all of them.
		node.safe_psql(
		    'postgres', "SELECT orioledb_insert_sys_xid_undo_location("
		    "5001, 1, 'F000/FFFF0'::pg_lsn);")

		thread = ThreadQueryExecutor(
		    node.connect(autocommit=True),
		    "SELECT orioledb_read_sys_xid_undo_location('F000/9C40'::pg_lsn);")
		thread.start()
		Thread.join(thread, 60)
		if thread.is_alive():
			node.stop(['-m', 'immediate'])
			node.is_started = False
			thread.join(30)
			self.fail("the mapping scan did not terminate")

		self.assertEqual(thread.join()[0][0], 1)

		# Everything below that position is gone now.
		self.assertEqual(self.read_mapping(node, 'FFFFFFFF/FFFFFFFF'),
		                 INVALID_UNDO_LOCATION)

		# And again, now that the mapping is empty.
		self.assertEqual(self.read_mapping(node, 'FFFFFFFF/FFFFFFFF'),
		                 INVALID_UNDO_LOCATION)

	def test_xid_mapping_answers_nothing_to_retain(self):
		node = self.start_node()

		# 1. Churners take their xids first, so all of them end up below the
		#    xid of the transaction that will be decoded.  One transaction per
		#    connection: a second one would get a fresh, higher xid.
		churners = []
		for _ in range(10):
			con = node.connect()
			con.execute("SELECT pg_current_xact_id();")
			churners.append(con)

		# 2. The transaction whose record we want decoded.  Its CSN is stamped
		#    now, before any of the DDL below runs.
		old_con = node.connect()
		old_con.execute("INSERT INTO o_decoded VALUES (1, 'old-csn');")
		old_xid = int(old_con.execute("SELECT pg_current_xact_id();")[0][0])

		# 3. Now the churners burn the log down.  Each commits before the next
		#    starts, so nobody's transactionUndoRetainLocation pins it.
		probe = node.connect(autocommit=True)
		target = None
		for i, con in enumerate(churners):
			self.churn(con, f'c{i}', 40)
			con.commit()
			last, retain = self.system_undo(probe)
			if target is None:
				target = last
			elif retain > target:
				break

		# Every churner has to be gone before the slot is created: an idle one
		# would still hold an xid below old_xid, which would both drag
		# catalog_xmin down and keep DecodingContextFindStartpoint() waiting.
		for con in churners:
			con.rollback()
			con.close()

		thread = self.start_slot_creation(node)
		self.assertEqual(
		    int(
		        node.execute(f"SELECT catalog_xmin FROM pg_replication_slots "
		                     f"WHERE slot_name = '{SLOT}';")[0][0]), old_xid,
		    "catalog_xmin should be the decoded transaction's xid, above "
		    "every xid that touched a system tree")

		self.finish_and_check_slot(node, thread, old_con)

		old_con.close()
		probe.close()

	def write_records_for_an_idle_slot(self, node):
		self.create_slot(node)
		node.safe_psql('postgres',
		               "INSERT INTO o_decoded VALUES (1, 'before-restart');")
		# o_tables has to move on after that INSERT's CSN, so that reading it
		# back at that CSN means walking the system undo.
		with node.connect(autocommit=True) as con:
			self.churn(con, 'r', 40)

	def check_survived_restart(self, node):
		self.assertEqual(node.status(), NodeStatus.Running,
		                 "node did not come back up")
		self.assertIn(
		    "table public.o_decoded: INSERT: id[integer]:1 v[text]:'before-restart'",
		    self.decoded(node))

	def test_idle_slot_survives_restart(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		node.restart()

		self.check_survived_restart(node)

	@unittest.skip("a page undo chain can still lead below the retained "
	               "floor after a crash: see issue #1081")
	def test_idle_slot_survives_crash_restart(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		node.stop(['-m', 'immediate'])
		node.start()

		self.check_survived_restart(node)

	@unittest.skip("a page undo chain can still lead below the retained "
	               "floor after a crash: see issue #1081")
	def test_idle_slot_survives_crash_restart_after_checkpoint(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		# The undo the slot needs is now behind the checkpoint the crash will
		# recover from, so keeping it takes the checkpoint having recorded it.
		node.safe_psql('postgres', "CHECKPOINT;")
		with node.connect(autocommit=True) as con:
			self.churn(con, 'after_chkp', 40)

		node.stop(['-m', 'immediate'])
		node.start()

		self.check_survived_restart(node)

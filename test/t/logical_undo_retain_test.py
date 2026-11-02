#!/usr/bin/env python3
# coding: utf-8

import os
import subprocess
import time
import unittest

from testgres.enums import NodeStatus
from testgres.utils import get_bin_path

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

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

	@unittest.skip("a page undo chain can still lead below the retained floor "
	               "after a crash: see issue #1081")
	@unittest.skip("a page undo chain can still lead below the retained "
	               "floor after a crash: see issue #1081")
	def test_idle_slot_survives_crash_restart(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		node.stop(['-m', 'immediate'])
		node.start()

		self.check_survived_restart(node)

	@unittest.skip("a page undo chain can still lead below the retained floor "
	               "after a crash: see issue #1081")
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

	def refresh_retain_floor(self, node):
		"""
		Let the system undo retain floor catch up with what is retained right
		now.  orioledb_get_undo_meta() runs update_min_undo_locations(), the
		same thing the background writer does on its own every now and then.
		"""
		node.execute("SELECT count(*) FROM orioledb_get_undo_meta();")

	def write_record_then_change_its_catalog(self, node):
		"""
		Stamp a WAL_REC_RELATION with the CSN of this moment, then change the
		o_tables row of the same table.  Reading that row back at the stamped
		CSN now means walking the system undo log.  Returns an LSN no later
		than the record.
		"""
		lsn = node.execute("SELECT pg_current_wal_insert_lsn();")[0][0]
		node.safe_psql('postgres',
		               "INSERT INTO o_decoded VALUES (1, 'before-ddl');")
		node.safe_psql('postgres',
		               "ALTER TABLE o_decoded ADD COLUMN extra int;")
		# The slot asks what is retained when it catches up; make that answer
		# current rather than whatever was published before the DDL.
		self.refresh_retain_floor(node)
		return lsn

	def assert_decoded_again_from(self, node, lsn):
		"""
		The next decoding session starts at restart_lsn, so it reads the
		record at lsn once more -- with its old CSN.
		"""
		restart_lsn = node.execute(
		    f"SELECT restart_lsn FROM pg_replication_slots "
		    f"WHERE slot_name = '{SLOT}';")[0][0]
		self.assertTrue(
		    node.execute(f"SELECT '{restart_lsn}'::pg_lsn <= '{lsn}';")[0][0],
		    f"restart_lsn {restart_lsn} is already past the record at {lsn}: "
		    f"nothing would be decoded again")

	def decode_or_fail(self, node, fn):
		try:
			return [
			    row[0] for row in node.execute(
			        f"SELECT data FROM pg_logical_slot_{fn}_changes("
			        f"'{SLOT}', NULL, NULL);")
			]
		except Exception as e:
			# restart_after_crash is off: a PANIC takes the node down.
			self.wait_stopped(node)
			self.fail(f"decoding a record a second time failed: {e}")

	def test_redecode_after_get_changes(self):
		"""
		Catching up moves the slot's retain to what is needed for WAL written
		from now on.  But the slot does not restart from here: the next
		session starts at restart_lsn, which lags behind confirmed_flush, and
		decodes the records in between once more, at their own old CSNs.
		"""
		node = self.start_node()
		self.create_slot(node)
		lsn = self.write_record_then_change_its_catalog(node)

		self.assertIn(
		    "table public.o_decoded: INSERT: id[integer]:1 v[text]:'before-ddl'",
		    self.decode_or_fail(node, 'get'))
		self.assert_decoded_again_from(node, lsn)

		self.refresh_retain_floor(node)

		# A fresh backend, so no descriptor is cached: the record above is
		# decoded again and its table is read from o_tables at its CSN.
		self.decode_or_fail(node, 'get')
		self.assertEqual(node.status(), NodeStatus.Running)

	def test_redecode_after_peek_changes(self):
		"""
		Peeking confirms nothing, so the next call decodes the very same
		records again.  The retain must not move on a peek.
		"""
		node = self.start_node()
		self.create_slot(node)
		lsn = self.write_record_then_change_its_catalog(node)

		row = "table public.o_decoded: INSERT: id[integer]:1 v[text]:'before-ddl'"
		self.assertIn(row, self.decode_or_fail(node, 'peek'))
		self.assert_decoded_again_from(node, lsn)

		self.refresh_retain_floor(node)

		self.assertIn(row, self.decode_or_fail(node, 'get'))
		self.assertEqual(node.status(), NodeStatus.Running)

	def recvlogical_until_now(self, node, out):
		"""
		Stream through a walsender up to the current end of WAL and
		disconnect, the way a subscriber does when its connection drops.
		"""
		endpos = node.execute("SELECT pg_current_wal_flush_lsn();")[0][0]
		try:
			return subprocess.run([
			    get_bin_path("pg_recvlogical"), "-d", "postgres", "-h",
			    "127.0.0.1", "-p",
			    str(node.port), "-S", SLOT, "--start", "--no-loop", "-E",
			    endpos, "-f", out
			],
			                      capture_output=True,
			                      text=True,
			                      timeout=60)
		except subprocess.TimeoutExpired as e:
			self.fail(f"pg_recvlogical did not reach {endpos}: {e}")

	def test_redecode_after_walsender_reconnect(self):
		"""
		The same through a walsender: it catches up and moves the retain,
		the client disconnects, and the walsender of the next connection
		starts at restart_lsn.
		"""
		node = self.start_node()
		self.create_slot(node)
		lsn = self.write_record_then_change_its_catalog(node)
		out = os.path.join(node.base_dir, 'recvlogical.out')

		first = self.recvlogical_until_now(node, out)
		self.assertEqual(first.returncode, 0, first.stderr)
		with open(out) as f:
			self.assertIn(
			    "table public.o_decoded: INSERT: id[integer]:1 "
			    "v[text]:'before-ddl'", f.read())
		self.assert_decoded_again_from(node, lsn)

		self.refresh_retain_floor(node)

		# Something new to stream, so that the second session has an end
		# point to reach.
		node.safe_psql('postgres',
		               "INSERT INTO o_decoded VALUES (2, 'after-reconnect');")
		second = self.recvlogical_until_now(node, out)
		if second.returncode != 0:
			# restart_after_crash is off: a PANIC takes the node down.
			self.wait_stopped(node)
			self.fail(f"walsender failed on reconnect: {second.stderr}")
		self.assertEqual(node.status(), NodeStatus.Running)

	def slot_retain(self, node):
		return node.execute(
		    "SELECT orioledb_slot_retain_undo_location();")[0][0]

	def test_slot_says_what_it_retains(self):
		"""
		The slot is the one holding the undo now, so its value has to behave:
		nothing without a slot, something once one exists, standing still
		while a transaction has a record stamped but not flushed, and moving
		on once that transaction is done, decoded, and restart_lsn has moved
		past it.
		"""
		node = self.start_node()
		self.assertIsNone(self.slot_retain(node),
		                  "nothing to retain while no slot exists")

		self.create_slot(node)
		self.assertIsNotNone(self.slot_retain(node),
		                     "a slot has to say what it retains")

		# A transaction that has stamped a record with a CSN but has not
		# flushed it: what that record needs is held from here on.
		old_con = node.connect()
		old_con.execute("INSERT INTO o_decoded VALUES (1, 'old-csn');")

		# Let the slot catch up once, so that its value has settled on what
		# the transaction above holds rather than on whatever it was given
		# when it was created.
		with node.connect(autocommit=True) as churn:
			self.churn(churn, 'settle', 20)
		self.decoded(node)
		held = self.slot_retain(node)

		# More of the same: with the transaction still open, catching up
		# again must not move the slot any further.
		with node.connect(autocommit=True) as churn:
			self.churn(churn, 'held', 20)
		self.decoded(node)

		self.assertEqual(
		    self.slot_retain(node), held,
		    "the slot moved past a record that is still waiting in its "
		    "transaction's WAL buffer")

		# Once that transaction is done and decoded, it may move -- but only
		# after restart_lsn has moved past the point where the slot asked, as
		# the next session reads everything from restart_lsn again.  It only
		# moves on a running_xacts record, so log some.  Decoding reads
		# flushed WAL only, and nothing flushes that record but the next
		# commit, so commit something with an xid after it.
		old_con.commit()
		old_con.close()
		with node.connect(autocommit=True) as churn:
			self.churn(churn, 'freed', 20)
		node.safe_psql('postgres', "CREATE TABLE flush_wal (i int);")
		for _ in range(10):
			node.execute("SELECT pg_log_standby_snapshot();")
			node.execute("SELECT pg_current_xact_id();")
			node.execute("INSERT INTO flush_wal VALUES (1);")
			self.decoded(node)
			if self.slot_retain(node) > held:
				break

		self.assertGreater(self.slot_retain(node), held,
		                   "the slot never moved on after catching up")

	def test_invalidated_slot_lets_go(self):
		"""
		An invalidated slot can never be decoded from again, so it must not
		keep holding the system undo -- PostgreSQL leaves such a slot out of
		its own xmin and LSN horizons for the same reason.
		"""
		# wal_keep_size keeps WAL on its own (testgres sets it), so the slot
		# would never fall out of what may be kept.
		self.node.append_conf(
		    'postgresql.conf', "wal_keep_size = 0\n"
		    "max_slot_wal_keep_size = 1MB\n")
		node = self.start_node()
		self.create_slot(node)
		self.assertIsNotNone(self.slot_retain(node),
		                     "a slot has to say what it retains")

		# Push the slot's restart_lsn out of the WAL it may keep, so that the
		# next checkpoint invalidates it.
		for i in range(4):
			node.safe_psql(
			    'postgres',
			    f"INSERT INTO o_decoded SELECT g, repeat('x', 100) "
			    f"FROM generate_series({i * 10000 + 10}, {i * 10000 + 9999}) g;"
			)
			node.safe_psql('postgres', "SELECT pg_switch_wal();")
		node.safe_psql('postgres', "CHECKPOINT;")
		node.poll_query_until(
		    f"SELECT wal_status = 'lost' FROM pg_replication_slots "
		    f"WHERE slot_name = '{SLOT}';",
		    sleep_time=0.5,
		    max_attempts=60)

		self.assertIsNone(self.slot_retain(node),
		                  "an invalidated slot still holds the system undo")

	def test_standby_decodes_with_its_own_slot(self):
		"""
		Logical decoding on a standby reads the undo the standby built while
		replaying, so the retain has to work there too -- the slot is local,
		and the records it reads are replayed ones.
		"""
		node = self.start_node()
		self.node.append_conf('postgresql.conf', "hot_standby_feedback = on\n")
		node.restart()

		with self.getReplica().start() as replica:
			self.catchup_orioledb(replica)
			replica.safe_psql(
			    'postgres', "SELECT pg_create_logical_replication_slot("
			    f"'{SLOT}', 'test_decoding');")

			node.safe_psql('postgres',
			               "INSERT INTO o_decoded VALUES (1, 'on-standby');")
			with node.connect(autocommit=True) as churn:
				self.churn(churn, 'standby', 40)
			self.catchup_orioledb(replica)

			self.assertIn(
			    "table public.o_decoded: INSERT: id[integer]:1 v[text]:'on-standby'",
			    self.decoded(replica))

	@unittest.skipIf(BaseTest.get_pg_version() < 17,
	                 "slot synchronisation needs PostgreSQL 17")
	def test_failover_slot_survives_promotion(self):
		"""
		A slot synchronised from the primary has decoded nothing on the
		standby, so nothing there can be held to a position the standby has
		reached.  After a promotion it starts decoding from where the primary
		left it, and the undo for that has to still be around.
		"""
		node = self.start_node()
		self.node.append_conf(
		    'postgresql.conf', "hot_standby_feedback = on\n"
		    "synchronized_standby_slots = ''\n")
		node.restart()
		node.safe_psql(
		    'postgres',
		    "SELECT pg_create_physical_replication_slot('standby_slot');")
		node.safe_psql(
		    'postgres', "SELECT pg_create_logical_replication_slot("
		    f"'{SLOT}', 'test_decoding', false, false, true);")

		with self.getReplica().start() as replica:
			# Slot synchronisation insists on a dbname it can connect to,
			# and the conninfo testgres writes has none.
			replica.append_conf(
			    'postgresql.conf',
			    f"primary_conninfo = 'port={node.port} user={node.os_ops.username} "
			    f"hostaddr=127.0.0.1 dbname=postgres'\n"
			    "primary_slot_name = 'standby_slot'\n"
			    "sync_replication_slots = on\n"
			    "hot_standby_feedback = on\n")
			replica.restart()

			# A slot is synchronised as temporary until it has a confirmed
			# position to be synchronised to, so give it one.
			self.decoded(node)
			# Ensure the standby has replayed up to the primary's
			# current WAL position so the slotsync worker can persist
			# the slot without waiting for catchup across multiple
			# sync cycles with exponential backoff.
			self.catchup_orioledb(replica)
			# Wake the slotsync worker from its exponential backoff
			# sleep so it retries immediately with WAL already caught up.
			replica.safe_psql('postgres', 'SELECT pg_reload_conf()')
			replica.poll_query_until(
			    f"SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
			    f"WHERE slot_name = '{SLOT}' AND synced AND NOT temporary);",
			    sleep_time=0.5,
			    max_attempts=240)

			# The transaction whose record the slot will decode after the
			# promotion, with catalog changes after it to undo.
			node.safe_psql(
			    'postgres',
			    "INSERT INTO o_decoded VALUES (1, 'before-failover');")
			with node.connect(autocommit=True) as churn:
				self.churn(churn, 'failover', 40)
			self.catchup_orioledb(replica)

			replica.promote()
			replica.poll_query_until("SELECT NOT pg_is_in_recovery();",
			                         sleep_time=0.5,
			                         max_attempts=120)

			self.assertIn(
			    "table public.o_decoded: INSERT: id[integer]:1 v[text]:'before-failover'",
			    self.decoded(replica))

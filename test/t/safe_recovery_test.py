#!/usr/bin/env python3
# coding: utf-8

# Tests for orioledb.safe_recovery: per-OXID buffering recovery that
# defers applying Oriole WAL records until COMMIT is observed.
#
# Reproducers:
#   - single backend: WAL flush from a backend that never commits.
#   - concurrent backends: two backends interleave containers in WAL,
#     one commits and one is killed before commit.
#
# After crash_with_os_buffer_loss(), we restart with safe_recovery=on
# and check that committed data is present, uncommitted data is absent,
# and per-record LOG output appears for the rejected OXID(s).

import os

from .base_test import BaseTest


def _read_log(node):
	with open(node.pg_log_file) as f:
		return f.read()


class SafeRecoveryTest(BaseTest):

	def _create_table(self, name='o_test'):
		self.node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE {name} (
				id int NOT NULL PRIMARY KEY,
				val text
			) USING orioledb;
			""")

	# A single uncommitted INSERT that overflows the 8 KB local WAL
	# buffer guarantees at least one ORIOLEDB_XLOG_CONTAINER is in WAL
	# without a COMMIT.  We then SIGKILL the postmaster.  Recovery with
	# safe_recovery=on must reject the OXID and log it.
	def test_single_backend(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.safe_recovery = on\n"
		    "log_min_messages = LOG\n")
		node.start()

		self._create_table()
		node.safe_psql(
		    'postgres', "INSERT INTO o_test "
		    "SELECT g, 'committed_' || g FROM generate_series(1, 50) g;")
		# Force an Oriole checkpoint so pre-crash data is durable on
		# disk and recovery only has to replay the uncommitted xact.
		node.safe_psql('postgres', "CHECKPOINT;")

		self.assertEqual(
		    node.execute(
		        "SELECT count(*) FROM o_test WHERE val LIKE 'committed_%';")[0][0],
		    50)

		con = node.connect()
		con.begin()
		# Each row payload of ~200 bytes; >=500 rows comfortably exceed
		# LOCAL_WAL_BUFFER_SIZE (8 KB), forcing flush_local_wal_if_needed
		# to push an ORIOLEDB_XLOG_CONTAINER to xlog mid-statement.
		con.execute(
		    "INSERT INTO o_test "
		    "SELECT g, repeat('x', 200) FROM generate_series(100000, 100500) g;"
		)
		# Force the Postgres WAL stream up to the in-flight backend's
		# container to be flushed to durable storage by committing a
		# subsequent record from a second connection.  Without this,
		# OS-buffer loss may erase the in-flight container too.
		# safe_psql commits on close; node.connect() does not.
		node.safe_psql('postgres',
		               "INSERT INTO o_test VALUES (999999, 'flusher');")

		# Do NOT commit con; leave it open so postmaster sees an
		# in-progress backend at crash time.
		self.crash_with_os_buffer_loss()

		node.start()

		# Pre-crash committed rows survive.
		self.assertEqual(
		    node.execute(
		        "SELECT count(*) FROM o_test WHERE val LIKE 'committed_%';")[0][0],
		    50)

		# Flusher's committed row survived.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test WHERE id = 999999;")[0][0],
		    1)

		# Aborted xact's rows must not be applied.
		self.assertEqual(
		    node.execute(
		        "SELECT count(*) FROM o_test WHERE id BETWEEN 100000 AND 100500;")
		    [0][0], 0)

		# Tree structure is consistent (no orphaned tuples / index/PK skew).
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_test'::regclass);")[0][0])

		log = _read_log(node)
		self.assertIn("orioledb recovery started in safe_recovery mode", log)
		self.assertIn("safe_recovery: rejecting transaction oxid=", log)
		self.assertIn("safe_recovery: rejecting oxid=", log)
		node.stop()

	# Two connections write into separate tables.  Their containers
	# interleave in WAL.  Conn A commits, Conn B is left in-flight when
	# the postmaster is killed.  Recovery with safe_recovery=on must
	# apply A's rows and reject B's.
	def test_concurrent_backends(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.safe_recovery = on\n"
		    "log_min_messages = LOG\n")
		node.start()

		self._create_table('o_test_a')
		self._create_table('o_test_b')
		node.safe_psql('postgres', "CHECKPOINT;")

		con_a = node.connect()
		con_b = node.connect()

		# Interleave inserts so both backends have flushed containers
		# in WAL before either decides commit/abort.  Each batch is
		# sized to overflow LOCAL_WAL_BUFFER_SIZE.
		con_a.begin()
		con_b.begin()
		con_a.execute(
		    "INSERT INTO o_test_a "
		    "SELECT g, repeat('a', 200) FROM generate_series(1, 100) g;")
		con_b.execute(
		    "INSERT INTO o_test_b "
		    "SELECT g, repeat('b', 200) FROM generate_series(1, 100) g;")
		con_a.execute(
		    "INSERT INTO o_test_a "
		    "SELECT g, repeat('a', 200) FROM generate_series(101, 200) g;")
		con_b.execute(
		    "INSERT INTO o_test_b "
		    "SELECT g, repeat('b', 200) FROM generate_series(101, 200) g;")

		# Conn A commits — its container plus the COMMIT record reach
		# WAL.  Conn B stays open with flushed containers and no
		# COMMIT.
		con_a.commit()

		self.crash_with_os_buffer_loss()
		node.start()

		# Conn A's xact is applied.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test_a;")[0][0], 200)

		# Conn B's xact is rejected.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test_b;")[0][0], 0)

		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_test_a'::regclass);")[0][0])
		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_test_b'::regclass);")[0][0])

		log = _read_log(node)
		# Exactly one OXID must be rejected.
		self.assertEqual(log.count("safe_recovery: rejecting transaction oxid="),
		                 1)
		node.stop()

	# Reproduce the crash loop the safe_recovery feature targets:
	#
	#   1. A backend writes Oriole container(s) into durable WAL then
	#      the postmaster is SIGKILLed with the OXID uncommitted (same
	#      crash signature as a real backend death mid-DML).
	#   2. Default (legacy) recovery replays those containers; the
	#      debug_modify_panic_reloid gate -- which fires only while
	#      is_recovery_in_progress() -- PANICs.  Recovery cannot
	#      complete; with restart_after_crash=off the postmaster
	#      shuts down.
	#   3. With orioledb.safe_recovery=on the buffered OXID is rejected
	#      (no COMMIT seen), o_btree_normal_modify is never re-invoked
	#      for it, the gate stays silent, and recovery completes.
	def test_crash_loop_safe_recovery_breaks_it(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "log_min_messages = LOG\n"
		    "restart_after_crash = off\n")
		node.start()

		self._create_table('o_bomb')
		# Separate orioledb table that the gate ignores; used as a
		# 'flusher' to make XLog durable past the doomed containers.
		self._create_table('o_flusher')
		# Push the redo LSN forward so recovery only replays the
		# post-CHECKPOINT WAL (the doomed inserts).
		node.safe_psql('postgres', "CHECKPOINT;")

		# The orioledb modify path keys on the primary index's
		# pg_class oid (desc->oids.reloid in BTreeDescr), not the
		# table's relid.  Fetch it from pg_index.
		bomb_reloid = node.execute(
		    "SELECT indexrelid FROM pg_index "
		    "WHERE indrelid = 'o_bomb'::regclass AND indisprimary;")[0][0]

		# Arm the recovery-only PANIC gate.  Persists across restarts.
		# ALTER SYSTEM cannot run inside a transaction block; safe_psql
		# wraps multi-statements, so issue the two calls separately.
		node.safe_psql(
		    'postgres',
		    f"ALTER SYSTEM SET orioledb.debug_modify_panic_reloid = "
		    f"{bomb_reloid};")
		node.safe_psql('postgres', "SELECT pg_reload_conf();")

		# Doomed transaction: many short inserts so the 8 KB
		# LOCAL_WAL_BUFFER overflows several times, pushing
		# containers into XLog.  The gate is recovery-only so these
		# run normally in the backend.
		bomb_con = node.connect()
		bomb_con.begin()
		bomb_con.execute(
		    "INSERT INTO o_bomb "
		    "SELECT g, 'doomed' FROM generate_series(1, 500) g;")

		# Make XLog durable up to (at least) the doomed transaction's
		# already-flushed containers by committing an unrelated write
		# on a different table (otherwise the flusher itself would
		# trip the gate during recovery).
		node.safe_psql('postgres',
		               "INSERT INTO o_flusher VALUES (1, 'flush');")

		# Now SIGKILL the postmaster with the bomb transaction still
		# open -- the crash signature the feature targets.
		self.crash_with_os_buffer_loss()

		# Attempt legacy recovery (safe_recovery still off).  The
		# is_recovery_in_progress() gate fires on the first replay of
		# an o_bomb modify, PANICs.  With restart_after_crash=off the
		# postmaster shuts down and node.start() fails.
		legacy_start_failed = False
		try:
			node.start()
		except Exception:
			legacy_start_failed = True
		else:
			if node.status() != 1:		# 1 == RUNNING in testgres
				legacy_start_failed = True
				node.is_started = False
		self.assertTrue(legacy_start_failed,
		                "legacy recovery should have PANICked on bomb reloid")
		log = _read_log(node)
		self.assertIn("recovery PANIC on modify of reloid", log)

		# Now switch on safe_recovery and try again.
		node.append_conf('postgresql.conf',
		                 "orioledb.safe_recovery = on\n")
		node.start()

		# The doomed inserts must not appear.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_bomb;")[0][0], 0)

		# Flusher's committed row survived.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_flusher;")[0][0], 1)

		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_bomb'::regclass);")[0][0])

		log = _read_log(node)
		self.assertIn("orioledb recovery started in safe_recovery mode", log)
		self.assertIn("safe_recovery: rejecting transaction oxid=", log)
		# After the safe-recovery start, the panic message must NOT
		# appear -- the rejected OXID's records were never re-applied
		# so o_btree_normal_modify's gate was never tripped.
		safe_section = log.rsplit(
		    "orioledb recovery started in safe_recovery mode", 1)[1]
		self.assertNotIn("recovery PANIC on modify of reloid", safe_section)

		# Disarm the bomb so tearDown stays clean.
		node.safe_psql(
		    'postgres',
		    "ALTER SYSTEM SET orioledb.debug_modify_panic_reloid = 0;")
		node.safe_psql('postgres', "SELECT pg_reload_conf();")
		node.stop()

	# Sanity: with safe_recovery=on but no crash, normal traffic
	# continues to work (every COMMIT flushes its buffer, nothing
	# accumulates).
	def test_clean_shutdown(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.safe_recovery = on\n")
		node.start()

		self._create_table()
		node.safe_psql(
		    'postgres', "INSERT INTO o_test "
		    "SELECT g, 'v' || g FROM generate_series(1, 1000) g;")
		node.safe_psql('postgres', "CHECKPOINT;")
		node.safe_psql('postgres',
		               "UPDATE o_test SET val = val || '_u' WHERE id % 7 = 0;")
		node.stop()

		node.start()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test;")[0][0], 1000)
		self.assertEqual(
		    node.execute(
		        "SELECT count(*) FROM o_test WHERE val LIKE '%_u';")[0][0],
		    142)  # ids 7,14,...,994 = 142 rows
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_test'::regclass);")[0][0])
		node.stop()

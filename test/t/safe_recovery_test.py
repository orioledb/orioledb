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

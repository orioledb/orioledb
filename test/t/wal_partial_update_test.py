#!/usr/bin/env python3
# coding: utf-8

import os
import signal
import threading
import time

from .base_test import BaseTest

SETUP = """
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE EXTENSION IF NOT EXISTS pg_walinspect;
	CREATE TABLE o_upd (
		id int PRIMARY KEY,
		a int,
		b text,
		c float8,
		d varchar(20),
		e int8,
		filler char(80) DEFAULT ''
	) USING orioledb;
	CREATE INDEX o_upd_a ON o_upd (a);
	INSERT INTO o_upd (id, a, b, c, d, e)
		SELECT i, i, repeat('x', i % 50), i / 3.0, 'v' || i, NULL
			FROM generate_series(1, 2000) i;
"""

UPDATES = """
	UPDATE o_upd SET a = a + 1 WHERE id % 2 = 0;
	UPDATE o_upd SET b = b || 'yy' WHERE id % 3 = 0;
	UPDATE o_upd SET e = id * 10 WHERE id % 5 = 0;
	UPDATE o_upd SET d = NULL WHERE id % 7 = 0;
	UPDATE o_upd SET c = c + 0.5, b = 'short' WHERE id % 11 = 0;
	UPDATE o_upd SET a = a WHERE id % 13 = 0;
	ALTER TABLE o_upd ADD COLUMN f int DEFAULT 42;
	UPDATE o_upd SET a = a + 100 WHERE id % 17 = 0;
	UPDATE o_upd SET f = 7 WHERE id % 19 = 0;
	UPDATE o_upd SET b = repeat('t', 5000) || id WHERE id % 97 = 0;
	UPDATE o_upd SET a = a + 1000 WHERE id % 97 = 0;
"""

CHECK = "SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM o_upd t;"


class WalPartialUpdateTest(BaseTest):
	"""
	Below wal_level = logical an UPDATE writes WAL_REC_UPDATE_PARTIAL: the
	primary key and the changed fields.  Replay rebuilds the row from the
	version it finds and has to end with the row the primary had.
	"""

	def wal_lsn(self, node):
		return node.execute("SELECT pg_current_wal_lsn();")[0][0]

	def count_records(self, node, start_lsn, name):
		"""Number of OrioleDB WAL records of the given kind since start_lsn."""
		descs = node.execute("""
			SELECT description FROM pg_get_wal_records_info('%s',
														  pg_current_wal_lsn())
				WHERE position('OrioleDB' in resource_manager) > 0;
		""" % start_lsn)
		return sum(d[0].count(' %s (' % name) for d in descs)

	def check_index(self, node):
		"""The index on a agrees with the table after recovery."""
		with node.connect() as con:
			seq = con.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SELECT id, a FROM o_upd WHERE a BETWEEN 1 AND 10000
					ORDER BY id;""")
			con.execute("SET enable_seqscan = off;")
			con.execute("SET enable_indexscan = on;")
			idx = con.execute("""
				SELECT id, a FROM o_upd WHERE a BETWEEN 1 AND 10000
					ORDER BY id;""")
		self.assertEqual(seq, idx)

	def test_partial_update_crash_recovery(self):
		node = self.node
		node.start()
		node.safe_psql(SETUP)
		node.safe_psql("CHECKPOINT;")
		start = self.wal_lsn(node)
		node.safe_psql(UPDATES)
		self.assertGreater(self.count_records(node, start, 'UPDATE_PARTIAL'),
		                   1000)
		# A row with a TOASTed value is written in full
		self.assertGreater(self.count_records(node, start, 'UPDATE'), 0)
		expected = node.execute(CHECK)[0][0]

		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute(CHECK)[0][0], expected)
		self.check_index(node)
		node.stop()

	def test_partial_update_not_written_for_logical(self):
		node = self.node
		node.append_conf('postgresql.conf', "wal_level = logical\n")
		node.start()
		node.safe_psql(SETUP)
		start = self.wal_lsn(node)
		node.safe_psql(UPDATES)
		self.assertEqual(self.count_records(node, start, 'UPDATE_PARTIAL'), 0)
		self.assertGreater(self.count_records(node, start, 'UPDATE'), 1000)
		node.stop()

	def test_partial_update_standby(self):
		node = self.node
		node.start()
		node.safe_psql(SETUP)
		with self.getReplica().start() as replica:
			node.safe_psql(UPDATES)
			expected = node.execute(CHECK)[0][0]
			self.catchup_orioledb(replica)
			self.assertEqual(replica.execute(CHECK)[0][0], expected)
		node.stop()

	def test_partial_update_single_process_recovery(self):
		"""
		After a backend crash the postmaster reinitializes and OrioleDB
		replays in a single process, without recovery workers.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "restart_after_crash = on\n"
		    "checkpoint_timeout = 1h\n")
		node.start()
		node.safe_psql(SETUP)
		node.safe_psql("CHECKPOINT;")
		node.safe_psql(UPDATES)
		expected = node.execute(CHECK)[0][0]

		since = os.path.getsize(node.pg_log_file)
		with node.connect() as victim:
			os.kill(victim.pid, signal.SIGKILL)
		self.wait_log(node, "database system is ready to accept connections",
		              since)
		with open(node.pg_log_file, errors='replace') as f:
			f.seek(since)
			self.assertIn("Unable to make multiprocess recovery", f.read())

		self.assertEqual(node.execute(CHECK)[0][0], expected)
		self.check_index(node)
		node.stop()

	def test_partial_update_during_checkpoints(self):
		"""
		A checkpoint image can hold a newer version of a row than the one a
		replayed record was made against.  The record sets its fields, later
		records set theirs, and the result is still the primary's row.
		"""
		node = self.node
		node.start()
		node.safe_psql(SETUP)
		stop = threading.Event()
		errors = []

		def updater(seed):
			try:
				with node.connect(autocommit=True) as con:
					i = 0
					while not stop.is_set():
						i += 1
						con.execute(
						    "UPDATE o_upd SET a = a + 1, c = c + %d, "
						    "e = coalesce(e, 0) + 1 WHERE mod(id, 50) = %d;" %
						    (i, (seed + i) % 50))
						con.execute(
						    "UPDATE o_upd SET b = left(b || '%d', 60), "
						    "d = CASE WHEN d IS NULL THEN 'n' ELSE NULL END "
						    "WHERE mod(id, 37) = %d;" % (i,
						                                 (seed * 7 + i) % 37))
			except Exception as e:
				errors.append(e)

		threads = [
		    threading.Thread(target=updater, args=(seed, ))
		    for seed in range(4)
		]
		for t in threads:
			t.start()
		try:
			for _ in range(3):
				time.sleep(0.5)
				node.safe_psql("CHECKPOINT;")
			time.sleep(0.5)
		finally:
			stop.set()
			for t in threads:
				t.join(timeout=60)
		self.assertEqual(errors, [])

		expected = node.execute(CHECK)[0][0]
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute(CHECK)[0][0], expected)
		self.check_index(node)
		node.stop()

	def wait_log(self, node, text, since, timeout=120):
		deadline = time.time() + timeout
		while time.time() < deadline:
			with open(node.pg_log_file, errors='replace') as f:
				f.seek(since)
				if text in f.read():
					return
			time.sleep(0.1)
		self.fail("%r never appeared in the log" % text)

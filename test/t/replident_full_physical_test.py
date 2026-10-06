#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class ReplidentFullPhysicalTest(BaseTest):
	"""
	REPLICA IDENTITY FULL adds old-tuple data to UPDATE and DELETE WAL
	records for logical decoding.  At wal_level < logical the extra data
	is never consumed, yet it was always written.  These tests verify the
	behavior and then the fix that skips the redundant data.
	"""

	def wal_lsn(self, node):
		return node.execute("SELECT pg_current_wal_lsn();")[0][0]

	def count_wal_descriptions(self, node, start_lsn, pattern):
		"""Count OrioleDB WAL records whose description contains pattern."""
		descs = node.execute("""
			SELECT description FROM pg_get_wal_records_info('%s',
			                                               pg_current_wal_lsn())
			    WHERE position('OrioleDB' in resource_manager) > 0;
		""" % start_lsn)
		return sum(1 for d in descs if pattern in d[0])

	def wal_total_len(self, node, start_lsn):
		"""Sum of total_record_length for OrioleDB WAL records."""
		rows = node.execute("""
			SELECT coalesce(sum(record_length), 0)
			    FROM pg_get_wal_records_info('%s', pg_current_wal_lsn())
			    WHERE position('OrioleDB' in resource_manager) > 0;
		""" % start_lsn)
		return int(rows[0][0])

	def test_replident_full_no_extra_wal_at_replica_level(self):
		"""
		At wal_level = replica, REPLICA IDENTITY FULL should not cause
		extra WAL data in UPDATE and DELETE records.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS pg_walinspect;
			CREATE TABLE o_ri (
				id int PRIMARY KEY,
				val text,
				filler char(200) DEFAULT ''
			) USING orioledb;
			INSERT INTO o_ri (id, val)
				SELECT i, repeat('x', 100) FROM generate_series(1, 500) i;
		""")

		node.safe_psql("CHECKPOINT;")
		lsn_default = self.wal_lsn(node)
		node.safe_psql("""
			UPDATE o_ri SET val = repeat('y', 100) WHERE id <= 250;
			DELETE FROM o_ri WHERE id > 400;
		""")
		bytes_default = self.wal_total_len(node, lsn_default)

		node.safe_psql("""
			ALTER TABLE o_ri REPLICA IDENTITY FULL;
			CHECKPOINT;
		""")
		lsn_full = self.wal_lsn(node)
		node.safe_psql("""
			UPDATE o_ri SET val = repeat('z', 100) WHERE id <= 250;
			DELETE FROM o_ri WHERE id BETWEEN 350 AND 400;
		""")
		bytes_full = self.wal_total_len(node, lsn_full)

		self.assertLessEqual(
		    bytes_full, bytes_default * 1.1,
		    "REPLICA IDENTITY FULL should not bloat WAL at wal_level=replica")
		node.stop()

	def test_replident_full_crash_recovery(self):
		"""
		Crash recovery works correctly for REPLICA IDENTITY FULL tables
		at wal_level = replica.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_ri_cr (
				id int PRIMARY KEY,
				a int,
				b text,
				c float8,
				filler char(80) DEFAULT ''
			) USING orioledb;
			ALTER TABLE o_ri_cr REPLICA IDENTITY FULL;
			INSERT INTO o_ri_cr (id, a, b, c)
				SELECT i, i, repeat('x', i % 50), i / 3.0
				FROM generate_series(1, 1000) i;
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("""
			UPDATE o_ri_cr SET a = a + 1 WHERE id % 2 = 0;
			UPDATE o_ri_cr SET b = b || 'yy' WHERE id % 3 = 0;
			DELETE FROM o_ri_cr WHERE id % 7 = 0;
			UPDATE o_ri_cr SET c = c + 0.5 WHERE id % 5 = 0;
		""")

		check = "SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM o_ri_cr t;"
		expected = node.execute(check)[0][0]

		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute(check)[0][0], expected)
		node.stop()

	def test_replident_full_standby(self):
		"""
		Streaming replication works with REPLICA IDENTITY FULL tables
		at wal_level = replica.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_ri_sb (
				id int PRIMARY KEY,
				val text,
				filler char(80) DEFAULT ''
			) USING orioledb;
			ALTER TABLE o_ri_sb REPLICA IDENTITY FULL;
			INSERT INTO o_ri_sb (id, val)
				SELECT i, repeat('x', 50) FROM generate_series(1, 500) i;
		""")
		with self.getReplica().start() as replica:
			node.safe_psql("""
				UPDATE o_ri_sb SET val = repeat('y', 50) WHERE id <= 250;
				DELETE FROM o_ri_sb WHERE id > 400;
			""")
			check = "SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM o_ri_sb t;"
			expected = node.execute(check)[0][0]
			self.catchup_orioledb(replica)
			self.assertEqual(replica.execute(check)[0][0], expected)
		node.stop()

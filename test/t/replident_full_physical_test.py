#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

SETUP = """
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE EXTENSION IF NOT EXISTS pg_walinspect;
	CREATE TABLE o_ri_default (
		id int PRIMARY KEY,
		val text,
		filler char(200) DEFAULT ''
	) USING orioledb;
	CREATE TABLE o_ri_full (LIKE o_ri_default INCLUDING ALL) USING orioledb;
	ALTER TABLE o_ri_full REPLICA IDENTITY FULL;
	INSERT INTO o_ri_default (id, val)
		SELECT i, repeat('x', 100) FROM generate_series(1, 500) i;
	INSERT INTO o_ri_full SELECT * FROM o_ri_default;
	CHECKPOINT;
"""

# Each operation in turn, the same on both tables.  Changing the primary key
# makes WAL_REC_REINSERT.
OPERATIONS = [
    "UPDATE %s SET val = repeat('y', 100) WHERE id <= 250;",
    "UPDATE %s SET id = id + 1000 WHERE id BETWEEN 251 AND 300;",
    "DELETE FROM %s WHERE id > 400 AND id < 1000;",
]


class ReplidentFullPhysicalTest(BaseTest):
	"""
	REPLICA IDENTITY FULL makes UPDATE, DELETE and REINSERT WAL records carry
	the old tuple, and adds a record naming the identity.  Only logical
	decoding reads them, so below wal_level = logical a FULL table must write
	the same WAL as a DEFAULT one.
	"""

	def wal_lsn(self, node):
		return node.execute("SELECT pg_current_wal_lsn();")[0][0]

	def orioledb_wal(self, node, start_lsn):
		"""
		Total length of the OrioleDB WAL records since start_lsn, and how many
		of the records they contain are RELREPLIDENT.
		"""
		rows = node.execute("""
			SELECT record_length, description
				FROM pg_get_wal_records_info('%s', pg_current_wal_lsn())
				WHERE position('OrioleDB' in resource_manager) > 0;
		""" % start_lsn)
		return (sum(r[0] for r in rows),
		        sum(r[1].count('RELREPLIDENT') for r in rows))

	def operation_wal(self, node, operation, table):
		start = self.wal_lsn(node)
		node.safe_psql(operation % table)
		return self.orioledb_wal(node, start)

	def both_tables_wal(self, node, operation):
		"""WAL of the operation on the DEFAULT table, then on the FULL one."""
		return (self.operation_wal(node, operation, 'o_ri_default'),
		        self.operation_wal(node, operation, 'o_ri_full'))

	def test_replident_full_no_extra_wal_at_replica_level(self):
		node = self.node
		node.start()
		node.safe_psql(SETUP)

		for operation in OPERATIONS:
			((default_len, default_ident),
			 (full_len, full_ident)) = self.both_tables_wal(node, operation)
			self.assertEqual(default_ident, 0, operation)
			self.assertEqual(full_ident, 0, operation)
			self.assertEqual(full_len, default_len, operation)
		node.stop()

	def test_replident_full_extra_wal_at_logical_level(self):
		"""
		At wal_level = logical the same operations still carry the old tuple
		and the identity, which also shows the check above can tell them
		apart.
		"""
		node = self.node
		node.append_conf(wal_level='logical')
		node.start()
		node.safe_psql(SETUP)

		for operation in OPERATIONS:
			((default_len, default_ident),
			 (full_len, full_ident)) = self.both_tables_wal(node, operation)
			self.assertEqual(default_ident, 0, operation)
			self.assertGreater(full_ident, 0, operation)
			self.assertGreater(full_len, default_len, operation)
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
			UPDATE o_ri_cr SET id = id + 10000 WHERE id % 11 = 0;
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
				UPDATE o_ri_sb SET id = id + 1000 WHERE id BETWEEN 251 AND 300;
				DELETE FROM o_ri_sb WHERE id > 400 AND id < 1000;
			""")
			check = "SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM o_ri_sb t;"
			expected = node.execute(check)[0][0]
			self.catchup_orioledb(replica)
			self.assertEqual(replica.execute(check)[0][0], expected)
		node.stop()

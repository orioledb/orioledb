#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class ReindexConcurrentTest(BaseTest):
	"""
	Coverage for REINDEX INDEX/TABLE CONCURRENTLY on orioledb tables.

	PG's standard CIC machinery drives the rebuild through our existing
	ambuild / validate_scan hooks (same plumbing as CREATE INDEX
	CONCURRENTLY).  The orioledb-specific twists are:

	  - REINDEX INDEX CONCURRENTLY on a primary index errors upfront
	    because PK == table in orioledb and the rebuild requires a
	    full table rewrite under AccessExclusiveLock.
	  - REINDEX TABLE CONCURRENTLY skips the primary index via the
	    ReindexConcurrentlySkipHook PG patch, NOT'icing the skip and
	    rebuilding only secondary indexes.
	  - The OIndex sys-tree entry for the dropped "_ccold" index is
	    cleaned up via an oid-based fallback lookup in the DROP hook.
	"""

	def test_reindex_index_concurrently_secondary(self):
		"""REINDEX INDEX CONCURRENTLY rebuilds a non-unique secondary."""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE t (
					id int NOT NULL PRIMARY KEY,
					val text NOT NULL
				) USING orioledb;
				INSERT INTO t
				SELECT g, 'v' || g FROM generate_series(1, 200) g;
				CREATE INDEX t_val_idx ON t (val);
			""")
			node.safe_psql("REINDEX INDEX CONCURRENTLY t_val_idx;")
			with node.connect() as c:
				c.execute("SET enable_seqscan = off")
				cnt = c.execute(
				    "SELECT count(*) FROM t WHERE val >= 'v' AND val < 'w';"
				)[0][0]
			self.assertEqual(cnt, 200)
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_reindex_index_concurrently_unique(self):
		"""REINDEX INDEX CONCURRENTLY rebuilds a UNIQUE secondary."""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE t (
					id int NOT NULL PRIMARY KEY,
					val text NOT NULL
				) USING orioledb;
				INSERT INTO t
				SELECT g, 'v' || g FROM generate_series(1, 200) g;
				CREATE UNIQUE INDEX t_val_uidx ON t (val);
			""")
			node.safe_psql("REINDEX INDEX CONCURRENTLY t_val_uidx;")
			# Uniqueness still enforced after the concurrent rebuild.
			with self.assertRaises(Exception):
				node.safe_psql("INSERT INTO t VALUES (201, 'v1');")
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_reindex_index_concurrently_pk_rejected(self):
		"""
		REINDEX INDEX CONCURRENTLY on a primary index must error: the
		primary IS the table in orioledb and a concurrent rebuild
		isn't possible.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE t (
					id int NOT NULL PRIMARY KEY,
					val text NOT NULL
				) USING orioledb;
				INSERT INTO t VALUES (1, 'v1');
			""")
			_, _, err = node.psql("REINDEX INDEX CONCURRENTLY t_pkey;")
			self.assertIn(
			    b"cannot REINDEX CONCURRENTLY primary index",
			    err,
			)
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_reindex_table_concurrently_skips_pk(self):
		"""
		REINDEX TABLE CONCURRENTLY skips the PK with a NOTICE and
		rebuilds every other index.  Verify by checking that the PK's
		orioledb relnode is unchanged and the secondary indexes'
		relnodes are.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE t (
					id int NOT NULL PRIMARY KEY,
					val text NOT NULL
				) USING orioledb;
				INSERT INTO t
				SELECT g, 'v' || g FROM generate_series(1, 50) g;
				CREATE INDEX t_val_idx ON t (val);
				CREATE UNIQUE INDEX t_val_uidx ON t (val);
			""")

			# Snapshot orioledb relnodes pre-REINDEX.
			pre = node.execute(
			    "SELECT index_type, index_relnode "
			    "FROM orioledb_index_oids() "
			    "WHERE table_reloid = 't'::regclass "
			    "  AND index_type IN ('primary', 'regular', 'unique') "
			    "ORDER BY index_type;")
			pre_by_type = {row[0]: row[1] for row in pre}

			con = node.connect(autocommit=True)
			con.execute("REINDEX TABLE CONCURRENTLY t;")
			con.close()
			notices = [(m[b'S'].decode('utf-8') + ":  " +
			            m[b'M'].decode('utf-8')) if isinstance(m, dict) else m
			           for m in con.connection.notices]
			joined = " | ".join(notices)
			self.assertIn("skipping reindex of index", joined)
			self.assertIn("t_pkey", joined)

			post = node.execute(
			    "SELECT index_type, index_relnode "
			    "FROM orioledb_index_oids() "
			    "WHERE table_reloid = 't'::regclass "
			    "  AND index_type IN ('primary', 'regular', 'unique') "
			    "ORDER BY index_type;")
			post_by_type = {row[0]: row[1] for row in post}

			# PK relnode is unchanged (skipped).
			self.assertEqual(pre_by_type['primary'], post_by_type['primary'])
			# Secondary relnodes changed (rebuilt).
			self.assertNotEqual(pre_by_type['regular'],
			                    post_by_type['regular'])
			self.assertNotEqual(pre_by_type['unique'], post_by_type['unique'])

			# Sys-tree has no orphan entries; we still have exactly
			# (primary + regular + unique + toast) = 4.
			n = node.execute("SELECT count(*) FROM orioledb_index_oids() "
			                 "WHERE table_reloid = 't'::regclass;")[0][0]
			self.assertEqual(n, 4)

			# Data still queryable.
			cnt = node.execute("SELECT count(*) FROM t;")[0][0]
			self.assertEqual(cnt, 50)
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_reindex_concurrently_no_orphan(self):
		"""
		After REINDEX INDEX CONCURRENTLY, the OIndex of the dropped
		"_ccold" index must NOT remain in the sys-tree.  Repeat a few
		times to make sure the leak doesn't accumulate.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE t (
					id int NOT NULL PRIMARY KEY,
					val text NOT NULL
				) USING orioledb;
				INSERT INTO t
				SELECT g, 'v' || g FROM generate_series(1, 10) g;
				CREATE INDEX t_val_idx ON t (val);
			""")

			def n_entries():
				return node.execute(
				    "SELECT count(*) FROM orioledb_index_oids() "
				    "WHERE table_reloid = 't'::regclass;")[0][0]

			baseline = n_entries()  # 3: primary + regular + toast
			self.assertEqual(baseline, 3)
			for _ in range(3):
				node.safe_psql("REINDEX INDEX CONCURRENTLY t_val_idx;")
				self.assertEqual(n_entries(), baseline)
		finally:
			try:
				node.stop()
			except Exception:
				pass

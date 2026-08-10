#!/usr/bin/env python3
# coding: utf-8

import re
import unittest

from .base_test import BaseTest


class IndexBridgingTest(BaseTest):

	@unittest.skipIf(not BaseTest.extension_installed("pageinspect"),
	                 "'pageinspect' is not installed")
	def test_ctid_overflow(self):
		node = self.node
		node.append_conf("orioledb.debug_max_bridge_ctid_blkno=1")
		node.start()

		def check(expected_ctids):
			self.assertEqual(
			    node.execute("""
					SELECT ctid FROM  generate_series(1,
													(SELECT relpages - 1 FROM pg_class
														WHERE oid = 'o_test_ix1'::regclass)) p,
						LATERAL bt_page_items('o_test_ix1', p)
						WHERE htid IS NOT NULL
						ORDER BY ctid;
				"""), expected_ctids)

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE EXTENSION pageinspect;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int
			) USING orioledb;

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (orioledb_index=off);
			CREATE INDEX o_test_ix2 on o_test using btree (j);
		""")

		nrows = 291  # MaxHeapTuplesPerPage
		node.safe_psql("""
			INSERT INTO o_test SELECT v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % nrows)

		expected_ctids = [(f'(0,{x})', ) for x in range(1, nrows + 1)]
		check(expected_ctids)

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 4) = 0;
		""")
		check(expected_ctids)

		_, _, err = node.psql("""
			VACUUM VERBOSE;
		""")
		vacuumed = err.decode("utf-8").split("INFO:  vacuuming")
		bridged = next(
		    filter(
		        lambda x: x.split('\n')[0] ==
		        ' bridged indexes "postgres.public.o_test"', vacuumed))
		dead = re.search(r"had (\d+) dead", bridged)[1]

		orig_len = len(expected_ctids)
		del expected_ctids[3::4]  # removed every 4th
		check(expected_ctids)
		self.assertTrue(dead, orig_len - len(expected_ctids))

		self.assertEqual(
		    len(expected_ctids),
		    node.execute("""
							SELECT reltuples FROM pg_class WHERE oid = 'o_test_ix1'::regclass
						 """)[0][0])

		nrows = 10
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 100, v * 200 FROM generate_series(1, %d) v;
		""" % nrows)
		expected_ctids.extend([(f'(0,{x*4})', ) for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

	@unittest.skipIf(not BaseTest.extension_installed("pageinspect"),
	                 "'pageinspect' is not installed")
	def test_ctid_overflow_two_times(self):
		node = self.node
		node.append_conf("orioledb.debug_max_bridge_ctid_blkno=1")
		node.start()

		def check(expected_ctids):
			self.assertEqual(
			    node.execute("""
					SELECT ctid FROM  generate_series(1,
													(SELECT relpages - 1 FROM pg_class
														WHERE oid = 'o_test_ix1'::regclass)) p,
						LATERAL bt_page_items('o_test_ix1', p)
						WHERE htid IS NOT NULL
						ORDER BY ctid;
				"""), expected_ctids)

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE EXTENSION pageinspect;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int,
				k int
			) USING orioledb;

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (orioledb_index=off);
			CREATE INDEX o_test_ix2 on o_test using btree (k);
		""")

		all_rows = 0
		nrows = 291  # MaxHeapTuplesPerPage
		node.safe_psql("""
			INSERT INTO o_test SELECT v, 10000 + v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % nrows)
		all_rows += nrows

		expected_ctids = [(f'(0,{x})', ) for x in range(1, nrows + 1)]
		check(expected_ctids)

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 4) = 0;
		""")
		check(expected_ctids)

		node.safe_psql("""
			VACUUM;
		""")

		del expected_ctids[3::4]  # removed every 4th
		check(expected_ctids)

		self.assertEqual(
		    len(expected_ctids),
		    node.execute("""
							SELECT reltuples FROM pg_class WHERE oid = 'o_test_ix1'::regclass
						 """)[0][0])

		nrows = 10
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 4, %d + v, v FROM generate_series(1, %d) v;
		""" % (10000 + all_rows, nrows))
		all_rows += nrows
		expected_ctids.extend([(f'(0,{x*4})', ) for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

		nrows = 291 - len(expected_ctids)
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 4, %d + v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % (10000 + all_rows, nrows))
		all_rows += nrows
		expected_ctids.extend([(f'(0,{(x+10)*4})', )
		                       for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 8) = 0;
		""")
		check(expected_ctids)

		node.safe_psql("""
			VACUUM;
		""")

		del expected_ctids[7::8]  # removed every 8th
		check(expected_ctids)

		nrows = 10
		node.safe_psql("""
			INSERT INTO o_test SELECT %d + v, %d + v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % (all_rows, 10000 + all_rows, nrows))
		all_rows += nrows
		expected_ctids.extend([(f'(0,{x*8})', ) for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

	def test_bridge_gin_dead_tids_on_earlier_page(self):
		"""When the bridge_ctid counter crosses
		MaxHeapTuplesPerPage (291), bridge index holds dead TIDs on block 0 and the
		live TID on block 1.  Bridge bitmap scan must advance past the
		all-dead page rather than terminating after it."""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id  int NOT NULL,
				arr bigint[],
				PRIMARY KEY (id)
			) USING orioledb;
			ALTER TABLE o_test SET (autovacuum_enabled = off);
			CREATE INDEX ON o_test USING GIN (arr);
			INSERT INTO o_test VALUES (1, ARRAY[1]::bigint[]);
			DO $$
			BEGIN
				FOR k IN 2..294 LOOP
					UPDATE o_test SET arr = ARRAY[k]::bigint[] WHERE id = 1;
				END LOOP;
				UPDATE o_test SET arr = ARRAY[1]::bigint[] WHERE id = 1;
			END $$;
		""")
		self.assertEqual(
		    node.execute("""
				SET enable_seqscan = off;
				SET enable_indexscan = off;
				SELECT count(*) FROM o_test WHERE arr @> ARRAY[1]::bigint[];
			""")[0][0], 1)

	def test_bridge_recovery(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION orioledb;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int,
				k int
			) USING orioledb;

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (orioledb_index=off);
			CREATE INDEX o_test_ix2 on o_test using btree (k);
		""")

		node.safe_psql("""
			INSERT INTO o_test SELECT v, 10000 + v, v FROM generate_series(1, 2000) v;
			ANALYZE o_test;
		""")

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 2) = 0;
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("""
			VACUUM;
		""")

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY j;
		""")[0][0][0]["Plan"]
		self.assertEqual('Index Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		tuples = node.execute("SELECT * FROM o_test ORDER BY j;")

		node.stop(['-m', 'immediate'])
		node.start()

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY j;
		""")[0][0][0]["Plan"]
		self.assertEqual('Index Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		self.assertEqual(
		    tuples,
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test ORDER BY j;
						 """))

	def _idx_exists(self, node, name):
		"""True if a relation named `name` exists in the catalogs."""
		return node.execute("SELECT to_regclass('%s');" % name)[0][0] is not None

	def _used_index(self, node, query):
		plan = node.execute("""
			SET enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON) %s;
		""" % query)[0][0][0]["Plan"]
		if plan["Node Type"] in ("Index Scan", "Index Only Scan",
		                         "Bitmap Index Scan"):
			return plan["Index Name"]
		return None

	def test_drop_bridge_index_then_readd(self):
		"""
		Drop a bridged (GiST) index and re-create it on a table that
		already had data.  The second add_bridge_index() rewrites the
		heap again; verify the new bridge index resolves TIDs from the
		newly-rewritten heap, not the stale pre-drop relnode.  Also
		check the old pre-drop relnode file is reclaimed after the drop.
		"""
		import os
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		self.assertEqual(
		    'o_test_gist', self._used_index(node, """
				SELECT id FROM o_test
					WHERE p <@ '((0,0),(100,100))'::box;
			"""))

		datoid = node.execute(
		    "SELECT oid FROM pg_database "
		    "WHERE datname = current_database();")[0][0]
		relnode_after_first = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]

		node.safe_psql("DROP INDEX o_test_gist;")
		self.assertFalse(self._idx_exists(node, 'o_test_gist'))

		# the heap was rewritten when the bridge was added; dropping the
		# bridge index does NOT revert the heap, so the relnode stays
		self.assertEqual(
		    relnode_after_first,
		    node.execute(
		        "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")
		    [0][0])

		# insert after the drop to add rows the re-created bridge must see
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 200 + id, point(200 + id, 200 + id)
				FROM generate_series(1, 50) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test
				WHERE p <@ '((0,0),(250,250))'::box;
		""")
		self.assertEqual(150, result[0][0])

		# the re-add rewrote the heap again; relnode changed
		relnode_after_second = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0]
		[0]
		self.assertNotEqual(relnode_after_first, relnode_after_second)

		# old (pre-second-add) relnode file must be reclaimed
		old_path = os.path.join(node.data_dir, "orioledb_data",
		                        str(datoid), str(relnode_after_first))
		self.assertFalse(os.path.exists(old_path),
		                 f"stale relnode {old_path} should be reclaimed")

		node.stop()

	def test_drop_bridge_index_crash(self):
		"""
		Crash the node right after DROP INDEX of a bridged index and
		verify recovery leaves the table usable via the primary key and
		the dropped bridge index is gone (no dangling sys-tree entry).
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			CHECKPOINT;
		""")
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))

		node.safe_psql("DROP INDEX o_test_gist;")
		self.assertFalse(self._idx_exists(node, 'o_test_gist'))

		node.stop(['-m', 'immediate'])
		node.start()

		# PK scan still works after recovery
		self.assertEqual(100, node.execute("SELECT count(*) FROM o_test;")[0][0])
		self.assertEqual(
		    42, node.execute("SELECT id FROM o_test WHERE id = 42;")[0][0])
		# bridge index stays dropped after recovery
		self.assertFalse(self._idx_exists(node, 'o_test_gist'))
		node.stop()

	def test_drop_pk_with_bridge_index(self):
		"""
		DROP the primary key of a table that also has a bridged (GiST)
		index.  Dropping the PK triggers a heap rewrite (rebuild_indices)
		while the bridge index stays attached; the bridge must be
		re-pointed to the new heap relnode.  Verify the GiST scan returns
		the right rows and the old pre-drop-PK relnode is reclaimed.
		"""
		import os
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		datoid = node.execute(
		    "SELECT oid FROM pg_database "
		    "WHERE datname = current_database();")[0][0]
		relnode_before = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]

		node.safe_psql("ALTER TABLE o_test DROP CONSTRAINT o_test_pkey;")

		relnode_after = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]
		self.assertNotEqual(relnode_before, relnode_after)

		# bridge index still usable via the rewritten heap
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test
				WHERE p <@ '((0,0),(100,100))'::box;
		""")
		self.assertEqual(100, result[0][0])

		# the bridge index is still registered and usable
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))

		# old relnode reclaimed
		old_path = os.path.join(node.data_dir, "orioledb_data",
		                        str(datoid), str(relnode_before))
		self.assertFalse(os.path.exists(old_path))
		node.stop()

	def test_drop_pk_with_bridge_index_crash(self):
		"""
		Same as test_drop_pk_with_bridge_index but crash the node right
		after DROP CONSTRAINT o_test_pkey, before the rewrite's
		meta-unlock is necessarily flushed.  Recovery must finish the
		rewrite and keep the bridge index usable.  This is the crash
		variant of the PK-drop-with-bridge scenario; cf.
		indices_build_test.test_drop_primary_recovery for the non-bridge
		case.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			CHECKPOINT;
		""")

		node.safe_psql("ALTER TABLE o_test DROP CONSTRAINT o_test_pkey;")
		node.stop(['-m', 'immediate'])
		node.start()

		# all rows survive the crash-during-rewrite
		self.assertEqual(100, node.execute("SELECT count(*) FROM o_test;")[0][0])
		# bridge index usable via the recovered heap
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test
				WHERE p <@ '((0,0),(100,100))'::box;
		""")
		self.assertEqual(100, result[0][0])
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		# no PK constraint after the drop (recovery must not resurrect it)
		self.assertEqual(
		    0,
		    node.execute("""
				SELECT count(*) FROM pg_constraint
					WHERE conrelid = 'o_test'::regclass AND contype = 'p';
			""")[0][0])
		node.stop()

	def test_add_pk_after_bridge_index(self):
		"""
		A table starts with no PK and a bridged (GiST) index.  Adding a
		PK later triggers a heap rewrite (rebuild_indices) that must
		re-point the existing bridge index to the new heap.  Verify the
		GiST scan still returns the right rows and the old (no-PK)
		relnode is reclaimed.
		"""
		import os
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		datoid = node.execute(
		    "SELECT oid FROM pg_database "
		    "WHERE datname = current_database();")[0][0]
		relnode_before = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]

		node.safe_psql("ALTER TABLE o_test ADD PRIMARY KEY (id);")
		relnode_after = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]
		self.assertNotEqual(relnode_before, relnode_after)

		# bridge index usable via the rewritten heap
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test
				WHERE p <@ '((0,0),(100,100))'::box;
		""")
		self.assertEqual(100, result[0][0])
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		# old relnode reclaimed
		old_path = os.path.join(node.data_dir, "orioledb_data",
		                        str(datoid), str(relnode_before))
		self.assertFalse(os.path.exists(old_path))
		node.stop()

	def test_add_pk_with_bridge_index_crash(self):
		"""
		Crash the node right after ADD PRIMARY KEY on a table that
		already has a bridged GiST index.  Recovery must finish the
		heap rewrite and keep the bridge index usable.  Crash variant
		of test_add_pk_after_bridge_index.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			CHECKPOINT;
		""")
		node.safe_psql("ALTER TABLE o_test ADD PRIMARY KEY (id);")
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(100, node.execute("SELECT count(*) FROM o_test;")[0][0])
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test
				WHERE p <@ '((0,0),(100,100))'::box;
		""")
		self.assertEqual(100, result[0][0])
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		# PK is present after recovery
		self.assertEqual(
		    1,
		    node.execute("""
				SELECT count(*) FROM pg_constraint
					WHERE conrelid = 'o_test'::regclass AND contype = 'p';
			""")[0][0])
		node.stop()

	def test_two_bridge_indices_drop_one(self):
		"""
		A table with two bridged indices (GiST + GIN).  Drop one and
		verify the other stays usable and points at the heap relnode
		resulting from the (unavoidable) rewrite, and the dropped index
		file is gone.  Exercises the multi-bridge bookkeeping when only
		some bridges are removed.
		"""
		import os
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point,
				arr int[]
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id), ARRAY[1, id]
				FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			CREATE INDEX o_test_gin ON o_test USING gin (arr);
		""")
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		self.assertTrue(self._idx_exists(node, 'o_test_gin'))

		gist_relnode = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test_gist';"
		)[0][0]
		gin_relnode = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test_gin';"
		)[0][0]

		node.safe_psql("DROP INDEX o_test_gist;")

		# GIN still usable
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test WHERE arr @> ARRAY[1]::int[];
		""")
		self.assertEqual(100, result[0][0])
		self.assertFalse(self._idx_exists(node, 'o_test_gist'))
		self.assertTrue(self._idx_exists(node, 'o_test_gin'))

		# the dropped GiST index file is gone from pg_class
		self.assertIsNone(
		    node.execute("SELECT to_regclass('o_test_gist');")[0][0])
		# the surviving GIN index file still exists
		self.assertEqual(
		    gin_relnode,
		    node.execute(
		        "SELECT relfilenode FROM pg_class WHERE relname = 'o_test_gin';")
		    [0][0])
		node.stop()

	def test_readd_pk_after_drop_with_bridge(self):
		"""
		DROP the PK of a bridge-indexed table, then re-ADD it.  Each
		operation rewrites the heap; the bridge index must follow the
		heap through both rewrites.  Verify the bridge resolves TIDs
		from the final heap and the relnode changes twice.  This is the
		sequence test_replication_rebuild_pk_after_checkpoint mirrors for
		the native-PK case, here for a bridged table without replication.
		"""
		import os
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		r0 = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]

		node.safe_psql("ALTER TABLE o_test DROP CONSTRAINT o_test_pkey;")
		r1 = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]
		self.assertNotEqual(r0, r1)

		node.safe_psql("ALTER TABLE o_test ADD PRIMARY KEY (id);")
		r2 = node.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test';")[0][0]
		self.assertNotEqual(r1, r2)

		# bridge index usable via the doubly-rewritten heap
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test
				WHERE p <@ '((0,0),(100,100))'::box;
		""")
		self.assertEqual(100, result[0][0])
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))

		# both intermediate relnodes reclaimed
		datoid = node.execute(
		    "SELECT oid FROM pg_database "
		    "WHERE datname = current_database();")[0][0]
		for old in (r0, r1):
			old_path = os.path.join(node.data_dir, "orioledb_data",
			                       str(datoid), str(old))
			self.assertFalse(os.path.exists(old_path),
			                 f"stale relnode {old_path} should be reclaimed")
		node.stop()

	def test_drop_then_add_different_bridge_am(self):
		"""
		Drop a GiST bridge index and add a GIN bridge index instead, on
		the same table with data.  The drop frees the GiST bridge; the
		add rewrites the heap for the new GIN bridge.  Verify only the
		GIN bridge remains and resolves the data, and the GiST bridge
		file is gone.
		"""
		import os
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int primary key,
				p point,
				arr int[]
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id), ARRAY[1, id]
				FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))

		node.safe_psql("DROP INDEX o_test_gist;")
		node.safe_psql("CREATE INDEX o_test_gin ON o_test USING gin (arr);")
		self.assertFalse(self._idx_exists(node, 'o_test_gist'))
		self.assertTrue(self._idx_exists(node, 'o_test_gin'))

		# GIN usable
		result = node.execute("""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test WHERE arr @> ARRAY[1]::int[];
		""")
		self.assertEqual(100, result[0][0])

		# GiST gone
		self.assertIsNone(
		    node.execute("SELECT to_regclass('o_test_gist');")[0][0])
		# GIN present
		self.assertIsNotNone(
		    node.execute("SELECT to_regclass('o_test_gin');")[0][0])
		node.stop()

	def _gist_scan_count(self, node, box="((0,0),(100,100))"):
		return node.execute(f"""
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM o_test WHERE p <@ '{box}'::box;
		""")[0][0]

	def test_move_bridged_table_tablespace_crash(self):
		"""
		ALTER TABLE ... SET TABLESPACE on a table that has a bridged
		(GiST) index, then crash the node before any post-move
		checkpoint.  Recovery must replay the tablespace move and the
		bridge index rebuild so that both the heap data and the GiST
		scan are intact.  Verifies the bridge index resolves TIDs into
		the new tablespace's relnode after crash recovery.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "allow_in_place_tablespaces = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("CREATE TABLESPACE ts1 LOCATION '';")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			CHECKPOINT;
		""")
		node.safe_psql("ALTER TABLE o_test SET TABLESPACE ts1;")
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 200 + id, point(200 + id, 200 + id)
				FROM generate_series(1, 50) id;
		""")
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(150, node.execute("SELECT count(*) FROM o_test;")[0][0])
		self.assertEqual(
		    150, self._gist_scan_count(node, "((0,0),(250,250))"))
		self.assertEqual(
		    1, node.execute("SELECT count(*) FROM o_test WHERE id = 1;")[0][0])
		self.assertEqual(
		    1, node.execute("SELECT count(*) FROM o_test WHERE id = 201;")[0]
		    [0])
		node.stop()

	def test_move_bridged_table_mixed_tablespaces_crash(self):
		"""
		The bridge index lives in ts1 while the table is moved from
		the default tablespace to ts2.  Crash before checkpoint.
		Recovery must rebuild the bridge index (which stays in ts1)
		reading the heap from the old default tablespace and writing
		the orioledb trees to ts2, while the bridge index's own files
		remain in ts1.  Exercises the mixed-tablespace case where the
		bridge and heap live in different tablespaces across the move.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "allow_in_place_tablespaces = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("CREATE TABLESPACE ts1 LOCATION '';")
		node.safe_psql("CREATE TABLESPACE ts2 LOCATION '';")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p) TABLESPACE ts1;
			CHECKPOINT;
		""")
		bridge_ts = node.execute(
		    "SELECT reltablespace FROM pg_class WHERE relname = 'o_test_gist';"
		)[0][0]
		node.safe_psql("ALTER TABLE o_test SET TABLESPACE ts2;")
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 200 + id, point(200 + id, 200 + id)
				FROM generate_series(1, 50) id;
		""")
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(150, node.execute("SELECT count(*) FROM o_test;")[0][0])
		self.assertEqual(
		    150, self._gist_scan_count(node, "((0,0),(250,250))"))
		# bridge index stays in its original tablespace across the move
		self.assertEqual(
		    bridge_ts,
		    node.execute(
		        "SELECT reltablespace FROM pg_class WHERE relname = 'o_test_gist';"
		    )[0][0])
		node.stop()

	def test_move_bridge_index_tablespace_crash(self):
		"""
		ALTER INDEX ... SET TABLESPACE on a bridged (GiST) index,
		then crash before checkpoint.  Recovery must replay the
		bridge-index move so the bridge resolves TIDs after recovery
		and lives in the new tablespace.  Exercises the ALTER INDEX
		path (o_define_index with a tablespace change) under crash.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "allow_in_place_tablespaces = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("CREATE TABLESPACE ts2 LOCATION '';")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			CHECKPOINT;
		""")
		node.safe_psql("ALTER INDEX o_test_gist SET TABLESPACE ts2;")
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 200 + id, point(200 + id, 200 + id)
				FROM generate_series(1, 50) id;
		""")
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(150, node.execute("SELECT count(*) FROM o_test;")[0][0])
		self.assertEqual(
		    150, self._gist_scan_count(node, "((0,0),(250,250))"))
		ts2 = node.execute(
		    "SELECT oid FROM pg_tablespace WHERE spcname = 'ts2';")[0][0]
		self.assertEqual(
		    ts2,
		    node.execute(
		        "SELECT reltablespace FROM pg_class WHERE relname = 'o_test_gist';"
		    )[0][0])
		node.stop()

	def test_combined_bridge_add_tablespace_move_crash(self):
		"""
		In one transaction: add a bridged GiST index to a no-PK table
		AND move the table to a new tablespace.  Crash before
		checkpoint.  The commit's meta-unlock carries both a bridge
		add (bridge_oids validity flips) and a tablespace change.
		Recovery's handle_o_tables_meta_unlock bridge branch must
		produce the rebuilt heap in the new tablespace with the bridge
		attached, reading source data from the old tablespace.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "allow_in_place_tablespaces = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("CREATE TABLESPACE ts2 LOCATION '';")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int,
				p point
			) USING orioledb;
			INSERT INTO o_test
				SELECT id, point(id, id) FROM generate_series(1, 100) id;
			CHECKPOINT;
		""")
		with node.connect() as con:
			con.begin()
			con.execute("CREATE INDEX o_test_gist ON o_test USING gist (p);")
			con.execute("ALTER TABLE o_test SET TABLESPACE ts2;")
			con.commit()
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 200 + id, point(200 + id, 200 + id)
				FROM generate_series(1, 50) id;
		""")
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(150, node.execute("SELECT count(*) FROM o_test;")[0][0])
		self.assertEqual(
		    150, self._gist_scan_count(node, "((0,0),(250,250))"))
		self.assertTrue(self._idx_exists(node, 'o_test_gist'))
		node.stop()

	def _bridge_tree_page_count(self, node, table_name='o_test'):
		"""Count leaf-level (level=0) pages in the internal index_bridge
		btree via orioledb_tbl_structure."""
		struct = node.execute(
		    "SELECT orioledb_tbl_structure('%s'::regclass, 'e');" %
		    table_name)[0][0]
		in_bridge = False
		count = 0
		for line in struct.splitlines():
			if line.startswith('Index ') and 'index_bridge' in line:
				in_bridge = True
				continue
			if in_bridge:
				if line.startswith('Index '):
					break
				if re.match(r'Page \d+: level = 0', line):
					count += 1
		return count

	def test_bridge_tree_vacuum(self):
		"""
		Exercise lazy_vacuum_bridge_index: populate enough bridge
		tree pages for a split, delete every third row, then VACUUM
		VERBOSE.  The dead bridge ctids must be reaped (reported in
		vacuum output) and subsequent bridge scans must return only
		live rows.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			INSERT INTO o_test
				SELECT i, point(i, i) FROM generate_series(1, 2000) i;
		""")
		# bridge tree should have split into multiple pages
		self.assertGreater(self._bridge_tree_page_count(node), 1)

		node.safe_psql("DELETE FROM o_test WHERE id % 3 = 0;")
		live = 2000 - (2000 // 3)  # 1334

		_, _, err = node.psql("VACUUM VERBOSE o_test;")
		err_text = err.decode("utf-8")
		self.assertIn("vacuuming bridged indexes", err_text)
		m = re.search(r'had (\d+) dead item identifiers removed',
		              err_text)
		self.assertIsNotNone(m)
		self.assertEqual(2000 // 3, int(m.group(1)))

		# bridge scan returns only live rows
		self.assertEqual(
		    live, self._gist_scan_count(node, "((0,0),(3000,3000))"))
		self.assertEqual(
		    live, node.execute("SELECT count(*) FROM o_test;")[0][0])
		node.stop()

	def test_bridge_tree_split(self):
		"""
		Insert enough rows that the bridge btree splits into multiple
		leaf pages.  Verify via orioledb_tbl_structure that the
		index_bridge tree has more than one leaf page, then check
		that a GiST scan spanning the split boundary returns the
		correct rows.  Insert more rows (forcing further splits) and
		re-check scan correctness.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				p point
			) USING orioledb;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
			INSERT INTO o_test
				SELECT i, point(i, i) FROM generate_series(1, 2000) i;
		""")
		# bridge tree must have split (>1 leaf page)
		self.assertGreater(self._bridge_tree_page_count(node), 1)
		# full-range scan returns all rows
		self.assertEqual(
		    2000, self._gist_scan_count(node, "((0,0),(3000,3000))"))
		# narrow window around a likely split boundary
		self.assertEqual(
		    101, self._gist_scan_count(node, "((950,950),(1050,1050))"))

		# insert more rows -> further splits
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 2000 + i, point(2000 + i, 2000 + i)
				FROM generate_series(1, 2000) i;
		""")
		self.assertEqual(
		    4000, self._gist_scan_count(node, "((0,0),(5000,5000))"))
		self.assertGreater(self._bridge_tree_page_count(node), 1)
		# scan the newly inserted range
		self.assertEqual(
		    201, self._gist_scan_count(node, "((2050,2050),(2250,2250))"))
		node.stop()

	@unittest.skipIf(not BaseTest.extension_installed("pg_trgm"),
	                 "'pg_trgm' is not installed")
	def test_toast_bridge_gin(self):
		"""
		A GIN trigram bridge index on a toasted text column must
		correctly return matching rows via LIKE and detoast the
		values through the bridge's bitmap heap scan path.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb; CREATE EXTENSION pg_trgm;")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				big text COLLATE "C"
			) USING orioledb;
			INSERT INTO o_test
				SELECT i, repeat('x', 3000) || ':' || i::text
				FROM generate_series(1, 100) i;
			CREATE INDEX o_test_gin ON o_test USING gin (big gin_trgm_ops);
		""")
		# all 100 rows contain 'xxxxx' (from repeat('x', 3000))
		self.assertEqual(100, node.execute("""
		SET enable_seqscan = off;
			SELECT count(*) FROM o_test WHERE big LIKE '%%xxxxx%%';
		""")[0][0])
		# detoast through the bridge: max length = 3000 + ':100' = 3004
		self.assertEqual(3004, node.execute("""
		SET enable_seqscan = off;
			SELECT max(length(big)) FROM o_test WHERE big LIKE '%%xxxxx%%';
		""")[0][0])
		# verify the GIN bridge index is actually used
		plan = node.execute("""
		SET enable_seqscan = off;
			EXPLAIN (COSTS OFF) SELECT count(*) FROM o_test
				WHERE big LIKE '%%xxxxx%%';
		""")
		plan_text = '\n'.join(r[0] for r in plan)
		self.assertIn('Bitmap Index Scan on o_test_gin', plan_text)
		node.stop()

	@unittest.skipIf(not BaseTest.extension_installed("pg_trgm"),
	                 "'pg_trgm' is not installed")
	def test_toast_bridge_gist(self):
		"""
		A GiST trigram bridge index on a toasted text column must
		correctly return matching rows via LIKE and detoast the
		values through the bridge scan path.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb; CREATE EXTENSION pg_trgm;")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				big text COLLATE "C"
			) USING orioledb;
			INSERT INTO o_test
				SELECT i, repeat('x', 3000) || ':' || i::text
				FROM generate_series(1, 100) i;
			CREATE INDEX o_test_gist ON o_test USING gist (big gist_trgm_ops);
		""")
		self.assertEqual(100, node.execute("""
		SET enable_seqscan = off;
			SELECT count(*) FROM o_test WHERE big LIKE '%%xxxxx%%';
		""")[0][0])
		self.assertEqual(3004, node.execute("""
		SET enable_seqscan = off;
			SELECT max(length(big)) FROM o_test WHERE big LIKE '%%xxxxx%%';
		""")[0][0])
		plan = node.execute("""
		SET enable_seqscan = off;
			EXPLAIN (COSTS OFF) SELECT count(*) FROM o_test
				WHERE big LIKE '%%xxxxx%%';
		""")
		plan_text = '\n'.join(r[0] for r in plan)
		self.assertIn('o_test_gist', plan_text)
		node.stop()

	@unittest.skipIf(not BaseTest.extension_installed("pg_trgm"),
	                 "'pg_trgm' is not installed")
	def test_toast_bridge_crash_recovery(self):
		"""
		Crash recovery of a table that has both a TOAST tree and a
		bridged GIN index on a toasted column.  After creating the
		table, inserting toasted data, and building the GIN bridge
		(heap rewrite + bridge creation), crash before checkpoint.
		Recovery must replay the bridge creation meta-unlock so that
		both the toast tree and bridge index are rebuilt — LIKE
		queries and detoast must work after restart.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb; CREATE EXTENSION pg_trgm;")
		node.safe_psql("""
			CREATE TABLE o_test (
				id int primary key,
				big text COLLATE "C"
			) USING orioledb;
			INSERT INTO o_test
				SELECT i, repeat('x', 3000) || ':' || i::text
				FROM generate_series(1, 100) i;
		""")
		# checkpoint the base data, then add the bridge index
		node.safe_psql("CHECKPOINT;")
		node.safe_psql(
		    "CREATE INDEX o_test_gin ON o_test USING gin (big gin_trgm_ops);")
		# insert a few more rows after the bridge is built
		node.safe_psql("""
			INSERT INTO o_test
				SELECT 100 + i, repeat('x', 3000) || ':' || (100 + i)::text
				FROM generate_series(1, 50) i;
		""")
		# crash before checkpoint of the bridge creation / new inserts
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTrue(self._idx_exists(node, 'o_test_gin'))
		self.assertEqual(150, node.execute("SELECT count(*) FROM o_test;")[0][0])
		self.assertEqual(150, node.execute("""
		SET enable_seqscan = off;
			SELECT count(*) FROM o_test WHERE big LIKE '%%xxxxx%%';
		""")[0][0])
		# max length: id=100 -> ':100' (4 chars) + 3000 = 3004;
		# id=150 -> ':150' (4 chars) + 3000 = 3004
		self.assertEqual(3004, node.execute("""
		SET enable_seqscan = off;
			SELECT max(length(big)) FROM o_test WHERE big LIKE '%%xxxxx%%';
		""")[0][0])
		node.stop()

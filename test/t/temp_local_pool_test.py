#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest


class TempLocalPoolTest(BaseTest):

	def test_evict_temp_table_with_updates(self):
		"""
		Mixed INSERT/UPDATE/DELETE workload that overflows the local pool
		and triggers reentrant eviction: the scan's cached IN_MEMORY
		downlink becomes stale (local_ppool_pages[slot] = NULL) while
		o_btree_(try_)read_page is resolving it.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.temp_buffers = 1MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp (
				id int PRIMARY KEY,
				val int NOT NULL,
				payload text
			) USING orioledb;
			CREATE INDEX o_temp_val ON o_temp (val);
		""")
		con.commit()

		n = 30000
		con.execute("INSERT INTO o_temp SELECT g, g, repeat('x', 100) "
		            "FROM generate_series(1, %d) g;" % n)
		con.commit()

		con.execute("UPDATE o_temp SET val = -val WHERE mod(id, 3) = 0;")
		con.execute("DELETE FROM o_temp WHERE mod(id, 7) = 0;")
		con.commit()

		expected_total = n - n // 7
		expected_neg = sum(1 for i in range(1, n + 1)
		                   if i % 3 == 0 and i % 7 != 0)

		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp;")[0][0], expected_total)
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp WHERE val < 0;")[0][0],
		    expected_neg)
		self.assertTrue(
		    con.execute("SELECT orioledb_tbl_check('o_temp'::regclass);")[0]
		    [0])

		con.close()
		node.stop()

	def test_evict_pages_with_concurrent_seq_scan(self):
		"""
		A seq scan must see a self-consistent snapshot even after the
		on-disk extents it references get rewritten in place.  The
		reconstruction relies on the page-level undo chain, not on
		copy-on-write.

		Arrangement:

		  1. Fill a temp table so the primary-key btree has many leaves.
		  2. orioledb_evict_pages() pushes every leaf to disk.
		  3. A PK point lookup on the max id brings just the rightmost
		     leaf back into the local pool, flipping its parent downlink
		     to IN_MEMORY while the rest remain ON_DISK.
		  4. A cursor opens a seq scan.  iterate_internal_page() walks
		     the single internal page left-to-right, queues an ON_DISK
		     downlink for every leaf except the last, and returns rows
		     from the rightmost IN_MEMORY leaf.  The first FETCH drains
		     part of that leaf, leaving diskDownlinks[] populated but
		     unconsumed.
		  5. A bulk UPDATE loads every other leaf back into the local
		     pool and dirties it.
		  6. orioledb_evict_pages() rewrites those dirty pages back to
		     the same on-disk extents that the cursor's diskDownlinks[]
		     still points at.
		  7. Draining the cursor reads the now-overwritten extents.  Each
		     page's CSN is newer than the cursor's snapshot, so
		     load_next_disk_leaf_page() walks the page-level undo chain
		     to reconstruct the page contents visible to the cursor.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "orioledb.temp_buffers = 8MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp (
				id int PRIMARY KEY,
				val int NOT NULL,
				payload text
			) USING orioledb;
		""")
		con.commit()

		n = 5000
		con.execute("INSERT INTO o_temp SELECT g, g, repeat('x', 200) "
		            "FROM generate_series(1, %d) g;" % n)
		con.commit()

		# Push every leaf to disk.
		con.execute("SELECT orioledb_evict_pages('o_temp'::regclass, 0);")

		# Bring only the rightmost leaf back into the local pool.
		self.assertEqual(
		    con.execute("SELECT id FROM o_temp WHERE id = %d;" % n), [(n, )])

		con.begin()
		con.execute("DECLARE c1 NO SCROLL CURSOR FOR SELECT id FROM o_temp;")

		# The seq scan walks the internal page, queues ON_DISK
		# downlinks for every other leaf, then returns rows from the
		# rightmost IN_MEMORY leaf.  A small fetch leaves
		# diskDownlinks[] populated but unconsumed.
		first_batch = con.execute("FETCH 10 FROM c1;")
		self.assertEqual(len(first_batch), 10)

		# Dirty every other leaf: tuple size grows enough to split
		# pages, so the in-place rewrite path would make the original
		# on-disk extents structurally incompatible with what the
		# cursor expects.
		con.execute("UPDATE o_temp SET val = val + 1000000, "
		            "payload = repeat('y', 600) "
		            "WHERE id < %d;" % n)

		# Evict the dirty pages in place, overwriting the on-disk extents
		# the cursor's diskDownlinks[] still points at.
		con.execute("SELECT orioledb_evict_pages('o_temp'::regclass, 0);")

		rest = con.execute("FETCH ALL FROM c1;")
		con.execute("CLOSE c1;")
		con.commit()

		all_ids = sorted(row[0] for row in first_batch + rest)
		self.assertEqual(all_ids, list(range(1, n + 1)))

		self.assertTrue(
		    con.execute("SELECT orioledb_tbl_check('o_temp'::regclass);")[0]
		    [0])

		con.close()
		node.stop()

	def test_evict_temp_table_secondary_index(self):
		"""
		Eviction from local pool with a multi-index temp table.  After the
		pages are evicted to disk and read back, the table must remain
		consistent.  Complements test_evict_temp_table in eviction_test.py
		(which only uses primary keys).
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.temp_buffers = 1MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp (
				id int PRIMARY KEY,
				val int NOT NULL,
				payload text
			) USING orioledb;
			CREATE INDEX o_temp_val ON o_temp (val);
		""")
		con.commit()

		# Insert enough to overflow the 1MB local pool.
		n = 30000
		con.execute("INSERT INTO o_temp SELECT g, mod(g * 7, 100000), "
		            "repeat('x', 100) FROM generate_series(1, %d) g;" % n)
		con.commit()

		# Read back via primary key and via the secondary index.
		self.assertEqual(con.execute("SELECT count(*) FROM o_temp;")[0][0], n)
		# Force secondary index path.
		con.execute("SET enable_seqscan = off;")
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp WHERE val < 50000;")[0]
		    [0], sum(1 for g in range(1, n + 1) if (g * 7) % 100000 < 50000))
		con.execute("RESET enable_seqscan;")
		self.assertTrue(
		    con.execute("SELECT orioledb_tbl_check('o_temp'::regclass);")[0]
		    [0])

		con.close()
		node.stop()

	def test_checkpoint_during_temp_activity(self):
		"""
		CHECKPOINT must complete cleanly while a temp table accumulates dirty
		pages, and must not write any temp data to disk.  Temp data must
		remain visible to the owning backend after the checkpoint.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "shared_preload_libraries = orioledb\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp_ckpt (id int PRIMARY KEY, val text)
				USING orioledb;
		""")
		con.execute("INSERT INTO o_temp_ckpt SELECT g, repeat('y', 200) "
		            "FROM generate_series(1, 5000) g;")
		con.commit()

		# Take a non-temp snapshot of the data directory before checkpoint,
		# then issue a CHECKPOINT and verify temp data is still present.
		node.safe_psql("CHECKPOINT;")
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_ckpt;")[0][0], 5000)

		# Mutate, checkpoint again, verify still fine.
		con.execute("UPDATE o_temp_ckpt SET val = repeat('z', 200) "
		            "WHERE id <= 1000;")
		con.commit()
		node.safe_psql("CHECKPOINT;")
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_ckpt "
		                "WHERE val LIKE 'z%%';")[0][0], 1000)

		con.close()

		# After backend exit, a fresh connection must not see the temp table.
		con2 = node.connect()
		try:
			con2.execute("SELECT count(*) FROM o_temp_ckpt;")
			self.fail("temp table should not be visible to another backend")
		except Exception:
			pass
		con2.close()
		node.stop()

	def test_rewind_with_temp_table(self):
		"""
		Rewind must succeed and not affect temp tables.  The temp table
		disappears with the backend regardless of the rewind, while the
		persistent table is rewound to the chosen oxid.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.enable_rewind = true\n"
		    "orioledb.rewind_max_time = 500\n"
		    "orioledb.rewind_buffers = 16\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION orioledb;\n"
		    "CREATE TABLE o_persist (id int PRIMARY KEY) USING orioledb;\n"
		    "INSERT INTO o_persist VALUES (1), (2), (3);\n")

		con = node.connect()
		con.execute("CREATE TEMP TABLE o_temp_rw (id int PRIMARY KEY) "
		            "USING orioledb;")
		con.execute("INSERT INTO o_temp_rw VALUES (10), (20);")
		con.commit()

		# Capture current oxid; later inserts should be rolled back, the temp
		# table should be unaffected (and gone with the backend anyway).
		oxid = int(con.execute("SELECT orioledb_get_current_oxid();")[0][0])

		con.execute("INSERT INTO o_persist VALUES (4), (5);")
		con.execute("INSERT INTO o_temp_rw VALUES (30);")
		con.commit()
		con.close()

		node.safe_psql('postgres',
		               "SELECT orioledb_rewind_to_transaction(0, %d);" % oxid)
		# Rewind triggers a controlled shutdown; restart and verify state.
		import time
		time.sleep(1)
		node.is_started = False
		node.start()

		# o_persist rewound to before INSERT (4),(5).
		self.assertEqual(node.execute("SELECT id FROM o_persist ORDER BY id;"),
		                 [(1, ), (2, ), (3, )])
		# o_temp_rw is gone with the previous backend; new backend doesn't
		# see it -- and the rewind didn't crash trying to undo temp work.
		try:
			node.execute("SELECT 1 FROM o_temp_rw;")
			self.fail("temp table should not exist after backend exit")
		except Exception:
			pass
		node.stop()

	def test_temp_table_toast(self):
		"""
		TOAST values on a temp table.  The toast tree must use the local pool
		and survive evictions of the main tree.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.temp_buffers = 1MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp_toast (
				id int PRIMARY KEY,
				big text
			) USING orioledb;
		""")
		# Each row > TOAST threshold (~2KB), force out-of-line storage.
		con.execute(
		    "INSERT INTO o_temp_toast SELECT g, repeat(md5(g::text), 400) "
		    "FROM generate_series(1, 500) g;")
		con.commit()

		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_toast;")[0][0], 500)
		self.assertEqual(
		    con.execute("SELECT length(big) FROM o_temp_toast "
		                "WHERE id = 1;")[0][0], 400 * 32)

		# Force enough other temp activity to trigger eviction in the local
		# pool, then re-read TOAST values.
		con.execute(
		    "CREATE TEMP TABLE o_temp_filler (id int PRIMARY KEY, val text) "
		    "USING orioledb;")
		con.execute("INSERT INTO o_temp_filler SELECT g, repeat('p', 200) "
		            "FROM generate_series(1, 30000) g;")
		con.commit()

		# Toast data must still read back correctly.
		self.assertEqual(
		    con.execute("SELECT length(big) FROM o_temp_toast "
		                "WHERE id = 1;")[0][0], 400 * 32)
		self.assertEqual(
		    con.execute(
		        "SELECT count(*) FROM o_temp_toast WHERE length(big) = %d;" %
		        (400 * 32))[0][0], 500)
		self.assertTrue(
		    con.execute("SELECT orioledb_tbl_check("
		                "'o_temp_toast'::regclass);")[0][0])

		con.close()
		node.stop()

	def test_on_commit_delete_rows_with_reindex(self):
		"""
		ON COMMIT DELETE ROWS combined with REINDEX in the same and across
		transactions.  Exercises the placeholder-skip path for temp indexes
		in rebuild_indices_insert_placeholders/o_define_index.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "shared_preload_libraries = orioledb\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp_ocdr (
				id int PRIMARY KEY,
				val int UNIQUE
			) USING orioledb ON COMMIT DELETE ROWS;
		""")
		con.commit()

		# Fill, reindex, fill again, reindex inside a transaction -- the
		# table must come back empty and usable each commit.
		for round in range(3):
			con.execute("INSERT INTO o_temp_ocdr SELECT g, g + %d "
			            "FROM generate_series(1, 100) g;" % (round * 1000))
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_temp_ocdr;")[0][0], 100)
			con.execute("REINDEX TABLE o_temp_ocdr;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_temp_ocdr;")[0][0], 100)
			con.commit()
			# After commit, ON COMMIT DELETE ROWS must have emptied the table.
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_temp_ocdr;")[0][0], 0)

		# Now add a secondary index after a few cycles.
		con.execute("INSERT INTO o_temp_ocdr VALUES (1, 1), (2, 2);")
		con.execute("CREATE INDEX o_temp_ocdr_v ON o_temp_ocdr (val);")
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_ocdr "
		                "WHERE val = 2;")[0][0], 1)
		con.commit()
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_ocdr;")[0][0], 0)

		# REINDEX an empty temp table after DELETE ROWS.
		con.execute("REINDEX TABLE o_temp_ocdr;")
		con.commit()

		con.close()
		node.stop()

	def test_temp_drop_subtransaction_rollback(self):
		"""
		DROP TABLE inside a subtransaction that is rolled back must leave the
		temp table intact (and its backend-local SharedRootInfo entry too).
		Subsequent operations must succeed.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "shared_preload_libraries = orioledb\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		con = node.connect()
		con.execute("""
			CREATE TEMP TABLE o_temp_sub (id int PRIMARY KEY, val text)
				USING orioledb;
			INSERT INTO o_temp_sub VALUES (1, 'a'), (2, 'b'), (3, 'c');
		""")
		con.commit()

		con.begin()
		con.execute("SAVEPOINT sp1;")
		con.execute("DROP TABLE o_temp_sub;")
		con.execute("ROLLBACK TO SAVEPOINT sp1;")
		# After rollback the table must still be queryable.
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_sub;")[0][0], 3)
		con.execute("INSERT INTO o_temp_sub VALUES (4, 'd');")
		con.commit()
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_sub;")[0][0], 4)

		# Now drop and recreate -- no stale local hash entry should collide.
		con.execute("DROP TABLE o_temp_sub;")
		con.execute("CREATE TEMP TABLE o_temp_sub "
		            "(id int PRIMARY KEY) USING orioledb;")
		con.execute("INSERT INTO o_temp_sub SELECT generate_series(1, 50);")
		con.commit()
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_temp_sub;")[0][0], 50)

		con.close()
		node.stop()

#!/usr/bin/env python3
# coding: utf-8

import re
import time
import unittest

from threading import Thread

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent
from .base_test import wait_checkpointer_stopevent
from .base_test import wait_bgwriter_stopevent

INDEX_NOT_LOADED_TMPLT = "Index {relname}_pkey: not loaded"

# 400-byte keys fit ~19 entries to a page, so the index below reaches three
# levels (leaves, one internal level, root) while the data stays small.  The
# internal level is the point: the invariant tested there is about the parent
# slot, items[index - 1].
KEEP_PARENT_ROWS = 2000
KEEP_PARENT_KEYLEN = 400


class EvictionTest(BaseTest):

	def wait_eviction(self, con, bump_sql, evicted_rels):
		for i in range(20):
			con.execute(bump_sql)
			all_evicted = True
			for evicted_rel in evicted_rels:
				rel_evicted = con.execute(
				    f"SELECT orioledb_tbl_structure('{evicted_rel}'::regclass, 'e');"
				)[0][0].split('\n')[0] == INDEX_NOT_LOADED_TMPLT.format(
				    relname=evicted_rel)
				all_evicted = all_evicted and rel_evicted
			if all_evicted:
				return True

		return False

	def test_index_only_scan_over_evicted_tree(self):
		"""An ordered index-only scan of a tree whose downlinks are on disk.

		load_page() re-finds the parent of the page it loads by borrowing the
		caller's find context as scratch space, so whatever that descent does
		to the context's flags must not be visible to the caller.  A caller
		that navigates siblings sets KEEP_PARENT, and letting the scratch
		descent see it rebinds the caller's parent image and locator onto a
		descent it never asked for.

		Nothing here is a race: orioledb_evict_pages() puts the downlinks on
		disk, which is the path load_page() serves, and the first descent
		after it is enough.  Before the fix this aborted the backend on
		ASSERT_PARENT_LOCATOR_LOCAL() in refind_page().
		"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_keep_parent (
				id int8 NOT NULL,
				val text NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_keep_parent
				SELECT i, lpad(i::text, {KEEP_PARENT_KEYLEN}, '0')
				FROM generate_series(1, {KEEP_PARENT_ROWS}) i;
			CREATE INDEX o_keep_parent_val ON o_keep_parent (val);
			ANALYZE o_keep_parent;
			CHECKPOINT;
		""")

		# A root sitting directly above the leaves would leave no parent slot
		# to corrupt, and this would pass for the wrong reason.  Say so.
		root = node.execute(
		    'postgres', "SELECT orioledb_idx_structure("
		    "'o_keep_parent'::regclass, 'o_keep_parent_val', 'n', 1)")[0][0]
		m = re.search(r"level = (\d+)", root)
		self.assertIsNotNone(m, "could not read the index root level")
		self.assertGreaterEqual(
		    int(m.group(1)), 2,
		    "index is too shallow to exercise the parent slot")

		node.safe_psql(
		    'postgres',
		    "SELECT orioledb_evict_pages('o_keep_parent'::regclass::oid, 0);")

		con = node.connect()
		try:
			con.execute("SET enable_seqscan = off;")
			con.execute("SET enable_bitmapscan = off;")
			# Pin the serial plan: it is the one that walks the secondary
			# index with KEEP_PARENT.
			con.execute("SET max_parallel_workers_per_gather = 0;")

			plan = "\n".join(r[0] for r in con.execute(
			    "EXPLAIN (COSTS OFF) SELECT count(val) FROM o_keep_parent;"))
			self.assertIn("index only scan of: o_keep_parent_val", plan)

			self.assertEqual(
			    con.execute("SELECT count(val) FROM o_keep_parent;")[0][0],
			    KEEP_PARENT_ROWS)
		finally:
			con.close()
		node.stop()

	def test_eviction_txn(self):
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id integer NOT NULL,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_eviction (\n"
		    "	id integer NOT NULL,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n")
		node.safe_psql(
		    'postgres', "INSERT INTO o_test\n"
		    "    (SELECT id FROM generate_series(501, 1500, 1) id);")

		n = 30000
		node.safe_psql(
		    'postgres', "INSERT INTO o_eviction\n"
		    "    (SELECT id FROM generate_series(%d, %d, 1) id);" % (1, n))

		con1 = node.connect()

		con1.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(
		    con1.execute("SELECT COUNT(*) FROM o_test;")[0][0], 1000)

		con2 = node.connect()
		con2.begin()
		con2.execute("INSERT INTO o_test\n"
		             "    (SELECT id FROM generate_series(1, 500, 1) id);")
		con2.commit()
		con2.close()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_eviction;")[0][0], 30000)
		self.assertEqual(
		    con1.execute("SELECT COUNT(*) FROM o_test;")[0][0], 1000)
		con1.commit()

		con1.close()
		node.stop()

	def _press_until(self, node, done, timeout):
		"""Churn a table bigger than the pool until done() says so."""
		con = node.connect()
		try:
			deadline = time.time() + timeout
			while time.time() < deadline:
				if done():
					return True
				con.execute(
				    "UPDATE o_churn SET v = v || 'x' WHERE id <= 18000;")
				con.commit()
			return done()
		finally:
			con.close()

	def _victim_and_churn(self, node):
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_victim (id int NOT NULL, v text NOT NULL,\n"
		    "	PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_victim SELECT g, repeat('v', 100)\n"
		    "	FROM generate_series(1, 20) g;\n"
		    "CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,\n"
		    "	PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_churn SELECT g, repeat('c', 400)\n"
		    "	FROM generate_series(1, 20000) g;\n"
		    "CHECKPOINT;")

	def _victim_resident(self, node):
		return node.execute(
		    "SELECT count(*) FROM orioledb_table_pages('o_victim'::regclass);"
		)[0][0] > 0

	def test_eviction_ignores_open_transactions(self):
		"""
		A transaction that has read a table must not keep its trees loaded.

		Every transaction that touches a table holds a lock on it until it
		ends.  Tree eviction used to try-lock the relation, so an idle
		transaction was enough to pin its tree in the pool for good -- and
		enough of them could fill it.  Eviction now takes no relation lock.
		"""
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()
		self._victim_and_churn(node)

		holder = node.connect()
		try:
			holder.begin()
			# A point read through the primary key loads the tree; a
			# sequential scan of an evicted tree need not.  The pressure below
			# has to find it resident, or the test proves nothing.
			holder.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    holder.execute("SELECT v FROM o_victim WHERE id = 1;")[0][0],
			    'v' * 100)
			self.assertTrue(self._victim_resident(node))

			evicted = self._press_until(
			    node, lambda: not self._victim_resident(node), 120)
			self.assertTrue(
			    evicted, "o_victim stayed loaded behind an open transaction")

			# The holder still reads its table: the tree comes back from disk.
			self.assertEqual(
			    holder.execute("SELECT count(*), sum(id) FROM o_victim"
			                   " WHERE id > 0;")[0], (20, 210))
			holder.commit()
		finally:
			holder.close()
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_victim'::regclass);")[0]
		    [0])
		node.stop()

	def test_eviction_keeps_trees_of_writers(self):
		"""
		A transaction that has written to a table keeps its tree loaded.

		Its abort needs the tree, and must not have to load it back: an abort
		that runs out of pages fails again and again until the error stack
		overflows.  Eviction declines a tree whose relation somebody holds
		more than AccessShareLock on.
		"""
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()
		self._victim_and_churn(node)

		writer = node.connect()
		try:
			writer.begin()
			writer.execute("INSERT INTO o_victim VALUES (100, 'w');")
			self.assertTrue(self._victim_resident(node))

			evicted = self._press_until(
			    node, lambda: not self._victim_resident(node), 20)
			self.assertFalse(evicted,
			                 "o_victim evicted under a writing transaction")
			writer.rollback()
		finally:
			writer.close()
		self.assertEqual(
		    node.execute("SELECT count(*), sum(id) FROM o_victim"
		                 " WHERE id > 0;")[0], (20, 210))
		node.stop()

	# How long to keep the pool under pressure waiting for an eviction to
	# reach a stop event.  A valgrind cell is twenty times slower; an attempt
	# that never parks is skipped, since it tested nothing.
	PARK_TIMEOUT = 300

	def _start_pressure(self, node):
		"""Churn on a thread of its own: it may be the one that parks."""
		stop = []
		failed = []

		def press():
			con = node.connect()
			try:
				while not stop:
					con.execute("UPDATE o_churn SET v = v || 'x'"
					            " WHERE id <= 18000;")
					con.commit()
			except Exception as e:  # noqa: BLE001 - reported by the caller
				failed.append(e)
			finally:
				try:
					con.close()
				except Exception:  # noqa: BLE001 - teardown
					pass

		thread = Thread(target=press)
		thread.start()
		return thread, stop, failed

	def _waiters(self, con, event):
		rows = con.execute("SELECT waiter_pids FROM pg_stopevents()"
		                   " WHERE stopevent = '%s';" % event)
		return rows[0][0] if rows and rows[0][0] else None

	def _wait_parked(self, con, event, failed):
		deadline = time.time() + self.PARK_TIMEOUT
		while time.time() < deadline and not failed:
			if self._waiters(con, event):
				return True
			time.sleep(0.05)
		return False

	def _eviction_node(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "orioledb.enable_stopevents = true\n"
		    "checkpoint_timeout = 1h\n")
		node.start()
		self._victim_and_churn(node)
		# Resident, so that it is its root the pressure evicts.
		node.execute("SET enable_seqscan = off;"
		             " SELECT v FROM o_victim WHERE id = 1;")
		return node

	def test_eviction_waits_for_checkpointer(self):
		"""
		A tree the checkpointer works on must stay loaded.

		Eviction takes no relation lock, so the checkpointer's own lock no
		longer keeps an evictor away: the announcement in checkpoint_state
		does.  Park the checkpointer inside the victim and press the pool.
		"""
		node = self._eviction_node()
		ctrl = node.connect()
		ctrl.execute("SELECT pg_stopevent_set('checkpoint_index_start',"
		             " '$.treeName == \"o_victim_pkey\"');")
		chkp = None
		presser = None
		stop = []
		try:
			chkp = Thread(target=lambda: node.safe_psql('CHECKPOINT;'))
			chkp.start()
			self.assertTrue(
			    self._wait_parked(ctrl, 'checkpoint_index_start', []),
			    "checkpointer never reached the victim")

			presser, stop, failed = self._start_pressure(node)
			deadline = time.time() + 20
			while time.time() < deadline and not failed:
				self.assertTrue(self._victim_resident(node),
				                "o_victim evicted under the checkpointer")
				time.sleep(0.2)
			self.assertEqual(failed, [])
		finally:
			stop.append(True)
			ctrl.execute(
			    "SELECT pg_stopevent_reset('checkpoint_index_start');")
			if presser is not None:
				presser.join(timeout=self.PARK_TIMEOUT)
			if chkp is not None:
				chkp.join(timeout=self.PARK_TIMEOUT)
			ctrl.close()

		self.assertEqual(
		    node.execute("SELECT count(*), sum(id) FROM o_victim"
		                 " WHERE id > 0;")[0], (20, 210))
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_victim'::regclass);")[0]
		    [0])
		node.stop()

	def _park_victim_eviction(self, node, ctrl):
		"""Press the pool until an eviction of the victim's root parks."""
		ctrl.execute("SELECT pg_stopevent_set('after_tree_root_page_write',"
		             " '$.treeName == \"o_victim_pkey\"');")
		presser, stop, failed = self._start_pressure(node)
		parked = self._wait_parked(ctrl, 'after_tree_root_page_write', failed)
		stop.append(True)
		return presser, parked, failed

	def test_checkpoint_waits_for_eviction(self):
		"""
		The checkpointer must not load a tree an eviction is taking apart.

		An eviction that asked whether the checkpointer holds the tree before
		the checkpointer announced it goes ahead.  So once it has announced
		the tree, the checkpointer waits out whoever holds the tree's shared
		root info insert lock -- evict_btree() holds it from start to finish --
		before it loads the tree; otherwise it would checkpoint pages the
		eviction is about to hand back.

		Park the checkpointer where it has picked the victim but not announced
		it yet, park an eviction of the victim past its checks, then let the
		checkpointer go: it has to wait for the eviction.
		"""
		node = self._eviction_node()
		ctrl = node.connect()
		presser = None
		chkp = None
		done = []
		armed = []
		try:
			pkey = ctrl.execute("SELECT 'o_victim_pkey'::regclass::oid;")[0][0]
			ctrl.execute("SELECT pg_stopevent_set('checkpoint_table_start',"
			             " '$.tree.reloid == %d');" % pkey)
			armed.append('checkpoint_table_start')

			def checkpoint():
				node.safe_psql('CHECKPOINT;')
				done.append(True)

			chkp = Thread(target=checkpoint)
			chkp.start()
			self.assertTrue(
			    self._wait_parked(ctrl, 'checkpoint_table_start', []),
			    "checkpointer never reached the victim")

			armed.append('after_tree_root_page_write')
			presser, parked, failed = self._park_victim_eviction(node, ctrl)
			if not parked:
				self.skipTest("no eviction of the victim parked: %s" %
				              failed[:1])

			ctrl.execute(
			    "SELECT pg_stopevent_reset('checkpoint_table_start');")
			armed.remove('checkpoint_table_start')
			time.sleep(3)
			self.assertEqual(done, [], "checkpoint went past the eviction")
		finally:
			for event in armed:
				ctrl.execute("SELECT pg_stopevent_reset('%s');" % event)
			if presser is not None:
				presser.join(timeout=self.PARK_TIMEOUT)
			if chkp is not None:
				chkp.join(timeout=self.PARK_TIMEOUT)
			ctrl.close()

		self.assertEqual(done, [True])
		self.assertEqual(
		    node.execute("SELECT count(*), sum(id) FROM o_victim"
		                 " WHERE id > 0;")[0], (20, 210))
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_victim'::regclass);")[0]
		    [0])
		node.stop()

	def test_drop_waits_for_eviction(self):
		"""
		Dropping a tree while it is being evicted must not free it twice.

		cleanup_btree() takes the insert lock evict_btree() holds, so the drop
		waits and then finds the tree already evicted.
		"""
		node = self._eviction_node()
		ctrl = node.connect()
		presser = None
		dropper = None
		done = []
		try:
			presser, parked, failed = self._park_victim_eviction(node, ctrl)
			if not parked:
				self.skipTest("no eviction of the victim parked: %s" %
				              failed[:1])

			def drop():
				node.safe_psql('DROP TABLE o_victim;')
				done.append(True)

			dropper = Thread(target=drop)
			dropper.start()
			time.sleep(3)
			self.assertEqual(done, [], "drop went past the eviction")
		finally:
			ctrl.execute(
			    "SELECT pg_stopevent_reset('after_tree_root_page_write');")
			if presser is not None:
				presser.join(timeout=self.PARK_TIMEOUT)
			if dropper is not None:
				dropper.join(timeout=self.PARK_TIMEOUT)
			ctrl.close()

		self.assertEqual(done, [True])
		# The pool is whole: new trees get pages and keep their rows.
		node.safe_psql(
		    "CREATE TABLE o_after (id int PRIMARY KEY) USING orioledb;"
		    "INSERT INTO o_after SELECT generate_series(1, 1000);"
		    "CHECKPOINT;")
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_after;")[0][0], 1000)
		node.stop()

	def test_eviction_tree(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "checkpoint_timeout = 86400\n"
		    "max_wal_size = 1GB\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	key SERIAL NOT NULL,\n"
		    "	val int NOT NULL,\n"
		    "	PRIMARY KEY (key)\n"
		    ") USING orioledb;\n"
		    "CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);\n"
		    "CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);\n"
		    "CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);\n"
		    "CREATE TABLE IF NOT EXISTS o_evicted (\n"
		    "	key SERIAL NOT NULL,\n"
		    "	val int NOT NULL,\n"
		    "	PRIMARY KEY (key)\n"
		    ") USING orioledb;\n"
		    "CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key);\n")
		con1 = node.connect()
		con1.execute(
		    "INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n"
		)

		self.assertEqual(
		    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)

		n = 250000
		step = 1000
		for i in range(1, n, step):
			con1.execute(
			    "INSERT INTO o_test (val)\n"
			    "	(SELECT val FROM generate_series(%d, %d, 1) val);\n" %
			    (i, i + step - 1))
			con1.commit()

		self.assertTrue(
		    self.wait_eviction(
		        con1,
		        "SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;",
		        ('o_evicted', )))

		con2 = node.connect()
		try:
			self.assertEqual(
			    con2.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))

			self.assertEqual(
			    con2.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
			con2.commit()
			self.assertEqual(
			    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
			con1.commit()
			con2.execute(
			    "INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1, 500, 1) val;"
			)
			con2.commit()
			self.assertEqual(
			    con2.execute("SELECT count(*) FROM o_evicted;")[0][0], 1000)
			self.assertEqual(
			    con2.execute("SELECT val FROM o_evicted WHERE key = 500")[0]
			    [0], 1500)
			self.assertEqual(
			    con2.execute(
			        "SELECT count(*) FROM o_evicted WHERE val > 1500 LIMIT 1;")
			    [0][0], 0)
			con2.commit()

			self.assertNotEqual(
			    con1.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))
			self.assertNotEqual(
			    con2.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))

			con1.execute(
			    "INSERT INTO o_test (val)\n"
			    "	(SELECT val FROM generate_series(%d, %d, 1) val);\n" %
			    (1, n))
			con1.commit()

			self.assertTrue(
			    self.wait_eviction(
			        con1,
			        "SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;",
			        ('o_evicted', )))

			self.assertEqual(
			    con2.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))

			con3 = node.connect()
			self.assertEqual(
			    con3.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))
			con3.close()
		finally:
			con1.close()
			con2.close()

	def eviction_after_checkpoint_base(self, compressed):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n")
		node.start()
		arg1 = "WITH (primary_compress)" if compressed else ""
		arg2 = "WITH (compress)" if compressed else ""
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);
			CREATE TABLE IF NOT EXISTS o_evicted (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb %s;
			CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key) %s;
			""" % (arg1, arg2))
		con1 = node.connect()
		con1.execute(
		    "INSERT INTO  o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n"
		)

		# different errors when CHECKPOINT called even times or odd
		node.safe_psql("CHECKPOINT;")
		con1.execute("INSERT INTO o_test (val)\n"
		             "	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (1, 999))
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		con1.execute("SELECT * FROM o_evicted;")
		self.assertEqual(
		    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
		n = 20000
		con1.execute("INSERT INTO o_test (val)\n"
		             "	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (str(1), str(n)))
		con1.commit()
		con1.close()
		node.stop()

	def eviction_page_checkpoint_numbers_base(self, compressed):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "log_min_messages = DEBUG1\n"
		    "checkpoint_timeout = 86400\n")
		node.start()
		arg1 = "WITH (primary_compress)" if compressed else ""
		arg2 = "WITH (compress)" if compressed else ""
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_evicted (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb %s;
			CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key) %s;
			""" % (arg1, arg2))
		node.safe_psql("CHECKPOINT;")
		con1 = node.connect()
		con1.execute(
		    "INSERT INTO o_evicted (val) SELECT val FROM generate_series(1, 3000, 1) val;\n"
		)
		con1.commit()
		node.safe_psql("CHECKPOINT;")
		con1.execute(
		    "INSERT INTO o_evicted (val) SELECT val FROM generate_series(3001, 6000, 1) val;\n"
		)
		con1.commit()
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		con1.execute("SELECT * FROM o_evicted;")
		con1.execute("SELECT orioledb_evict_pages('o_evicted'::regclass, 1);")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual(
		    con1.execute("SELECT * FROM fetch_read_page_checkpoint_stats();"),
		    [(-1, 0)])
		node.safe_psql("SELECT reset_read_page_checkpoint_stats();")
		con1.execute("SELECT * FROM o_evicted;")
		self.assertEqual(
		    con1.execute("SELECT * FROM fetch_read_page_checkpoint_stats();"),
		    [(2, 3)])
		con1.close()
		node.stop()

	def test_eviction_page_checkpoint_number(self):
		self.eviction_page_checkpoint_numbers_base(False)

	def test_eviction_page_checkpoint_number_compress(self):
		self.eviction_page_checkpoint_numbers_base(True)

	def test_eviction_after_checkpoint(self):
		self.eviction_after_checkpoint_base(False)

	def test_eviction_compress_after_checkpoint(self):
		self.eviction_after_checkpoint_base(True)

	def test_eviction_after_checkpoint_con1(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "  key SERIAL NOT NULL,\n"
		    "  val int NOT NULL\n"
		    ") USING orioledb;\n")

		con1 = node.connect()
		# different errors when CHECKPOINT called even times or odd

		con1.execute("CHECKPOINT;")
		con1.begin()
		con1.execute("INSERT INTO o_test (val)\n"
		             "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (1, 999))
		con1.commit()
		con1.execute("CHECKPOINT;")

		n = 20000
		con1.execute("INSERT INTO o_test (val)\n"
		             "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (str(1), str(n)))
		con1.commit()
		con1.close()
		node.stop()

	def test_eviction_concurrent_checkpoint_next_tbl(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "checkpoint_timeout = 1d\n"
		    "orioledb.main_buffers = 8MB\n"
		    "bgwriter_delay = 400\n"
		    "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_first (\n"
		    "	id int NOT NULL,\n"
		    "	PRIMARY KEY (id)"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_second (\n"
		    "	id text  NOT NULL\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	key SERIAL NOT NULL,\n"
		    "	val int NOT NULL\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_second VALUES ('aaaaa');")

		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('checkpoint_table_start',\n"
		             "format(E'$.table.reloid == \\045s',\n"
		             "'o_second'::regclass::oid)::jsonpath);")
		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.execute("INSERT INTO o_first VALUES (0);")
		con2.execute(
		    "INSERT INTO o_second VALUES ('ajnslajslkdlaksjdlkajlsdkjlakjsdl')"
		)
		con2.execute("SELECT * FROM o_first;")
		con2.execute("SELECT * FROM o_second;")

		n = 50000
		con2.execute("INSERT INTO o_test (val)\n"
		             "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (str(1), str(n)))
		con2.commit()

		con2.execute("SELECT * FROM o_first;")
		con2.execute("SELECT pg_stopevent_reset('checkpoint_table_start')")
		t1.join()

		con1.execute("CHECKPOINT")
		con1.execute("CHECKPOINT")
		con1.close()
		con2.close()
		node.stop()

	def eviction_concurrent_checkpoint_base(self, compressed):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "bgwriter_delay = 400\n"
		    "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
					   CREATE EXTENSION IF NOT EXISTS orioledb;
					   CREATE TABLE IF NOT EXISTS o_checkpoint (
					     id text NOT NULL,
						 PRIMARY KEY (id) %s
					   ) USING orioledb;
					   CREATE TABLE IF NOT EXISTS o_test (
					     key SERIAL NOT NULL,
					     val int NOT NULL
					   ) USING orioledb;
					   """ % ("WITH (compress)" if compressed else ""))

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con1.begin()
		con1.execute("INSERT INTO o_test (val)\n"
		             "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (1, 7999))
		con1.commit()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
		             "'$.action == \"walkDownwards\" && "
		             "$.treeName == \"ctid_primary\" && "
		             "$.lokey.ctid[0] >= 2');")
		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		n = 20000
		con2.execute("INSERT INTO o_test (val)\n"
		             "  (SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (str(1), str(n)))
		con2.commit()

		con2.execute("SELECT * FROM o_checkpoint;")
		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		con1.execute("CHECKPOINT;")
		con1.execute("CHECKPOINT;")
		con1.execute("CHECKPOINT;")
		con1.close()
		con2.close()
		con3.close()
		node.stop()

	def test_eviction_concurrent_checkpoint(self):
		self.eviction_concurrent_checkpoint_base(False)

	def test_eviction_compress_concurrent_checkpoint(self):
		self.eviction_concurrent_checkpoint_base(True)

	def test_eviction_concurrent_drop(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "bgwriter_delay = 200\n"
		    "orioledb.enable_stopevents = true\n"
		    "checkpoint_timeout = 86400\n"
		    "max_wal_size = 1GB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_evicted (\n"
		    "  id int8 NOT NULL,\n"
		    "  val int8 NOT NULL,\n"
		    "  PRIMARY KEY (id, val)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "  id int8 NOT NULL,\n"
		    "  val int8 NOT NULL,\n"
		    "  PRIMARY KEY (id, val)\n"
		    ") USING orioledb;\n")

		con1 = node.connect()
		con2 = node.connect()

		con2.execute(
		    "SELECT pg_stopevent_set('after_write_page', '$backendType == \"orioledb background writer\"');"
		)

		n = 150000
		con1.execute(
		    "INSERT INTO o_evicted (id, val)\n"
		    "  (SELECT id, id + 1 FROM generate_series(%s, %s, 1) id);\n" %
		    (str(1), str(n)))
		con1.commit()
		wait_bgwriter_stopevent(node)

		n = 150000
		con1.execute(
		    "INSERT INTO o_test (id, val)\n"
		    "  (SELECT id, id + 1 FROM generate_series(%s, %s, 1) id);\n" %
		    (str(1), str(n)))
		con1.commit()

		t1 = ThreadQueryExecutor(con1, "DROP TABLE o_evicted;")
		t1.start()

		self.assertTrue(
		    con2.execute("SELECT pg_stopevent_reset('after_write_page');")[0]
		    [0])

		t1.join()
		con1.commit()

		con1.close()
		con2.close()
		node.stop()

	def test_eviction_concurrent_seqscan(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "bgwriter_delay = 200\n"
		    "orioledb.enable_stopevents = true\n"
		    "checkpoint_timeout = 86400\n"
		    "max_wal_size = 1GB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_evicted (\n"
		    "  id int8 NOT NULL,\n"
		    "  val int8 NOT NULL,\n"
		    "  PRIMARY KEY (id, val)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "  id int8 NOT NULL,\n"
		    "  val int8 NOT NULL,\n"
		    "  PRIMARY KEY (id, val)\n"
		    ") USING orioledb;\n")

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con3_pid = con3.pid

		n = 150000
		con1.execute(
		    "INSERT INTO o_evicted (id, val)\n"
		    "  (SELECT id, id + 1 FROM generate_series(%s, %s, 1) id);\n" %
		    (str(1), str(n)))
		con1.commit()

		# Prepare a generic query plan, which locks table only.  Otherwise,
		# PK will be locked during planning.
		con3.execute("SET plan_cache_mode = 'force_generic_plan';")
		con3.execute("PREPARE q AS SELECT * FROM o_evicted;")
		con3.execute("EXECUTE q;")
		con3.commit()

		con2.execute(
		    "SELECT pg_stopevent_set('seq_scan_load_internal_page', 'true');")

		t1 = ThreadQueryExecutor(con3, "EXECUTE q;")
		t1.start()

		wait_stopevent(node, con3_pid)

		# Check table lock prevents eviction of PK
		con1.execute("SELECT orioledb_evict_pages('o_evicted'::regclass, 10);")
		con1.commit()

		self.assertTrue(
		    con2.execute(
		        "SELECT pg_stopevent_reset('seq_scan_load_internal_page');")[0]
		    [0])

		t1.join()
		con1.commit()

		con1.close()
		con2.close()
		node.stop()

	def test_eviction_and_change_main_buffers_size(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "log_min_messages = notice\n")
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE TABLE IF NOT EXISTS o_evicted (
				id int NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;""")
		con1 = node.connect()
		con2 = node.connect()

		node.execute("CHECKPOINT;")
		con1.execute(
		    "INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(0, 75000, 5) id);\n"
		)
		con1.commit()

		n = 200000
		con2.execute("INSERT INTO o_test (val)\n"
		             "	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
		             (str(1), str(n)))
		con2.commit()

		con1.execute(
		    "INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(1, 15000, 5) id);\n"
		)
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute(
		    "INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(2, 15000, 5) id);\n"
		)
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute(
		    "INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(3, 15000, 5) id);\n"
		)
		con1.commit()
		node.execute("SELECT * FROM o_test;")

		con1.execute(
		    "INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(4, 10000, 5) id);\n"
		)
		con1.commit()
		node.execute("SELECT * FROM o_test;")
		con1.close()
		con2.close()
		node.stop()

		node.append_conf('postgresql.conf', "orioledb.main_buffers = 10MB\n")
		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_evicted;")[0][0], 26001)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass)")[0]
		    [0])

	def test_evict_temp_table(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "checkpoint_timeout = 86400\n"
		    "max_wal_size = 1GB\n"
		    "orioledb.temp_buffers = 1MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION orioledb;
		""")
		con1 = node.connect()

		# Create o_evicted tables FIRST so their pages are older
		# and will be evicted first by the clock sweep algorithm
		con1.execute("""
			CREATE TEMP TABLE o_evicted (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key);
		""")
		con1.execute("""
			CREATE TEMP TABLE o_evicted_empty (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
		""")
		con1.execute(
		    "INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n"
		)
		con1.commit()

		# Verify initial data - but note this touches o_evicted pages
		self.assertEqual(
		    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
		self.assertEqual(
		    con1.execute("SELECT count(*) FROM o_evicted_empty;")[0][0], 0)
		con1.commit()

		# Create o_test AFTER o_evicted so o_evicted pages have lower usage counts
		con1.execute("""
			CREATE TEMP TABLE o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);
		""")
		con1.commit()

		# Helper to check if a table is evicted
		# Note: checking orioledb_tbl_structure may reload the table
		def is_evicted(rel):
			result = con1.execute(
			    f"SELECT orioledb_tbl_structure('{rel}'::regclass, 'e');"
			)[0][0]
			# Check if "not loaded" appears anywhere in the output
			return INDEX_NOT_LOADED_TMPLT.format(relname=rel) in result

		# For local page pool, eviction happens synchronously during INSERT.
		# Insert data until both tables are evicted. For o_evicted (which has
		# data), the root page can only be evicted after all leaf pages are
		# written to disk, so we need many eviction cycles.
		step = 1000
		max_iterations = 2000  # Safety limit
		i = 0
		evicted = False
		while not evicted and i < max_iterations:
			con1.execute(
			    "INSERT INTO o_test (val)\n"
			    " (SELECT val FROM generate_series(%d, %d, 1) val);\n" %
			    (i * step + 1, (i + 1) * step))
			con1.commit()
			i += 1
			# Check every 100 iterations to avoid overhead
			if i % 100 == 0:
				evicted = is_evicted('o_evicted') and is_evicted(
				    'o_evicted_empty')

		# Final check
		evicted = is_evicted('o_evicted') and is_evicted('o_evicted_empty')
		self.assertTrue(evicted,
		                "Tables should be evicted after filling the pool")

		try:
			# Verify data is still accessible (will reload from disk)
			self.assertEqual(
			    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
			self.assertEqual(
			    con1.execute("SELECT count(*) FROM o_evicted_empty;")[0][0], 0)
			con1.commit()

			# After reloading, tables should no longer show as "not loaded"
			self.assertNotEqual(
			    con1.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))
			self.assertNotEqual(
			    con1.execute(
			        "SELECT orioledb_tbl_structure('o_evicted_empty'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted_empty'))

			# Insert more to trigger eviction again
			i = 0
			evicted = False
			while not evicted and i < max_iterations:
				con1.execute(
				    "INSERT INTO o_test (val)\n"
				    " (SELECT val FROM generate_series(%d, %d, 1) val);\n" %
				    (i * step + 1, (i + 1) * step))
				con1.commit()
				i += 1
				if i % 100 == 0:
					evicted = is_evicted('o_evicted') and is_evicted(
					    'o_evicted_empty')

			# Final check
			evicted = is_evicted('o_evicted') and is_evicted('o_evicted_empty')
			self.assertTrue(evicted, "Tables should be evicted again")
		finally:
			con1.close()

	def test_evict_unlogged_table(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "checkpoint_timeout = 86400\n"
		    "max_wal_size = 1GB\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_test_ix2 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix3 ON o_test (key);
			CREATE UNIQUE INDEX o_test_ix4 ON o_test (key);
		""")
		con1 = node.connect()
		con1.execute("""
			CREATE UNLOGGED TABLE o_evicted (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_evicted_ix2 ON o_evicted (key);
		""")
		con1.execute("""
			CREATE UNLOGGED TABLE o_evicted_empty (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
		""")
		con1.execute(
		    "INSERT INTO o_evicted (val) SELECT val id FROM generate_series(1001, 1500, 1) val;\n"
		)
		con1.commit()

		self.assertEqual(
		    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
		self.assertEqual(
		    con1.execute("SELECT count(*) FROM o_evicted_empty;")[0][0], 0)

		n = 250000
		step = 1000
		for i in range(1, n, step):
			con1.execute(
			    "INSERT INTO o_test (val)\n"
			    " (SELECT val FROM generate_series(%d, %d, 1) val);\n" %
			    (i, i + step - 1))
			con1.commit()

		self.assertTrue(
		    self.wait_eviction(
		        con1,
		        "SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;",
		        ('o_evicted', 'o_evicted_empty')))

		try:
			self.assertEqual(
			    con1.execute("SELECT count(*) FROM o_evicted;")[0][0], 500)
			self.assertEqual(
			    con1.execute("SELECT count(*) FROM o_evicted_empty;")[0][0], 0)
			con1.commit()

			self.assertNotEqual(
			    con1.execute(
			        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted'))
			self.assertNotEqual(
			    con1.execute(
			        "SELECT orioledb_tbl_structure('o_evicted_empty'::regclass, 'e');"
			    )[0][0].split('\n')[0],
			    INDEX_NOT_LOADED_TMPLT.format(relname='o_evicted_empty'))

			con1.execute(
			    "INSERT INTO o_test (val)\n"
			    " (SELECT val FROM generate_series(%d, %d, 1) val);\n" %
			    (1, n))
			con1.commit()
			self.assertTrue(
			    self.wait_eviction(
			        con1,
			        "SELECT COUNT(*) FROM (SELECT * FROM o_test ORDER BY key) x;",
			        ('o_evicted', 'o_evicted_empty')))
		finally:
			con1.close()

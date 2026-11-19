#!/usr/bin/env python3
# coding: utf-8

import re

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

# A 400-byte filler keeps fewer than 20 rows on a leaf while the int8 keys
# keep internal pages wide, so these rows give a root over a few internal
# pages over about a thousand leaves.
ROWS = 20000
PAD = 400
# MOVED_ID sits on one of the first leaves.  The checkpointer is parked a few
# leaves later, when it is about to go down to a child whose low key is past
# PARK_AFTER_ID.
MOVED_ID = 5
PARK_AFTER_ID = 100


class CheckpointMovedChildTest(BaseTest):
	"""
	A child that the checkpointer has already passed is written to a new
	location while the checkpointer is still on its parent.  The parent must
	stay dirty after the checkpoint.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "checkpoint_timeout = 1d\n"
		    "orioledb.main_buffers = 64MB\n"
		    "orioledb.enable_stopevents = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		self.node.start()
		self.node.safe_psql(
		    'postgres', f"""
				CREATE EXTENSION IF NOT EXISTS orioledb;
				CREATE TABLE o_moved (
					id int8 NOT NULL,
					val int8 NOT NULL,
					pad text NOT NULL,
					PRIMARY KEY (id)
				) USING orioledb;
				INSERT INTO o_moved
					SELECT i, i, repeat('x', {PAD})
					FROM generate_series(1, {ROWS}) i;
				CHECKPOINT;
		""")

		# The tests need an internal level between the root and the leaves.
		root = self.node.execute(
		    "SELECT orioledb_idx_structure('o_moved'::regclass, "
		    "'o_moved_pkey', 'n', 1)")[0][0]
		m = re.search(r"level = (\d+)", root)
		self.assertIsNotNone(m, "could not read the root level")
		self.assertGreaterEqual(int(m.group(1)), 2,
		                        "tree is too shallow to have internal pages")

	def checkpoint_moving_child(self, evict=True):
		"""
		Park the checkpointer past the leaf holding MOVED_ID, change that row
		and write the leaves out, evicting them or keeping them in memory.
		"""
		node = self.node

		# Give the checkpointer something to write in this tree.
		node.safe_psql(f"UPDATE o_moved SET val = val + 1 WHERE id = {ROWS};")

		con = node.connect()
		con.execute("SELECT pg_stopevent_set('checkpoint_step', "
		            "'$.action == \"walkDownwards\" && "
		            "$.treeName == \"o_moved_pkey\" && "
		            f"$.level == 1 && $.lokey.id > {PARK_AFTER_ID}');")
		con.commit()

		chkp_con = node.connect()
		t = ThreadQueryExecutor(chkp_con, "CHECKPOINT;")
		t.start()
		wait_checkpointer_stopevent(node)

		con.execute(
		    f"UPDATE o_moved SET val = val + 1000 WHERE id = {MOVED_ID};")
		con.commit()
		if evict:
			con.execute("SELECT orioledb_evict_pages('o_moved'::regclass, 0);")
		else:
			con.execute("SELECT orioledb_write_pages('o_moved'::regclass);")
		con.commit()

		con.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		con.commit()
		t.join()
		chkp_con.close()
		con.close()

	def check_rows(self, extra_rows=0, extra_sum=0):
		node = self.node
		self.assertEqual(
		    node.execute(f"SELECT val FROM o_moved WHERE id = {MOVED_ID};")[0]
		    [0], MOVED_ID + 1000)
		self.assertEqual(
		    node.execute("SELECT count(*), sum(val) FROM o_moved;")[0],
		    (ROWS + extra_rows, ROWS * (ROWS + 1) // 2 + 1000 + 1 + extra_sum))
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_moved'::regclass);")[0]
		    [0])

	def test_moved_child_old_location_intact(self):
		"""
		The child's old location still holds its old image, so a stale parent
		loses the update silently.
		"""
		self.checkpoint_moving_child()
		self.node.safe_psql(
		    "SELECT orioledb_evict_pages('o_moved'::regclass, 1);")
		self.check_rows()

	def test_moved_child_written_without_eviction(self):
		"""
		The leaf stays in memory after it is written, as the background
		writer leaves it.  Evicting it clean later does not dirty the parent.
		"""
		self.checkpoint_moving_child(evict=False)
		self.node.safe_psql(
		    "SELECT orioledb_evict_pages('o_moved'::regclass, 1);")
		self.check_rows()

	def test_moved_child_old_location_reused(self):
		"""
		Let the next checkpoint free the child's old location and insert
		enough rows for new leaves to take it.  A stale parent then leads to
		another page.
		"""
		node = self.node
		self.checkpoint_moving_child()

		node.safe_psql(f"UPDATE o_moved SET val = val + 1 WHERE id = {ROWS};")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql(f"""
			INSERT INTO o_moved
				SELECT i, 0, repeat('x', {PAD})
				FROM generate_series({ROWS + 1}, {2 * ROWS}) i;
		""")
		node.safe_psql("SELECT orioledb_evict_pages('o_moved'::regclass, 1);")
		self.check_rows(extra_rows=ROWS, extra_sum=1)

#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class TblCheckFreeExtentsTest(BaseTest):
	"""orioledb_tbl_check() has to know about extents freed since the map file.

	It reconciles two sets: the extents the tree's pages point at, and the
	extents the free stream says are free.  They have to tile the file with
	no gap and no overlap.  With force_map_check the free side used to come
	from the last checkpoint's map file alone, so anything freed after that
	file was written -- by the bgwriter rewriting a page, by a merge, by a
	copy-blkno rewrite -- belonged to neither set:

	    NOTICE:  Extent 0 460 is neither free or busy
	    NOTICE:  Extent 461 322 is neither free or busy

	against a tree that is perfectly sound.  A checkpoint right before the
	check hid it, which is why it surfaced as a flake: `evict_stale_descr_test`
	checkpoints and then checks three tables in turn, and by the time it
	reaches the one the bgwriter is still evicting, that tree has freed a few
	extents again.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf',
		    # 1024 pages against a table several times that: the pool evicts
		    # continuously, which is what frees extents behind the map file.
		    "orioledb.main_buffers = 8MB\n"
		    "orioledb.bgwriter_num_workers = 1\n"
		    "checkpoint_timeout = 1h\n"
		    "max_wal_size = 4GB\n")

	def tbl_check(self, con, table):
		"""orioledb_tbl_check(), with whatever it said about a failure."""
		del con.connection.notices[:]
		ok = con.execute("SELECT orioledb_tbl_check('%s'::regclass, true);" %
		                 table)[0][0]
		con.commit()
		return ok, [line.strip() for line in con.connection.notices]

	def test_check_after_frees_behind_the_map_file(self):
		node = self.node
		node.start()
		node.safe_psql(
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_churn SELECT g, repeat('c', 400)"
		    "  FROM generate_series(1, 20000) g;\n")
		node.safe_psql("CHECKPOINT;")

		con = node.connect()
		try:
			# Right after a checkpoint the map file is current and the check
			# has always agreed; this is the control for the loop below.
			ok, said = self.tbl_check(con, 'o_churn')
			self.assertTrue(
			    ok, "the check disagrees straight after a"
			    " checkpoint: %s" % said)

			# Now free extents without checkpointing again: every rewrite of
			# a dirty page puts its old extent in the .tmp stream, and some
			# of it is still buffered in shared memory.
			for _ in range(3):
				node.safe_psql(
				    "UPDATE o_churn SET v = v || 'x' WHERE id <= 18000;")
				node.safe_psql(
				    "SELECT orioledb_evict_pages('o_churn'::regclass, 0);")

			for i in range(3):
				ok, said = self.tbl_check(con, 'o_churn')
				self.assertTrue(
				    ok, "round %d: the check disagrees with a sound tree"
				    " after extents were freed behind the map file: %s" %
				    (i, said[:3]))
		finally:
			con.close()

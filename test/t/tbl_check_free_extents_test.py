#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import re
import struct

from .base_test import BaseTest

# sizeof(CheckpointFileHeader)
MAP_HEADER_SIZE = 48


class TblCheckFreeExtentsTest(BaseTest):
	"""orioledb_tbl_check() has to know about extents freed since the map file.

	It reconciles two sets: the extents the tree's pages point at, and the
	extents the free stream says are free.  They have to tile the file with
	no gap and no overlap.  With force_map_check the free side used to come
	from the last checkpoint's map file alone, so anything freed after that
	file was written -- by the bgwriter rewriting a page, by a merge, by a
	copy-blkno rewrite -- belonged to neither set:

	    NOTICE:  Extent 0 460 is neither free nor busy
	    NOTICE:  Extent 461 322 is neither free nor busy

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

	def test_check_after_allocations_behind_the_map_file(self):
		"""
		The other half: extents taken after the map file was written.  Once
		a checkpoint has free extents to list -- old copies of pages
		rewritten before it -- every page written afterwards takes one of
		them, and the map file still says it is free.  Read whole, it made
		each of those extents both busy and free:

		    NOTICE:  Excess busy extent 1375 1

		which is how evict_stale_descr_test kept failing under valgrind,
		with the tree perfectly sound.
		"""
		node = self.node
		node.start()
		node.safe_psql(
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_churn SELECT g, repeat('c', 400)"
		    "  FROM generate_series(1, 20000) g;\n")
		node.safe_psql("CHECKPOINT;")
		# Rewrite every page, so that the next map file has free extents
		node.safe_psql("UPDATE o_churn SET v = repeat('d', 400);")
		node.safe_psql("CHECKPOINT;")

		con = node.connect()
		try:
			ok, said = self.tbl_check(con, 'o_churn')
			self.assertTrue(
			    ok, "the check disagrees straight after a"
			    " checkpoint: %s" % said[:3])

			# Write pages without checkpointing again: each write takes an
			# extent the map file lists as free
			for i in range(3):
				node.safe_psql("UPDATE o_churn SET v = v || 'x' "
				               "WHERE id %% 3 = %d;" % i)
				node.safe_psql(
				    "SELECT orioledb_evict_pages('o_churn'::regclass, 0);")
				ok, said = self.tbl_check(con, 'o_churn')
				self.assertTrue(
				    ok, "round %d: the check disagrees with a sound tree"
				    " after extents were taken behind the map file: %s" %
				    (i, said[:3]))

			# And the next checkpoint agrees too
			node.safe_psql("CHECKPOINT;")
			ok, said = self.tbl_check(con, 'o_churn')
			self.assertTrue(ok, "after the next checkpoint: %s" % said[:3])
		finally:
			con.close()

	def test_check_after_frees_for_the_next_checkpoint(self):
		"""
		The map file is not the free stream in another form.  It starts with
		what was freed for its own checkpoint, which the free buffer reads
		last, from the .tmp file of that checkpoint -- and in another order,
		since the two files are written without a common lock.  Read as the
		free side, any tail of it was wrong:

		    NOTICE:  Excess busy extent 445 1
		    NOTICE:  Extent 2351 1 is neither free nor busy
		"""
		node = self.node
		node.start()
		node.safe_psql(
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_churn SELECT g, repeat('c', 400)"
		    "  FROM generate_series(1, 20000) g;\n"
		    "CHECKPOINT;\n")

		con = node.connect()
		try:
			for i, c in enumerate("defg"):
				node.safe_psql(f"UPDATE o_churn SET v = repeat('{c}', 400);")
				node.safe_psql(
				    "SELECT orioledb_evict_pages('o_churn'::regclass, 0);")
				node.safe_psql("CHECKPOINT;")
				node.safe_psql(
				    "UPDATE o_churn SET v = v || 'x' WHERE id % 5 = 0;")
				node.safe_psql(
				    "SELECT orioledb_evict_pages('o_churn'::regclass, 0);")
				ok, said = self.tbl_check(con, 'o_churn')
				self.assertTrue(ok, f"round {i}: {said[:3]}")
		finally:
			con.close()

	def test_check_after_a_checkpoint_skipped_the_tree(self):
		"""
		Evicting a tree finalizes its map file of the next checkpoint, header
		left unwritten: it holds whatever the seq buf page had there.  An
		unmodified tree is then skipped by that checkpoint, and the file stays
		so, while the image is still the one of the checkpoint before.  That
		file is planted here, after a checkpoint has skipped the tree for not
		being loaded, with a header that looks valid but is not.
		"""
		node = self.node
		node.start()
		node.safe_psql(
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_skip (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_skip SELECT g, repeat('s', 400)"
		    "  FROM generate_series(1, 2000) g;\n")
		datoid, relnode = node.execute(
		    "SELECT (SELECT oid FROM pg_database"
		    "        WHERE datname = current_database()),"
		    "       (SELECT relfilenode FROM pg_class"
		    "        WHERE relname = 'o_skip_pkey');")[0]
		node.restart()
		node.safe_psql("CHECKPOINT;")
		with open(node.pg_log_file) as f:
			chkp_num = max(
			    int(n) for n in re.findall(
			        r"orioledb checkpoint (\d+) complete", f.read()))
		path = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                    f'{relnode}-{chkp_num}.map')
		self.assertFalse(os.path.exists(path))
		# ctid, bridgeCtid, a root downlink of a valid length, datafileLength,
		# numFreeBlocks (as seen in CI), leafPagesNum, padding
		root_downlink = (1 << 63) | (1 << 48) | 3
		header = struct.pack('<QQQQQIi', 0, 0, root_downlink, 4,
		                     0x1eb800f800110010, 1, 0)
		self.assertEqual(len(header), MAP_HEADER_SIZE)
		with open(path, 'wb') as f:
			f.write(header)

		con = node.connect()
		try:
			ok, said = self.tbl_check(con, 'o_skip')
			self.assertTrue(ok, said[:3])
		finally:
			con.close()

	def map_file_mismatch_base(self, compress):
		"""
		With force_map_check, the map file of the last checkpoint has to
		match the image that checkpoint wrote.  Duplicate one of its free
		extents over another, and only the forced check notices.
		"""
		node = self.node
		node.start()
		with_compress = " WITH (compress = 1)" if compress else ""
		node.safe_psql(
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,\n"
		    f"  PRIMARY KEY (id)) USING orioledb{with_compress};\n"
		    "INSERT INTO o_churn SELECT g, repeat('c', 400)"
		    "  FROM generate_series(1, 20000) g;\n"
		    "CHECKPOINT;\n"
		    # Rewrite every page, so that the next map file has free extents
		    "UPDATE o_churn SET v = repeat('d', 400);\n"
		    "CHECKPOINT;\n")
		datoid, relnode = node.execute(
		    "SELECT (SELECT oid FROM pg_database"
		    "        WHERE datname = current_database()),"
		    "       (SELECT relfilenode FROM pg_class"
		    "        WHERE relname = 'o_churn_pkey');")[0]
		pattern = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                       f'{relnode}-*.map')
		map_file = max(glob.glob(pattern),
		               key=lambda f: int(f.rsplit('-', 1)[1].split('.')[0]))
		record_size = 8 if compress else 4

		con = node.connect()
		try:
			ok, said = self.tbl_check(con, 'o_churn')
			self.assertTrue(ok, said[:3])

			with open(map_file, 'rb') as f:
				saved = f.read()
			records = saved[MAP_HEADER_SIZE:]
			self.assertGreaterEqual(len(records), 2 * record_size)
			# Copy the last but one record over the last: the file stays
			# sorted, but one extent is free twice and another not at all
			with open(map_file, 'r+b') as f:
				f.seek(len(saved) - record_size)
				f.write(records[-2 * record_size:-record_size])

			ok, said = self.tbl_check(con, 'o_churn')
			self.assertFalse(ok)
			self.assertTrue(
			    any("does not match the image of checkpoint" in line
			        for line in said), said)

			ok = con.execute("SELECT orioledb_tbl_check("
			                 "'o_churn'::regclass, false);")[0][0]
			con.commit()
			self.assertTrue(ok)

			with open(map_file, 'wb') as f:
				f.write(saved)
			ok, said = self.tbl_check(con, 'o_churn')
			self.assertTrue(ok, said[:3])
		finally:
			con.close()

	def test_map_file_mismatch(self):
		self.map_file_mismatch_base(compress=False)

	def test_map_file_mismatch_compressed(self):
		self.map_file_mismatch_base(compress=True)

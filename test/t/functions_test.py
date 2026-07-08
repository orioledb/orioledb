#!/usr/bin/env python3
# coding: utf-8
# Tests for some user SQL functions

import os
import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException


class FunctionTest(BaseTest):

	def test_undo_log_size(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', "checkpoint_timeout = 86400\n"
		    "orioledb.undo_buffers = 128\n")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

            CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO oriole_table(t) select repeat('a', 270) FROM  generate_series(1, 80000) as i;
		""")

		# Hold an old snapshot so the undo produced below cannot be freed and is
		# forced out to the on-disk undo files that orioledb_undo_size() reads
		# (with sparse undo files, unretained undo is reclaimed before it ever
		# reaches disk).  The workload below is sized so the retained row undo
		# overruns the per-type circular buffer floor
		# (max_procs * 2 * O_MAX_UNDO_RECORD_SIZE), forcing evict-on-overflow.
		con = node.connect()
		con.begin()
		con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		con.execute("SELECT count(*) FROM oriole_table;")

		# Delete two thirds of the rows (interleaved, so survivors share pages
		# with the deleted tuples), then grow the survivors.  The growing update
		# forces page compaction/splits that physically drop the committed-dead
		# tuples, which take a full-page undo image.  (A split or merge that
		# drops nothing now uses a tiny differential image; the old same-size
		# UPDATE produced only such differential page undo, which no longer
		# spills to disk.)
		node.safe_psql("""
			DELETE FROM oriole_table WHERE MOD(i, 3) <> 0;
		""")
		node.safe_psql("""
			UPDATE oriole_table set t = repeat('c', 1200) WHERE i > 5000;
		""")

		self.assertGreaterEqual(
		    node.execute(
		        "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'row';"
		    )[0][0], 100000)
		self.assertGreaterEqual(
		    node.execute(
		        "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'page';"
		    )[0][0], 200000)

		con.rollback()
		con.close()
		node.stop()

	def test_undo_log_size_system(self):

		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

            CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO oriole_table(t) select repeat('a', 270) FROM  generate_series(1, 30000) as i;
		""")
		node.safe_psql("""
			UPDATE oriole_table set t = repeat('c', 270) WHERE i > 5000;
		""")
		node.safe_psql("""
		        CREATE INDEX on oriole_table(t);
		        REINDEX TABLE oriole_table;
		        REINDEX TABLE oriole_table;
		        REINDEX TABLE oriole_table;
		""")

		node.safe_psql("""
			CHECKPOINT;
		""")

		self.assertGreaterEqual(
		    node.execute(
		        "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'system';"
		    )[0][0], 10000)

		node.stop()

	def test_undo_log_size_over_int32(self):
		# Regression: orioledb_undo_size() accumulated per-stream file
		# sizes into an int[] and returned each element as a bare Datum.
		# Once a stream exceeded INT32_MAX (~2.147 GiB) the sum wrapped
		# to a negative or truncated value.  At TPC-C scale the row
		# stream easily reaches tens of GiB.
		#
		# Fabricate one sparse file just past INT32_MAX and confirm the
		# reported total is at least its apparent size.  Sparse keeps
		# the test cheap on constrained CI: no blocks consumed, no
		# workload required to produce the bytes.
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		undo_dir = os.path.join(node.data_dir, "orioledb_undo")
		# File naming follows include/transam/undo.h templates:
		#   ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE  "/%02X%08Xrow"
		# INT32_MAX == 2^31 - 1 bytes ≈ 2.147 GiB.  2.5 GiB trips the
		# wrap unambiguously without leaving the counter near a
		# power-of-two boundary that could accidentally look correct.
		fake_size = int(2.5 * 1024 * 1024 * 1024)
		fake_path = os.path.join(undo_dir, "0000000F00row")
		try:
			with open(fake_path, "wb") as f:
				os.ftruncate(f.fileno(), fake_size)

			total = node.execute("SELECT undo_size FROM orioledb_undo_size() "
			                     "WHERE undo_type = 'row';")[0][0]
			self.assertGreaterEqual(total, fake_size)
		finally:
			try:
				os.remove(fake_path)
			except FileNotFoundError:
				pass
			node.stop()

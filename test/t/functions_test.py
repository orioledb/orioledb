#!/usr/bin/env python3
# coding: utf-8
# Tests for some user SQL functions

import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException


class FunctionTest(BaseTest):

	def test_undo_log_size(self):

		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

            CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO oriole_table(t) select repeat('a', 270) FROM  generate_series(1, 40000) as i;
		""")

		# Hold an old snapshot so the undo produced below cannot be freed and is
		# forced out to the on-disk undo files that orioledb_undo_size() reads
		# (with sparse undo files, unretained undo is reclaimed before it ever
		# reaches disk).
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

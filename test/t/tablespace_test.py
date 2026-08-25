#!/usr/bin/env python3
# coding: utf-8
"""
An OrioleDB table in a non-default tablespace must keep its primary key tree
in that tablespace.
A large insert forces the PK tree to be backed on disk.
Check that primary key tree is kept in the same non default tablespace
and is properly resolved.
Before the fix its tablespace defaulted to the database
default and the backend died with
"could not open data file orioledb_data/<db>/<relnode>: No such file or directory".
"""

import shutil
import tempfile

from .base_test import BaseTest


class TablespaceTest(BaseTest):

	def test_primary_tree_follows_table_tablespace(self):
		tbsp_dir = tempfile.mkdtemp(prefix='oriole_tbsp_')
		self.addCleanup(shutil.rmtree, tbsp_dir, ignore_errors=True)

		node = self.node
		# Small pool so insert overflows it and forces the PK tree onto disk.
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()

		node.safe_psql('postgres', "CREATE EXTENSION orioledb;")
		node.safe_psql('postgres',
		               f"CREATE TABLESPACE ots LOCATION '{tbsp_dir}';")
		node.safe_psql(
		    'postgres', """
			CREATE TABLE o_ts (k int PRIMARY KEY, v text NOT NULL)
				USING orioledb TABLESPACE ots;
		""")
		node.safe_psql(
		    'postgres', """
			INSERT INTO o_ts
				SELECT i, repeat('x', 2000) FROM generate_series(1, 20000) i;
		""")

		self.assertEqual(
		    node.execute('postgres', "SELECT count(*) FROM o_ts;")[0][0],
		    20000)
		node.stop()

	def test_secondary_index_survives_table_tablespace_move(self):
		"""
		ALTER TABLE ... SET TABLESPACE moves only the table's own storage;
		a secondary index keeps its own tablespace and must keep working.
		The move rewrites the table under a new relnode, so the index is
		rebuilt -- but it must be rebuilt in the tablespace it still lives in
		instead of being unlinked and left behind, which made even a plain
		count(*) fail with "could not open file ...".
		"""
		ts1_dir = tempfile.mkdtemp(prefix='oriole_ts1_')
		ts2_dir = tempfile.mkdtemp(prefix='oriole_ts2_')
		self.addCleanup(shutil.rmtree, ts1_dir, ignore_errors=True)
		self.addCleanup(shutil.rmtree, ts2_dir, ignore_errors=True)

		node = self.node
		node.start()

		node.safe_psql('postgres',
		               f"CREATE TABLESPACE ots1 LOCATION '{ts1_dir}';")
		node.safe_psql('postgres',
		               f"CREATE TABLESPACE ots2 LOCATION '{ts2_dir}';")
		# The database itself lives in ots1, so the table and its index both
		# default to ots1 and only the table is moved away.
		node.safe_psql('postgres',
		               "CREATE DATABASE ts_move_db TABLESPACE ots1;")
		node.safe_psql('ts_move_db', "CREATE EXTENSION orioledb;")
		node.safe_psql(
		    'ts_move_db', """
			CREATE TABLE test_tbl (id int, val int, data text) USING orioledb;
			CREATE INDEX test_tbl_data_idx ON test_tbl (data);
			INSERT INTO test_tbl
				SELECT x, 2 * x, 'test' || x FROM generate_series(1, 1000) x;
		""")
		node.safe_psql('ts_move_db', "CHECKPOINT;")

		node.safe_psql('ts_move_db',
		               "ALTER TABLE test_tbl SET TABLESPACE ots2;")

		self.assertEqual(
		    node.execute('ts_move_db', "SELECT count(*) FROM test_tbl;")[0][0],
		    1000)
		with node.connect('ts_move_db') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM test_tbl "
			                "WHERE data = 'test500';")[0][0], 1)

		# The rebuilt index must also survive a checkpoint and a restart.
		node.safe_psql('ts_move_db', "CHECKPOINT;")
		node.restart()

		self.assertEqual(
		    node.execute('ts_move_db', "SELECT count(*) FROM test_tbl;")[0][0],
		    1000)
		with node.connect('ts_move_db') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM test_tbl "
			                "WHERE data = 'test500';")[0][0], 1)
		node.stop()

	def test_secondary_index_in_own_tablespace_survives_table_move(self):
		"""
		The index's own tablespace is the one it must be rebuilt in, which is
		not necessarily either side of the table's move.  Here the table goes
		ots1 -> pg_default while the index stays pinned to ots2.
		"""
		ts1_dir = tempfile.mkdtemp(prefix='oriole_ts1_')
		ts2_dir = tempfile.mkdtemp(prefix='oriole_ts2_')
		self.addCleanup(shutil.rmtree, ts1_dir, ignore_errors=True)
		self.addCleanup(shutil.rmtree, ts2_dir, ignore_errors=True)

		node = self.node
		node.start()

		node.safe_psql('postgres',
		               f"CREATE TABLESPACE ots1 LOCATION '{ts1_dir}';")
		node.safe_psql('postgres',
		               f"CREATE TABLESPACE ots2 LOCATION '{ts2_dir}';")
		node.safe_psql('postgres', "CREATE EXTENSION orioledb;")
		node.safe_psql(
		    'postgres', """
			CREATE TABLE t_split (id int, data text)
				USING orioledb TABLESPACE ots1;
			CREATE INDEX t_split_data_idx ON t_split (data) TABLESPACE ots2;
			INSERT INTO t_split
				SELECT x, 'w' || x FROM generate_series(1, 500) x;
		""")
		node.safe_psql('postgres', "CHECKPOINT;")

		node.safe_psql('postgres',
		               "ALTER TABLE t_split SET TABLESPACE pg_default;")

		# The index stayed in ots2 and must still resolve rows there.
		self.assertEqual(
		    node.execute(
		        'postgres', """
			SELECT ts.spcname FROM pg_class c
				JOIN pg_tablespace ts ON ts.oid = c.reltablespace
				WHERE c.relname = 't_split_data_idx';
		""")[0][0], 'ots2')
		with node.connect('postgres') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM t_split "
			                "WHERE data = 'w250';")[0][0], 1)

		node.safe_psql('postgres', "CHECKPOINT;")
		node.restart()

		with node.connect('postgres') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM t_split "
			                "WHERE data = 'w250';")[0][0], 1)
		self.assertEqual(
		    node.execute('postgres', "SELECT count(*) FROM t_split;")[0][0],
		    500)
		node.stop()

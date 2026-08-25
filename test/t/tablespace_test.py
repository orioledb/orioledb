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

	def test_secondary_index_survives_rolled_back_tablespace_move(self):
		"""
		A SET TABLESPACE performed inside a subtransaction that is then
		rolled back must leave the table and every secondary index usable.
		This exercises the abort path of the relocate: the new primary/toast
		trees built for the move are dropped on rollback, the old primary is
		kept, and -- critically -- the secondary index trees and their
		OIndex sys-tree chunks (shared between the old and the never-committed
		new o_table) must survive the rollback intact.  A regression here
		corrupts the secondary index on any rolled-back SET TABLESPACE.
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

		node.safe_psql('postgres',
		               "CREATE DATABASE ts_rollback_db TABLESPACE ots1;")
		node.safe_psql('ts_rollback_db', "CREATE EXTENSION orioledb;")
		node.safe_psql(
		    'ts_rollback_db', """
			CREATE TABLE rb_tbl (id int, val int, data text) USING orioledb;
			CREATE INDEX rb_tbl_data_idx ON rb_tbl (data);
			INSERT INTO rb_tbl
				SELECT x, 2 * x, 'rb' || x FROM generate_series(1, 1000) x;
		""")
		node.safe_psql('ts_rollback_db', "CHECKPOINT;")

		# Move the table to ots2 inside a savepoint, then roll it back.
		# The table must end up unchanged and still in ots1, with its
		# secondary index fully intact.
		with node.connect('ts_rollback_db') as con:
			con.execute("SAVEPOINT sp;")
			con.execute("ALTER TABLE rb_tbl SET TABLESPACE ots2;")
			con.execute("ROLLBACK TO sp;")

		# The index must still resolve every row it held before the move.
		with node.connect('ts_rollback_db') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM rb_tbl "
			                "WHERE data = 'rb500';")[0][0], 1)
			self.assertEqual(
			    con.execute("SELECT count(*) FROM rb_tbl;")[0][0], 1000)

		# A subsequent, committing SET TABLESPACE must still work after the
		# rolled-back one -- the abort must not have left the relocate undo
		# machinery in a wedged state.
		node.safe_psql('ts_rollback_db',
		               "ALTER TABLE rb_tbl SET TABLESPACE ots2;")
		with node.connect('ts_rollback_db') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM rb_tbl "
			                "WHERE data = 'rb500';")[0][0], 1)
		self.assertEqual(
		    node.execute('ts_rollback_db',
		                 "SELECT count(*) FROM rb_tbl;")[0][0], 1000)
		node.stop()

	def test_secondary_index_survives_table_move_crash(self):
		"""
		Crash (SIGKILL via -m immediate) after SET TABLESPACE and CHECKPOINT,
		then restart and verify the rebuilt/ relocated primary and the carried
		secondary index still resolve.  Exercises the relnode-undo callback
		replay on recovery for the tablespace relocate path.
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
		node.safe_psql('postgres',
		               "CREATE DATABASE ts_crash_db TABLESPACE ots1;")
		node.safe_psql('ts_crash_db', "CREATE EXTENSION orioledb;")
		node.safe_psql(
		    'ts_crash_db', """
			CREATE TABLE crash_tbl (id int, val int, data text) USING orioledb;
			CREATE INDEX crash_tbl_data_idx ON crash_tbl (data);
			INSERT INTO crash_tbl
				SELECT x, 2 * x, 'cr' || x FROM generate_series(1, 1000) x;
		""")
		node.safe_psql('ts_crash_db', "CHECKPOINT;")

		node.safe_psql('ts_crash_db',
		               "ALTER TABLE crash_tbl SET TABLESPACE ots2;")
		node.safe_psql('ts_crash_db', "CHECKPOINT;")

		# Crash: -m immediate skips the shutdown checkpoint, forcing WAL
		# replay (and the relnode undo callback) on restart.
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(
		    node.execute('ts_crash_db',
		                 "SELECT count(*) FROM crash_tbl;")[0][0], 1000)
		with node.connect('ts_crash_db') as con:
			con.execute("SET enable_seqscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM crash_tbl "
			                "WHERE data = 'cr500';")[0][0], 1)
		node.stop()

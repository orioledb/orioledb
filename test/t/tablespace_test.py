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

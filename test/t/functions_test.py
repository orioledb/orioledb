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
			INSERT INTO oriole_table(t) select repeat('a', 270) FROM  generate_series(1, 30000) as i;
		""")
		node.safe_psql("""
			UPDATE oriole_table set t = repeat('c', 270) WHERE i > 5000;
		""")

		row_undo_size = node.execute(
		    "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'row';"
		)[0][0]
		page_undo_size = node.execute(
		    "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'page';"
		)[0][0]
		row_written_location, row_cleaned_location = node.execute("""
			SELECT writtenlocation, cleanedlocation
			FROM orioledb_get_undo_meta()
			WHERE undo_type IN ('row', 'regular');
		""")[0]

		self.assertGreater(int(page_undo_size), 4000000)
		self.assertGreaterEqual(int(row_undo_size), 0)
		self.assertGreater(int(row_written_location),
		                   int(row_cleaned_location))

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

		system_undo_size = node.execute(
		    "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'system';"
		)[0][0]
		system_written_location, system_cleaned_location = node.execute("""
			SELECT writtenlocation, cleanedlocation
			FROM orioledb_get_undo_meta()
			WHERE undo_type = 'system';
		""")[0]

		self.assertGreater(int(system_undo_size), 0)
		self.assertGreaterEqual(int(system_written_location),
		                        int(system_cleaned_location))

		node.stop()

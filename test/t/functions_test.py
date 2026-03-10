#!/usr/bin/env python3
# coding: utf-8
# Tests for some user SQL functions

import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException


class FunctionTest(BaseTest):

	def test_undo_log_size(self):

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
			CHECKPOINT;
		""")

		self.assertEqual(
		    "[(Decimal('11000000'),)]",
		    str(
		        node.execute(
		            "SELECT round(undo_size, -6) FROM orioledb_undo_size() WHERE undo_type = 'row';"
		        )))
		self.assertEqual(
		    "[(Decimal('6000000'),)]",
		    str(
		        node.execute(
		            "SELECT round(undo_size, -6) FROM orioledb_undo_size() WHERE undo_type = 'page';"
		        )))
		self.assertEqual(
		    "[(Decimal('25000'),)]",
		    str(
		        node.execute(
		            "SELECT round(undo_size, -3) FROM orioledb_undo_size() WHERE undo_type = 'system';"
		        )))

		node.stop()

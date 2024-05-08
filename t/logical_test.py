#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class LogicalTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")

	def squashLogicalChanges(self, rows):
		result = ''
		for row in rows:
			line = row[2]
			if line.startswith('BEGIN') or line.startswith('COMMIT'):
				line = line[0:line.index(' ')]
			result = result + line + "\n"
		return result

	def test_simple(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE data(id serial primary key, data text);\n")

		node.safe_psql(
		    'postgres',
		    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
		)

		node.safe_psql(
		    'postgres', "BEGIN;\n"
		    "INSERT INTO data(data) VALUES('1');\n"
		    "INSERT INTO data(data) VALUES('2');\n"
		    "COMMIT;\n")

		result = self.squashLogicalChanges(
		    node.execute(
		        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
		    ))
		self.assertEqual(
		    result,
		    "BEGIN\ntable public.data: INSERT: id[integer]:1 data[text]:'1'\ntable public.data: INSERT: id[integer]:2 data[text]:'2'\nCOMMIT\n"
		)

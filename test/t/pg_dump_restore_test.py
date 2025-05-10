#!/usr/bin/env python3
# coding: utf-8

import os
import testgres
import textwrap

from .base_test import BaseTest


class PgDumpRestoreTest(BaseTest):

	def setUp(self):
		super().setUp()

		self.node.start()
		self.node.execute("""
			CREATE SCHEMA extensions;
			CREATE EXTENSION orioledb WITH SCHEMA extensions;
		""")

	def execute_multiline(self, node: testgres.PostgresNode,
	                      query: str) -> str:
		res = node.execute(query)
		assert res is not None

		return '\n'.join([line.rstrip() for line in (res[0][0].split('\n'))])

	def test_pg_dump_restore(self):
		node = self.node
		node.execute("""
			CREATE TABLE pg_dump_restore_table (
				key integer NOT NULL,
				payload text,
				hash integer GENERATED ALWAYS AS (mod(key, 2)) STORED,
				PRIMARY KEY(key)
			) USING orioledb;
			CREATE INDEX ON pg_dump_restore_table (payload);
			INSERT INTO pg_dump_restore_table
			SELECT i, '*' || i || repeat('*', mod(i, 5)) FROM generate_series(1, 10) AS i;
			""")
		node.dump("pg_dump_restore")

		restoredNode = self.restoreNode(self.getBasePort() + 1,
		                                "pg_dump_restore")

		self.assertEqual(
		    self.execute_multiline(
		        restoredNode, """
				SELECT extensions.orioledb_table_description('pg_dump_restore_table'::regclass);
				"""),
		    textwrap.dedent("""\
				Compress = -1, Primary compress = -1, TOAST compress = -1
				  Column |    Type | Collation | Nullable | Droped
				     key | integer |    (null) |    false |  false
				 payload |    text |   default |     true |  false
				    hash | integer |    (null) |     true |  false
				"""))

		self.assertEqual(
		    self.execute_multiline(
		        restoredNode, """
				SELECT extensions.orioledb_tbl_indices('pg_dump_restore_table'::regclass);
				"""),
		    textwrap.dedent("""\
				Index pg_dump_restore_table_pkey
				    Index type: primary, unique
				    Leaf tuple size: 3, non-leaf tuple size: 1
				    Non-leaf tuple fields: key
				    Leaf tuple fields: key, payload, hash
				Index pg_dump_restore_table_payload_idx
				    Index type: secondary
				    Leaf tuple size: 2, non-leaf tuple size: 2
				    Non-leaf tuple fields: payload, key
				    Leaf tuple fields: payload, key
				"""))

		self.assertEqual(
		    restoredNode.execute("SELECT * FROM pg_dump_restore_table"),
		    [(1, "*1*", 1), (2, "*2**", 0), (3, "*3***", 1), (4, "*4****", 0),
		     (5, "*5", 1), (6, "*6*", 0), (7, "*7**", 1), (8, "*8***", 0),
		     (9, "*9****", 1), (10, "*10", 0)])

		os.unlink("pg_dump_restore")
		node.stop()
		restoredNode.stop()

	def test_pg_dump_restore_options(self):
		node = self.node
		node.execute("""
			CREATE TABLE pg_dump_restore_table (
				key integer NOT NULL,
				payload text,
				hash integer GENERATED ALWAYS AS (mod(key, 2)) STORED,
				PRIMARY KEY(key)
			) USING orioledb WITH (compress = 11, toast_compress = 13, fillfactor = 70);
			INSERT INTO pg_dump_restore_table
			SELECT i, '*' || i || repeat('*', mod(i, 5)) FROM generate_series(1, 10) AS i;
			""")
		node.dump("pg_dump_restore")

		restoredNode = self.restoreNode(self.getBasePort() + 1,
		                                "pg_dump_restore")

		self.assertEqual(
		    self.execute_multiline(
		        restoredNode, """
				SELECT extensions.orioledb_table_description('pg_dump_restore_table'::regclass);
				"""),
		    textwrap.dedent("""\
				Compress = 11, Primary compress = 11, TOAST compress = 13
				  Column |    Type | Collation | Nullable | Droped
				     key | integer |    (null) |    false |  false
				 payload |    text |   default |     true |  false
				    hash | integer |    (null) |     true |  false
				"""))

		self.assertEqual(
		    restoredNode.execute(
		        "SELECT reloptions FROM pg_catalog.pg_class WHERE relname = 'pg_dump_restore_table'"
		    ), [(['compress=11', 'toast_compress=13', 'fillfactor=70'], )])

		self.assertEqual(
		    restoredNode.execute("SELECT * FROM pg_dump_restore_table"),
		    [(1, "*1*", 1), (2, "*2**", 0), (3, "*3***", 1), (4, "*4****", 0),
		     (5, "*5", 1), (6, "*6*", 0), (7, "*7**", 1), (8, "*8***", 0),
		     (9, "*9****", 1), (10, "*10", 0)])

		os.unlink("pg_dump_restore")
		node.stop()
		restoredNode.stop()

	def test_pg_dump_restore_bridging(self):
		node = self.node
		node.execute("""
			CREATE TABLE pg_dump_restore_table (
				key integer NOT NULL,
				payload text,
				hash integer GENERATED ALWAYS AS (mod(key, 2)) STORED,
				PRIMARY KEY(key)
			) USING orioledb WITH (index_bridging);
			INSERT INTO pg_dump_restore_table
			SELECT i, '*' || i || repeat('*', mod(i, 5)) FROM generate_series(1, 10) AS i;
			""")
		node.dump("pg_dump_restore")

		restoredNode = self.restoreNode(self.getBasePort() + 1,
		                                "pg_dump_restore")

		self.assertEqual(
		    self.execute_multiline(
		        restoredNode, """
				SELECT extensions.orioledb_table_description('pg_dump_restore_table'::regclass);
				"""),
		    textwrap.dedent("""\
				Compress = -1, Primary compress = -1, TOAST compress = -1
				  Column |    Type | Collation | Nullable | Droped
				     key | integer |    (null) |    false |  false
				 payload |    text |   default |     true |  false
				    hash | integer |    (null) |     true |  false
				"""))

		self.assertEqual(
		    self.execute_multiline(
		        restoredNode, """
				SELECT extensions.orioledb_tbl_indices('pg_dump_restore_table'::regclass);
				"""),
		    textwrap.dedent("""\
				Index pg_dump_restore_table_pkey
				    Index type: primary, unique
				    Leaf tuple size: 4, non-leaf tuple size: 1
				    Non-leaf tuple fields: key
				    Leaf tuple fields: index_bridging_ctid, key, payload, hash
				"""))

		self.assertEqual(
		    restoredNode.execute("SELECT * FROM pg_dump_restore_table"),
		    [(1, "*1*", 1), (2, "*2**", 0), (3, "*3***", 1), (4, "*4****", 0),
		     (5, "*5", 1), (6, "*6*", 0), (7, "*7**", 1), (8, "*8***", 0),
		     (9, "*9****", 1), (10, "*10", 0)])

		os.unlink("pg_dump_restore")
		node.stop()
		restoredNode.stop()

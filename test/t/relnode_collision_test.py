#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from testgres.exceptions import QueryException


class RelnodeCollisionTest(BaseTest):

	def test_cross_relation_lookup_rejected(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t1 (a int, t1_marker text) USING orioledb;
			CREATE TABLE o_t2 (b int, t2_marker text) USING orioledb;
		""")

		# Fetch (reloid, relnode) for each table.
		t1_reloid, t1_relnode = node.execute(
		    'postgres', """
			SELECT reloid, relnode FROM orioledb_table_oids()
			WHERE reloid = 'o_t1'::regclass
		""")[0]
		t2_reloid, t2_relnode = node.execute(
		    'postgres', """
			SELECT reloid, relnode FROM orioledb_table_oids()
			WHERE reloid = 'o_t2'::regclass
		""")[0]
		datoid = node.execute(
		    'postgres', """
			SELECT oid FROM pg_database WHERE datname = current_database()
		""")[0][0]

		# Sanity: each table is describable by its own (reloid, relnode).
		desc_t1 = node.execute(
		    'postgres', """
			SELECT orioledb_table_description(%s, %s, %s)
		""" % (datoid, t1_reloid, t1_relnode))[0][0]
		self.assertIn('t1_marker', desc_t1)

		desc_t2 = node.execute(
		    'postgres', """
			SELECT orioledb_table_description(%s, %s, %s)
		""" % (datoid, t2_reloid, t2_relnode))[0][0]
		self.assertIn('t2_marker', desc_t2)

		# The O_TABLES tree comparator orders by (datoid, relnode,
		# chunknum) and omits reloid, so a lookup keyed by o_t1's reloid
		# but o_t2's relnode lands on o_t2's chunks.  No relation has
		# this (reloid, relnode) pair, so the description must not be
		# found instead of returning o_t2's metadata.
		with self.assertRaises(QueryException) as e:
			node.safe_psql(
			    'postgres', """
				SELECT orioledb_table_description(%s, %s, %s)
			""" % (datoid, t1_reloid, t2_relnode))
		self.assertErrorMessageEquals(
		    e, 'unable to find orioledb table description.')

		node.stop()

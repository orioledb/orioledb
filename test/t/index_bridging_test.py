#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class IndexBridgingTest(BaseTest):

	def test_ctid_overflow(self):
		node = self.node
		node.append_conf("orioledb.debug_max_bridge_ctid_blkno=1")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test (
				i int NOT NULL,
				j int
			) USING orioledb WITH (index_bridging);

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (index_bridging);
		""")

		node.safe_psql("""
			INSERT INTO o_test SELECT v, v FROM generate_series(1, 2046) v;
		""")
		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 4) = 0;
		""")
		node.safe_psql("""
			VACUUM;
		""")
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 100, v * 200 FROM generate_series(1, 100) v;
		""")

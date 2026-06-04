#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
import json


class QueryPlansTest(BaseTest):

	def test_set_rel_pathlist_hook(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		# Init schema
		node.safe_psql("""
            CREATE TABLE oriole_test (
                id SERIAL,
                col_a INT,
                col_b INT,
                payload TEXT,
                PRIMARY KEY (id)
            ) USING orioledb;

            -- insert some data
            INSERT INTO oriole_test (col_a, col_b, payload)
                SELECT x % 100, x % 50, 'data_' || x
                FROM generate_series(1, 10000) x;

            -- create indexes (order matters)
            CREATE INDEX idx_a ON oriole_test (col_a);
            CREATE INDEX idx_b ON oriole_test (col_b);

            -- populate statistics

            ANALYZE oriole_test;
            """)

		explain = node.safe_psql("""
            EXPLAIN (FORMAT JSON, COSTS OFF)
                SELECT col_a, id FROM oriole_test WHERE col_a >= 0;
            """)
		explain = json.loads(explain)[0]["Plan"]
		self.assertEqual(explain["Custom Plan Provider"], "o_scan",
		                 "The planner correctly choses o_scan over SeqScan")

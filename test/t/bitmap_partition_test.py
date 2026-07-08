#!/usr/bin/env python3
# coding: utf-8

import json

from .base_test import BaseTest


class BitmapPartitionTest(BaseTest):

	def test_bitmapscan_partition_explain_json(self):
		"""
        Test for bitmap scan on partitioned tables with EXPLAIN FORMAT JSON.

        This test verifies that the grouping stack is correctly managed when
        using EXPLAIN with JSON format on partitioned tables with bitmap scans.
        """
		node = self.node
		node.start()

		node.safe_psql("""
            CREATE EXTENSION orioledb;
        """)

		node.safe_psql("""
            CREATE TABLE partition_bitmap_test (
                id int
            ) PARTITION BY RANGE (id);
        """)

		for i in range(7):
			start = i * 10
			end = (i + 1) * 10
			node.safe_psql(f"""
                CREATE TABLE partition_bitmap_test_p{i}
                    PARTITION OF partition_bitmap_test
                    FOR VALUES FROM ({start}) TO ({end}) USING orioledb;
            """)

		node.safe_psql("""
            CREATE INDEX partition_bitmap_test_idx ON partition_bitmap_test (id);
        """)

		node.safe_psql("""
            INSERT INTO partition_bitmap_test
                SELECT generate_series(0, 69);
        """)

		count_result = node.execute("""
            SELECT count(*) FROM partition_bitmap_test;
        """)
		self.assertEqual(count_result[0][0], 70)

		# Disable seqscan and indexscan to force bitmap scan
		node.safe_psql("""
            SET enable_seqscan = off;
            SET enable_indexscan = off;
        """)

		# Run EXPLAIN (FORMAT JSON) to test grouping stack handling
		explain_result = node.safe_psql("""
            EXPLAIN (FORMAT JSON, COSTS OFF)
            SELECT * FROM partition_bitmap_test WHERE id BETWEEN 0 AND 100;
        """).decode('utf-8')

		# Parse the JSON result
		explain_json = json.loads(explain_result)

		# Verify we got a valid plan
		self.assertIsInstance(explain_json, list)
		self.assertGreater(len(explain_json), 0)

		# The plan should contain a Custom Scan (Bitmap Scan) or similar
		plan = explain_json[0]["Plan"]

		# Verify the plan structure is valid
		self.assertIn("Node Type", plan)

		# Run the actual query to make sure it returns correct results
		query_result = node.execute("""
            SELECT * FROM partition_bitmap_test WHERE id BETWEEN 0 AND 100;
        """)
		self.assertEqual(len(query_result), 70)

		node.safe_psql("""
            RESET enable_seqscan;
            RESET enable_indexscan;
        """)

		node.stop()

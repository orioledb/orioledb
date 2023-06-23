#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

class IncludeIndicesTest(BaseTest):
	def test_include_index_recovery(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_include_box (
				val_1 int,
				val_4 box
			) USING orioledb;

			CREATE UNIQUE INDEX o_test_include_box_ix1
				ON o_test_include_box (val_1) INCLUDE (val_4);

			INSERT INTO o_test_include_box
				SELECT 1 * 10 ^ v, box(point(2 * 10 ^ v, 3 * 10 ^ v),
									   point(4 * 10 ^ v, 5 * 10 ^ v))
					FROM generate_series(0, 5) v;
		""")

		plan = node.execute("""
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test_include_box ORDER BY val_1;
		""")[0][0][0]["Plan"]
		self.assertEqual('Custom Scan', plan["Node Type"])
		self.assertEqual('o_test_include_box_ix1', plan['Index Name'])
		self.assertEqual(
			[(1, '(4,5),(2,3)'),
			 (10, '(40,50),(20,30)'),
			 (100, '(400,500),(200,300)'),
			 (1000, '(4000,5000),(2000,3000)'),
			 (10000, '(40000,50000),(20000,30000)'),
			 (100000, '(400000,500000),(200000,300000)')],
			node.execute("SELECT * FROM o_test_include_box ORDER BY val_1")
		)

		node.stop(['-m', 'immediate'])

		node.start()

		plan = node.execute("""
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test_include_box ORDER BY val_1;
		""")[0][0][0]["Plan"]
		self.assertEqual('Custom Scan', plan["Node Type"])
		self.assertEqual('o_test_include_box_ix1', plan['Index Name'])
		self.assertEqual(
			[(1, '(4,5),(2,3)'),
			 (10, '(40,50),(20,30)'),
			 (100, '(400,500),(200,300)'),
			 (1000, '(4000,5000),(2000,3000)'),
			 (10000, '(40000,50000),(20000,30000)'),
			 (100000, '(400000,500000),(200000,300000)')],
			node.execute("SELECT * FROM o_test_include_box ORDER BY val_1")
		)

		node.safe_psql("""
			TRUNCATE o_test_include_box;

			INSERT INTO o_test_include_box
				SELECT (v + 1), box(point(2 * (v + 1), 3 * (v + 1)),
							  point(4 * (v + 1), 5 * (v + 1)))
					FROM generate_series(0, 3) v;
		""")

		self.assertEqual(
			[(1, '(4,5),(2,3)'),
			 (2, '(8,10),(4,6)'),
			 (3, '(12,15),(6,9)'),
			 (4, '(16,20),(8,12)')],
			node.execute("SELECT * FROM o_test_include_box ORDER BY val_1")
		)

		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(
			[(1, '(4,5),(2,3)'),
			 (2, '(8,10),(4,6)'),
			 (3, '(12,15),(6,9)'),
			 (4, '(16,20),(8,12)')],
			node.execute("SELECT * FROM o_test_include_box ORDER BY val_1")
		)

		node.safe_psql("""
			DROP INDEX o_test_include_box_ix1;
		""")

		plan = node.execute("""
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test_include_box ORDER BY val_1;
		""")[0][0][0]["Plan"]
		self.assertNotEqual('Custom Scan', plan["Node Type"])
		self.assertEqual(
			[(1, '(4,5),(2,3)'),
			 (2, '(8,10),(4,6)'),
			 (3, '(12,15),(6,9)'),
			 (4, '(16,20),(8,12)')],
			node.execute("SELECT * FROM o_test_include_box ORDER BY val_1")
		)

		node.stop(['-m', 'immediate'])

		node.start()

		plan = node.execute("""
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test_include_box ORDER BY val_1;
		""")[0][0][0]["Plan"]
		self.assertNotEqual('Custom Scan', plan["Node Type"])
		self.assertEqual(
			[(1, '(4,5),(2,3)'),
			 (2, '(8,10),(4,6)'),
			 (3, '(12,15),(6,9)'),
			 (4, '(16,20),(8,12)')],
			node.execute("SELECT * FROM o_test_include_box ORDER BY val_1")
		)
		node.stop()

	def test_include_index_replication(self):
		with self.node as master:
			master.start()
			with self.getReplica().start() as replica:
				master.safe_psql("""CREATE EXTENSION orioledb;""")
				master.safe_psql("""
					CREATE TABLE o_test_expr_include_index(
						val_1 text NOT NULL,
						val_2 text
					)USING orioledb;

					CREATE UNIQUE INDEX o_test_expr_include_index_ix1
						ON o_test_expr_include_index(lower(val_1))
							INCLUDE (val_2);

					INSERT INTO o_test_expr_include_index(val_1, val_2)
						SELECT (v%4*10+v)::text, (v * 10)::text
							FROM generate_series(1, 10) v;
				""")

				plan = master.execute("""
					SET LOCAL enable_seqscan = off;
					EXPLAIN (COSTS OFF, FORMAT JSON)
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
				""")[0][0][0]["Plan"]
				if plan["Node Type"] == 'Result':
					plan = plan['Plans'][0]
				self.assertEqual('Custom Scan', plan["Node Type"])
				self.assertEqual('o_test_expr_include_index_ix1',
								 plan['Index Name'])
				self.assertEqual(
					[('11', '10'), ('15', '50'), ('19', '90'), ('22', '20'),
					 ('26', '60'), ('30', '100'), ('33', '30'), ('37', '70'),
					 ('4', '40'), ('8', '80')],
					master.execute("""
						SET LOCAL enable_seqscan = off;
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				self.catchup_orioledb(replica)

				plan = replica.execute("""
					SET LOCAL enable_seqscan = off;
					EXPLAIN (COSTS OFF, FORMAT JSON)
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
				""")[0][0][0]["Plan"]
				if plan["Node Type"] == 'Result':
					plan = plan['Plans'][0]
				self.assertEqual('Custom Scan', plan["Node Type"])
				self.assertEqual('o_test_expr_include_index_ix1',
								 plan['Index Name'])
				self.assertEqual(
					[('11', '10'), ('15', '50'), ('19', '90'), ('22', '20'),
					 ('26', '60'), ('30', '100'), ('33', '30'), ('37', '70'),
					 ('4', '40'), ('8', '80')],
					replica.execute("""
						SET LOCAL enable_seqscan = off;
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				master.safe_psql("""
					TRUNCATE o_test_expr_include_index;
				""")

				self.assertEqual(
					[],
					master.execute("""
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				self.catchup_orioledb(replica)

				self.assertEqual(
					[],
					replica.execute("""
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				master.safe_psql("""
					INSERT INTO o_test_expr_include_index(val_1, val_2)
						SELECT (v%2*10+v)::text, (v*10+v%3)::text
							FROM generate_series(1, 7) v;
				""")

				self.assertEqual(
					[('11', '11'), ('13', '30'), ('15', '52'), ('17', '71'),
					 ('2', '22'), ('4', '41'), ('6', '60')],
					master.execute("""
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				self.catchup_orioledb(replica)

				self.assertEqual(
					[('11', '11'), ('13', '30'), ('15', '52'), ('17', '71'),
					 ('2', '22'), ('4', '41'), ('6', '60')],
					replica.execute("""
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				master.safe_psql("""
					DROP INDEX o_test_expr_include_index_ix1;
				""")

				plan = master.execute("""
					SET LOCAL enable_seqscan = off;
					EXPLAIN (COSTS OFF, FORMAT JSON)
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
				""")[0][0][0]["Plan"]
				if plan["Node Type"] == 'Result':
					plan = plan['Plans'][0]
				self.assertNotEqual('Custom Scan', plan["Node Type"])
				self.assertEqual(
					[('11', '11'), ('13', '30'), ('15', '52'), ('17', '71'),
					 ('2', '22'), ('4', '41'), ('6', '60')],
					master.execute("""
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

				self.catchup_orioledb(replica)

				plan = replica.execute("""
					SET LOCAL enable_seqscan = off;
					EXPLAIN (COSTS OFF, FORMAT JSON)
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
				""")[0][0][0]["Plan"]
				if plan["Node Type"] == 'Result':
					plan = plan['Plans'][0]
				self.assertNotEqual('Custom Scan', plan["Node Type"])
				self.assertEqual(
					[('11', '11'), ('13', '30'), ('15', '52'), ('17', '71'),
					 ('2', '22'), ('4', '41'), ('6', '60')],
					replica.execute("""
						SELECT * FROM o_test_expr_include_index
							ORDER BY lower(val_1);
					""")
				)

	def test_recovery_spread_idx_with_null(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1 (
				val_1 int,
				val_2 int,
				PRIMARY KEY (val_1) INCLUDE (val_2)
			) USING orioledb;

			INSERT INTO o_test_1 SELECT x * 5, NULL
				FROM generate_series(1,10) AS x;
		""")
		self.assertEqual(
			[(5, None), (10, None), (15, None), (20, None), (25, None),
			 (30, None), (35, None), (40, None), (45, None), (50, None)],
			node.execute("""
				SELECT * FROM o_test_1 ORDER BY val_1;
			""")
		)
		node.stop(['-m','immediate'])

		node.start()
		self.assertEqual(
			[(5, None), (10, None), (15, None), (20, None), (25, None),
			 (30, None), (35, None), (40, None), (45, None), (50, None)],
			node.execute("""
				SELECT * FROM o_test_1 ORDER BY val_1;
			""")
		)
		node.stop()

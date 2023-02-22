#!/usr/bin/env python3
# coding: utf-8


from .base_test import BaseTest

class CheckpointSameTrxTest(BaseTest):
	def test_create_drop_index_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		with node.connect() as con:
			with node.connect() as con2:
				con.execute("""
					CREATE TABLE o_test_1 (
						val_1 int,
						val_2 int,
						val_3 int,
						val_4 int,
						val_5 int,
						val_6 int,
						val_7 text,
						val_8 int,
						val_9 int,
						val_10 int,
						val_11 int,
						val_12 int
					) USING orioledb;
				""")
				con.execute("""
					INSERT INTO o_test_1
						SELECT v, v+1, v+2, v+3, v+4, v+5, v+6,
							v+7, v+8, v+9, v+10, v+11
							FROM generate_series(1, 10) v;
				""")
				con.execute("""
					CREATE UNIQUE INDEX o_test_1_pkey_like_ix
						ON o_test_1 (val_1);
				""")
				con.execute("""
					CREATE UNIQUE INDEX o_test_1_ix1
						ON o_test_1 (val_1, val_2, val_3, val_4,
									val_5, val_6, val_7, val_8);
				""")
				con.execute("""
					CREATE UNIQUE INDEX o_test_1_ix2
						ON o_test_1 (val_2, val_3);
				""")
				con.execute("""
					DROP INDEX o_test_1_ix1;
				""")
				con2.execute("""
					CHECKPOINT;
				""")
				con.commit()
				con.execute("""
					SELECT * FROM o_test_1;
				""")
		node.stop(['-m', 'immediate'])
		node.start()

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test_1 ORDER BY val_1;
		""")[0][0][0]["Plan"]
		self.assertEqual('Custom Scan', plan["Node Type"])
		self.assertEqual('o_test_1_pkey_like_ix', plan['Index Name'])
		self.assertEqual([(1, 2, 3, 4, 5, 6, '7', 8, 9, 10, 11, 12),
						  (2, 3, 4, 5, 6, 7, '8', 9, 10, 11, 12, 13),
						  (3, 4, 5, 6, 7, 8, '9', 10, 11, 12, 13, 14),
						  (4, 5, 6, 7, 8, 9, '10', 11, 12, 13, 14, 15),
						  (5, 6, 7, 8, 9, 10, '11', 12, 13, 14, 15, 16),
						  (6, 7, 8, 9, 10, 11, '12', 13, 14, 15, 16, 17),
						  (7, 8, 9, 10, 11, 12, '13', 14, 15, 16, 17, 18),
						  (8, 9, 10, 11, 12, 13, '14', 15, 16, 17, 18, 19),
						  (9, 10, 11, 12, 13, 14, '15', 16, 17, 18, 19, 20),
						  (10, 11, 12, 13, 14, 15, '16', 17, 18, 19, 20, 21)],
						 node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test_1 ORDER BY val_1
						 """))

	def test_drop_index_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1 (val_1 int, val_2 int)USING orioledb;
			CREATE INDEX ON o_test_1(val_1);
			CREATE TABLE o_test_2 (LIKE o_test_1)USING orioledb;
			CREATE INDEX ON o_test_1(val_1);
			DROP INDEX o_test_1_val_1_idx;
			CHECKPOINT;
		""")
		node.stop(['-m', 'immediate'])
		node.start()
		node.stop()

	def test_drop_pkey_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1 (
				val_1 int,
				val_2 int,
				val_3 int,
				val_4 text PRIMARY KEY
			) USING orioledb;
			CREATE UNIQUE INDEX ind_1
				ON o_test_1 USING btree(val_1, val_2)
					INCLUDE (val_3, val_4);
			DROP INDEX ind_1;
			ALTER TABLE o_test_1 DROP CONSTRAINT o_test_1_pkey;
			CHECKPOINT;
		""")
		node.stop(['-m', 'immediate'])
		node.start()

	def test_drop_table_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1 (
				val_1 int,
				val_2 int,
				val_3 int,
				val_4 int,
				UNIQUE(val_1, val_2) INCLUDE(val_3, val_4)
			) USING orioledb;
			ALTER TABLE o_test_1 DROP COLUMN val_3;
			ALTER TABLE o_test_1 DROP COLUMN val_1;
			DROP TABLE o_test_1;
			CHECKPOINT;
		""")
		node.stop(['-m', 'immediate'])
		node.start()
		node.stop()

	def test_drop_expr_index_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1 (
				val_1 int,
				val_2 int
			) USING orioledb;
			CREATE INDEX ind_1 ON o_test_1 (val_1, (val_1+1));
			DROP TABLE o_test_1;
			CHECKPOINT;
		""")
		node.stop(['-m', 'immediate'])
		node.start()
		node.stop()

	def test_add_column_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			""")
		node.safe_psql("""
			CREATE TABLE o_test_1(
				val_1 text,
				UNIQUE (val_1)
			)USING orioledb;

			ALTER TABLE o_test_1 ADD COLUMN val_10 text;

			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_drop_table_checkpoint_2(self):
		node = self.node
		node.start()
		node.safe_psql("""

			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 int PRIMARY KEY,
				val_2 int
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO o_test_1 VALUES (1, 2);

			INSERT INTO o_test_2 VALUES (3, 4);

			DROP TABLE o_test_2;

			CHECKPOINT;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

		node.stop()

	def test_drop_column_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql("""

			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int,
				val_3 int,
				val_4 int
			)USING orioledb;

			CREATE UNIQUE INDEX ind_1
				ON o_test_1 (val_1, val_2) INCLUDE(val_3, val_4);

			ALTER TABLE o_test_1 DROP COLUMN val_3;

			DROP TABLE o_test_1;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int,
				val_3 int,
				val_4 int,
				UNIQUE(val_1, val_2) INCLUDE(val_3, val_4)
			)USING orioledb;

			ALTER TABLE o_test_1 DROP COLUMN val_3;

			ALTER TABLE o_test_1 DROP COLUMN val_1;

			DROP TABLE o_test_1;

			CHECKPOINT;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

		node.stop()
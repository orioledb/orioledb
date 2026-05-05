from .base_test import BaseTest
from testgres.connection import ProgrammingError
from testgres.exceptions import QueryException


class ReindexTest(BaseTest):

	def test_2(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int,
				val_3 int,
				val_4 int,
				val_5 int,
				val_6 int,
				val_7 int,
				val_8 int,
				val_9 int,
				val_10 int
			)USING orioledb;

			CREATE INDEX ind_val_1 ON o_test_1(val_1);
			CREATE INDEX ind_val_2 ON o_test_1(val_2);
			CREATE INDEX ind_val_3 ON o_test_1(val_3);
			CREATE INDEX ind_val_4 ON o_test_1(val_4);
			CREATE INDEX ind_val_5 ON o_test_1(val_5);
			CREATE INDEX ind_val_6 ON o_test_1(val_6);
			CREATE INDEX ind_val_7 ON o_test_1(val_7);
			CREATE INDEX ind_val_8 ON o_test_1(val_8);
			CREATE INDEX ind_val_9 ON o_test_1(val_9);
			CREATE INDEX ind_val_10 ON o_test_1(val_10);

			INSERT INTO o_test_1
				(SELECT val_1, val_1, val_1, val_1,
					val_1, val_1, val_1, val_1, val_1,
					val_1 FROM generate_series(1, 50) AS val_1);
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX (VERBOSE) TABLE o_test_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_3(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE INDEX ind_val_1 ON o_test_1(val_1);

			ALTER INDEX ind_val_1 RENAME TO ind_val_11;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 FROM generate_series(1, 50) AS val_1);

			ALTER INDEX ind_val_11 RENAME TO ind_val_1;
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX INDEX ind_val_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_4(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 FROM generate_series(1, 50) AS val_1);

			CREATE INDEX ind_val_1 ON o_test_1(val_1);
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX INDEX ind_val_1;")
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()


#NOTICE:  table "o_test_1" has no indexes to reindex

	def test_5(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;
		""")
		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX TABLE o_test_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_6(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 FROM generate_series(1, 50) AS val_1);

			ALTER TABLE o_test_1 ADD COLUMN val_3 int;

			CREATE INDEX ind_val_3 ON o_test_1(val_3);
		""")
		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX INDEX ind_val_3;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_7(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE IF NOT EXISTS o_test_1(
				val_1 int4 NOT NULL,
				val_2 oid NOT NULL,
				val_3 regconfig NOT NULL,
				val_4 regproc NOT NULL,
				val_5 regoper NOT NULL,
				val_6 regoperator NOT NULL,
				val_7 regrole NOT NULL,
				val_8 regprocedure NOT NULL,
				val_10 regnamespace  NOT NULL,
				val_11 regclass NOT NULL,
				val_12 regdictionary NOT NULL,
				val_13 regtype NOT NULL
			)USING orioledb;

			INSERT INTO o_test_1(val_1, val_2, val_3, val_4, val_5, val_6,
								 val_7, val_8, val_10, val_11, val_12, val_13)
						VALUES(1, 2, 'german', 'namein', '||/'::regoper,
								'=(integer,integer)', 'pg_stat_scan_tables',
								'abs(numeric)', 'information_schema',
								'pg_type', 'english_stem', 'int2vector');

			CREATE INDEX ind_val_1 ON o_test_1(val_1);
			CREATE INDEX ind_val_2 ON o_test_1(val_2);
			CREATE INDEX ind_val_3 ON o_test_1(val_3);
			CREATE INDEX ind_val_4 ON o_test_1(val_4);
			CREATE INDEX ind_val_5 ON o_test_1(val_5);
			CREATE INDEX ind_val_6 ON o_test_1(val_6);
			CREATE INDEX ind_val_7 ON o_test_1(val_7);
			CREATE INDEX ind_val_8 ON o_test_1(val_8);
			CREATE INDEX ind_val_10 ON o_test_1(val_10);
			CREATE INDEX ind_val_11 ON o_test_1(val_11);
			CREATE INDEX ind_val_12 ON o_test_1(val_12);
			CREATE INDEX ind_val_13 ON o_test_1(val_13);
		""")
		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX TABLE o_test_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_8(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1 (val_1, val_2)USING orioledb
				AS (SELECT val_1, val_1 + 100 FROM generate_series (1, 5)val_1);

			CREATE UNIQUE INDEX ind_val_1 ON o_test_1(val_1);
			CREATE UNIQUE INDEX ind_val_2 ON o_test_1(val_1);
			CREATE UNIQUE INDEX ind_val_3 ON o_test_1(val_1);
		""")
		con1 = node.connect(autocommit=True)
		con1.execute("REINDEX TABLE o_test_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_replication_reindex_secondary(self):
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_test_1(
							val_1 int PRIMARY KEY,
							val_2 int,
							val_3 text
						) USING orioledb;

						CREATE UNIQUE INDEX o_test_1_val2_idx ON o_test_1(val_2);
						CREATE INDEX o_test_1_val3_idx ON o_test_1(val_3);
						INSERT INTO o_test_1 SELECT x, 2 * x, 'test_data' || x FROM generate_series(1, 1000) x;
					""")

					con1.commit()

				master.execute("DELETE FROM o_test_1 WHERE val_1 %% 2 = 1;")
				master.execute("REINDEX TABLE o_test_1;")

				self.catchup_orioledb(replica)

				set_scan = "set enable_seqscan = {}; set enable_indexscan = {}; set enable_bitmapscan = {};"

				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('on', 'off', 'off')} SELECT * FROM o_test_1 WHERE val_2 = 202;"
				    ), [])
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')}  SELECT * FROM o_test_1 WHERE val_2 = 202;"
				    ), [])
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')}  SELECT * FROM o_test_1 WHERE val_3 = 'test_data101';"
				    ), [])

				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('on', 'off', 'off')} SELECT * FROM o_test_1 WHERE val_2 = 500;"
				    ), [(250, 500, 'test_data250')])
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')}  SELECT * FROM o_test_1 WHERE val_2 = 500;"
				    ), [(250, 500, 'test_data250')])
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')}  SELECT * FROM o_test_1 WHERE val_3 = 'test_data250';"
				    ), [(250, 500, 'test_data250')])

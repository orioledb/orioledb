import unittest
from .base_test import BaseTest
from testgres.connection import DatabaseError

class VacuumTest(BaseTest):
	def test_vacuum_parallel(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("VACUUM (PARALLEL 1) o_test_1;")
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_vacuum_disable_page_skipping(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("VACUUM (DISABLE_PAGE_SKIPPING) o_test_1;")
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_4(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

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
				PRIMARY KEY (val_1)
			)USING orioledb;

			INSERT INTO o_test_1
				(SELECT val_1, val_1, val_1, val_1,
					val_1, val_1, val_1, val_1,
					val_1  FROM generate_series(1, 50) AS val_1);

			CREATE INDEX ind_val_2 ON o_test_1(val_2);
			CREATE INDEX ind_val_3 ON o_test_1(val_3);
			CREATE INDEX ind_val_4 ON o_test_1(val_4);
			CREATE INDEX ind_val_5 ON o_test_1(val_5);
			CREATE INDEX ind_val_6 ON o_test_1(val_6);
			CREATE INDEX ind_val_7 ON o_test_1(val_7);
			CREATE INDEX ind_val_8 ON o_test_1(val_8);
			CREATE INDEX ind_val_9 ON o_test_1(val_9);
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("VACUUM (INDEX_CLEANUP) o_test_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_5(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS o_test_1(
				val_int4 int4 NOT NULL,
				val_oid oid NOT NULL,
				val_regconfig regconfig NOT NULL,
				val_regproc regproc NOT NULL,
				val_regoper regoper NOT NULL,
				val_regoperator regoperator NOT NULL,
				val_regrole regrole NOT NULL,
				val_regprocedure regprocedure NOT NULL,
				val_regnamespace regnamespace  NOT NULL,
				val_regclass regclass NOT NULL,
				val_regdictionary regdictionary NOT NULL,
				val_regtype regtype NOT NULL
			)USING orioledb;

			INSERT INTO o_test_1(val_int4, val_oid,
					val_regconfig, val_regproc, val_regoper,
					val_regoperator, val_regrole, val_regprocedure,
					val_regnamespace, val_regclass,
					val_regdictionary, val_regtype)
			VALUES(1, 2, 'german', 'namein', '||/'::regoper,
					'=(integer,integer)', 'pg_stat_scan_tables', 'abs(numeric)',
					'information_schema', 'pg_type',
					'english_stem', 'int2vector');
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("VACUUM (TRUNCATE) o_test_1;")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_6(self):
		node = self.node
		node.start()

		con1 = node.connect()
		con1.begin()
		con1.execute("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1 (val_1, val_2)USING orioledb
				AS (SELECT val_1, val_1 + 100 FROM generate_series (1, 5)val_1);

			SAVEPOINT my_savepoint;

			UPDATE o_test_1 SET val_1 = val_2 WHERE mod(val_1, 2) = 0;
			DELETE FROM o_test_1 WHERE val_1 = val_2;

			ROLLBACK TO SAVEPOINT my_savepoint;

		""")
		con1.commit()
		con1.close()
		con2 = node.connect(autocommit=True)
		con2.execute("VACUUM (FREEZE) o_test_1;")
		con2.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	@unittest.skipIf(BaseTest.get_pg_version() < 14,
					 'PROCESS_TOAST option added in postgres 14')
	def test_7(self):
		node = self.node
		node.start()
		with self.assertRaises(DatabaseError) as e:
			node.safe_psql("""
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE TABLE o_test_1 (val_1, val_2)USING orioledb
				AS (SELECT val_1, val_1 + 100 FROM generate_series (1, 5)val_1);

				UPDATE o_test_1 SET val_1 = val_2 WHERE mod(val_1, 2) = 0;
			""")

			con1 = node.connect(autocommit=True)
			con1.execute("VACUUM (PROCESS_TOAST) o_test_1(val_1);")

		self.assertErrorMessageEquals(e, "ANALYZE option must be specified when a column list is provided")
		con1.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_8(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1 (val_1, val_2)USING orioledb
			AS (SELECT val_1, val_1 + 100 FROM generate_series (1, 5)val_1)
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("VACUUM (DISABLE_PAGE_SKIPPING, ANALYZE) o_test_1(val_1);")
		con1.close()

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

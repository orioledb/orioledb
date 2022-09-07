import unittest
from .base_test import BaseTest
from testgres.exceptions import QueryException

class SchemaTest(BaseTest):

	@unittest.skipIf(BaseTest.get_pg_version() < 14,
					 'CURRENT_ROLE option added in postgres 14')
	def test_1(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1 AUTHORIZATION CURRENT_ROLE
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			ALTER SCHEMA test_schema_1 OWNER TO CURRENT_USER;

			ALTER SCHEMA test_schema_1 OWNER TO SESSION_USER;

			ALTER SCHEMA test_schema_1 OWNER TO CURRENT_ROLE;
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_2(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE OR REPLACE FUNCTION trg_func() RETURNS trigger AS
			$$BEGIN
				RAISE NOTICE 'old: (%, %), new: (%, %)', OLD.id, OLD.value, NEW.id, NEW.value;
				RETURN NEW;
			END;$$ LANGUAGE plpgsql;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb

			CREATE VIEW my_view AS
				SELECT * FROM o_test_1
				WHERE mod(val_1, 2) = 0

			CREATE INDEX o_test_1_val_2 ON test_schema_1.o_test_1 (val_2)

			CREATE SEQUENCE o_test_seq

			CREATE TRIGGER before_update_trg BEFORE UPDATE
				ON test_schema_1.o_test_1 FOR EACH ROW
				EXECUTE PROCEDURE trg_func()

			GRANT INSERT ON test_schema_1.o_test_1 TO PUBLIC;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_3(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE OR REPLACE FUNCTION trg_func() RETURNS trigger AS
			$$BEGIN
				RAISE NOTICE 'old: (%, %), new: (%, %)', OLD.id, OLD.value, NEW.id, NEW.value;
				RETURN NEW;
			END;$$ LANGUAGE plpgsql;

			CREATE SCHEMA test_schema_1;

			CREATE TABLE test_schema_1.o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE VIEW test_schema_1.my_view AS
				SELECT * FROM test_schema_1.o_test_1
				WHERE mod(val_1, 2) = 0;

			CREATE INDEX o_test_1_val_2 ON test_schema_1.o_test_1 (val_2);

			CREATE SEQUENCE test_schema_1.o_test_seq;

			CREATE TRIGGER before_update_trg BEFORE UPDATE
				ON test_schema_1.o_test_1 FOR EACH ROW
				EXECUTE PROCEDURE trg_func();

			GRANT INSERT ON test_schema_1.o_test_1 TO PUBLIC;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_4(self):
		node = self.node
		node.start()
		with self.assertRaises(QueryException) as e:
			node.safe_psql("""
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE SCHEMA test_schema_1
					CREATE TABLE o_test_1(
					val_1 int,
					val_2 int
				)USING orioledb;

				DROP SCHEMA test_schema_1 RESTRICT;
			""")
		self.assertEqual(e.exception.message,
						 "ERROR:  cannot drop schema test_schema_1 because other objects depend on it\n" +
						 "DETAIL:  table test_schema_1.o_test_1 depends on schema test_schema_1\n"
						 "HINT:  Use DROP ... CASCADE to drop the dependent objects too.\n")

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_5(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_6(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			ALTER SCHEMA test_schema_1 RENAME TO test_schema_11;
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_7(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

			UPDATE test_schema_1.o_test_1 SET val_1 = val_1 + 100;

			DELETE FROM test_schema_1.o_test_1 WHERE mod(val_2, 2) = 0;

			ALTER TABLE test_schema_1.o_test_1 RENAME val_1 TO val_11;
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	@unittest.skipIf(BaseTest.get_pg_version() < 15,
					 'MERGE command added in postgres 15')
	def test_8(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE SCHEMA test_schema_2
				CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

			INSERT INTO test_schema_2.o_test_2 (val_3, val_4)
				(SELECT val_3, val_3 + 100 FROM generate_series (3, 7) val_3);

			MERGE INTO test_schema_1.o_test_1
				USING test_schema_2.o_test_2
				ON test_schema_1.o_test_1.val_1 = test_schema_2.o_test_2.val_3
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_2 + 50;
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	@unittest.skipIf(BaseTest.get_pg_version() < 15,
					 'MERGE command added in postgres 15')
	def test_9(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE SCHEMA test_schema_2
				CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

			INSERT INTO test_schema_2.o_test_2 (val_3, val_4)
				(SELECT val_3, val_3 + 100 FROM generate_series (3, 7) val_3);

			MERGE INTO test_schema_1.o_test_1
				USING test_schema_2.o_test_2
				ON test_schema_1.o_test_1.val_1 = test_schema_2.o_test_2.val_3
			WHEN MATCHED THEN
				DELETE;
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	@unittest.skipIf(BaseTest.get_pg_version() < 15,
					 'MERGE command added in postgres 15')
	def test_10(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE SCHEMA test_schema_1
				CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE SCHEMA test_schema_2
				CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO test_schema_1.o_test_1 (val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (1, 5) val_1);

			INSERT INTO test_schema_2.o_test_2 (val_3, val_4)
				(SELECT val_3, val_3 + 100 FROM generate_series (3, 7) val_3);

			MERGE INTO test_schema_1.o_test_1
				USING test_schema_2.o_test_2
				ON test_schema_1.o_test_1.val_1 = test_schema_2.o_test_2.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333);
		""")
		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()
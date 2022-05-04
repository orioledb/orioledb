from .base_test import BaseTest, ThreadQueryExecutor
from testgres.exceptions import QueryException

class MergeIntoTest(BaseTest):

	def assertTblCount(self, size):
		self.assertEqual(size,
						 self.node.execute('postgres',
										   'SELECT count(*) FROM orioledb_table_oids();')[0][0])

	def test_1(self):

		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
					val_1 int,
					val_2 int
				)USING orioledb;

				CREATE TABLE o_test_2(
					val_3 int,
					val_4 int
				)USING orioledb;
			
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 15) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (1, 10) val_3);	

			INSERT INTO o_test_1(val_1)
				(SELECT val_1 FROM generate_series (30, 35) val_1);

			ALTER TABLE o_test_2 DROP COLUMN val_4;
		""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100), (2, 200),
			(3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900),
			(10, 1000), (11, 1100), (12, 1200), (13, 1300), (14, 1400), (15, 1500),
			(30, None), (31, None), (32, None), (33, None), (34, None), (35, None)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(1,), (2,),
			(3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
		
		node.safe_psql('postgres', """
			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333)
			WHEN MATCHED THEN
				DELETE;
		""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(11, 1100), (12, 1200),
			(13, 1300), (14, 1400), (15, 1500), (30, None), (31, None), (32, None),
			(33, None), (34, None), (35, None)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(1,), (2,),
			(3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])

		node.safe_psql('postgres', """
			INSERT INTO o_test_1(val_1)
				(SELECT val_1 FROM generate_series (1, 11) val_1);
			
			UPDATE o_test_1 SET val_2 = val_2 + 100 WHERE val_2 = 333;
		""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(11, 1100), (12, 1200),
			(13, 1300), (14, 1400), (15, 1500), (30, None), (31, None), (32, None),
			(33, None), (34, None), (35, None), (1, None), (2, None), (3, None),
			(4, None), (5, None), (6, None), (7, None), (8, None), (9, None),
			(10, None), (11, None)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(1,), (2,),
			(3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
		
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(11, 1100), (12, 1200),
			(13, 1300), (14, 1400), (15, 1500), (30, None), (31, None), (32, None),
			(33, None), (34, None), (35, None), (1, None), (2, None), (3, None),
			(4, None), (5, None), (6, None), (7, None), (8, None), (9, None),
			(10, None), (11, None)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(1,), (2,),
			(3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
		
		node.stop()
	
	def test_2(self):

		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
					val_1 int,
					val_2 int
				)USING orioledb;

				CREATE TABLE o_test_2(
					val_3 int,
					val_4 int
				)USING orioledb;
			
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (40, 50) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (45, 55) val_3);	
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(40, 4000), (41, 4100),
			(42, 4200), (43, 4300), (44, 4400), (45, 4500), (46, 4600), (47, 4700),
			(48, 4800), (49, 4900), (50, 5000)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(45, 9000), (46, 9200),
			(47, 9400), (48, 9600), (49, 9800), (50, 10000), (51, 10200), (52, 10400), 
			(53, 10600), (54, 10800), (55, 11000)])

		node.safe_psql('postgres', """

			ALTER TABLE o_test_2 DROP COLUMN val_4;

			UPDATE o_test_1 SET val_1 = 100 WHERE val_1 % 10 = 0;

			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333)
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_1 + val_2;

			ALTER TABLE o_test_1 DROP COLUMN val_2;

			UPDATE o_test_1 SET val_1 = 200 WHERE val_1 % 10 = 0; 
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(200,), (41,),
			(42,), (43,), (44,), (45,), (46,), (47,), (48,), (49,), (200,), (None,),
			(None,), (None,), (None,), (None,), (None,)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(45,), (46,),
			(47,), (48,), (49,), (50,), (51,), (52,), (53,), (54,), (55,)])

		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(200,), (41,),
			(42,), (43,), (44,), (45,), (46,), (47,), (48,), (49,), (200,), (None,),
			(None,), (None,), (None,), (None,), (None,)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(45,), (46,),
			(47,), (48,), (49,), (50,), (51,), (52,), (53,), (54,), (55,)])

		node.stop()

	def test_3(self):

		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
					val_1 int,
					val_2 int
				)USING orioledb;

				CREATE TABLE o_test_2(
					val_3 int,
					val_4 int
				)USING orioledb;
			
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (90, 105) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (11, 16) val_3);
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(90, 9000), (91, 9100), 
			(92, 9200), (93, 9300), (94, 9400), (95, 9500), (96, 9600), (97, 9700), (98, 9800), 
			(99, 9900), (100, 10000), (101, 10100), (102, 10200),
			(103, 10300), (104, 10400), (105, 10500)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(11, 2200), (12, 2400),
			(13, 2600), (14, 2800), (15, 3000), (16, 3200)])

		node.safe_psql('postgres', """
			ALTER TABLE o_test_2 DROP COLUMN val_4;

			ALTER TABLE o_test_2 ADD COLUMN val_4 int;

			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333)
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_1 + val_2
			WHEN MATCHED THEN
				DELETE;

			INSERT INTO o_test_2(val_4)
				(SELECT val_4 FROM generate_series (1, 5) val_4);

			ALTER TABLE o_test_2 DROP COLUMN val_4;
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(90, 9000), (91, 9100), 
			(92, 9200), (93, 9300), (94, 9400), (95, 9500), (96, 9600), (97, 9700), (98, 9800), 
			(99, 9900), (100, 10000), (101, 10100), (102, 10200),
			(103, 10300), (104, 10400), (105, 10500), (None, 333),
			(None, 333), (None, 333), (None, 333), (None, 333), (None, 333)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(11,), (12,),
			(13,), (14,), (15,), (16,), (None,),(None,),(None,), (None,),(None,)])
		
		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(90, 9000), (91, 9100), 
			(92, 9200), (93, 9300), (94, 9400), (95, 9500), (96, 9600), (97, 9700), (98, 9800), 
			(99, 9900), (100, 10000), (101, 10100), (102, 10200),
			(103, 10300), (104, 10400), (105, 10500), (None, 333),
			(None, 333), (None, 333), (None, 333), (None, 333), (None, 333)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [(11,), (12,),
			(13,), (14,), (15,), (16,), (None,),(None,),(None,), (None,),(None,)])

		node.stop()
	
	def test_4(self):

		node = self.node
		node.start()

		with self.assertRaises(QueryException) as e:
			node.safe_psql('postgres', """
				CREATE EXTENSION orioledb;

				CREATE TABLE o_test_1(
						val_1 int,
						val_2 int
					) USING orioledb;

					CREATE TABLE o_test_2(
						val_3 int,
						val_4 int
					) USING orioledb;
				
				INSERT INTO o_test_1(val_1, val_2)
					(SELECT val_1, val_1 + 100 FROM generate_series (1, 115) val_1);

				INSERT INTO o_test_2(val_3, val_4)
					(SELECT val_3, val_3 + 200 FROM generate_series (1, 50) val_3);	

				INSERT INTO o_test_2(val_3, val_4)
					(SELECT val_3, val_3 + 25 FROM generate_series (1, 50) val_3);

				UPDATE o_test_2 SET val_4 = 10 WHERE val_4 > 100;

				MERGE INTO o_test_1 t
				USING o_test_2 s
				ON t.val_1 = s.val_3
				WHEN MATCHED THEN
					DELETE
				WHEN MATCHED THEN
					UPDATE SET val_2 = val_1 + val_2;
				""")
		self.assertEqual(e.exception.message,
						 "ERROR:  MERGE command cannot affect row a second time\n" +
						 "HINT:  Ensure that not more than one source row matches any one target row.\n")

		node.stop(['-m', 'immediate'])

		node.start()
		node.stop()

	def test_5(self):

		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
					val_1 int,
					val_2 int
				)USING orioledb;

				CREATE TABLE o_test_2(
					val_3 int,
					val_4 int
				)USING orioledb;
			
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 + 100 FROM generate_series (2, 5) val_1);

			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 + 200 FROM generate_series (1, 5) val_3);	

			ALTER TABLE o_test_2 RENAME COLUMN val_3 to val_33;

			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_33
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333)
			WHEN MATCHED THEN
				DELETE
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_1 + val_2;

			CREATE TABLE o_test_3(
					val_5 int,
					val_6 int
				)USING orioledb;
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [
			(None,333)])
		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(1,201), (2, 202), (3, 203), (4, 204), (5, 205)])
		self.assertEqual(node.execute("SELECT * FROM o_test_3"), [])

		self.assertTblCount(3)

		node.safe_psql('postgres', """

			INSERT INTO o_test_3(val_5, val_6)
				(SELECT val_5, val_5 + 200 FROM generate_series (1, 4) val_5);	
			
			UPDATE o_test_2 SET val_4 = val_4 * 10 WHERE val_4 % 10 = 0;

			DROP TABLE o_test_2;

			MERGE INTO o_test_1 k
			USING o_test_3 p
			ON k.val_2 = p.val_6
			WHEN MATCHED THEN
				UPDATE SET val_1 = val_1 + val_2
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (3)
			WHEN MATCHED THEN
				DELETE;
			""")

		self.assertTblCount(2)

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [
			(None, 333), (None, 3), (None, 3), (None, 3), (None, 3)])

		self.assertEqual(node.execute("SELECT * FROM o_test_3"), [
			(1,201), (2,202), (3,203), (4,204)])

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(2)

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [
			(None, 333), (None, 3), (None, 3), (None, 3), (None, 3)])

		self.assertEqual(node.execute("SELECT * FROM o_test_3"), [
			(1,201), (2,202), (3,203), (4,204)])

		node.stop()
	
	def test_6(self):
		node = self.node
		node.start()
		
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int,
				PRIMARY KEY (val_1)
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 9) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (3, 8) val_3);

			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333)
			WHEN MATCHED THEN
				UPDATE SET val_2 = 10 + val_2;	
			""")
		
		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [
			(1, 100), (2, 200), (3, 310), (4, 410), (5, 510), (6, 610), 
			(7, 710), (8, 810), (9, 900)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(3, 600), (4, 800), (5, 1000), (6, 1200), (7, 1400), (8, 1600)])

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [
			(1, 100), (2, 200), (3, 310), (4, 410), (5, 510), (6, 610), 
			(7, 710), (8, 810), (9, 900)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(3, 600), (4, 800), (5, 1000), (6, 1200), (7, 1400), (8, 1600)])

		node.stop()

	def test_7(self):
		node = self.node
		node.start()
		
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 5) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (3, 8) val_3);
			""")
		
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()
		
		con1.execute("""
			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333)
			WHEN MATCHED THEN
				DELETE
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_1 + val_2;
		""")
		t2 = ThreadQueryExecutor(con2, """
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 5) val_1);
			
			UPDATE o_test_2 SET val_4 = val_4 + 50;
		""")
		t2.start()
		con1.commit()
		t2.join()
		con1.close()

		t3 = ThreadQueryExecutor(con3, """
			ALTER TABLE o_test_1 ADD COLUMN val_11 int;

			ALTER TABLE o_test_1 ADD COLUMN val_22 int;

			INSERT INTO o_test_1(val_11, val_22)
				(SELECT val_11, val_11 * 100 FROM generate_series (1, 5) val_11);

			ALTER TABLE o_test_1 DROP COLUMN val_22;
		""")
		t3.start()
		con2.commit()
		t3.join()
		con3.commit()
		con2.close()
		con3.close()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100, None),(2, 200, None), 
			(None, 333, None),(None, 333, None),(None, 333, None),(1, 100, None),(2, 200, None),(3, 300, None), 
			(4, 400, None),(5, 500, None),(None, None, 1),(None, None, 2),(None, None, 3),
			(None, None, 4),(None, None, 5)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(3,650), (4,850), (5,1050), (6,1250), (7,1450), (8,1650)])
			
		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100, None),(2, 200, None), 
			(None, 333, None),(None, 333, None),(None, 333, None),(1, 100, None),(2, 200, None),(3, 300, None), 
			(4, 400, None),(5, 500, None),(None, None, 1),(None, None, 2),(None, None, 3),
			(None, None, 4),(None, None, 5)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(3,650), (4,850), (5,1050), (6,1250), (7,1450), (8,1650)])

		node.stop()

	def test_8(self):
		node = self.node
		node.start()
		
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_3 text,
				val_4 text
			)USING orioledb;

			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 5) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (3, 8) val_3);	
			""")

		con1 = node.connect()
		con2 = node.connect()

		con1.execute("""
			ALTER TABLE o_test_2 DROP COLUMN val_3;
			ALTER TABLE o_test_2 ADD COLUMN val_3 int;

			ALTER TABLE o_test_2 DROP COLUMN val_4;
			ALTER TABLE o_test_2 ADD COLUMN val_4 int;
		""")

		t2 = ThreadQueryExecutor(con2,"""
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (3, 8) val_3);

			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN MATCHED THEN
				DELETE
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_1 + val_2;
		""")
		t2.start()
		con1.commit()
		t2.join()

		con2.execute("""
			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333);
		""")

		con2.commit()
		con1.close()
		con2.close()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100), (2, 200), 
			(3, 300), (4, 400), (5, 500), (None, 333),(None, 333), (None, 333),
			 (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 333)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(None,None), (None,None), (None,None), (None,None), (None,None),
			(None,None), (None,None), (None,None), (None,None), (None,None),
			(None,None), (None,None)])

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100), (2, 200), 
			(3, 300), (4, 400), (5, 500), (None, 333),(None, 333), (None, 333),
			 (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 333)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(None,None), (None,None), (None,None), (None,None), (None,None),
			(None,None), (None,None), (None,None), (None,None), (None,None),
			(None,None), (None,None)])

		node.stop()

	def test_9(self):
		node = self.node
		node.start()
		
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;

			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 5) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (3, 8) val_3);	
			""")

		con1 = node.connect()
		con2 = node.connect()

		con1.execute("""
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 5) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (3, 8) val_3);	
			
			ALTER TABLE o_test_2 DROP COLUMN val_4;
		""")

		t2 = ThreadQueryExecutor(con2,"""
			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN MATCHED THEN
				DELETE
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (333);
		""")
		t2.start()
		con1.commit()
		t2.join()

		con2.execute("""
			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON t.val_1 = s.val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (val_3)
			WHEN MATCHED THEN
				DELETE;
		""")

		con2.commit()
		con1.close()
		con2.close()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100), (2, 200), 
			(1, 100), (2, 200), (None, 333), (None, 333),(None, 333), (None, 333),
			 (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 3), (None, 3),(None, 4), (None, 4),
			  (None, 5), (None, 5),(None, 6), (None, 6),
			  (None, 7), (None, 7),(None, 8), (None, 8)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(3,), (4,), (5,), (6,), (7,), (8,), (3,), (4,), (5,), (6,), (7,), (8,)])

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100), (2, 200), 
			(1, 100), (2, 200), (None, 333), (None, 333),(None, 333), (None, 333),
			 (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 333), (None, 333),(None, 333), (None, 333),
			  (None, 3), (None, 3),(None, 4), (None, 4),
			  (None, 5), (None, 5),(None, 6), (None, 6),
			  (None, 7), (None, 7),(None, 8), (None, 8)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(3,), (4,), (5,), (6,), (7,), (8,), (3,), (4,), (5,), (6,), (7,), (8,)])

		node.stop()

	def test_10(self):

		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_3 int,
				val_4 int
			)USING orioledb;
			
			INSERT INTO o_test_1(val_1, val_2)
				(SELECT val_1, val_1 * 100 FROM generate_series (1, 11) val_1);
			INSERT INTO o_test_2(val_3, val_4)
				(SELECT val_3, val_3 * 200 FROM generate_series (1, 5) val_3);	

			MERGE INTO o_test_2 s
			USING o_test_1 t
			ON val_3 = val_2
			WHEN NOT MATCHED THEN
				INSERT (val_4) VALUES (100)
			WHEN MATCHED THEN
				UPDATE SET val_3 = val_2 + val_4
			WHEN MATCHED THEN
				DELETE;
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 100), (2, 200), 
			(3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), 
			(10, 1000), (11, 1100)])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(1,200), (2, 400), (3, 600), (4, 800), (5, 1000), (None, 100), (None, 100),
			(None, 100), (None, 100), (None, 100), (None, 100), (None, 100), (None, 100),
			(None, 100), (None, 100), (None, 100)])


		node.safe_psql('postgres', """
			UPDATE o_test_1 SET val_1 = val_1 + 10 WHERE val_1 % 10 = 0;
			UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_2 % 20 = 0;
			UPDATE o_test_2 SET val_3 = val_3 + 30 WHERE val_3 % 30 = 0;
			UPDATE o_test_2 SET val_4 = val_4 + 40 WHERE val_4 % 40 = 0;

			MERGE INTO o_test_1 t
			USING o_test_2 s
			ON val_1 = val_3
			WHEN NOT MATCHED THEN
				INSERT (val_2) VALUES (10)
			WHEN MATCHED THEN
				UPDATE SET val_2 = val_1 + val_2
			WHEN MATCHED THEN
				DELETE;
			""")

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 121), (2, 222), 
			(3, 323), (4, 424), (5, 525), (6, 620), (7, 720), (8, 820), (9, 920), 
			(20, 1020), (11, 1120), (None, 10), (None, 10), (None, 10), (None, 10),
			(None, 10), (None, 10), (None, 10), (None, 10), (None, 10), (None, 10), (None, 10),])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(1,240), (2, 440), (3, 640), (4, 840), (5, 1040), (None, 100), (None, 100),
			(None, 100), (None, 100), (None, 100), (None, 100), (None, 100), (None, 100),
			(None, 100), (None, 100), (None, 100)])

		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1"), [(1, 121), (2, 222), 
			(3, 323), (4, 424), (5, 525), (6, 620), (7, 720), (8, 820), (9, 920), 
			(20, 1020), (11, 1120), (None, 10), (None, 10), (None, 10), (None, 10),
			(None, 10), (None, 10), (None, 10), (None, 10), (None, 10), (None, 10), (None, 10),])

		self.assertEqual(node.execute("SELECT * FROM o_test_2"), [
			(1,240), (2, 440), (3, 640), (4, 840), (5, 1040), (None, 100), (None, 100),
			(None, 100), (None, 100), (None, 100), (None, 100), (None, 100), (None, 100),
			(None, 100), (None, 100), (None, 100)])

		node.stop()




		
		
	
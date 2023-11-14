import unittest
import testgres
import time
import os
import re
import subprocess
import time
from testgres.enums import NodeStatus
from testgres.exceptions import QueryException
from testgres import get_new_node
from testgres import BackupException
from .base_test import ThreadQueryExecutor

from .base_test import BaseTest

def catchup_orioledb(replica):
	replica.catchup()
	replica.poll_query_until("SELECT orioledb_recovery_synchronized();", expected = True)

class ReplicationTest(BaseTest):

	def get_tbl_count(self, node):
		return node.execute('postgres',
			'SELECT count(*) FROM orioledb_table_oids();')[0][0]
	
	def has_only_one_relnode(self, node):
		orioledb_files = self.get_orioledb_files(node)
		oid_list = [re.match(r'(\d+_\d+).*', x).group(1) for x
					in orioledb_files]
		if len(list(set(oid_list))) != 1:
			print(oid_list)
		return len(list(set(oid_list))) == 1

	def get_orioledb_files(self, node):
		orioledb_dir = node.data_dir + "/orioledb_data"
		all_files = []
		for f in os.listdir(orioledb_dir):
			m = re.match(r'(\d)+_(\d)+.*', f)
			if m and int(m.group(1)) > 1:
				# do not check o_tables BTree files
				all_files.append(f)
		return all_files

	def all_tables_dropped(self, node):
		return len(self.get_orioledb_files(node)) == 0
	

	def test_1(self):
		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(
							val_1 int, 
							val_2 int, 
							val_3 int
						)PARTITION BY RANGE (val_1);
						
						CREATE INDEX ind_1 
							ON o_test_1 (val_1);
						
						CREATE INDEX ind_2 
							ON o_test_1 (val_2, val_3);
						
						CREATE TABLE o_test_2 
							(LIKE o_test_1)USING orioledb;

					""")
					con1.execute("""
						
						DROP INDEX ind_1, ind_2;

						ALTER TABLE o_test_1 
							ATTACH PARTITION o_test_2 
								FOR VALUES FROM (0) TO (10000);

						CREATE INDEX ind_1 
							ON ONLY o_test_1 (val_3);

						CREATE INDEX ind_2 
							ON o_test_2 (val_3);

						ALTER INDEX ind_1 
							ATTACH PARTITION ind_2;
										
					""")

					con1.commit()
					
					self.assertEqual(master.execute("TABLE o_test_2"),[])
					self.catchup_orioledb(replica)
					self.assertEqual(replica.execute("TABLE o_test_2"),[])

					

	def test_2(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(
							a int,
							b int
						)PARTITION BY RANGE  (a);
						
						CREATE TABLE o_test_2(
							a int, 
							b int PRIMARY KEY
						)USING orioledb;

						INSERT INTO o_test_2 (a, b)
							(SELECT a, a * 5 FROM generate_series (1, 3) a);

					""")

					con1.execute("""
	
						CREATE INDEX ON o_test_2 (a) WHERE b > 0;
						CREATE INDEX ON o_test_2 ((a + 1));
						CREATE INDEX ON o_test_2 (a, a);
						CREATE INDEX ON o_test_2 (a);
						
						ALTER TABLE o_test_1 
							ATTACH PARTITION o_test_2 
								FOR VALUES FROM (0) to (10000);
						
					""")
				
					con1.commit()

					self.assertEqual([(1, 5), (2, 10), (3, 15)], master.execute("""
									SELECT * FROM o_test_2;"""))
					

					catchup_orioledb(replica)	
					replica.safe_psql('CHECKPOINT;')
					replica.stop()
					replica.start()	

					self.assertEqual([(1, 5), (2, 10), (3, 15)], master.execute("""
									SELECT * FROM o_test_2;"""))
					

					con1.begin()
					con1.execute("""
						
						DROP TABLE o_test_1, o_test_2;
					
					""")

					con1.commit()

					catchup_orioledb(replica)
					replica.safe_psql('CHECKPOINT;')
					self.assertEqual(0, self.get_tbl_count(replica))
	
	def test_3(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;
						
						CREATE TABLE o_test_1(
							val_1 int, 
							val_2 int, 
							val_3 int
						)USING orioledb;

						INSERT INTO o_test_1(val_1, val_2, val_3)
							(SELECT val_1 + 10, val_1 - 4, val_1 FROM generate_series (1, 3) val_1);
						
						ALTER TABLE o_test_1 
							ADD PRIMARY KEY (val_1);

						CREATE INDEX ind_1 
							ON o_test_1 (val_3);
						CREATE INDEX ind_2 
							ON o_test_1 ((val_1 + val_3)) WHERE val_3 > 0;
						CREATE INDEX ind_3 
							ON o_test_1 ((val_3 + val_2)) WHERE val_3 > 1;
						CREATE INDEX ind_4 
							ON o_test_1 ((val_3 + val_3));

						ALTER TABLE o_test_1
							DROP CONSTRAINT o_test_1_pkey;	
						ALTER TABLE o_test_1 
							DROP COLUMN val_1, DROP COLUMN val_2;
					""")
				
					con1.commit()

					self.assertEqual([(1,), (2,), (3,)], master.execute("""
									SELECT * FROM o_test_1;"""))

					catchup_orioledb(replica)	

					self.assertEqual(1, self.get_tbl_count(replica))
					self.assertEqual([(1,), (2,), (3,)], master.execute("""
									SELECT * FROM o_test_1;"""))

	def test_4(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;
						
						CREATE TABLE o_test_1(
							a int PRIMARY KEY
						)USING orioledb;

						INSERT INTO o_test_1(a)
							(SELECT a - 10 FROM generate_series (1, 15) a);

						CREATE UNIQUE INDEX ind_1 
							ON o_test_1(a) INCLUDE (a);
						CREATE UNIQUE INDEX ind_2 
							ON o_test_1(a);

						TRUNCATE o_test_1;

						ALTER TABLE o_test_1 
							DROP COLUMN a CASCADE;
												
					""")
				
					con1.commit()

					self.assertEqual([], master.execute("""
									SELECT * FROM o_test_1;"""))

					catchup_orioledb(replica)	

					replica.safe_psql("SELECT * FROM o_test_1;")

					self.assertEqual([], master.execute("""
										SELECT * FROM o_test_1;"""))
					self.assertEqual(1, self.get_tbl_count(replica))



	def test_5(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;
						
						CREATE TABLE o_test_1(
							a int, 
							b int
						)USING orioledb;

						CREATE UNIQUE INDEX ind_1 
							ON o_test_1 (a) INCLUDE (a);

						INSERT INTO o_test_1 (a, b)
							(SELECT a, a - 10 FROM generate_series (1, 5) a);
						
						CREATE TABLE o_test_2()INHERITS (o_test_1)USING orioledb;

						ALTER TABLE o_test_1 
							ADD PRIMARY KEY USING INDEX ind_1;

						ALTER TABLE o_test_1 
							DROP CONSTRAINT ind_1;

					""")
				
					con1.commit()

					catchup_orioledb(replica)

					
	def test_6(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;
						
						CREATE TABLE o_test_p1(
							a int, 
							b text, 
							c text
						) PARTITION BY LIST (a);
						CREATE TABLE o_test_p2(
							a int,
							b text, 
							c text
						) PARTITION BY LIST (b);
						CREATE TABLE o_test_1(
							a int,
							b text, 
							c text
						)USING orioledb;

						CREATE INDEX ON o_test_p1 (left(c, 2));

						INSERT INTO o_test_1(a,b,c)
							VALUES (1, 'qwe', 'rty');

						ALTER TABLE o_test_p1 
							ATTACH PARTITION o_test_p2 
								FOR VALUES IN (1);

						CREATE INDEX ON o_test_p1 (right(c, 3));
						
					""")
				
					con1.commit()
					self.assertEqual([(1, 'qwe', 'rty')], master.execute("""
									SELECT * FROM o_test_1;"""))

					catchup_orioledb(replica)	
					self.assertEqual([(1, 'qwe', 'rty')], master.execute("""
									SELECT * FROM o_test_1;"""))
					self.assertEqual(1, self.get_tbl_count(replica))

	def test_7(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(
				  			a int, 
				  			b int
						)USING orioledb;
				  
						CREATE TABLE o_test_2()INHERITS (o_test_1)USING orioledb;

						CREATE UNIQUE INDEX ind_1 ON o_test_1 (b);
				  
						ALTER TABLE o_test_1 
							ADD PRIMARY KEY USING INDEX ind_1;
				  
						ALTER INDEX ind_1 RENAME TO ind_2;
				  
						CREATE TABLE o_test_3(
				  			a int
						)USING orioledb;
				  
				  		CREATE UNIQUE INDEX ind_1 ON o_test_3(a);

					""")
				
					con1.commit()
					catchup_orioledb(replica)	
					self.assertEqual(master.execute("TABLE o_test_1"),[])	
					self.assertEqual(master.execute("TABLE o_test_2"),[])
					self.assertEqual(master.execute("TABLE o_test_3"),[])
					self.assertEqual(3, self.get_tbl_count(master))
					

	def test_8(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(
				  			val_1 int
						)USING orioledb;
						
				  		CREATE TABLE o_test_2(check(
				  			val_1 = 4 or val_1 = null)
				  		)INHERITS(o_test_1)USING orioledb;
				  
						CREATE INDEX ON o_test_1(val_1);
				  		CREATE INDEX ON o_test_2(val_1);
						
				  		INSERT INTO o_test_2 VALUES(1);
						INSERT INTO o_test_2 VALUES(2);
						INSERT INTO o_test_2 VALUES(null);
				  
				  		ALTER TABLE o_test_2 ADD COLUMN val_2 int;
				  
				  		CREATE INDEX ON o_test_2 (val_1, val_2);
						CREATE UNIQUE INDEX ON o_test_2 (val_1, val_2) INCLUDE (val_2, val_2);					
				  
						UPDATE o_test_1 SET val_1 = val_1 + 1;
						UPDATE o_test_1 SET val_1 = val_1 + 2;
						UPDATE o_test_1 SET val_1 = val_1 + 3;

						DROP TABLE o_test_1 CASCADE;

					""")
				
					con1.commit()
					catchup_orioledb(replica)	
					self.assertEqual(0, self.get_tbl_count(master))

					
	
	def test_9(self):

		node = self.node
		node.start()
		
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					
					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(
							a int PRIMARY KEY, 
							b int
						)USING orioledb;

						INSERT INTO o_test_1 
							VALUES (1, 0), (4, 5), (6, 10), (8, 3);

						CREATE TABLE o_test_2(
							b int,
							a int REFERENCES o_test_1(a) ON DELETE CASCADE
						)USING orioledb;

						TRUNCATE TABLE o_test_1 CASCADE;

						DROP TABLE o_test_2;
						
					""")
				
					con1.commit()
					self.assertEqual(master.execute("TABLE o_test_1"),[])
					self.assertEqual(1, self.get_tbl_count(master))
					catchup_orioledb(replica)
					self.assertEqual(master.execute("TABLE o_test_1"),[])	
					self.assertEqual(1, self.get_tbl_count(master))



	def test_10(self):
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
				ON o_test_1 USING btree(val_1, val_2, val_3, val_4);
							 
			CREATE INDEX ind_2 
				ON o_test_1 (val_1, (val_1+0)) INCLUDE (val_2);
			
			INSERT INTO o_test_1 
				SELECT a, 2*a, 3*a, 4*a
				 	FROM generate_series(1,5) AS a;
							 
			CREATE INDEX ON o_test_1 USING btree(val_1, val_2) INCLUDE (val_3, val_4);
			CREATE INDEX ON o_test_1 USING btree(val_1, val_2) INCLUDE (val_3, val_4);
				 
			ALTER TABLE o_test_1 DROP COLUMN val_3;
			
			CHECKPOINT;
	
		""")
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.safe_psql("TABLE o_test_1"),b'1|2|4\n2|4|8\n3|6|12\n4|8|16\n5|10|20\n')	
		node.stop()


	def test_11(self):
		node = self.node
		node.start()

		node.safe_psql("""

			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 int PRIMARY KEY, 
				val_2 text
			)USING orioledb;
			
			INSERT INTO o_test_1(val_1, val_2)
        		VALUES (1, 'a'), (2, 'b'), (3, 'c');

			CREATE UNIQUE INDEX ind_1 ON o_test_1(val_1, val_2);
			CREATE UNIQUE INDEX ind_2 ON o_test_1(val_1, val_2 collate "C");
			CREATE UNIQUE INDEX ind_3 ON o_test_1(val_1, val_2 collate "C");
			CREATE UNIQUE INDEX ind_4 ON o_test_1(val_1, lower(val_2) collate "C");

			CHECKPOINT;
	
		""")
		self.assertEqual(node.safe_psql("TABLE o_test_1"),b'1|a\n2|b\n3|c\n')	
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.safe_psql("TABLE o_test_1"),b'1|a\n2|b\n3|c\n')	
		node.stop()


	def test_12(self):
		node = self.node
		node.start()

		node.safe_psql("""

			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE o_test_1(
				val_1 text,
				val_2 float,
				val_3 int		
			)USING orioledb;

			CREATE TABLE o_test_2(
				val_4 char(2)
			)INHERITS (o_test_1)USING orioledb;

			CREATE UNIQUE INDEX ON o_test_1 (val_1);
			CREATE UNIQUE INDEX ON o_test_2 (val_1);

			INSERT INTO o_test_1 VALUES ('abc', 6.3, 6);
			INSERT INTO o_test_1 VALUES ('qwe', 2.5555, 88);
			INSERT INTO o_test_1 VALUES ('rty', 0, 0);
			INSERT INTO o_test_2 VALUES ('zxc', 6.65, 6, 'ab');
			INSERT INTO o_test_2 VALUES ('tyu', 9.004, 2345, 'a');
	
			CHECKPOINT;
	
		""")	
		self.assertEqual(node.safe_psql("TABLE o_test_1"),b'abc|6.3|6\nqwe|2.5555|88\nrty|0|0\nzxc|6.65|6\ntyu|9.004|2345\n')
		self.assertEqual(node.safe_psql("TABLE o_test_2"),b'zxc|6.65|6|ab\ntyu|9.004|2345|a \n')
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.safe_psql("TABLE o_test_1"),b'abc|6.3|6\nqwe|2.5555|88\nrty|0|0\nzxc|6.65|6\ntyu|9.004|2345\n')	
		self.assertEqual(node.safe_psql("TABLE o_test_2"),b'zxc|6.65|6|ab\ntyu|9.004|2345|a \n')
		node.stop()


					
				
		
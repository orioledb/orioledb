from .base_test import BaseTest

class SetDataTypeTest(BaseTest):			 	
	def assertTblCount(self, size):
		self.assertEqual(size,
						 self.node.execute('postgres',
										   'SELECT count(*) FROM orioledb_table_oids();')[0][0])
	def test_to_int(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_int4 int4 NOT NULL,
				val_oid oid NOT NULL,
				val_regconfig regconfig NOT NULL,
				val_regproc regproc NOT NULL,
				val_regoper regoper NOT NULL,
				val_regoperator regoperator NOT NULL,
				val_regrole regrole NOT NULL,
				val_regprocedure regprocedure NOT NULL,
				val_regcollation regcollation NOT NULL,
				val_regnamespace regnamespace  NOT NULL,
				val_regclass regclass NOT NULL,
				val_regdictionary regdictionary NOT NULL,
				val_regtype regtype NOT NULL
			) USING orioledb;

			INSERT INTO o_test(val_int4, val_oid,
				val_regconfig, val_regproc, val_regoper, 
				val_regoperator, val_regrole, val_regprocedure,
				val_regcollation, val_regnamespace, val_regclass,
				val_regdictionary, val_regtype)

					VALUES(1, 2, 'german', 'namein', '||/'::regoper,
					'=(integer,integer)', 'pg_stat_scan_tables', 'abs(numeric)',
					'"en_AG"', 'information_schema', 'pg_type',
					'english_stem', 'int2vector');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_int4 SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_oid SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regconfig SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regproc SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regoper SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regoperator SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regrole SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regprocedure SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regcollation SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regnamespace SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regclass SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regdictionary SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_regtype SET DATA TYPE int4;
		""")

		node.stop(['-m', 'immediate'])
		node.start()
		
		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)

		node.stop()

	def test_from_int(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_int4_1 int4 NOT NULL,
				val_int4_2 int4 NOT NULL,
				val_int4_3 int4 NOT NULL,
				val_int4_4 int4 NOT NULL,
				val_int4_5 int4 NOT NULL,
				val_int4_6 int4 NOT NULL,
				val_int4_7 int4 NOT NULL,
				val_int4_8 int4 NOT NULL,
				val_int4_9 int4 NOT NULL,
				val_int4_10 int4 NOT NULL,
				val_int4_11 int4 NOT NULL,
				val_int4_12 int4 NOT NULL
			) USING orioledb;

			INSERT INTO o_test (val_int4_1, val_int4_2, val_int4_3,
					val_int4_4, val_int4_5, val_int4_6,
					val_int4_7, val_int4_8, val_int4_9,
					val_int4_10, val_int4_11, val_int4_12)
				VALUES(1, 12597, 1244, 82, 76, 4570, 35, 12547, 2200, 2836, 12592, 22);
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_int4_1 SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_int4_2 SET DATA TYPE regconfig;
			ALTER TABLE o_test ALTER COLUMN val_int4_3 SET DATA TYPE regproc;
			ALTER TABLE o_test ALTER COLUMN val_int4_4 SET DATA TYPE regoper;
			ALTER TABLE o_test ALTER COLUMN val_int4_5 SET DATA TYPE regoperator;
			ALTER TABLE o_test ALTER COLUMN val_int4_6 SET DATA TYPE regrole;
			ALTER TABLE o_test ALTER COLUMN val_int4_7 SET DATA TYPE regprocedure;
			ALTER TABLE o_test ALTER COLUMN val_int4_8 SET DATA TYPE regcollation;
			ALTER TABLE o_test ALTER COLUMN val_int4_9 SET DATA TYPE regnamespace;
			ALTER TABLE o_test ALTER COLUMN val_int4_10 SET DATA TYPE regclass;
			ALTER TABLE o_test ALTER COLUMN val_int4_11 SET DATA TYPE regdictionary;
			ALTER TABLE o_test ALTER COLUMN val_int4_12 SET DATA TYPE regtype;
		""")

		node.stop(['-m', 'immediate'])
		node.start()
		
		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)

		node.stop()
	
	def test_to_oid(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_oid oid NOT NULL,
				val_regtype regtype NOT NULL,
				val_regprocedure regprocedure NOT NULL,
				val_regconfig regconfig NOT NULL,
				val_regdictionary regdictionary NOT NULL,
				val_regcollation regcollation NOT NULL,
				val_regnamespace regnamespace NOT NULL,
				val_regrole regrole NOT NULL,
				val_regoperator regoperator NOT NULL,
				val_regoper regoper NOT NULL,
				val_regclass regclass NOT NULL,
				val_regproc regproc NOT NULL
			) USING orioledb;

			INSERT INTO o_test (val_oid, val_regtype, val_regprocedure,
					val_regconfig, val_regdictionary, val_regcollation,
					val_regnamespace, val_regrole, val_regoperator,
					val_regoper, val_regclass, val_regproc)
				VALUES(1, 'int2vector', 'abs(numeric)',
					'dutch', 'finnish_stem', '"ucs_basic"',
					'pg_catalog', 'pg_stat_scan_tables', '+(int4,int4)',
					'||/', 'orioledb_table', 'nameout');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_oid SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regtype SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regprocedure SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regconfig SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regdictionary SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regcollation SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regnamespace SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regrole SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regoperator SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regoper SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regclass SET DATA TYPE oid;
			ALTER TABLE o_test ALTER COLUMN val_regproc SET DATA TYPE oid;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()
	
	def test_from_oid(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_oid_1 oid NOT NULL,
				val_oid_2 oid NOT NULL,
				val_oid_3 oid NOT NULL,
				val_oid_4 oid NOT NULL,
				val_oid_5 oid NOT NULL,
				val_oid_6 oid NOT NULL,
				val_oid_7 oid NOT NULL,
				val_oid_8 oid NOT NULL,
				val_oid_9 oid NOT NULL,
				val_oid_10 oid NOT NULL,
				val_oid_11 oid NOT NULL,
				val_oid_12 oid NOT NULL
			) USING orioledb;

			INSERT INTO o_test (val_oid_1, val_oid_2, val_oid_3,
					val_oid_4, val_oid_5, val_oid_6, val_oid_7,
					val_oid_8, val_oid_9, val_oid_10, val_oid_11,
					val_oid_12)
				VALUES(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
		""")

		node.safe_psql('postgres', """
			--ALTER TABLE o_test ALTER COLUMN val_oid_1 SET DATA TYPE int4;
			ALTER TABLE o_test ALTER COLUMN val_oid_2 SET DATA TYPE regconfig;
			ALTER TABLE o_test ALTER COLUMN val_oid_3 SET DATA TYPE regproc;
			ALTER TABLE o_test ALTER COLUMN val_oid_4 SET DATA TYPE regoper;
			ALTER TABLE o_test ALTER COLUMN val_oid_5 SET DATA TYPE regoperator;
			ALTER TABLE o_test ALTER COLUMN val_oid_6 SET DATA TYPE regrole;
			ALTER TABLE o_test ALTER COLUMN val_oid_7 SET DATA TYPE regprocedure;
			ALTER TABLE o_test ALTER COLUMN val_oid_8 SET DATA TYPE regcollation;
			ALTER TABLE o_test ALTER COLUMN val_oid_9 SET DATA TYPE regnamespace;
			ALTER TABLE o_test ALTER COLUMN val_oid_10 SET DATA TYPE regclass;
			ALTER TABLE o_test ALTER COLUMN val_oid_11 SET DATA TYPE regdictionary;
			ALTER TABLE o_test ALTER COLUMN val_oid_12 SET DATA TYPE regtype;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()

	def test_regoper_regoperator(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_regoperator regoperator NOT NULL,
				val_regoper regoper NOT NULL
			) USING orioledb;

			INSERT INTO o_test (val_regoperator, val_regoper)
				VALUES('+(int4,int4)', '||/');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_regoperator SET DATA TYPE regoper;
			ALTER TABLE o_test ALTER COLUMN val_regoper SET DATA TYPE regoperator;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()

	def test_bit_varbit(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_bit bit(3) NOT NULL,
				val_varbit bit varying(5)
			) USING orioledb;

			INSERT INTO o_test (val_bit, val_varbit)
				VALUES(B'101'::bit(3), B'1001');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_bit SET DATA TYPE bit varying(5);
			ALTER TABLE o_test ALTER COLUMN val_varbit SET DATA TYPE bit(3);
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()

	def test_text_type(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_text text NOT NULL,
				val_varchar varchar(50) NOT NULL,
				val_bpchar bpchar NOT NULL

			) USING orioledb;

			INSERT INTO o_test(val_text, val_varchar, val_bpchar)
				VALUES('abc', 'abcabc', 'ab c');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_text SET DATA TYPE varchar;
			ALTER TABLE o_test ALTER COLUMN val_varchar SET DATA TYPE text;

			ALTER TABLE o_test ALTER COLUMN val_text SET DATA TYPE bpchar;

			ALTER TABLE o_test ALTER COLUMN val_varchar SET DATA TYPE bpchar;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()
	
	def test_regproc_regprocedure(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """
			CREATE TABLE IF NOT EXISTS o_test (
				val_regproc regproc NOT NULL,
				val_regprocedure regprocedure NOT NULL

			) USING orioledb;

			INSERT INTO o_test(val_regproc, val_regprocedure)
				VALUES('nameout', 'abs(numeric)');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_regproc SET DATA TYPE regprocedure;				
			ALTER TABLE o_test ALTER COLUMN val_regprocedure SET DATA TYPE regproc;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()
	
	def test_cidr(self):
		
		node = self.node
		node.append_conf('postgresql.conf',
		"""
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql('postgres', """

			CREATE TABLE IF NOT EXISTS o_test (
				val_cidr cidr NOT NULL
			) USING orioledb;

			INSERT INTO o_test(val_cidr)
				VALUES('192.168.100.128/25');
		""")

		node.safe_psql('postgres', """
			ALTER TABLE o_test ALTER COLUMN val_cidr SET DATA TYPE inet;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_test;')[0][0], 1)
			
		node.stop()
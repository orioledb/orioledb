import logging
import os

from .s3_base_test import S3BaseTest


def catchup_orioledb(replica):
	replica.catchup()
	replica.poll_query_until("SELECT orioledb_recovery_synchronized();",
	                         expected=True)


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

dir_path = os.path.dirname(os.path.realpath(__file__))


class S3InheritsTest(S3BaseTest):

	def test_1(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create table o_test_1(
				a int,
				b int
			)USING orioledb;
			insert into o_test_1 values(1,2);
	    	alter table o_test_1 add primary key(b);
            create table o_test_2() inherits (o_test_1)USING orioledb;
            update o_test_2 set a = a + b;
            update o_test_2 set a = a + 1 where true returning b, a;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_2(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

		    create table o_test_1(a int)USING orioledb;
		    create table o_test_2() inherits (o_test_1)USING orioledb;
		    create table o_test_3() inherits (o_test_2)USING orioledb;
            update o_test_2 set a = a + 1 where true;
            update o_test_2 set a = a + 1;
		    alter table o_test_3 inherit o_test_1;
	    	alter table o_test_3 alter a set not null;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_3(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create table o_test_1(a int)USING orioledb;
            create table o_test_2(b text, c int) inherits (o_test_1)USING orioledb;
            create table o_test_3(d float) inherits(o_test_1,o_test_2)USING orioledb;
            alter table o_test_2 add column a2 int constraint nn not null;
			alter table o_test_3 alter column a set not null;
            alter table o_test_1 alter column a drop not null;
            alter table o_test_1 alter column a set not null;
            alter table o_test_3 alter column a2 set not null;
            alter table o_test_2 alter column a2 drop not null;
            alter table o_test_1 add primary key (a);

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_4(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create table o_test_1(a int)USING orioledb;
            create table o_test_2 (check (a = 1 or a = null)) inherits(o_test_1)USING orioledb;
            insert into o_test_2 values(1);
            insert into o_test_2 values(2);
            insert into o_test_2 values(null);
            drop table o_test_1 cascade;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_5(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create table o_test_1 (a serial primary key, b text)USING orioledb;
            create table o_test_2 (a int primary key) inherits (o_test_1)USING orioledb;
			insert into o_test_2 (b) values ('q'),('w');
            create table o_test_3 (a int primary key) inherits (o_test_1)USING orioledb;
            insert into o_test_3 (b) values ('e'),('r');
            create table o_test_4 (a int) inherits (o_test_1)USING orioledb;
            insert into o_test_4 (b) values ('t'),('y');
            create unique index ind1 on o_test_1 (a) include(a,b);
			create unique index ind2 on o_test_2 (a) include(a,b);
            create unique index ind3 on o_test_3 (a) include(a,b);
			create unique index ind4 on o_test_4 (a) include(a,b);

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_6(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create temp table o_test_1 (a, b) USING orioledb as select b, b from generate_series(0,1000) b;
            create temp table o_test_2() inherits (o_test_1)USING orioledb;
            insert into o_test_2 select b, b from generate_series(0,10) b;
			create temp table o_test_3() inherits (o_test_1)USING orioledb;
            insert into o_test_3 select b, b from generate_series(0,10) b;
            create index ind1 on o_test_1(a);
            create index ind2 on o_test_2(a);
            create index ind3 on o_test_3(a);

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_7(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create table o_test_1(a int constraint c1 CHECK (a > 0))USING orioledb;
            create table o_test_2(a int constraint c1 CHECK (a > 0))USING orioledb;
            create unique index ind1 on o_test_2(a) include(a,a,a);
            create table o_test_3 (a int) inherits (o_test_1, o_test_2)USING orioledb;
            create table o_test_4 (a int) inherits (o_test_1, o_test_2)USING orioledb;
            alter table o_test_2 drop constraint c1;
            alter table o_test_1 drop constraint c1;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_8(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create table o_test_1(a int)USING orioledb;
            create table o_test_2() inherits(o_test_1)USING orioledb;
            create unique index ind1 on o_test_1(a) include(a, a);
			drop index ind1;
			create unique index ind1 on o_test_1(a) include(a, a);
            alter table o_test_1 add constraint c1 check (a > 0);
            alter table o_test_2 add constraint c2 check (a < 10);

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_9(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""


			CREATE TABLE o_test_1 (a int, b int)USING orioledb;
            CREATE TABLE o_test_2 (b int, c int)USING orioledb;
            CREATE TABLE o_test_3 (d int) INHERITS (o_test_1, o_test_2)USING orioledb;
            ALTER TABLE o_test_1 RENAME a TO aa;
            ALTER TABLE o_test_3 RENAME d TO dd;

			CHECKPOINT;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_10(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

	        create table o_test_1 (a text)USING orioledb;
            alter table o_test_1 add constraint c1 check (a is not null);
            create table o_test_2 (b text) inherits (o_test_1)USING orioledb;
			create unique index ind1 on o_test_2(b);
            create unique index ind2 on o_test_2(b);
            alter table o_test_1 drop constraint c1;
            alter table o_test_1 add check (a is not null);
            create unique index ind3 on o_test_2(b) include(b);
            alter table o_test_1 add constraint c1 check (a is not null);
            alter table o_test_2 no inherit o_test_1;
            alter table o_test_2 drop constraint c1;
            alter table o_test_1 drop constraint c1;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_11(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

			create temp table o_test_1(a int, b int)USING orioledb;
            create temp table o_test_2(a int, c bigint)USING orioledb;
            create temp table o_test_3(d int) inherits(o_test_1, o_test_2)USING orioledb;
            alter table o_test_1 alter column b type bigint;
            create table o_test_4(f int)USING orioledb;
            alter table o_test_4 add constraint c1 check (f > 0) no inherit;
            alter table o_test_4 add constraint c2 check (f > 10);
            create table o_test_5 () inherits (o_test_4)USING orioledb;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_12(self):

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""

	        create temp table o_test_1(a int, b int)USING orioledb;
            insert into o_test_1 values(1,1),(3,3);
            create temp table o_test_2(c int) inherits (o_test_1)USING orioledb;
            insert into o_test_2 values(2,2,2),(3,3,3);
            create temp table o_test_3(a int, b int)USING orioledb;
            insert into o_test_3 values(1,1),(2,2),(3,3),(4,4);
			update o_test_3 set b = b + 100 where a in (select a from o_test_1);
            create temp table o_test_4(c int) inherits (o_test_3)USING orioledb;
            insert into o_test_4 values(1,1,1),(2,2,2),(3,3,3),(4,4,4);
            update o_test_4 set b = b + 100;

			checkpoint;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

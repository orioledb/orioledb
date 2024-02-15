import logging
import os

from .s3_base_test import S3BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


class S3UnloggedTest(S3BaseTest):

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

			CREATE UNLOGGED TABLE o_test_1 (a int, b int, c oid)USING orioledb;
			ALTER TABLE o_test_1 ALTER COLUMN c SET DATA TYPE int;
			INSERT INTO o_test_1 VALUES (1, 3, 5), (2, 4, 6);
			ALTER TABLE o_test_1 DROP COLUMN c;
			CHECKPOINT;

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

			CREATE UNLOGGED TABLE o_test_1 (a int)USING orioledb;
			WITH o_test_1 as (select 11) INSERT INTO o_test_1 SELECT * FROM o_test_1;
			CHECKPOINT;

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

			CREATE UNLOGGED TABLE o_test_1(val_1 int primary key)USING orioledb;
			CREATE UNLOGGED TABLE o_test_2(LIKE o_test_1)USING orioledb;
			INSERT INTO o_test_1 (val_1) VALUES (1);
			INSERT INTO o_test_2 (val_1) VALUES (2);
			ALTER TABLE o_test_1 DROP CONSTRAINT o_test_1_pkey;
			CHECKPOINT;

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

			CREATE UNLOGGED TABLE o_test_1(a text, b text)USING orioledb;
			CREATE INDEX ind_1 ON o_test_1 ((a || b));
			CREATE UNLOGGED TABLE o_test_2 (ab text primary key)USING orioledb;
			INSERT INTO o_test_1 VALUES ('a', 'b'), ('x', 'y');
			INSERT INTO o_test_2 VALUES ('ab'), ('xy');

			CREATE UNLOGGED TABLE o_test_3(b text, a text)USING orioledb;
			ALTER TABLE o_test_3 INHERIT o_test_1;
			CREATE UNLOGGED TABLE o_test_4(primary key (ab)) INHERITS (o_test_2)USING orioledb;
			INSERT INTO o_test_3 VALUES ('a', 'b'), ('c', 'd'), ('q', 'w'), ('e', 'r');
			INSERT INTO o_test_4 VALUES ('er'), ('qw'), ('cd'), ('ab');
			CREATE INDEX ind_2 on o_test_3 ((a || b));

			CHECKPOINT;

		""")

		node.stop(['-m', 'immediate'])

		node.start()

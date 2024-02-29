import json
import logging
import os
import time
import signal
from tempfile import mkdtemp, mkstemp
from threading import Thread
from typing import Optional

import boto3
import testgres
from moto.server import DomainDispatcherApplication, create_backend_app
from testgres.consts import DATA_DIR
from testgres.defaults import default_dbname
from testgres.enums import NodeStatus
from testgres.exceptions import StartNodeException

from werkzeug.serving import BaseWSGIServer, make_server, make_ssl_devcert
import urllib3

from .base_test import BaseTest
from .base_test import generate_string as gen_str

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

dir_path = os.path.dirname(os.path.realpath(__file__))


class S3Test(BaseTest):
	bucket_name = "test-bucket"
	host = "localhost"
	port = 5001
	user = "ORDB_USER"
	region = "us-east-1"

	@classmethod
	def setUpClass(cls):
		urllib3.util.connection.HAS_IPV6 = False
		cls.ssl_key = make_ssl_devcert('/tmp/ordb_test_key', cn=cls.host)
		cls.s3_server = MotoServerSSL(ssl_context=cls.ssl_key)
		cls.s3_server.start()

		iam = boto3.client('iam',
		                   endpoint_url=f"https://{cls.host}:{cls.port}",
		                   aws_access_key_id="",
		                   aws_secret_access_key="",
		                   region_name=cls.region,
		                   verify=cls.ssl_key[0])
		iam.create_user(UserName=cls.user)
		policy_document = {
		    "Version": "2012-10-17",
		    "Statement": {
		        "Effect": "Allow",
		        "Action": "*",
		        "Resource": "*"
		    }
		}
		policy = iam.create_policy(PolicyName="ORDB_POLICY",
		                           PolicyDocument=json.dumps(policy_document))
		policy_arn = policy["Policy"]["Arn"]
		iam.attach_user_policy(UserName=cls.user, PolicyArn=policy_arn)
		response = iam.create_access_key(UserName=cls.user)
		cls.access_key_id = response["AccessKey"]["AccessKeyId"]
		cls.secret_access_key = response["AccessKey"]["SecretAccessKey"]

	@classmethod
	def tearDownClass(cls):
		cls.s3_server.stop()

	def setUp(self):
		super().setUp()

		session = boto3.Session(aws_access_key_id=self.access_key_id,
		                        aws_secret_access_key=self.secret_access_key,
		                        region_name=self.region)
		host_port = f"https://{self.host}:{self.port}"
		self.client = session.client("s3",
		                             endpoint_url=host_port,
		                             verify=self.ssl_key[0])
		self.loader = OrioledbS3Loader(self.access_key_id,
		                               self.secret_access_key, self.region,
		                               host_port, self.ssl_key[0])
		try:
			self.client.head_bucket(Bucket=self.bucket_name)
		except:
			self.client.create_bucket(Bucket=self.bucket_name)

	def tearDown(self):
		super().tearDown()
		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		while objects != []:
			objects = list({"Key": x["Key"]} for x in objects)
			self.client.delete_objects(Bucket=self.bucket_name,
			                           Delete={"Objects": objects})
			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])

		self.client.delete_bucket(Bucket=self.bucket_name)
		self.client.close()

	def test_s3_put_get(self):
		fd, s3_test_file = mkstemp()
		with os.fdopen(fd, 'wt') as fp:
			fp.write("HELLO\nIT'S A ME\nMARIO\n")

		self.client.upload_file(Bucket=self.bucket_name,
		                        Filename=s3_test_file,
		                        Key="wal/314159")
		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		objects = sorted(list(x["Key"] for x in objects))
		self.assertEqual(objects, ['wal/314159'])

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(f"SELECT s3_put('wal/926535', '{s3_test_file}');")
		node.safe_psql(f"SELECT s3_put('5/LICENSE', '{s3_test_file}');")
		node.safe_psql(f"SELECT s3_put('LICENSE', '{s3_test_file}');")

		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		objects = sorted(list(x["Key"] for x in objects))
		self.assertEqual(objects,
		                 ['5/LICENSE', 'LICENSE', 'wal/314159', 'wal/926535'])
		object = self.client.get_object(Bucket=self.bucket_name,
		                                Key="5/LICENSE")
		boto_object_body = object["Body"].readlines()
		boto_object_body = [x.decode("utf-8") for x in boto_object_body]
		boto_object_body = ''.join(boto_object_body)
		orioledb_object_body = node.execute(f"SELECT s3_get('5/LICENSE');")
		orioledb_object_body = orioledb_object_body[0][0]
		self.assertEqual(boto_object_body, orioledb_object_body)
		with open(f"{s3_test_file}", "r") as f:
			file_content = ''.join(f.readlines())
			self.assertEqual(file_content, orioledb_object_body)
		node.stop(['-m', 'immediate'])
		os.unlink(s3_test_file)

	def test_s3_credential_check(self):
		node = self.node

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = 'BOB:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
		""")
		with self.assertRaises(StartNodeException) as e:
			node.start()
		self.assertEqual(e.exception.message, "Cannot start node")
		with open(node.pg_log_file) as f:
			log = f.readlines()
		message = log[0].split('] ')[-1].strip()
		self.assertEqual(
		    message,
		    "FATAL:  could not list objects in S3 bucket, check orioledb s3 configs"
		)

	def get_file_occupied_size(self, path):
		try:
			result = 0
			zero = b'\0' * 8192
			f = open(path, "rb")
			data = f.read(8192)
			while len(data) > 0:
				if data != zero:
					result = result + len(data)
				data = f.read(8192)
			f.close()
			return result
		except:  # We could be here due to concurrent operation, e.g. file removal
			return 0

	def get_data_size(self):
		node = self.node
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(
		    f"{node.data_dir}/orioledb_data"):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# skip if it is symbolic link
				if not os.path.islink(fp):
					total_size += self.get_file_occupied_size(fp)
		return total_size

	def test_s3_data_eviction(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_desired_size = 20MB

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql("""
			BEGIN;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				value text NOT NULL
			) USING orioledb;
			INSERT INTO o_test (id, value) (SELECT id, repeat('x', 2500) FROM generate_series(1, 20000) id);
			COMMIT;
		""")
		node.safe_psql("CHECKPOINT")
		while True:
			dataSize = self.get_data_size()
			if dataSize <= 20 * 1024 * 1024:
				break
			time.sleep(1)
		self.assertEqual(20000,
		                 node.execute("SELECT COUNT(*) FROM o_test")[0][0])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(20000,
		                 node.execute("SELECT COUNT(*) FROM o_test")[0][0])
		node.stop()

	def test_s3_data_dir_load(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'
			orioledb.s3_num_workers = 3

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.append_conf(f"""
			orioledb.recovery_pool_size = 1
			orioledb.recovery_idx_pool_size = 1
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop(['--no-wait'])

		new_temp_dir = mkdtemp(prefix=self.myName + '_tgsb_')

		while self.client.list_objects(Bucket=self.bucket_name,
		                               Prefix='wal/') == []:
			pass
		os.kill(archiver_pid, signal.SIGUSR2)
		while node.status() == NodeStatus.Running:
			pass

		with testgres.get_new_node('test', base_dir=new_temp_dir) as new_node:
			self.loader.download(self.bucket_name, new_node.data_dir)
			new_node.port = self.getBasePort() + 1
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
	
	def test_1(self):
		
		node = self.node
		node.append_conf('postgresql.conf', f"""
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
		
			CREATE TABLE o_test_1(
				a integer PRIMARY KEY, 
				b integer DEFAULT -1
			)USING orioledb;
			CREATE TABLE o_test_2(
				c integer, 
				d integer
			)USING orioledb;
			INSERT INTO o_test_1(a, b) VALUES (1, 100);
			INSERT INTO o_test_2 (d, c) VALUES (101, 200);
			CREATE UNIQUE INDEX ON o_test_1(a) INCLUDE(b);
			CREATE UNIQUE INDEX ON o_test_2(c) INCLUDE(d);

			MERGE INTO o_test_1 t
			USING o_test_2 s ON t.a = s.d
			WHEN NOT MATCHED THEN
				INSERT (a) VALUES (s.d);
				
			MERGE INTO o_test_1 t
			USING o_test_2 s ON t.a = s.d
			WHEN NOT MATCHED AND s.c <> 100 THEN
				INSERT (a) VALUES (s.d);
				 
			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_2(self):
		
		node = self.node
		node.append_conf('postgresql.conf', f"""
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
		
			CREATE TABLE o_test_1(
				a integer PRIMARY KEY
			)PARTITION BY LIST (a);
			CREATE TABLE o_test_2 
				PARTITION OF o_test_1 DEFAULT;
			CREATE TABLE o_test_3(b integer)USING orioledb;
			INSERT INTO o_test_3 VALUES (1), (2), (3);
			ALTER TABLE o_test_3 ADD COLUMN nn int;
			MERGE INTO o_test_1 t USING o_test_3 s ON t.a = s.b
				WHEN NOT MATCHED THEN INSERT VALUES (s.b);			
			INSERT INTO o_test_3 VALUES (4), (5), (6);
			DROP TABLE o_test_2;
				 
			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_3(self):
		
		node = self.node
		node.append_conf('postgresql.conf', f"""
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
		
			CREATE TABLE o_test_1(
				a int not null,
				b date not null,
				c int,
				d int
			)USING orioledb;
			CREATE TABLE o_test_2(
				CHECK ( b >= DATE '2006-02-01' AND b < DATE '2006-03-01' )
			)INHERITS (o_test_1)USING orioledb;
			CREATE TABLE o_test_3(
				CHECK ( b >= DATE '2006-03-01' AND b < DATE '2006-04-01' )
			)INHERITS (o_test_1)USING orioledb;
			CREATE TABLE o_test_4(
				a int not null,
				b date not null,
				c int,
				d int,
				f text
			)USING orioledb;
				 
			ALTER TABLE o_test_4 DROP COLUMN f;
			ALTER TABLE o_test_4 INHERIT o_test_1;
							
			CREATE TABLE o_test_5(LIKE o_test_1)USING orioledb;
	
			MERGE into o_test_1 m
			USING o_test_5 nm ON
				(m.a = nm.a and m.b=nm.b)
			WHEN MATCHED AND nm.c IS NULL THEN DELETE
			WHEN MATCHED THEN UPDATE
				SET c = greatest(m.c, nm.c),
					d = m.d + coalesce(nm.d, 0)
			WHEN NOT MATCHED THEN INSERT
				(a, b, c, d)
			VALUES (a, b, c, d);

			MERGE INTO o_test_5 nm
			USING ONLY o_test_1 m ON
				(nm.a = m.a and nm.b=m.b)
			WHEN MATCHED THEN DELETE;

			MERGE INTO o_test_5 nm
			USING o_test_1 m ON
				(nm.a = m.a and nm.b=m.b)
			WHEN MATCHED THEN DELETE;

			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_4(self):
		
		node = self.node
		node.append_conf('postgresql.conf', f"""
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
		
			create table merge_target_table (a int primary key, b text)USING orioledb;
			create table merge_source_table (a int, b text)USING orioledb;
			insert into merge_source_table
			values (1, 'initial1'), (2, 'initial2'),
					(3, 'initial3'), (4, 'initial4');

			merge into merge_target_table t
			using merge_source_table s
			on t.a = s.a
			when not matched then
			insert values (a, b);

			merge into merge_target_table t
			using merge_source_table s
			on t.a = s.a
			when matched and s.a <= 2 then
				update set b = t.b || ' updated by merge'
			when matched and s.a > 2 then
				delete
			when not matched then
			insert values (a, b);

			merge into merge_target_table t
			using merge_source_table s
			on t.a = s.a
			when matched and s.a <= 2 then
				update set b = t.b || ' updated again by merge'
			when matched and s.a > 2 then
				delete
			when not matched then
			insert values (a, b);
			drop table merge_source_table, merge_target_table;
				 
			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_5(self):
		
		node = self.node
		node.append_conf('postgresql.conf', f"""
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
			create table o_test_2(b int) USING orioledb;
			create function func_1() returns trigger language plpgsql
			as $$ begin return null; end $$;
			create trigger trig_1 after insert on o_test_1
			for each row execute procedure func_1();
			create trigger trig_2 after insert on o_test_1
			for statement execute procedure func_1();
			alter table o_test_1 disable trigger user;
			drop table o_test_1, o_test_2;
							
			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

	def test_6(self):
		
		node = self.node
		node.append_conf('postgresql.conf', f"""
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
				a int4 primary key, 
				b text
			)USING orioledb;
			create function func_1()
			returns trigger language plpgsql as
			$$
			begin
			if (TG_OP = 'UPDATE') then
				raise warning 'before update (new): %', new.*::text;
			end if;
			return new;
			end;
			$$;
			create trigger trig_1 before insert or update on o_test_1
			for each row execute procedure func_1();
			create function func_2()
			returns trigger language plpgsql as
			$$
			begin
			if (TG_OP = 'UPDATE') then
				raise warning 'after update (old): %', old.*::text;
				raise warning 'after update (new): %', new.*::text;
			elsif (TG_OP = 'INSERT') then
				raise warning 'after insert (new): %', new.*::text;
			end if;
			return null;
			end;
			$$;
			create trigger trig_2 after insert or update on o_test_1
			for each row execute procedure func_2();

			insert into o_test_1 values(1, 'q') on conflict (a) do update set b = 'updated ' || o_test_1.b;
			insert into o_test_1 values(3, 'w') on conflict (a) do update set b = 'updated ' || o_test_1.b;
			insert into o_test_1 values(4, 'e') on conflict (a) do update set b = 'updated ' || o_test_1.b;
			insert into o_test_1 values(6, 'r') on conflict (a) do update set b = 'updated ' || o_test_1.b;
			insert into o_test_1 values(8, 't') on conflict (a) do update set b = 'updated ' || o_test_1.b;
						
			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()


class OrioledbS3Loader:

	def __init__(self,
	             aws_access_key_id,
	             aws_secret_access_key,
	             aws_region,
	             endpoint_url,
	             verify=None):
		os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
		os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
		os.environ["AWS_DEFAULT_REGION"] = aws_region

		self._aws_region = aws_region
		self._endpoint_url = endpoint_url
		self._verify = verify

	def download(self, bucket_name, path, verbose=False, logfile=None):
		args = [f"{dir_path}/../orioledb_s3_loader.py"]
		args += ["--bucket-name", bucket_name]
		args += ["--endpoint", self._endpoint_url]
		args += ["--cert-file", self._verify]
		args += ["-d", path]
		if verbose:
			args += ["--verbose"]
		testgres.utils.execute_utility(args, logfile)


class MotoServerSSL:

	def __init__(self,
	             host: str = "localhost",
	             port: int = 5001,
	             ssl_context=None):
		self._host = host
		self._port = port
		self._thread: Optional[Thread] = None
		self._server: Optional[BaseWSGIServer] = None
		self._server_ready = False
		self._ssl_context = ssl_context

	def _server_entry(self) -> None:
		app = DomainDispatcherApplication(create_backend_app)

		self._server = make_server(self._host,
		                           self._port,
		                           app,
		                           False,
		                           ssl_context=self._ssl_context,
		                           passthrough_errors=True)
		self._server_ready = True
		self._server.serve_forever()

	def start(self) -> None:
		self._thread = Thread(target=self._server_entry, daemon=True)
		self._thread.start()
		while not self._server_ready:
			time.sleep(0.1)

	def stop(self) -> None:
		self._server_ready = False
		if self._server:
			self._server.shutdown()

		self._thread.join()  # type: ignore[union-attr]

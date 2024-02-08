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
from botocore import UNSIGNED
from botocore.config import Config
from moto.core import set_initial_no_auth_action_count
from moto.server import DomainDispatcherApplication, create_backend_app
from testgres.consts import DATA_DIR
from testgres.defaults import default_dbname
from testgres.enums import NodeStatus

from werkzeug.serving import BaseWSGIServer, make_server, make_ssl_devcert
import urllib3

from .base_test import BaseTest
from .base_test import generate_string as gen_str

def catchup_orioledb(replica):
	replica.catchup()
	replica.poll_query_until("SELECT orioledb_recovery_synchronized();", expected = True)


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

dir_path = os.path.dirname(os.path.realpath(__file__))


class S3Test(BaseTest):
	bucket_name = "test-bucket"
	host = "localhost"
	port = 5002
	iam_port = 5001
	user = "ORDB_USER"
	region = "us-east-1"

	@classmethod
	@set_initial_no_auth_action_count(4)
	def setUpClass(cls):
		urllib3.util.connection.HAS_IPV6 = False
		cls.ssl_key = make_ssl_devcert('/tmp/ordb_test_key', cn=cls.host)
		cls.s3_server = MotoServerSSL(ssl_context=cls.ssl_key)
		cls.s3_server.start()
		cls.iam_server = MotoServerSSL(port=cls.iam_port,
		                               service='iam',
		                               ssl_context=cls.ssl_key)
		cls.iam_server.start()

		iam_config = Config(signature_version=UNSIGNED)

		iam = boto3.client('iam',
		                   config=iam_config,
		                   endpoint_url=f"https://{cls.host}:{cls.iam_port}",
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
		cls.iam_server.stop()

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
	             port: int = 5002,
	             service: Optional[str] = None,
	             ssl_context=None):
		self._host = host
		self._port = port
		self._service = service
		self._thread: Optional[Thread] = None
		self._server: Optional[BaseWSGIServer] = None
		self._server_ready = False
		self._ssl_context = ssl_context

	def _server_entry(self) -> None:
		app = DomainDispatcherApplication(create_backend_app, self._service)

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

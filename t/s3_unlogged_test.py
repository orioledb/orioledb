import json
import logging
import os
import time
from threading import Thread
from typing import Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from moto.core import set_initial_no_auth_action_count
from moto.server import DomainDispatcherApplication, create_backend_app
from testgres.enums import NodeStatus
from werkzeug.serving import (BaseWSGIServer, make_ssl_devcert, make_server)

from .base_test import BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


class S3Test(BaseTest):
	bucket_name = "test-bucket"
	host = "localhost"
	port = 5000
	iam_port = 5001
	dir_path = os.path.dirname(os.path.realpath(__file__))
	user = "ORDB_USER"
	region = "us-east-1"

	@classmethod
	@set_initial_no_auth_action_count(4)
	def setUpClass(cls):
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


class MotoServerSSL:

	def __init__(self,
	             host: str = "localhost",
	             port: int = 5000,
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

		self._thread.join()

import json
import logging
import os
import time
from tempfile import mkdtemp, mkstemp
from threading import Thread
from typing import Optional

import boto3
import testgres
from moto.server import DomainDispatcherApplication, create_backend_app

from werkzeug.serving import BaseWSGIServer, make_server, make_ssl_devcert
import urllib3

from .base_test import BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

dir_path = os.path.dirname(os.path.realpath(__file__))


class S3BaseTest(BaseTest):
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

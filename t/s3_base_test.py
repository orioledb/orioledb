import inspect
import json
import logging
import os
import re
from re import RegexFlag
import runpy
import socket
from socket import AddressFamily, SocketKind
import sys
import time
from threading import Thread
from typing import Optional, Any
from urllib3.util import connection

import boto3
from botocore.config import Config
from moto.server import DomainDispatcherApplication, create_backend_app
from moto.s3.responses import S3Response
from moto.s3.exceptions import InvalidRequest, PreconditionFailed, \
 S3ClientError, InvalidRequest
from moto.core.common_types import TYPE_RESPONSE

from werkzeug.serving import BaseWSGIServer, make_server, make_ssl_devcert
import urllib3

from .base_test import BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


def s3_test_attrs(**_):
	""" Stub decorator to parse in S3BaseTest.setUp """

	def attr_decorator(fn):
		return fn

	return attr_decorator


orig_put_object = S3Response.put_object


# moto[s3] doesn't implement 409 HTTP error code therefore we need to do that
# by ourselves.
class ConditionalRequestConflict(S3ClientError):
	code = 409

	def __init__(self, failed_condition: str, **kwargs: Any):
		kwargs.setdefault("template", "condition_error")
		super().__init__(
		    "ConditionalRequestConflict",
		    "At least one of the pre-conditions you specified did not hold",
		    condition=failed_condition,
		    **kwargs,
		)


# Patch moto[s3]'s put_object() until it releases support "If-None-Match".
# Relevant PR was merged https://github.com/getmoto/moto/pull/8109 but wasn't
# released yet.
def mock_put_object(self) -> TYPE_RESPONSE:
	key_name = self.parse_key_name()
	if_none_match = self.headers.get("If-None-Match")

	if (if_none_match == "*"
	    and self.backend.get_object(self.bucket_name, key_name) is not None):
		raise PreconditionFailed("If-None-Match")

	return orig_put_object(self)


# Patch moto[s3]'s put_object() to alwasy return 409 code
def mock_put_object_conflict(self) -> TYPE_RESPONSE:
	raise ConditionalRequestConflict("If-None-Match")


def mock_moto_unkown_error(self) -> TYPE_RESPONSE:
	raise InvalidRequest("mock_moto_unkown_error")


class S3BaseTest(BaseTest):
	bucket_name = "test-bucket"
	host = "localhost"
	port = 5001
	user = "ORDB_USER"
	region = "us-east-1"
	policy_name = "ORDB_POLICY"
	optional_prefix = "prefix/test/"

	def setUp(self):
		super().setUp()

		protocol = 'https'
		prefix = None
		host_param = None

		testMethod = getattr(self, self._testMethodName)
		source = inspect.getsource(testMethod)
		index = source.find("def ")
		attrs = []
		for raw_attrs in re.findall(r"@s3_test_attrs\(((?:\n|.)*?)\)",
		                            source[:index].strip(),
		                            RegexFlag.MULTILINE):
			for attr in raw_attrs.split(','):
				name, val = attr.strip().split('=')
				attrs += [[name.strip(), val.strip()]]
		for name, val in attrs:
			if name == "http" and val == "True":
				protocol = 'http'
			elif name == "prefix":
				prefix = eval(val)
			elif name == "host":
				host_param = eval(val)
		urllib3.util.connection.HAS_IPV6 = False
		host_name = self.host
		if host_param:
			host_name = host_param
		host_port = f"{protocol}://{host_name}:{self.port}"
		self.ssl_key = make_ssl_devcert(f'/tmp/ordb_test_key', cn=host_name)
		if protocol == 'https':
			ssl_context = self.ssl_key
			self.s3_cainfo = self.ssl_key[0]
		else:
			ssl_context = None
			self.s3_cainfo = None
		self.s3_server = MotoServerSSL(ssl_context=ssl_context)
		self.s3_server.start()

		self._orig_create_connection = connection.create_connection

		def s3_test_resolver(host):
			logging.debug(f"s3_test_resolver: {host}")
			if host == host_param:
				return "127.0.0.1"
			else:
				addrlist = socket.getaddrinfo(host, 80)
				for address in addrlist:
					if address[0] == AddressFamily.AF_INET and address[
					    1] == SocketKind.SOCK_STREAM:
						break
				return address[4][0]

		def patched_create_connection(address, *args, **kwargs):
			host, port = address
			hostname = s3_test_resolver(host)

			return self._orig_create_connection((hostname, port), *args,
			                                    **kwargs)

		connection.create_connection = patched_create_connection

		config = None
		if self.bucket_name in host_port:
			config = Config(s3={'addressing_style': 'virtual'})
		self.iam_client = boto3.client('iam',
		                               endpoint_url=host_port,
		                               aws_access_key_id="",
		                               aws_secret_access_key="",
		                               region_name=self.region,
		                               verify=self.s3_cainfo,
		                               config=config)
		self.iam_client.create_user(UserName=self.user)
		policy_document = {
		    "Version": "2012-10-17",
		    "Statement": {
		        "Effect": "Allow",
		        "Action": "*",
		        "Resource": "*"
		    }
		}
		policy = self.iam_client.create_policy(
		    PolicyName=self.policy_name,
		    PolicyDocument=json.dumps(policy_document))
		self.policy_arn = policy["Policy"]["Arn"]
		self.iam_client.attach_user_policy(UserName=self.user,
		                                   PolicyArn=self.policy_arn)
		response = self.iam_client.create_access_key(UserName=self.user)
		self.access_key_id = response["AccessKey"]["AccessKeyId"]
		self.secret_access_key = response["AccessKey"]["SecretAccessKey"]

		session = boto3.Session(aws_access_key_id=self.access_key_id,
		                        aws_secret_access_key=self.secret_access_key,
		                        region_name=self.region)
		host_port = f"{protocol}://{self.host}:{self.port}"
		self.client = session.client("s3",
		                             endpoint_url=host_port,
		                             verify=self.s3_cainfo,
		                             config=config)

		host_port = f"{protocol}://{host_name}:{self.port}"
		if not host_param and not prefix:
			host_port += f"/{self.bucket_name}/"
		self.loader = OrioledbS3Loader(self.access_key_id,
		                               self.secret_access_key, self.region,
		                               host_port, prefix, self.s3_cainfo)
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

		self.iam_client.detach_user_policy(UserName=self.user,
		                                   PolicyArn=self.policy_arn)
		self.iam_client.delete_policy(PolicyArn=self.policy_arn)
		self.iam_client.delete_user(UserName=self.user)
		self.s3_server.stop()
		connection.create_connection = self._orig_create_connection


class OrioledbS3Loader:

	def __init__(self,
	             aws_access_key_id,
	             aws_secret_access_key,
	             aws_region,
	             endpoint_url,
	             prefix=None,
	             verify=None):
		os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
		os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
		os.environ["AWS_DEFAULT_REGION"] = aws_region

		self._aws_region = aws_region
		self._endpoint_url = endpoint_url
		self._prefix = prefix
		self._verify = verify

	def download(self, path, verbose=False):
		dir_path = os.path.dirname(os.path.realpath(__file__))
		old_argv = sys.argv
		sys.argv = [f"{dir_path}/../orioledb_s3_loader.py"]
		sys.argv += ["--endpoint", self._endpoint_url]
		if self._prefix:
			sys.argv += ["--prefix", self._prefix]
		if self._verify:
			sys.argv += ["--cert-file", self._verify]
		sys.argv += ["-d", path]
		if verbose:
			sys.argv += ["--verbose"]
		runpy.run_path(sys.argv[0], None, "__main__")
		sys.argv = old_argv


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

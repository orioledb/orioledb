import json
import logging
import os
import time
import re
import struct
from concurrent.futures import ThreadPoolExecutor
from tempfile import mkdtemp
from threading import Thread, Event
from typing import Optional

import boto3
from boto3.s3.transfer import TransferConfig
import testgres
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from moto.core import set_initial_no_auth_action_count
from moto.server import DomainDispatcherApplication, create_backend_app
from testgres.consts import DATA_DIR
from testgres.defaults import default_dbname, default_username

from testgres.utils import clean_on_error
from werkzeug.serving import BaseWSGIServer, make_server, make_ssl_devcert
import urllib3

from .base_test import BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

class S3Test(BaseTest):
	bucket_name = "test-bucket"
	host="localhost"
	port=5000
	iam_port=5001
	dir_path = os.path.dirname(os.path.realpath(__file__))
	user="ORDB_USER"
	region="us-east-1"

	@classmethod
	@set_initial_no_auth_action_count(4)
	def setUpClass(cls):
		urllib3.util.connection.HAS_IPV6 = False
		cls.ssl_key = make_ssl_devcert('/tmp/ordb_test_key', cn=cls.host)
		cls.s3_server = MotoServerSSL(ssl_context=cls.ssl_key)
		cls.s3_server.start()
		cls.iam_server = MotoServerSSL(port=cls.iam_port, service='iam',
									   ssl_context=cls.ssl_key)
		cls.iam_server.start()

		iam_config = Config(signature_version = UNSIGNED)

		iam = boto3.client('iam', config=iam_config,
						   endpoint_url=f"https://{cls.host}:{cls.iam_port}",
						   verify=cls.ssl_key[0])
		iam.create_user(UserName=cls.user)
		policy_document = {
			"Version": "2012-10-17",
			"Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}
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

		session = boto3.Session(
			aws_access_key_id=self.access_key_id,
			aws_secret_access_key=self.secret_access_key,
			region_name=self.region
		)
		host_port = f"https://{self.host}:{self.port}"
		self.client = session.client("s3", endpoint_url=host_port,
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
									   Delete={"Objects":objects})
			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])

		self.client.delete_bucket(Bucket=self.bucket_name)
		self.client.close()

	def test_s3_put_get(self):
		s3_test_file = f"{self.dir_path}/s3_test_data"

		self.client.upload_file(Bucket=self.bucket_name, Filename=s3_test_file,
								Key="wal/314159")
		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		objects = sorted(list(x["Key"] for x in objects))
		self.assertEqual(objects, ['wal/314159'])

		node = self.node
		node.append_conf('postgresql.conf', f"""
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
		self.assertEqual(objects, ['5/LICENSE', 'LICENSE',
								   'wal/314159', 'wal/926535'])
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

	def test_s3_checkpoint(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.ssl_key[0]}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")
		node.start()
		datname = default_dbname()
		datoid = node.execute(f"""
			SELECT oid from pg_database WHERE datname = '{datname}'
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql("""
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("""
			CREATE TABLE o_test_2 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_2 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT")
		self.assertEqual([(1,), (2,), (3,), (4,), (5,)],
						 node.execute("SELECT * FROM o_test_1"))

		node.stop(['-m', 'immediate'])

		orioledb_dir = node.data_dir + "/orioledb_data"
		chkp_num = 0
		obj_prefix = f'orioledb_data/{chkp_num}'
		files = []
		for path, _, filenames in os.walk(orioledb_dir):
			path = path.removeprefix(node.data_dir).split('/')[1:]
			if path == ['orioledb_data']:
				if not filenames:
					break
				chkp_num = [x.split('.')[0] for x in filenames
								if x.endswith('.xid')][0]
				obj_prefix = f'orioledb_data/{chkp_num}'
			elif path == ['orioledb_data', '1']:
				continue
			else:
				for name in filenames:
					name = name.split('/')[-1].split('.')
					if len(name) > 1:
						postfix = name[-1]
					else:
						postfix = None
					name[0] = name[0].split('-')
					if postfix == 'map':
						if name[0][1] == chkp_num:
							name = f"{name[0][0]}.map"
						else:
							name = None
					else:
						if name[0][1] == chkp_num:
							name = f"{name[0][0]}.0.0"
						else:
							name = None
					if name:
						files += [f"{obj_prefix}/{path[-1]}/{name}"]

		objects = self.client.list_objects(Bucket=self.bucket_name,
										   Prefix=f'{obj_prefix}/{datoid}')
		objects = objects.get("Contents", [])
		objects = sorted(list(x["Key"] for x in objects))
		files = sorted(files)
		self.assertEqual(objects, files)
		node.start()
		self.assertEqual([(1,), (2,), (3,), (4,), (5,)],
						 node.execute("SELECT * FROM o_test_1"))
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

			orioledb.recovery_pool_size = 1
			orioledb.recovery_idx_pool_size = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE TABLE pg_test_1 (
				val_1 int
			);
			INSERT INTO pg_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT")
		self.assertEqual([(1,), (2,), (3,), (4,), (5,)],
						 node.execute("SELECT * FROM pg_test_1"))
		node.stop()

		new_temp_dir = mkdtemp(prefix = self.myName + '_tgsb_')
		new_data_dir = os.path.join(new_temp_dir, DATA_DIR)
		host_port = f"https://{self.host}:{self.port}"
		loader = OrioledbS3ObjectLoader(self.access_key_id,
										self.secret_access_key,
										self.region,
										host_port,
										self.ssl_key[0])
		loader.download_files_in_directory(self.bucket_name, 'data/',
											new_data_dir)
		self.replica = testgres.get_new_node('test', base_dir=new_temp_dir)

		replica = self.replica
		replica.port = self.getBasePort() + 1
		replica.append_conf(port=replica.port)
		replica.append_conf("""
			orioledb.s3_mode = false
			archive_mode = false
		""")
		replica._assign_master(node)
		replica._create_recovery_conf(username=default_username())

		node.start()
		replica.start()
		replica.catchup()
		self.assertEqual([(1,), (2,), (3,), (4,), (5,)],
						node.execute("SELECT * FROM pg_test_1"))
		replica.stop(['-m', 'immediate'])
		node.stop(['-m', 'immediate'])

class OrioledbS3ObjectLoader:
	def __init__(self, aws_access_key_id, aws_secret_access_key, aws_region,
				 endpoint_url, verify):
		session = boto3.Session(
			aws_access_key_id=aws_access_key_id,
			aws_secret_access_key=aws_secret_access_key,
			region_name=aws_region
		)
		self.s3 = session.client("s3", endpoint_url=endpoint_url,
								 verify=verify)
		self._error_occurred = Event()

	def list_objects_last_checkpoint(self, bucket_name, directory):
		objects = []
		paginator = self.s3.get_paginator('list_objects_v2')

		greatest_number = -1
		greatest_number_dir = None
		for page in paginator.paginate(Bucket=bucket_name, Prefix=directory,
									   Delimiter='/'):
			if 'CommonPrefixes' in page:
				for prefix in page['CommonPrefixes']:
					prefix_key = prefix['Prefix'].rstrip('/')
					subdirectory = prefix_key.split('/')[-1]
					try:
						number = int(subdirectory)
						if number > greatest_number:
							greatest_number = number
							greatest_number_dir = prefix['Prefix']
					except ValueError:
						pass
		if greatest_number_dir:
			objects = self.list_objects(bucket_name, greatest_number_dir)

		return objects

	def list_objects(self, bucket_name, directory):
		objects = []
		paginator = self.s3.get_paginator('list_objects_v2')

		for page in paginator.paginate(Bucket=bucket_name, Prefix=directory):
			if 'Contents' in page:
				page_objs = [x["Key"] for x in page['Contents']]
				objects.extend(page_objs)

		return objects

	def download_file(self, bucket_name, file_key, local_path):
		try:
			transfer_config = TransferConfig(use_threads=False,
											 max_concurrency=1)
			if file_key[-1] == '/':
				dirs = local_path
			else:
				dirs = '/'.join(local_path.split('/')[:-1])
			os.makedirs(dirs, exist_ok=True, mode=0o700)
			if file_key[-1] != '/':
				self.s3.download_file(
					bucket_name, file_key, local_path, Config=transfer_config
				)
			if re.match(r'.*/orioledb_data/small_files_\d+$', local_path):
				base_dir = '/'.join(local_path.split('/')[:-2])
				with open(local_path, 'rb') as file:
					data = file.read()
				numFiles = struct.unpack('i', data[0:4])[0]
				for i in range(0, numFiles):
					(nameOffset, dataOffset, dataLength) = struct.unpack('iii', data[4 + i * 12: 16 + i * 12])
					name = data[nameOffset: data.find(b'\0', nameOffset)].decode('ascii')
					fullname = f"{base_dir}/{name}"
					os.makedirs(os.path.dirname(dirs), exist_ok=True, mode=0o700)
					with open(fullname, 'wb') as file:
						file.write(data[dataOffset: dataOffset + dataLength])
					os.chmod(fullname, 0o600)

		except ClientError as e:
			if e.response['Error']['Code'] == "404":
				print(f"File not found: {file_key}")
			else:
				print(f"An error occurred: {e}")
			self._error_occurred.set()

	def download_files_in_directory(self, bucket_name, directory,
									local_directory, last_checkpoint=True):
		if last_checkpoint:
			objects = self.list_objects_last_checkpoint(bucket_name, directory)
		else:
			objects = self.list_objects(bucket_name, directory)
		max_threads = os.cpu_count()

		with ThreadPoolExecutor(max_threads) as executor:
			futures = []

			for file_key in objects:
				if last_checkpoint:
					local_file = '/'.join(file_key.split('/')[2:])
				else:
					local_file = '/'.join(file_key.split('/')[1:])
				local_path = f"{local_directory}/{local_file}"
				future = executor.submit(self.download_file, bucket_name,
										 file_key, local_path)
				futures.append(future)

			for future in futures:
				future.result()

				if self._error_occurred.is_set():
					print("An error occurred. Stopping all downloads.")
					executor.shutdown(wait=False, cancel_futures=True)
					break


class MotoServerSSL:
	def __init__(self, host: str = "localhost", port: int = 5000,
				 service: Optional[str] = None, ssl_context=None):
		self._host = host
		self._port = port
		self._service = service
		self._thread: Optional[Thread] = None
		self._server: Optional[BaseWSGIServer] = None
		self._server_ready = False
		self._ssl_context = ssl_context

	def _server_entry(self) -> None:
		app = DomainDispatcherApplication(create_backend_app, self._service)

		self._server = make_server(self._host, self._port, app, False,
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

import logging
import os
import time
import signal
from tempfile import mkdtemp, mkstemp

import testgres
from testgres.defaults import default_dbname
from testgres.enums import NodeStatus
from testgres.exceptions import StartNodeException

from .s3_base_test import S3BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

dir_path = os.path.dirname(os.path.realpath(__file__))


class S3Test(S3BaseTest):

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

	# TODO: Other tests not working on MacOs because of credential check, fix it
	def _test_s3_credential_check(self):
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
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
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
				chkp_num = [
				    x.split('.')[0] for x in filenames if x.endswith('.xid')
				][0]
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
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop()

	def test_s3_ddl_recovery(self):
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
			CREATE TABLE o_test_1(
				val_1 text NOT NULL COLLATE "C",
				PRIMARY KEY (val_1)
			) USING orioledb;
			ALTER TABLE o_test_1 ALTER val_1 TYPE text COLLATE "POSIX";
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("""
			ALTER TABLE o_test_1
				DROP CONSTRAINT o_test_1_pkey;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

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

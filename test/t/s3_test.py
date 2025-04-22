import logging
import os
import time
import re
import shutil
import signal
from tempfile import mkdtemp, mkstemp

from unittest.mock import patch

import testgres
from testgres.defaults import default_dbname
from testgres.enums import NodeStatus
from testgres.exceptions import StartNodeException, QueryException

from .s3_base_test import S3BaseTest, s3_test_attrs, mock_put_object, \
 mock_put_object_conflict, mock_moto_unkown_error

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
			orioledb.s3_cainfo = '{self.s3_cainfo}'
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(f"SELECT s3_put('wal/926535', '{s3_test_file}');")
		node.safe_psql(f"SELECT s3_put('5/LICENSE', '{s3_test_file}');")
		node.safe_psql(f"SELECT s3_put('LICENSE', '{s3_test_file}');")

		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		objects = sorted(list(x["Key"] for x in objects))
		self.assertEqual(objects, [
		    '5/LICENSE', 'LICENSE', 'data/orioledb_data/s3_lock', 'wal/314159',
		    'wal/926535'
		])
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
			orioledb.s3_cainfo = '{self.s3_cainfo}'
		""")
		with self.assertRaises(StartNodeException) as e:
			node.start()
		self.assertEqual(e.exception.message, "Cannot start node")
		with open(node.pg_log_file) as f:
			log = f.readlines()
		message = log[0].split('] ')[-1].strip()
		self.assertEqual(message, "FATAL:  could not put object to S3")

	def test_s3_checkpoint(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

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

	def test_s3_checkpoint_unchanged(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1

			autovacuum = off
		""")
		node.start()

		# 1st CHECKPOINT
		node.safe_psql("""
			CREATE TABLE test_1 (
				val_1 int
			) USING heap;
			INSERT INTO test_1 SELECT * FROM generate_series(1, 2000);
			CREATE TABLE test_2 (
				val_2 int
			) USING heap;
			INSERT INTO test_2 SELECT * FROM generate_series(2000, 4000);
		""")

		test_1_filepath = node.execute(
		    "SELECT pg_catalog.pg_relation_filepath('test_1'::regclass)")[0][0]
		test_2_filepath = node.execute(
		    "SELECT pg_catalog.pg_relation_filepath('test_2'::regclass)")[0][0]

		node.safe_psql("CHECKPOINT")

		objects_1 = self.client.list_objects(Bucket=self.bucket_name)
		objects_1 = objects_1.get("Contents", [])
		objects_1 = sorted(list(x["Key"] for x in objects_1))

		self.assertIn("data/1/orioledb_data/file_checksums", objects_1)
		self.assertIn("data/1/" + test_1_filepath, objects_1)
		self.assertIn("data/1/" + test_2_filepath, objects_1)

		# 2nd CHECKPOINT
		node.safe_psql("""
			INSERT INTO test_2 SELECT * FROM generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT")

		objects_2 = self.client.list_objects(Bucket=self.bucket_name)
		objects_2 = objects_2.get("Contents", [])
		objects_2 = sorted(list(x["Key"] for x in objects_2))

		self.assertIn("data/2/orioledb_data/file_checksums", objects_2)
		self.assertNotIn("data/2/" + test_1_filepath, objects_2)
		self.assertIn("data/2/" + test_2_filepath, objects_2)

		node.stop()

	def test_s3_checkpoint_checksum_error(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1

			autovacuum = off
		""")
		node.start()

		small_file_checksums = os.path.join(node.data_dir, "orioledb_data",
		                                    "small_file_checksums")

		# Test that CHECKPOINT will fail in case of lack of permissions
		open(small_file_checksums, "a").close()
		os.chmod(small_file_checksums, 0)

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn(
			    'ERROR:  could not read file "orioledb_data/small_file_checksums": Permission denied',
			    f.read())

		os.unlink(small_file_checksums)

		# Test that CHECKPOINT will fail in case of incorrect checkpoint number
		node.safe_psql("CHECKPOINT")
		shutil.copy(small_file_checksums, f"{small_file_checksums}.copy")

		pattern_str = r"^FILE: (?P<filename>.+), CHECKSUM: (?P<checksum>.+), CHECKPOINT: (?P<checkpoint>\d+)$"
		pattern = re.compile(pattern_str)

		with open(small_file_checksums, "r+") as f:
			m = pattern.search(f.readline().rstrip())
			assert m is not None

			f.seek(0, os.SEEK_END)
			f.write(
			    f"FILE: {m.groups()[0]}, CHECKSUM: {m.groups()[1]}, CHECKPOINT: 100"
			)

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn(
			    'ERROR:  unexpected checkpoint number in the checksum file',
			    f.read())

		# Test that CHECKPOINT will fail in case of duplicated entries
		shutil.copy(f"{small_file_checksums}.copy", small_file_checksums)

		with open(small_file_checksums, "r+") as f:
			line = f.readline().rstrip()
			f.seek(0, os.SEEK_END)
			f.write(line)

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn(
			    'ERROR:  the file name is duplicated in the checksum file',
			    f.read())

		# Test that CHECKPOINT will fail in case of invalid file
		shutil.copy(f"{small_file_checksums}.copy", small_file_checksums)

		with open(small_file_checksums, "r+") as f:
			m = pattern.search(f.readline().rstrip())
			assert m is not None

			f.seek(0, os.SEEK_END)
			f.write(f"FILE: {m.groups()[0]}, CHECKSUM: {m.groups()[1]}")

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn('ERROR:  invalid line format of the checksum file',
			              f.read())

	def test_s3_ddl_recovery(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
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
			orioledb.s3_cainfo = '{self.s3_cainfo}'
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
			orioledb.s3_cainfo = '{self.s3_cainfo}'
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

		with testgres.get_new_node('test',
		                           base_dir=new_temp_dir,
		                           port=self.getBasePort() + 1) as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()

	@s3_test_attrs(
	    http=True,
	    prefix=f'{S3BaseTest.bucket_name}/{S3BaseTest.optional_prefix}')
	def test_s3_http_and_prefix(self):
		prefix = f'{self.bucket_name}/{self.optional_prefix}'
		node = self.node
		self.client.put_object(Bucket=self.bucket_name,
		                       Key=self.optional_prefix)

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}'
			orioledb.s3_prefix = '{prefix}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = false

			archive_mode = on
			archive_library = 'orioledb'
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

		wal_prefix = os.path.join(prefix, 'wal/')
		while self.client.list_objects(Bucket=self.bucket_name,
		                               Prefix=wal_prefix) == []:
			pass
		os.kill(archiver_pid, signal.SIGUSR2)
		while node.status() == NodeStatus.Running:
			pass

		with testgres.get_new_node('test',
		                           base_dir=new_temp_dir,
		                           port=self.getBasePort() + 1) as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	@s3_test_attrs(host=f'{S3BaseTest.bucket_name}.{S3BaseTest.host}',
	               prefix=S3BaseTest.optional_prefix)
	def test_s3_prefix_no_bucket(self):
		node = self.node
		self.client.put_object(Bucket=self.bucket_name,
		                       Key=self.optional_prefix)

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.bucket_name}.{self.host}:{self.port}'
			orioledb.s3_prefix = {self.optional_prefix}
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = true

			archive_mode = on
			archive_library = 'orioledb'
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

		wal_prefix = os.path.join(self.optional_prefix, 'wal/')
		while self.client.list_objects(Bucket=self.bucket_name,
		                               Prefix=wal_prefix) == []:
			pass
		os.kill(archiver_pid, signal.SIGUSR2)
		while node.status() == NodeStatus.Running:
			pass

		with testgres.get_new_node('test',
		                           base_dir=new_temp_dir,
		                           port=self.getBasePort() + 1) as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	@s3_test_attrs(http=True,
	               host=f'{S3BaseTest.bucket_name}.{S3BaseTest.host}')
	def test_s3_http_virtual_host(self):
		node = self.node

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.bucket_name}.{self.host}:{self.port}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = false

			archive_mode = on
			archive_library = 'orioledb'
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

		wal_prefix = os.path.join(self.optional_prefix, 'wal/')
		while self.client.list_objects(Bucket=self.bucket_name,
		                               Prefix=wal_prefix) == []:
			pass
		os.kill(archiver_pid, signal.SIGUSR2)
		while node.status() == NodeStatus.Running:
			pass

		with testgres.get_new_node('test',
		                           base_dir=new_temp_dir,
		                           port=self.getBasePort() + 1) as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	@s3_test_attrs(host=f'{S3BaseTest.bucket_name}.{S3BaseTest.host}')
	def test_s3_https_virtual_host(self):
		node = self.node

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.bucket_name}.{self.host}:{self.port}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = true

			archive_mode = on
			archive_library = 'orioledb'
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

		wal_prefix = os.path.join(self.optional_prefix, 'wal/')
		while self.client.list_objects(Bucket=self.bucket_name,
		                               Prefix=wal_prefix) == []:
			pass
		os.kill(archiver_pid, signal.SIGUSR2)
		while node.status() == NodeStatus.Running:
			pass

		with testgres.get_new_node('test',
		                           base_dir=new_temp_dir,
		                           port=self.getBasePort() + 1) as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	def test_s3_check_control(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			control_filename = os.path.join(node.data_dir, "orioledb_data",
			                                "control")
			shutil.copy(control_filename, f"{control_filename}.copy")

			node.stop()

			# Test that PostgreSQL won't start in case of absent control file
			new_node = self.initNode(self.getBasePort() + 1, 'tgsb')
			new_node.append_conf(f"""
				orioledb.s3_mode = true
				orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
				orioledb.s3_region = '{self.region}'
				orioledb.s3_accesskey = '{self.access_key_id}'
				orioledb.s3_secretkey = '{self.secret_access_key}'
				orioledb.s3_cainfo = '{self.s3_cainfo}'
			""")

			with self.assertRaises(StartNodeException) as e:
				new_node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(new_node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  OrioleDB can be incompatible with the S3 bucket because the control file exists on the S3 bucket',
				    f.read())

			# Test that PostgreSQL won't start in case of different control identifier
			new_node.append_conf("orioledb.s3_prefix = 'temp_prefix'")
			new_node.start()
			new_node.safe_psql("CHECKPOINT")
			new_node.stop()

			new_node.append_conf("orioledb.s3_prefix = ''")

			with self.assertRaises(StartNodeException) as e:
				new_node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(new_node.pg_log_file) as f:
				self.assertIn('differs from the S3 bucket identifier',
				              f.read())

			# Test that PostgreSQL won't start in case of wrong CRC of the control file
			with open(control_filename, "rb+") as f:
				f.write(b'\x11\x11\x11\x11\x11\x11\x11\x11')

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn('FATAL:  Wrong CRC in control file', f.read())

			# Test that PostgreSQL won't start in case of corrupted control file
			with open(control_filename, "w+") as f:
				f.truncate(10)

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  could not read data from control file "orioledb_data/control"',
				    f.read())

			# Test that PostgreSQL won't start in case of different checkpoint number
			shutil.copy(f"{control_filename}.copy", control_filename)
			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    "FATAL:  OrioleDB misses new changes and checkpoints from the S3 bucket and they are incompatible with each other",
				    f.read())

			# Test that PostgreSQL won't start in case of lack of permissions
			os.chmod(control_filename, 0)
			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  could not open file "orioledb_data/control": Permission denied',
				    f.read())

		# Patch put_object() to test "If-None-Match" and 409 error code
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object_conflict):
			with self.assertRaises(StartNodeException) as e:
				node.start()

			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  failed to create a lock file "data/orioledb_data/s3_lock" because of a concurrent process',
				    f.read())

		# Test unkown error handling
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_moto_unkown_error):
			with self.assertRaises(StartNodeException) as e:
				node.start()

			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn('FATAL:  could not put object to S3', f.read())

	def test_s3_put_lock_file(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()

			lock_filename = os.path.join(node.data_dir, "orioledb_data",
			                             "s3_lock")
			shutil.copy(lock_filename, f"{lock_filename}.copy")

			node.stop()

			# Test that PostgreSQL won't start in case of corrupted lock file 1
			fstat = os.stat(lock_filename)
			with open(lock_filename, "r+") as f:
				f.write("\0" * fstat.st_size)

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn('FATAL:  incorrect value of lock identifier 0',
				              f.read())

			# Test that PostgreSQL won't start in case of corrupted lock file 2
			with open(lock_filename, "w+") as f:
				f.truncate(0)

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  could not read data from lock file "orioledb_data/s3_lock"',
				    f.read())

			# Test that PostgreSQL won't start in case of lack of permissions
			shutil.copy(f"{lock_filename}.copy", lock_filename)
			os.chmod(lock_filename, 0)

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  could not open file "orioledb_data/s3_lock": Permission denied',
				    f.read())

	def test_s3_lock_file(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])
			objects = sorted(list(x["Key"] for x in objects))
			self.assertIn('data/orioledb_data/s3_lock', objects)
			self.assertIn('data/orioledb_data/control', objects)

			with self.getReplica() as new_node:
				# Remove the lock file since pg_basebackup will copy it
				os.remove(
				    os.path.join(new_node.data_dir, "orioledb_data",
				                 "s3_lock"))

				with self.assertRaises(StartNodeException) as e:
					new_node.start()
				self.assertEqual(e.exception.message, "Cannot start node")
				with open(new_node.pg_log_file) as f:
					log = f.readlines()
				message = log[0].split('] ')[-1].strip()
				self.assertEqual(
				    message,
				    "FATAL:  A lock file from a different OrioleDB instance already exists on the S3 bucket"
				)

				new_node.cleanup()

			node.stop()

			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])
			objects = sorted(list(x["Key"] for x in objects))
			self.assertNotIn('data/orioledb_data/s3_lock', objects)

	def test_s3_control_consistency(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])
			objects = sorted(list(x["Key"] for x in objects))
			self.assertIn('data/orioledb_data/s3_lock', objects)
			self.assertIn('data/orioledb_data/control', objects)

			with self.getReplica() as new_node:
				# Remove the lock file since pg_basebackup will copy it
				os.remove(
				    os.path.join(new_node.data_dir, "orioledb_data",
				                 "s3_lock"))

				node.stop(["-m", "immediate"])

				objects = self.client.list_objects(Bucket=self.bucket_name)
				objects = objects.get("Contents", [])
				objects = sorted(list(x["Key"] for x in objects))
				self.assertNotIn('data/orioledb_data/s3_lock', objects)
				self.assertIn('data/orioledb_data/control', objects)

				new_node.start()

				objects = self.client.list_objects(Bucket=self.bucket_name)
				objects = objects.get("Contents", [])
				objects = sorted(list(x["Key"] for x in objects))
				self.assertIn('data/orioledb_data/s3_lock', objects)
				self.assertIn('data/orioledb_data/control', objects)

				new_node.stop()
				new_node.cleanup()

	def test_s3_control_inconsistency(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			with self.getReplica() as new_node:
				# Remove the lock file since pg_basebackup will copy it
				os.remove(
				    os.path.join(new_node.data_dir, "orioledb_data",
				                 "s3_lock"))

				node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
				node.safe_psql("""
					CREATE TABLE o_test (
						val_1 int
					) USING orioledb;
					INSERT INTO o_test SELECT * FROM generate_series(1, 5);
				""")
				node.stop()

				with self.assertRaises(StartNodeException) as e:
					new_node.start()
				self.assertEqual(e.exception.message, "Cannot start node")
				with open(new_node.pg_log_file) as f:
					log = f.readlines()
				message = log[0].split('] ')[-1].strip()
				self.assertEqual(
				    message,
				    "FATAL:  OrioleDB misses new changes and checkpoints from the S3 bucket and they are incompatible with each other"
				)

				new_node.cleanup()

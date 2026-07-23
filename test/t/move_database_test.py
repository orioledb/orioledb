#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from testgres.exceptions import QueryException
import os
import shutil
import subprocess
import testgres
import inspect
import time
from threading import Thread, Event
import tempfile


class ThreadLoopQueryExecutor(Thread):

	def __init__(self, connection, sql_query):
		super().__init__()
		self._connection = connection
		self._sql_query = sql_query
		self._stop_event = Event()
		self._result = None

	def run(self):
		try:
			while not self._stop_event.is_set():
				try:
					res = self._connection.execute(self._sql_query)
					self._result = res
				except Exception as e:
					self._result = e
					break
		finally:
			pass

	def join(self, timeout=None):
		self._stop_event.set()
		super().join(timeout=timeout)
		if isinstance(self._return, Exception):
			raise self._result
		return self._result

	def stop(self):
		self._stop_event.set()


def create_and_clear_directory(base_path, dir_name):
	new_dir_path = os.path.join(base_path, dir_name)

	if os.path.exists(new_dir_path):
		shutil.rmtree(new_dir_path)

	os.makedirs(new_dir_path)

	return new_dir_path


def get_files_in_directory(dir, files):
	if dir is None:
		return []
	existed_files = []
	for root, _, filenames in os.walk(dir):
		for filename in filenames:
			if filename in files:
				existed_files.append(filename)
	return sorted(existed_files)


def get_catversion(data_dir):
	result = subprocess.run(['pg_controldata', data_dir],
	                        capture_output=True,
	                        text=True)
	for line in result.stdout.splitlines():
		if 'Catalog version number' in line:
			return line.split(':')[-1].strip()
	raise RuntimeError("Catalog version not found")


class TablespaceTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.ts_tmpdir = tempfile.mkdtemp()
		user_ts1_path = create_and_clear_directory(self.ts_tmpdir, 'user_ts1')
		user_ts2_path = create_and_clear_directory(self.ts_tmpdir, 'user_ts2')
		self.node.start()
		self.node.safe_psql(
		    f"CREATE TABLESPACE user_ts1 LOCATION '{user_ts1_path}';")
		self.node.safe_psql(
		    f"CREATE TABLESPACE user_ts2 LOCATION '{user_ts2_path}';")
		major_version = int(
		    self.node.execute("SHOW server_version_num;")[0][0]) // 10000
		cat_version = get_catversion(self.node.data_dir)
		self.node.stop()
		self.ts_cat_version = f"PG_{major_version}_{cat_version}"
		self.user_ts1_path = os.path.join(user_ts1_path, self.ts_cat_version)
		self.user_ts2_path = os.path.join(user_ts2_path, self.ts_cat_version)

	def tearDown(self):
		super().tearDown()
		if os.path.exists(self.node.data_dir):
			shutil.move(self.ts_tmpdir, self.node.data_dir)

	def ensureDataAccess(self, node, dbname, plan_setup, data_query,
	                     plan_expected, data_expected):
		self.assertTrue(plan_expected in node.safe_psql(
		    dbname, f"{plan_setup} EXPLAIN {data_query}").decode('utf-8'))
		res = node.safe_psql(dbname, f"{plan_setup} {data_query}")
		res = [
		    tuple([int(x)]) for x in res.decode('utf-8').strip().splitlines()
		    if x.strip()
		]
		self.assertEqual(data_expected, res)

	def ensureDataAccessReplication(self, master, replica, table,
	                                seq_scan_expected, index_scan_expected):
		check_seq_scan_query = f"SELECT i FROM {table} WHERE mod(i,10) = 0;"
		set_prefix_for_index_scan = "set enable_seqscan = off; set enable_bitmapscan = off;"
		check_index_scan_query = f"SELECT i FROM {table} WHERE t like 'test%9' ORDER BY i;"

		self.ensureDataAccess(master, "testdb", "", check_seq_scan_query,
		                      "Seq Scan", seq_scan_expected)
		self.ensureDataAccess(replica, "testdb", "", check_seq_scan_query,
		                      "Seq Scan", seq_scan_expected)
		self.ensureDataAccess(master, "testdb", set_prefix_for_index_scan,
		                      check_index_scan_query, "Index Scan",
		                      index_scan_expected)
		self.ensureDataAccess(replica, "testdb", set_prefix_for_index_scan,
		                      check_index_scan_query, "Index Scan",
		                      index_scan_expected)

	def get_relfilenode(self, dbname, relname):
		node = self.node
		return str(
		    node.execute(
		        dbname,
		        f"select relfilenode from pg_class where relname='{relname}';")
		    [0][0])

	def move_database_test(self, ddl_query, created_ts, moved_ts, stay_ts,
	                       moved_relations, stay_relations):
		node = self.node
		node.start()

		ts_suffix = f"TABLESPACE {created_ts[0]}" if created_ts[
		    0] is not None else ""
		node.safe_psql(f"CREATE DATABASE testdb {ts_suffix};")

		dbOid = node.execute(
		    f"SELECT oid FROM pg_database WHERE datname='testdb';")[0][0]
		created_db_dir = os.path.join(created_ts[1], str(dbOid))
		moved_db_dir = os.path.join(moved_ts[1], str(dbOid))
		stay_db_dir = os.path.join(stay_ts[1],
		                           str(dbOid)) if stay_ts is not None else None

		node.safe_psql("testdb", """CREATE EXTENSION orioledb;""")
		node.safe_psql("testdb", ddl_query)
		node.safe_psql(
		    "testdb",
		    """INSERT INTO test_tbl_ctid SELECT i, i * 2, 'test'||i FROM generate_series(1, 1000) i;
                                    INSERT INTO test_tbl_pkey SELECT i, i * 2, 'test'||i FROM generate_series(1, 1000) i;
                                 """)

		node.safe_psql("CHECKPOINT;")

		moved_files = [
		    self.get_relfilenode("testdb", x) for x in moved_relations
		]
		moved_files.sort()

		stay_files = [
		    self.get_relfilenode("testdb", x) for x in stay_relations
		]
		stay_files.sort()

		self.assertEqual(moved_files,
		                 get_files_in_directory(created_db_dir, moved_files))
		self.assertEqual(stay_files,
		                 get_files_in_directory(stay_db_dir, stay_files))

		node.safe_psql(f"ALTER DATABASE testdb SET TABLESPACE {moved_ts[0]};")

		self.assertEqual([], get_files_in_directory(created_db_dir,
		                                            moved_files))
		self.assertEqual(moved_files,
		                 get_files_in_directory(moved_db_dir, moved_files))
		self.assertEqual(stay_files,
		                 get_files_in_directory(stay_db_dir, stay_files))
		self.assertEqual([], get_files_in_directory(moved_db_dir, stay_files))

		# Check after move
		node.safe_psql(
		    "testdb",
		    """INSERT INTO test_tbl_ctid SELECT i, i * 2, 'test'||i FROM generate_series(1001, 2000) i;
                                    INSERT INTO test_tbl_pkey SELECT i, i * 2, 'test'||i FROM generate_series(1001, 2000) i;"""
		)

		self.assertEqual([], get_files_in_directory(created_db_dir,
		                                            moved_files))
		self.assertEqual(moved_files,
		                 get_files_in_directory(moved_db_dir, moved_files))
		self.assertEqual(stay_files,
		                 get_files_in_directory(stay_db_dir, stay_files))
		self.assertEqual([], get_files_in_directory(moved_db_dir, stay_files))

		self.ensureDataAccess(
		    node, "testdb", "set enable_seqscan = off;",
		    "SELECT count(*) FROM test_tbl_ctid WHERE mod(j, 3) = 0;",
		    "Index Only Scan", [(666, )])
		self.ensureDataAccess(
		    node, "testdb", "set enable_seqscan = off;",
		    "SELECT count(*) FROM test_tbl_ctid WHERE t like 'test11%';",
		    "Index Only Scan", [(111, )])
		self.ensureDataAccess(
		    node, "testdb", "",
		    "SELECT count(*) FROM test_tbl_ctid WHERE mod(i, 10) = 0;",
		    "Seq Scan", [(200, )])
		self.ensureDataAccess(
		    node, "testdb", "set enable_seqscan = off;",
		    "SELECT count(*) FROM test_tbl_pkey WHERE mod(j, 3) = 0;",
		    "Index Only Scan", [(666, )])
		self.ensureDataAccess(
		    node, "testdb", "set enable_seqscan = off;",
		    "SELECT count(*) FROM test_tbl_pkey WHERE t like 'test11%';",
		    "Index Only Scan", [(111, )])
		self.ensureDataAccess(
		    node, "testdb", "",
		    "SELECT count(*) FROM test_tbl_pkey WHERE mod(i, 10) = 0;",
		    "Seq Scan", [(200, )])

		node.stop()
		self.assertEqual([], get_files_in_directory(created_db_dir,
		                                            moved_files))
		self.assertEqual(moved_files,
		                 get_files_in_directory(moved_db_dir, moved_files))
		self.assertEqual(stay_files,
		                 get_files_in_directory(stay_db_dir, stay_files))
		self.assertEqual([], get_files_in_directory(moved_db_dir, stay_files))

	def test_move_from_user_ts_full_database(self):
		ddl_query = """ CREATE TABLE test_tbl_ctid(i int, j int, t text) USING orioledb;
                        CREATE TABLE test_tbl_pkey(i int PRIMARY KEY, j int, t text) USING orioledb;
                        CREATE INDEX secondary_idx_ctid1 ON test_tbl_ctid (t);
                        CREATE INDEX secondary_idx_ctid2 ON test_tbl_ctid (j);
                        CREATE INDEX secondary_idx_pkey1 ON test_tbl_pkey (t);
                        CREATE INDEX secondary_idx_pkey2 ON test_tbl_pkey (j);"""
		moved_relations = [
		    'test_tbl_ctid', 'secondary_idx_ctid1', 'secondary_idx_ctid2',
		    'test_tbl_pkey_pkey', 'secondary_idx_pkey1', 'secondary_idx_pkey2'
		]
		created_ts = ("user_ts1",
		              os.path.join(self.user_ts1_path, 'orioledb_data'))
		moved_ts = ("user_ts2",
		            os.path.join(self.user_ts2_path, 'orioledb_data'))
		self.move_database_test(ddl_query, created_ts, moved_ts, None,
		                        moved_relations, [])

	def test_move_from_default_ts_full_database(self):
		ddl_query = """ CREATE TABLE test_tbl_ctid(i int, j int, t text) USING orioledb;
                        CREATE TABLE test_tbl_pkey(i int PRIMARY KEY, j int, t text) USING orioledb;
                        CREATE INDEX secondary_idx_ctid1 ON test_tbl_ctid (t);
                        CREATE INDEX secondary_idx_ctid2 ON test_tbl_ctid (j);
                        CREATE INDEX secondary_idx_pkey1 ON test_tbl_pkey (t);
                        CREATE INDEX secondary_idx_pkey2 ON test_tbl_pkey (j);"""
		moved_relations = [
		    'test_tbl_ctid', 'secondary_idx_ctid1', 'secondary_idx_ctid2',
		    'test_tbl_pkey_pkey', 'secondary_idx_pkey1', 'secondary_idx_pkey2'
		]
		created_ts = (None,
		              os.path.join(self.node.base_dir, 'data',
		                           'orioledb_data'))
		moved_ts = ("user_ts2",
		            os.path.join(self.user_ts2_path, 'orioledb_data'))
		self.move_database_test(ddl_query, created_ts, moved_ts, None,
		                        moved_relations, [])

	def test_move_from_default_ts_only_indexes(self):
		ddl_query = """ CREATE TABLE test_tbl_pkey(i int PRIMARY KEY, j int, t text) USING orioledb TABLESPACE user_ts1;
                        CREATE INDEX secondary_idx_pkey1 ON test_tbl_pkey (t);
                        CREATE INDEX secondary_idx_pkey2 ON test_tbl_pkey (j);
                        CREATE TABLE test_tbl_ctid(i int, j int, t text) USING orioledb TABLESPACE user_ts1;
                        CREATE INDEX secondary_idx_ctid1 ON test_tbl_ctid (t);
                        CREATE INDEX secondary_idx_ctid2 ON test_tbl_ctid (j);"""
		# Primary key relation always create in database default tablespace.
		# So it moved with database.
		moved_relations = [
		    'secondary_idx_ctid1', 'secondary_idx_ctid2', 'test_tbl_pkey_pkey',
		    'secondary_idx_pkey1', 'secondary_idx_pkey2'
		]
		stay_relations = ['test_tbl_ctid']
		created_ts = (None,
		              os.path.join(self.node.base_dir, 'data',
		                           'orioledb_data'))
		moved_ts = ("user_ts2",
		            os.path.join(self.user_ts2_path, 'orioledb_data'))
		stay_ts = ("user_ts1", os.path.join(self.user_ts1_path,
		                                    'orioledb_data'))
		self.move_database_test(ddl_query, created_ts, moved_ts, stay_ts,
		                        moved_relations, stay_relations)

	def test_move_from_default_ts_only_tables(self):
		ddl_query = """ CREATE TABLE test_tbl_pkey(i int PRIMARY KEY, j int, t text) USING orioledb;
                        CREATE INDEX secondary_idx_pkey1 ON test_tbl_pkey (t) TABLESPACE user_ts1;
                        CREATE INDEX secondary_idx_pkey2 ON test_tbl_pkey (j)TABLESPACE user_ts1;
                        CREATE TABLE test_tbl_ctid(i int, j int, t text) USING orioledb;
                        CREATE INDEX secondary_idx_ctid1 ON test_tbl_ctid (t) TABLESPACE user_ts1;
                        CREATE INDEX secondary_idx_ctid2 ON test_tbl_ctid (j) TABLESPACE user_ts1;"""
		# Primary key relation always create in database default tablespace.
		# So it moved with database.
		moved_relations = ['test_tbl_ctid', 'test_tbl_pkey_pkey']
		stay_relations = [
		    'secondary_idx_ctid1', 'secondary_idx_ctid2',
		    'secondary_idx_pkey1', 'secondary_idx_pkey2'
		]
		created_ts = (None,
		              os.path.join(self.node.base_dir, 'data',
		                           'orioledb_data'))
		moved_ts = ("user_ts2",
		            os.path.join(self.user_ts2_path, 'orioledb_data'))
		stay_ts = ("user_ts1", os.path.join(self.user_ts1_path,
		                                    'orioledb_data'))
		self.move_database_test(ddl_query, created_ts, moved_ts, stay_ts,
		                        moved_relations, stay_relations)

	def test_move_from_default_ts_some_relations(self):
		ddl_query = """ CREATE TABLE test_tbl_pkey(i int PRIMARY KEY, j int, t text) USING orioledb ;
                        CREATE INDEX secondary_idx_pkey1 ON test_tbl_pkey (t);
                        CREATE INDEX secondary_idx_pkey2 ON test_tbl_pkey (j)TABLESPACE user_ts1;
                        CREATE TABLE test_tbl_ctid(i int, j int, t text) USING orioledb TABLESPACE user_ts1;
                        CREATE INDEX secondary_idx_ctid1 ON test_tbl_ctid (t) TABLESPACE user_ts1;
                        CREATE INDEX secondary_idx_ctid2 ON test_tbl_ctid (j);"""

		moved_relations = [
		    'test_tbl_pkey_pkey', 'secondary_idx_pkey1', 'secondary_idx_ctid2'
		]
		stay_relations = [
		    'secondary_idx_pkey2', 'test_tbl_ctid', 'secondary_idx_ctid1'
		]
		created_ts = (None,
		              os.path.join(self.node.base_dir, 'data',
		                           'orioledb_data'))
		moved_ts = ("user_ts2",
		            os.path.join(self.user_ts2_path, 'orioledb_data'))
		stay_ts = ("user_ts1", os.path.join(self.user_ts1_path,
		                                    'orioledb_data'))
		self.move_database_test(ddl_query, created_ts, moved_ts, stay_ts,
		                        moved_relations, stay_relations)

	def getReplica_TS(self,
	                  options,
	                  has_restoring: bool = False) -> testgres.PostgresNode:
		if self.replica is None:
			(test_path, t) = os.path.split(
			    os.path.dirname(inspect.getfile(self.__class__)))
			baseDir = os.path.join(test_path, 'tmp_check_t',
			                       self.myName + '_tgsb')
			if os.path.exists(baseDir):
				shutil.rmtree(baseDir)
			replica = self.node.backup(
			    base_dir=baseDir, options=options).spawn_replica('replica')
			replica.append_conf(port=replica.port)

			self.replica = replica

			if has_restoring:
				self.enableRestoring(
				    self.replica, os.path.join(self.node.base_dir, "archives"))

		return self.replica

	def test_replication_database_move(self):
		master = self.node
		master_ts1_path = os.path.dirname(self.user_ts1_path)
		master_ts2_path = os.path.dirname(self.user_ts2_path)
		replica_ts1_path = create_and_clear_directory(self.ts_tmpdir,
		                                              'replica_user_ts1')
		replica_ts2_path = create_and_clear_directory(self.ts_tmpdir,
		                                              'replica_user_ts2')
		ts_mapping = [
		    '-T', f"{master_ts1_path}={replica_ts1_path}", '-T',
		    f"{master_ts2_path}={replica_ts2_path}"
		]

		replica_ts1_path = os.path.join(replica_ts1_path, self.ts_cat_version)
		replica_ts2_path = os.path.join(replica_ts2_path, self.ts_cat_version)

		master.start()
		with self.getReplica_TS(ts_mapping).start() as replica:
			self.catchup_orioledb(replica)
			master.safe_psql("CREATE DATABASE testdb TABLESPACE user_ts1;")
			dbOid = master.execute(
			    f"SELECT oid FROM pg_database WHERE datname='testdb';")[0][0]

			master_ts1_path = os.path.join(self.user_ts1_path, 'orioledb_data',
			                               str(dbOid))
			master_ts2_path = os.path.join(self.user_ts2_path, 'orioledb_data',
			                               str(dbOid))
			replica_ts1_path = os.path.join(replica_ts1_path, 'orioledb_data',
			                                str(dbOid))
			replica_ts2_path = os.path.join(replica_ts2_path, 'orioledb_data',
			                                str(dbOid))

			master.safe_psql(
			    "testdb", """CREATE EXTENSION orioledb;
                                          CREATE TABLE test_tbl (i int, j int, t text) USING orioledb;
                                          CREATE INDEX test_tbl_t_idx ON test_tbl(t);
                                          CREATE TABLE test_tbl2 (i int, j int PRIMARY KEY, t text) USING orioledb;
                                          CREATE INDEX test_tbl2_t_idx ON test_tbl2(t);"""
			)

			files_to_check = [
			    self.get_relfilenode("testdb", "test_tbl"),
			    self.get_relfilenode("testdb", "test_tbl_t_idx"),
			    self.get_relfilenode("testdb", "test_tbl2_pkey"),
			    self.get_relfilenode("testdb", "test_tbl2_t_idx")
			]

			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(1, 10) i;
			       INSERT INTO test_tbl2 SELECT i, i * 2, 'test'||i FROM generate_series(1, 10) i;"""
			)
			self.catchup_orioledb(replica)

			con1 = replica.connect("testdb")
			t1 = ThreadLoopQueryExecutor(con1,
			                             "SELECT count(*) FROM test_tbl;")
			t1.start()
			master.safe_psql("ALTER DATABASE testdb SET TABLESPACE user_ts2;")
			self.catchup_orioledb(replica)
			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(11, 20) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(21, 30) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(31, 40) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)

			try:
				t1.stop()
				t1.join()
				self.assertTrue(False)
			except:
				self.assertTrue(True)

			seq_scan_expected1 = [(10, ), (20, ), (30, ), (40, )]
			index_scan_expected1 = [(9, ), (19, ), (29, ), (39, )]
			self.ensureDataAccessReplication(master, replica, 'test_tbl',
			                                 seq_scan_expected1,
			                                 index_scan_expected1)

			seq_scan_expected2 = [(10, )]
			index_scan_expected2 = [(9, )]
			self.ensureDataAccessReplication(master, replica, 'test_tbl2',
			                                 seq_scan_expected2,
			                                 index_scan_expected2)

			master.stop()
			replica.stop()

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

			master.start()
			replica.start()
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(41, 50) i;"""
			)
			self.catchup_orioledb(replica)
			seq_scan_expected1.append((50, ))
			index_scan_expected1.append((49, ))
			self.ensureDataAccessReplication(master, replica, 'test_tbl',
			                                 seq_scan_expected1,
			                                 index_scan_expected1)
			self.ensureDataAccessReplication(master, replica, 'test_tbl2',
			                                 seq_scan_expected2,
			                                 index_scan_expected2)
			master.stop()
			replica.stop()

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

	def test_replication_of_moving_database_after_restart(self):
		master = self.node
		master_ts1_path = os.path.dirname(self.user_ts1_path)
		master_ts2_path = os.path.dirname(self.user_ts2_path)
		replica_ts1_path = create_and_clear_directory(self.ts_tmpdir,
		                                              'replica_user_ts1')
		replica_ts2_path = create_and_clear_directory(self.ts_tmpdir,
		                                              'replica_user_ts2')
		ts_mapping = [
		    '-T', f"{master_ts1_path}={replica_ts1_path}", '-T',
		    f"{master_ts2_path}={replica_ts2_path}"
		]

		replica_ts1_path = os.path.join(replica_ts1_path, self.ts_cat_version)
		replica_ts2_path = os.path.join(replica_ts2_path, self.ts_cat_version)

		master.start()
		with self.getReplica_TS(ts_mapping).start() as replica:
			self.catchup_orioledb(replica)
			master.safe_psql("CREATE DATABASE testdb TABLESPACE user_ts1;")
			dbOid = master.execute(
			    f"SELECT oid FROM pg_database WHERE datname='testdb';")[0][0]

			master_ts1_path = os.path.join(self.user_ts1_path, 'orioledb_data',
			                               str(dbOid))
			master_ts2_path = os.path.join(self.user_ts2_path, 'orioledb_data',
			                               str(dbOid))
			replica_ts1_path = os.path.join(replica_ts1_path, 'orioledb_data',
			                                str(dbOid))
			replica_ts2_path = os.path.join(replica_ts2_path, 'orioledb_data',
			                                str(dbOid))

			master.safe_psql(
			    "testdb", """CREATE EXTENSION orioledb;
                                          CREATE TABLE test_tbl (i int, j int, t text) USING orioledb;
                                          CREATE INDEX test_tbl_t_idx ON test_tbl(t);
                                          CREATE TABLE test_tbl2 (i int, j int PRIMARY KEY, t text) USING orioledb;
                                          CREATE INDEX test_tbl2_t_idx ON test_tbl2(t);"""
			)

			files_to_check = [
			    self.get_relfilenode("testdb", "test_tbl"),
			    self.get_relfilenode("testdb", "test_tbl_t_idx")
			]

			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(1, 10) i;
			       INSERT INTO test_tbl2 SELECT i, i * 2, 'test'||i FROM generate_series(1, 10) i;"""
			)
			self.catchup_orioledb(replica)
			master.stop()
			replica.stop()

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts1_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts1_path, files_to_check))

			master.start()
			replica.start()

			master.safe_psql("ALTER DATABASE testdb SET TABLESPACE user_ts2;")
			self.catchup_orioledb(replica)
			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(11, 20) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(21, 30) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(31, 40) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)

			seq_scan_expected1 = [(10, ), (20, ), (30, ), (40, )]
			index_scan_expected1 = [(9, ), (19, ), (29, ), (39, )]
			self.ensureDataAccessReplication(master, replica, 'test_tbl',
			                                 seq_scan_expected1,
			                                 index_scan_expected1)

			seq_scan_expected2 = [(10, )]
			index_scan_expected2 = [(9, )]
			self.ensureDataAccessReplication(master, replica, 'test_tbl2',
			                                 seq_scan_expected2,
			                                 index_scan_expected2)

			master.stop()
			replica.stop()

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

			master.start()
			replica.start()
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(41, 50) i;"""
			)
			self.catchup_orioledb(replica)
			seq_scan_expected1.append((50, ))
			index_scan_expected1.append((49, ))
			self.ensureDataAccessReplication(master, replica, 'test_tbl',
			                                 seq_scan_expected1,
			                                 index_scan_expected1)
			self.ensureDataAccessReplication(master, replica, 'test_tbl2',
			                                 seq_scan_expected2,
			                                 index_scan_expected2)
			master.stop()
			replica.stop()

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

	def test_replication_of_moving_database_evicted_relation(self):
		master = self.node
		master_ts1_path = os.path.dirname(self.user_ts1_path)
		master_ts2_path = os.path.dirname(self.user_ts2_path)
		replica_ts1_path = create_and_clear_directory(self.ts_tmpdir,
		                                              'replica_user_ts1')
		replica_ts2_path = create_and_clear_directory(self.ts_tmpdir,
		                                              'replica_user_ts2')
		ts_mapping = [
		    '-T', f"{master_ts1_path}={replica_ts1_path}", '-T',
		    f"{master_ts2_path}={replica_ts2_path}"
		]

		replica_ts1_path = os.path.join(replica_ts1_path, self.ts_cat_version)
		replica_ts2_path = os.path.join(replica_ts2_path, self.ts_cat_version)

		master.start()
		with self.getReplica_TS(ts_mapping).start() as replica:
			self.catchup_orioledb(replica)
			master.safe_psql("CREATE DATABASE testdb TABLESPACE user_ts1;")
			dbOid = master.execute(
			    f"SELECT oid FROM pg_database WHERE datname='testdb';")[0][0]

			master_ts1_path = os.path.join(self.user_ts1_path, 'orioledb_data',
			                               str(dbOid))
			master_ts2_path = os.path.join(self.user_ts2_path, 'orioledb_data',
			                               str(dbOid))
			replica_ts1_path = os.path.join(replica_ts1_path, 'orioledb_data',
			                                str(dbOid))
			replica_ts2_path = os.path.join(replica_ts2_path, 'orioledb_data',
			                                str(dbOid))

			master.safe_psql(
			    "testdb", """CREATE EXTENSION orioledb;
                                          CREATE TABLE test_tbl (i int, j int, t text) USING orioledb;
                                          CREATE INDEX test_tbl_t_idx ON test_tbl(t);
                                          CREATE TABLE test_tbl2 (i int, j int PRIMARY KEY, t text) USING orioledb;
                                          CREATE INDEX test_tbl2_t_idx ON test_tbl2(t);"""
			)

			files_to_check = [
			    self.get_relfilenode("testdb", "test_tbl"),
			    self.get_relfilenode("testdb", "test_tbl_t_idx")
			]

			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(1, 10) i;
				   INSERT INTO test_tbl2 SELECT i, i * 2, 'test'||i FROM generate_series(1, 10) i;"""
			)
			self.catchup_orioledb(replica)

			replica.safe_psql(
			    "testdb",
			    "SELECT orioledb_evict_pages('test_tbl'::regclass, -1);")
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts1_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts1_path, files_to_check))

			master.safe_psql("ALTER DATABASE testdb SET TABLESPACE user_ts2;")
			self.catchup_orioledb(replica)
			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(11, 20) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(21, 30) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(31, 40) i;"""
			)
			master.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			seq_scan_expected1 = [(10, ), (20, ), (30, ), (40, )]
			index_scan_expected1 = [(9, ), (19, ), (29, ), (39, )]
			self.ensureDataAccessReplication(master, replica, 'test_tbl',
			                                 seq_scan_expected1,
			                                 index_scan_expected1)

			seq_scan_expected2 = [(10, )]
			index_scan_expected2 = [(9, )]
			self.ensureDataAccessReplication(master, replica, 'test_tbl2',
			                                 seq_scan_expected2,
			                                 index_scan_expected2)

			master.stop()
			replica.stop()

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

			master.start()
			replica.start()
			master.safe_psql(
			    "testdb",
			    """INSERT INTO test_tbl SELECT i, i * 2, 'test'||i FROM generate_series(41, 50) i;"""
			)
			self.catchup_orioledb(replica)
			seq_scan_expected1.append((50, ))
			index_scan_expected1.append((49, ))
			self.ensureDataAccessReplication(master, replica, 'test_tbl',
			                                 seq_scan_expected1,
			                                 index_scan_expected1)
			self.ensureDataAccessReplication(master, replica, 'test_tbl2',
			                                 seq_scan_expected2,
			                                 index_scan_expected2)
			master.stop()
			replica.stop()

			self.assertFalse(os.path.exists(master_ts1_path))
			self.assertFalse(os.path.exists(replica_ts1_path))

			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(master_ts2_path, files_to_check))
			self.assertEqual(
			    files_to_check,
			    get_files_in_directory(replica_ts2_path, files_to_check))

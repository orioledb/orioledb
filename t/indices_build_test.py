#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent, wait_checkpointer_stopevent

from itertools import groupby
import os
import re


class IndicesBuildTest(BaseTest):

	def test_index_build_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_indices0
			(
				key bigint NOT NULL,
				val int,
				val2 int,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE INDEX o_indices0_idx0 ON o_indices0 (val2);
			INSERT INTO o_indices0 SELECT 1000 + i, 3000 + i,
										   3000 + i FROM
												generate_series(1, 500) AS i;
		""")
		node.safe_psql(
		    'postgres', """
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop()

		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop()

	def test_index_build_recovery(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "shared_preload_libraries = orioledb\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_indices0
			(
				key TEXT NOT NULL,
				val int,
				val2 int,
				PRIMARY KEY (key)
			) USING orioledb;
			INSERT INTO o_indices0 SELECT (1000 + i)::text, 3000 + i,
										   3000 + i FROM
												generate_series(1, 500) AS i;
		""")
		node.safe_psql(
		    'postgres', """
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop(['-m', 'immediate'])

		node.start()
		explain = node.safe_psql("""
			SET enable_seqscan = off;
			EXPLAIN SELECT * FROM o_indices0
				WHERE val2 > 0 ORDER BY val2;""").decode('utf-8')
		self.assertEqual(explain.find('o_indices0_pkey'), -1)
		self.assertNotEqual(explain.find('o_indices0_idx1'), -1)
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop()

	def test_drop_primary_recovery(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_indices0
			(
				key TEXT NOT NULL,
				val int,
				val2 int,
				PRIMARY KEY (key)
			) USING orioledb;
			INSERT INTO o_indices0 SELECT (1000 + i)::text, 3000 + i,
										   3000 + i FROM
												generate_series(1, 500) AS i;
		""")
		node.safe_psql(
		    'postgres', """
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.safe_psql(
		    'postgres', """
			ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop()

	def test_create_primary_recovery(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_indices0
			(
				key TEXT NOT NULL,
				val int,
				val2 int,
				PRIMARY KEY (key)
			) USING orioledb;
			INSERT INTO o_indices0 SELECT (1000 + i)::text, 3000 + i,
										   3000 + i FROM
												generate_series(1, 500) AS i;
		""")
		node.safe_psql(
		    'postgres', """
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.safe_psql(
		    'postgres', """
			ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
		""")
		node.safe_psql(
		    'postgres', """
			ALTER TABLE o_indices0
				ADD CONSTRAINT o_indices0_pkey
					PRIMARY KEY (key);
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0],
		    500)
		self.assertEqual(
		    node.execute(
		        'postgres',
		        'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0],
		    500)
		node.stop()

	def test_toast_recovery(self):
		node = self.node
		node.start()
		string = self.genString(1, 4000)
		test_tuple = (2, 'test', string)
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_indices0
			(
				key integer NOT NULL,
				val text COLLATE "C" NOT NULL,
				val2 text COLLATE "C" NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;

			INSERT INTO o_indices0 VALUES(2, 'test', '%s');
		""" % string)
		self.assertEqual(
		    node.execute('postgres', 'SELECT * FROM o_indices0;')[0],
		    test_tuple)
		node.safe_psql(
		    'postgres', """
			ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT * FROM o_indices0;')[0],
		    test_tuple)
		node.safe_psql(
		    'postgres', """
			ALTER TABLE o_indices0
				ADD CONSTRAINT o_indices0_pkey
					PRIMARY KEY (key);
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT * FROM o_indices0;')[0],
		    test_tuple)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
		    node.execute('postgres', 'SELECT * FROM o_indices0;')[0],
		    test_tuple)
		node.stop()

	def check_used_index(self, node, query, expected_index):
		explain = node.safe_psql("SET enable_seqscan = off; EXPLAIN %s;" %
		                         query).decode('utf-8')
		groups = re.search(r'index (?:only )?scan of: (\w+).*', explain)
		if groups:
			used_index = groups.group(1)
		else:
			groups = re.search(r'Seq Scan on (\w+).*', explain)
			used_index = groups.group(1)
		self.assertEqual(expected_index, used_index)
		self.assertEqual(
		    node.execute("""
			WITH o_test_cte AS (
				%s
			) SELECT COUNT(*) FROM o_test_cte;
		""" % query)[0][0], 500)
		node.poll_query_until("SELECT orioledb_has_retained_undo();",
		                      expected=False)

	def test_index_build_replication(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', """
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_indices0
						(
							key TEXT NOT NULL,
							val int,
							val2 int,
							PRIMARY KEY (key)
						) USING orioledb;
						INSERT INTO o_indices0
							SELECT 1000 + i, 3000 + i,
								   3000 + i FROM generate_series(1, 500) AS i;
					""")

				# CREATE INDEX replication of rollback and commit
				with master.connect() as con:
					con.execute("""
							CREATE UNIQUE INDEX o_indices0_idx1
							ON o_indices0 (val2);
						""")
					con.rollback()
				self.catchup_orioledb(replica)
				self.check_used_index(
				    replica, """SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY key""", 'o_indices0_pkey')

				master.safe_psql(
				    'postgres', """
						CREATE UNIQUE INDEX o_indices0_idx1
							ON o_indices0 (val2);
						""")
				self.catchup_orioledb(replica)
				self.check_used_index(
				    replica, """SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY val2""", 'o_indices0_idx1')

				# primary index drop replication of rollback and commit
				with master.connect() as con:
					con.execute("""
							ALTER TABLE o_indices0
								DROP CONSTRAINT o_indices0_pkey;
						""")
					con.rollback()
				self.catchup_orioledb(replica)
				self.check_used_index(replica,
				                      "SELECT * FROM o_indices0 ORDER BY key",
				                      'o_indices0_pkey')

				master.safe_psql(
				    'postgres', """
						ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
					""")
				self.catchup_orioledb(replica)
				self.check_used_index(replica, "SELECT * FROM o_indices0",
				                      'o_indices0')

				# primary index creation replication of rollback and commit
				with master.connect() as con:
					con.execute("""
							ALTER TABLE o_indices0
								ADD CONSTRAINT o_indices0_pkey
									PRIMARY KEY (key);
						""")
					con.rollback()
				self.catchup_orioledb(replica)
				self.check_used_index(replica, "SELECT * FROM o_indices0",
				                      'o_indices0')

				master.safe_psql(
				    'postgres', """
						ALTER TABLE o_indices0
							ADD CONSTRAINT o_indices0_pkey
								PRIMARY KEY (key);
					""")
				self.catchup_orioledb(replica)
				self.check_used_index(replica,
				                      "SELECT * FROM o_indices0 ORDER BY key",
				                      'o_indices0_pkey')

				# DROP INDEX replication of rollback and commit
				with master.connect() as con:
					con.execute("""
							DROP INDEX o_indices0_idx1;
						""")
					con.rollback()
				self.catchup_orioledb(replica)
				self.check_used_index(
				    replica, """SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY val2""", 'o_indices0_idx1')

				master.safe_psql(
				    'postgres', """
						DROP INDEX o_indices0_idx1;
					""")
				self.catchup_orioledb(replica)
				self.check_used_index(
				    replica, """SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY key""", 'o_indices0_pkey')

				# primary index drop without any indices
				master.safe_psql(
				    'postgres', """
						ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
					""")
				self.catchup_orioledb(replica)
				self.check_used_index(replica, "SELECT * FROM o_indices0",
				                      'o_indices0')

				# primary index create without any indices
				master.safe_psql(
				    'postgres', """
						ALTER TABLE o_indices0
							ADD CONSTRAINT o_indices0_pkey
								PRIMARY KEY (key);
					""")
				self.catchup_orioledb(replica)
				self.check_used_index(replica,
				                      "SELECT * FROM o_indices0 ORDER BY key",
				                      'o_indices0_pkey')

	def test_multiple_indices_build_replication(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				columns = ",\n".join(["val%d int" % x for x in range(1, 10)])
				values = ", ".join(
				    ["i + %d" % ((x + 1) * 1000) for x in range(0, 10)])

				master.safe_psql(
				    'postgres', f"""
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_indices0
					(
						key TEXT NOT NULL,
						PRIMARY KEY (key),
						{columns}
					) USING orioledb;
					INSERT INTO o_indices0
						SELECT {values} FROM generate_series(1, 500) AS i;
				""")

				# multiple CREATE INDEX in the same transaction
				with master.connect() as con:
					con.begin()
					for i in range(1, 10):
						con.execute(f"""
							CREATE UNIQUE INDEX o_indices0_idx{i}
							ON o_indices0 (val{i});
						""")
					con.commit()
				self.catchup_orioledb(replica)
				for i in range(1, 10):
					self.check_used_index(
					    replica, f"""SELECT * FROM o_indices0
								WHERE val{i} > 0 ORDER BY val{i}""", f'o_indices0_idx{i}')

	def test_indices_build_xip(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "checkpoint_timeout = 1d\n"
		    "orioledb.enable_stopevents = true\n")
		node.start()

		columns = ",\n".join(["val%d int" % x for x in range(1, 10)])
		values = ", ".join(["i + %d" % ((x + 1) * 1000) for x in range(0, 10)])

		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_indices0
			(
				key TEXT NOT NULL,
				PRIMARY KEY (key),
				{columns}
			) USING orioledb;
			INSERT INTO o_indices0
				SELECT {values} FROM generate_series(1, 499) AS i;
		""")

		con1 = node.connect()
		chkp_con = node.connect()

		con1.begin()
		for i in range(1, 9):
			con1.execute(f"""
				CREATE UNIQUE INDEX o_indices0_idx{i}
				ON o_indices0 (val{i});
			""")
		con1.execute(
		    f"INSERT INTO o_indices0 SELECT {values} FROM generate_series(500, 500) AS i;"
		)
		con1.execute(
		    "SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_indices0_idx1\"');"
		)
		chkp_thread = ThreadQueryExecutor(chkp_con, "CHECKPOINT;")
		chkp_thread.start()
		wait_checkpointer_stopevent(node)

		con1.execute(f"""
			CREATE UNIQUE INDEX o_indices0_idx9
			ON o_indices0 (val9);
		""")

		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start');")
		chkp_thread.join()
		con1.commit()
		con1.close()
		chkp_con.close()

		node.stop(['-m', 'immediate'])

		node.start()
		for i in range(1, 10):
			self.check_used_index(
			    node, f"""SELECT * FROM o_indices0
					WHERE val{i} > 0 ORDER BY val{i}""", f'o_indices0_idx{i}')
		node.stop()

	def test_alter_column_type(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_ddl_check
			(
				f1 text NOT NULL COLLATE "C",
				f2 varchar NOT NULL,
				f3 integer,
				PRIMARY KEY (f1)
			) USING orioledb;

			INSERT INTO o_ddl_check VALUES ('ABC1', 'ABC2', NULL);
			INSERT INTO o_ddl_check VALUES ('ABC2', 'ABC4', NULL);
			INSERT INTO o_ddl_check VALUES ('ABC3', 'ABC6', NULL);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f2 TYPE char
				USING substr(f2, substr(f2,4,1)::int / 2, 1);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'A', None), ('ABC2', 'B', None),
		                  ('ABC3', 'C', None)])
		node.safe_psql("""
			INSERT INTO o_ddl_check VALUES ('ABC4', 'D', NULL);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'A', None), ('ABC2', 'B', None),
		                  ('ABC3', 'C', None), ('ABC4', 'D', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'A', None), ('ABC2', 'B', None),
		                  ('ABC3', 'C', None), ('ABC4', 'D', None)])
		node.stop()

	def test_indices_build_concurrent_checkpoint(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_indices
			(
				key int NOT NULL,
				val int,
				PRIMARY KEY (key)
			) USING orioledb;
			INSERT INTO o_indices
				SELECT i, i + 1000 FROM generate_series(1, 500) AS i;
		""")
		dboid = node.execute("""
			SELECT oid FROM pg_database WHERE datname = 'postgres';
		""")[0][0]
		ix_relnodes = node.execute("""
			SELECT index_relnode FROM orioledb_index_oids();
		""")
		filter_files = [(dboid, x[0]) for x in ix_relnodes]

		con1 = node.connect()
		con1_pid = con1.pid
		con2 = node.connect()
		con2.execute("CHECKPOINT;")
		con2.execute("CHECKPOINT;")

		con2.execute("""
			SELECT pg_stopevent_set('build_index_placeholder_inserted',
									'$.treeName == \"o_indices_ix1\"');
		""")
		con1_thread = ThreadQueryExecutor(
		    con1, """
			CREATE UNIQUE INDEX o_indices_ix1 ON o_indices (val);
		""")
		con1_thread.start()
		wait_stopevent(node, con1_pid)

		con2.execute("CHECKPOINT;")
		# o_indices_ix1 shouldn't be checkpointed
		self.assertEqual(self.get_map_files(filter_files), {})

		con2.execute("""
			SELECT pg_stopevent_reset('build_index_placeholder_inserted');
		""")
		con1_thread.join()
		con1.commit()
		con1.close()
		ix_oid = con2.execute("""
			SELECT 'o_indices_ix1'::regclass::oid;
		""")[0][0]
		con2.close()
		# single o_indices_ix1 map file should present
		self.assertEqual(self.get_map_files(filter_files),
		                 {f"{dboid}_{ix_oid}": [3]})

		node.stop(['-m', 'immediate'])
		node.start()
		node.execute("CHECKPOINT;")

		self.assertEqual(self.get_map_files(filter_files),
		                 {f"{dboid}_{ix_oid}": [3]})
		self.check_used_index(
		    node, "SELECT * FROM o_indices WHERE val > 0 ORDER BY val",
		    'o_indices_ix1')
		node.stop()

	def get_map_files(self, filter_files):
		map_files = []

		orioledb_dir = self.node.data_dir + "/orioledb_data"
		for ff in os.listdir(orioledb_dir):
			dbDir = os.path.join(orioledb_dir, ff)
			if os.path.isdir(dbDir):
				for f in os.listdir(dbDir):
					if re.match(".*\.map$", f):
						map_files.append(ff + '_' + f)

		map_files = [re.split(r'\.|-|_', f) for f in map_files]
		map_files = [[*[int(x) for x in f[:3]], f[3]] for f in map_files]
		map_files = sorted(map_files, reverse=True)
		map_files = [f for f in map_files if f[0] != 1]
		map_files = [f for f in map_files if (f[0], f[1]) not in filter_files]
		map_files = {
		    k: [x[2] for x in v]
		    for k, v in groupby(
		        map_files, key=(
		            lambda x: '_'.join([str(x[0]), str(x[1])])))
		}
		return map_files

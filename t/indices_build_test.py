#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest, wait_checkpointer_stopevent
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

import re

class IndicesBuildTest(BaseTest):
	def test_index_build_checkpoint(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',"""
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
		node.safe_psql('postgres',"""
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop()

		node.start()
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop()

	def test_index_build_recovery(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "shared_preload_libraries = orioledb\n")
		node.start()
		node.safe_psql('postgres',"""
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
		node.safe_psql('postgres',"""
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop(['-m', 'immediate'])

		node.start()
		explain = node.safe_psql("""
			SET enable_seqscan = off;
			EXPLAIN SELECT * FROM o_indices0
				WHERE val2 > 0 ORDER BY val2;""").decode('utf-8')
		self.assertEqual(explain.find('o_indices0_pkey'), -1)
		self.assertNotEqual(explain.find('o_indices0_idx1'), -1)
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop()

	def test_drop_primary_recovery(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',"""
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
		node.safe_psql('postgres',"""
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.safe_psql('postgres',"""
			ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
		""")
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop()

	def test_create_primary_recovery(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',"""
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
		node.safe_psql('postgres',"""
			CREATE INDEX o_indices0_idx1 ON o_indices0 (val2);
		""")
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.safe_psql('postgres',"""
			ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
		""")
		node.safe_psql('postgres',"""
			ALTER TABLE o_indices0
				ADD CONSTRAINT o_indices0_pkey
					PRIMARY KEY (key);
		""")
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute('postgres', 'SELECT count(*) FROM o_indices0;')[0][0], 500)
		self.assertEqual(node.execute('postgres', 'SELECT count(val2) FROM o_indices0 WHERE val2 > 0;')[0][0], 500)
		node.stop()

	def test_toast_recovery(self):
		node = self.node
		node.start()
		string = self.genString(1, 4000)
		test_tuple = (2, 'test', string)
		node.safe_psql('postgres',"""
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
		self.assertEqual(node.execute('postgres', 'SELECT * FROM o_indices0;')[0], test_tuple)
		node.safe_psql('postgres',"""
			ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
		""")
		self.assertEqual(node.execute('postgres', 'SELECT * FROM o_indices0;')[0], test_tuple)
		node.safe_psql('postgres',"""
			ALTER TABLE o_indices0
				ADD CONSTRAINT o_indices0_pkey
					PRIMARY KEY (key);
		""")
		self.assertEqual(node.execute('postgres', 'SELECT * FROM o_indices0;')[0], test_tuple)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute('postgres', 'SELECT * FROM o_indices0;')[0], test_tuple)
		node.stop()

	def check_used_index(self, node, query, expected_index):
		explain = node.safe_psql("SET enable_seqscan = off; EXPLAIN %s;" % query).decode('utf-8')
		groups = re.search(r'index (?:only )?scan of: (\w+).*', explain)
		if groups:
			used_index = groups.group(1)
		else:
			groups = re.search(r'Seq Scan on (\w+).*', explain)
			used_index = groups.group(1)
		self.assertEqual(expected_index, used_index)
		self.assertEqual(node.execute("""
			WITH o_test_cte AS (
				%s
			) SELECT COUNT(*) FROM o_test_cte;
		""" % query)[0][0], 500)
		node.poll_query_until("SELECT orioledb_has_retained_undo();",
									expected = False)

	def test_index_build_replication(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
					master.safe_psql('postgres', """
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
					self.check_used_index(replica,
						"""SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY key""",
						'o_indices0_pkey')

					master.safe_psql('postgres', """
						CREATE UNIQUE INDEX o_indices0_idx1
							ON o_indices0 (val2);
						""")
					self.catchup_orioledb(replica)
					self.check_used_index(replica,
						"""SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY val2""",
						'o_indices0_idx1')

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

					master.safe_psql('postgres', """
						ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
					""")
					self.catchup_orioledb(replica)
					self.check_used_index(replica,
						"SELECT * FROM o_indices0",
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
					self.check_used_index(replica,
						"SELECT * FROM o_indices0",
						'o_indices0')

					master.safe_psql('postgres', """
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
					self.check_used_index(replica,
						"""SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY val2""",
						'o_indices0_idx1')

					master.safe_psql('postgres', """
						DROP INDEX o_indices0_idx1;
					""")
					self.catchup_orioledb(replica)
					self.check_used_index(replica,
						"""SELECT * FROM o_indices0
							WHERE val2 > 0 AND val > 0 ORDER BY key""",
						'o_indices0_pkey')

					# primary index drop without any indices
					master.safe_psql('postgres', """
						ALTER TABLE o_indices0 DROP CONSTRAINT o_indices0_pkey;
					""")
					self.catchup_orioledb(replica)
					self.check_used_index(replica,
						"SELECT * FROM o_indices0",
						'o_indices0')

					# primary index create without any indices
					master.safe_psql('postgres', """
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
				columns = ",\n".join(
					["val%d int" % x for x in range(1, 10)])
				values = ", ".join(
					["i + %d" % ((x + 1) * 1000) for x in range(0, 10)])

				master.safe_psql('postgres', f"""
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
						self.check_used_index(replica,
							f"""SELECT * FROM o_indices0
								WHERE val{i} > 0 ORDER BY val{i}""",
							f'o_indices0_idx{i}')

	def test_indices_build_xip(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()

		columns = ",\n".join(
			["val%d int" % x for x in range(1, 10)])
		values = ", ".join(
			["i + %d" % ((x + 1) * 1000) for x in range(0, 10)])

		node.safe_psql('postgres', f"""
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

		con1 = node.connect()
		chkp_con = node.connect()

		con1.begin()
		for i in range(1, 9):
			con1.execute(f"""
				CREATE UNIQUE INDEX o_indices0_idx{i}
				ON o_indices0 (val{i});
			""")
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', '$.treeName == \"o_indices0_idx1\"');")
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
			self.check_used_index(node,
				f"""SELECT * FROM o_indices0
					WHERE val{i} > 0 ORDER BY val{i}""",
				f'o_indices0_idx{i}')
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
						 [('ABC1', 'ABC2', None),
						  ('ABC2', 'ABC4', None),
						  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'ABC2', None),
						  ('ABC2', 'ABC4', None),
						  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'ABC2', None),
						  ('ABC2', 'ABC4', None),
						  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'ABC2', None),
						  ('ABC2', 'ABC4', None),
						  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'ABC2', None),
						  ('ABC2', 'ABC4', None),
						  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'ABC2', None),
						  ('ABC2', 'ABC4', None),
						  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f2 TYPE char
				USING substr(f2, substr(f2,4,1)::int / 2, 1);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'A', None),
						  ('ABC2', 'B', None),
						  ('ABC3', 'C', None)])
		node.safe_psql("""
			INSERT INTO o_ddl_check VALUES ('ABC4', 'D', NULL);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'A', None),
						  ('ABC2', 'B', None),
						  ('ABC3', 'C', None),
						  ('ABC4', 'D', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
						 [('ABC1', 'A', None),
						  ('ABC2', 'B', None),
						  ('ABC3', 'C', None),
						  ('ABC4', 'D', None)])
		node.stop()
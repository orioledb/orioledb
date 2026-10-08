#!/usr/bin/env python3
# coding: utf-8

import os
import shutil
import tempfile
import unittest

from .base_test import BaseTest


class ToastOptionalTest(BaseTest):

	def test_rewrite_without_redundant_primary_rebuild(self):
		node = self.node
		node.append_conf('postgresql.conf', """
			log_min_messages = debug1
			max_parallel_maintenance_workers = 0
		""")
		node.start()
		node.safe_psql('postgres', 'CREATE EXTENSION orioledb;')
		for bridging in (False, True):
			table = 'with_bridge' if bridging else 'without_bridge'
			node.safe_psql(
			    'postgres', f"""
				CREATE TABLE {table} (id integer PRIMARY KEY, value bigint)
				  USING orioledb
				  WITH (index_bridging = {'true' if bridging else 'false'});
				CREATE INDEX {table}_value ON {table} (value);
				CREATE SEQUENCE {table}_seq;
				INSERT INTO {table} SELECT i, i FROM generate_series(1, 1000) i;
			""")
			if bridging:
				node.safe_psql('postgres', f"""
					CREATE INDEX {table}_bridged ON {table} ((value + id))
					  WITH (orioledb_index = false);
				""")

		log_path = os.path.join(node.logs_dir, 'postgresql.log')
		with open(log_path) as log:
			log.seek(0, os.SEEK_END)
			log_offset = log.tell()
		for table in ('with_bridge', 'without_bridge'):
			node.safe_psql('postgres', f"""
				ALTER TABLE {table} ALTER COLUMN value TYPE integer;
				ALTER TABLE {table} ADD COLUMN extra integer
				  DEFAULT nextval('{table}_seq');
			""")
			node.safe_psql('postgres', f"""
				BEGIN;
				ALTER TABLE {table} ALTER COLUMN value TYPE bigint;
				ROLLBACK;
			""")
			self.assertEqual(
			    node.execute('postgres', f"""
				SELECT reltoastrelid, orioledb_tbl_check(oid),
				       (SELECT count(*) FROM {table}),
				       (SELECT sum(value) FROM {table}),
				       (SELECT sum(extra) FROM {table}),
				       (SELECT last_value FROM {table}_seq)
				FROM pg_class WHERE oid = '{table}'::regclass
			"""), [(0, True, 1000, 500500, 500500, 1000)])

		# A schema rewrite copies the rows directly into the final primary
		# tree.  These messages identify an additional rebuild of all trees:
		# previously, finish_heap_swap's REINDEX dropped the primary key and
		# then recreated it, copying the same rows twice more.  Exclude setup,
		# where creating the primary key can legitimately rebuild the table.
		with open(log_path) as log:
			log.seek(log_offset)
			rewrite_log = log.read()
		self.assertEqual(rewrite_log.count('serial index rebuild:'), 0)
		self.assertEqual(rewrite_log.count('parallel index rebuild:'), 0)

		# Core REINDEX assigns new secondary-index relfilenodes.  Skipping
		# ambuild after an early rewrite would avoid extra copies but leave
		# those indexes empty; verify their contents after crash recovery too.
		node.stop(['-m', 'immediate'])
		node.start()
		with node.connect('postgres') as con:
			con.execute('SET enable_seqscan = off')
			for table in ('with_bridge', 'without_bridge'):
				plan = con.execute(
				    f'EXPLAIN SELECT id, extra FROM {table} WHERE value = 1000')
				self.assertIn(f'{table}_value', str(plan))
				self.assertEqual(
				    con.execute(
				        f'SELECT id, extra FROM {table} WHERE value = 1000'),
				    [(1000, 1000)])
			plan = con.execute('EXPLAIN SELECT id FROM with_bridge '
			                   'WHERE value + id = 2000')
			self.assertIn('with_bridge_bridged', str(plan))
			self.assertEqual(
			    con.execute('SELECT id FROM with_bridge '
			                'WHERE value + id = 2000'),
			    [(1000,)])

	def test_refresh_without_toast(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE source (id integer, value integer) USING orioledb;
			INSERT INTO source SELECT i, i FROM generate_series(1, 1000) i;
			CREATE MATERIALIZED VIEW mv USING orioledb AS SELECT * FROM source;
			CREATE UNIQUE INDEX mv_id ON mv (id);
			CREATE INDEX mv_value ON mv (value);
			INSERT INTO source VALUES (1001, 1001);
			REFRESH MATERIALIZED VIEW mv;
		""")
		node.safe_psql('postgres', """
			BEGIN;
			INSERT INTO source VALUES (1002, 1002);
			REFRESH MATERIALIZED VIEW mv;
			ROLLBACK;
		""")
		self.assertEqual(
		    node.execute('postgres', 'SELECT count(*), sum(value) FROM mv'),
		    [(1001, 501501)])
		# REFRESH has no final post-alter hook on the surviving relation.
		# Both population and WITH NO DATA must finish at the transient drop.
		node.safe_psql('postgres', 'REFRESH MATERIALIZED VIEW mv WITH NO DATA;')
		self.assertEqual(
		    node.execute('postgres', """
			SELECT reltoastrelid, relispopulated FROM pg_class
			WHERE oid = 'mv'::regclass
		"""), [(0, False)])
		node.safe_psql('postgres', 'REFRESH MATERIALIZED VIEW mv;')
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(
		    node.execute('postgres', """
			SELECT reltoastrelid, orioledb_tbl_check(oid),
			       (SELECT count(*) FROM mv), (SELECT sum(value) FROM mv)
			FROM pg_class WHERE oid = 'mv'::regclass
		"""), [(0, True, 1001, 501501)])
		with node.connect('postgres') as con:
			con.execute('SET enable_seqscan = off')
			self.assertIn(
			    'mv_value',
			    str(con.execute('EXPLAIN SELECT id FROM mv '
			                    'WHERE value = 1001')))
			self.assertEqual(
			    con.execute('SELECT id FROM mv WHERE value = 1001'), [(1001,)])

	def test_rewrite_tablespace_and_recovery_without_toast(self):
		ts_dir = tempfile.mkdtemp(prefix='oriole_optional_toast_')
		self.addCleanup(shutil.rmtree, ts_dir, ignore_errors=True)
		node = self.node
		node.start()
		node.safe_psql('postgres', 'CREATE EXTENSION orioledb;')
		node.safe_psql('postgres',
		               f"CREATE TABLESPACE optional_ts LOCATION '{ts_dir}';")
		node.safe_psql('postgres', """
			CREATE TABLE data (id integer PRIMARY KEY, value bigint) USING orioledb;
			CREATE INDEX data_value ON data(value);
			INSERT INTO data SELECT i, i FROM generate_series(1, 1000) i;
			CHECKPOINT;
		""")
		node.safe_psql('postgres', """
			BEGIN;
			ALTER TABLE data SET TABLESPACE optional_ts;
			ROLLBACK;
			ALTER TABLE data ALTER COLUMN value TYPE integer;
			ALTER TABLE data SET TABLESPACE optional_ts;
			ALTER TABLE data DROP CONSTRAINT data_pkey;
			ALTER TABLE data ADD PRIMARY KEY (id);
			INSERT INTO data VALUES (1001, 1001);
		""")
		node.safe_psql('postgres', """
			CREATE TABLE boundary (value text STORAGE PLAIN PRIMARY KEY) USING orioledb;
			ALTER TABLE boundary ALTER COLUMN value SET STORAGE EXTERNAL;
			INSERT INTO boundary VALUES (repeat('x', 2670));
			ALTER TABLE boundary DROP CONSTRAINT boundary_pkey;
		""")
		# Replay the committed rewrite, move, and index rebuilds after a crash.
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(
		    node.execute('postgres', """
			SELECT reltoastrelid, (SELECT sum(value) FROM data),
			       orioledb_tbl_check(oid)
			FROM pg_class WHERE oid = 'data'::regclass
		"""), [(0, 501501, True)])
		self.assertEqual(
		    node.execute('postgres', """
			SELECT reltoastrelid <> 0, (SELECT length(value) FROM boundary),
			       orioledb_tbl_check(oid)
			FROM pg_class WHERE oid = 'boundary'::regclass
		"""), [(True, 2670, True)])
		with node.connect('postgres') as con:
			con.execute('SET enable_seqscan = off')
			self.assertEqual(
			    con.execute('SELECT id FROM data WHERE value = 1001'),
			    [(1001,)])

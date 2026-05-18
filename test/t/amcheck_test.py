#!/usr/bin/env python3
# coding: utf-8
"""
Tests for amcheck / pg_amcheck integration with orioledb tables.
"""

import glob
import os
import subprocess
import time

from .base_test import BaseTest, ThreadQueryExecutor, wait_checkpointer_stopevent


class AmcheckTest(BaseTest):

	def test_verify_heapam_clean(self):
		"""verify_heapam returns no rows on a healthy orioledb relation"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_t (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			CREATE INDEX o_t_v_idx ON o_t (v);
			INSERT INTO o_t SELECT i, 'v' || i FROM generate_series(1, 100) i;
			CHECKPOINT;
		""")

		rows = node.execute('postgres',
		                    "SELECT * FROM verify_heapam('o_t'::regclass);")
		self.assertEqual(rows, [])

		# Deep check (force_file_check via the check_toast arg) also clean.
		rows = node.execute(
		    'postgres', "SELECT * FROM verify_heapam('o_t'::regclass, "
		    "                            check_toast := true);")
		self.assertEqual(rows, [])

	def test_pg_amcheck_clean(self):
		"""pg_amcheck on a healthy orioledb table exits 0 with no output"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_t (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			INSERT INTO o_t SELECT i, 'v' || i FROM generate_series(1, 50) i;
			CHECKPOINT;
		""")

		res = subprocess.run([
		    'pg_amcheck', '-d', 'postgres', '-t', 'public.o_t', '-p',
		    str(node.port), '-h', node.host
		],
		                     capture_output=True,
		                     text=True)
		self.assertEqual(
		    res.returncode, 0, f"pg_amcheck failed: stdout={res.stdout!r} "
		    f"stderr={res.stderr!r}")
		self.assertEqual(res.stdout, '')

	def test_pg_amcheck_whole_database(self):
		"""pg_amcheck on a whole clean database mixing heap/orioledb tables,
		secondary indexes and sequences"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;

			CREATE SEQUENCE h_seq;
			CREATE TABLE h_t (
				k int PRIMARY KEY DEFAULT nextval('h_seq'),
				v text NOT NULL
			);
			CREATE INDEX h_t_v_idx ON h_t (v);
			INSERT INTO h_t (v)
				SELECT 'h' || i FROM generate_series(1, 100) i;

			CREATE SEQUENCE o_seq;
			CREATE TABLE o_t (
				k int PRIMARY KEY DEFAULT nextval('o_seq'),
				v text NOT NULL
			) USING orioledb;
			CREATE INDEX o_t_v_idx ON o_t (v);
			INSERT INTO o_t (v)
				SELECT 'o' || i FROM generate_series(1, 100) i;

			CHECKPOINT;
		""")

		res = subprocess.run([
		    'pg_amcheck', '-d', 'postgres', '-p',
		    str(node.port), '-h', node.host
		],
		                     capture_output=True,
		                     text=True)
		self.assertEqual(
		    res.returncode, 0, f"pg_amcheck failed: stdout={res.stdout!r} "
		    f"stderr={res.stderr!r}")
		self.assertEqual(res.stdout, '')

	def test_verify_heapam_reports_corruption(self):
		"""
		Corrupting the on-disk CheckpointFileHeader.datafileLength of an
		index's .map file makes check_btree see a mismatch between the
		in-memory busy/free extent counts and the loaded data_file_len.
		The callback then emits one row per failed index.
		"""
		import struct

		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_corrupt (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			INSERT INTO o_corrupt
				SELECT i, repeat('x', 200) FROM generate_series(1, 1000) i;
			CHECKPOINT;
		""")

		datoid, pkey_relnode = node.execute(
		    'postgres', """
			SELECT (SELECT oid FROM pg_database WHERE datname='postgres'),
			       (SELECT relfilenode FROM pg_class
			        WHERE relname='o_corrupt_pkey');
		""")[0]

		node.stop()

		# Pick the latest .map file and overwrite its datafileLength field
		# (offset 24 in struct CheckpointFileHeader) with a bogus value.
		# On restart, recovery loads this wrong length into the meta page;
		# the next check_btree finds busy+free != data_file_len.
		pattern = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                       f'{pkey_relnode}-*.map')
		map_files = sorted(glob.glob(pattern))
		self.assertTrue(map_files, f"no .map files matched {pattern}")
		with open(map_files[-1], 'r+b') as f:
			f.seek(24)
			f.write(struct.pack('<Q', 0xDEADBEEF))

		node.start()
		rows = node.execute(
		    'postgres',
		    "SELECT msg FROM verify_heapam('o_corrupt'::regclass);")
		self.assertTrue(
		    rows,
		    "verify_heapam should have reported at least one corrupt index")
		self.assertTrue(any('check failed' in r[0] for r in rows),
		                f"unexpected verify_heapam output: {rows!r}")

	def test_verify_heapam_during_checkpoint(self):
		"""
		With the checkpointer parked inside our pkey tree via a stopevent,
		verify_heapam must block on the gate (rather than spuriously emit
		"Tree is under checkpoint now") and only complete after the
		checkpoint releases the boundary.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_chkp (
				k int PRIMARY KEY,
				v int NOT NULL
			) USING orioledb;
			INSERT INTO o_chkp
				SELECT i, i FROM generate_series(1, 2000) i;
			CHECKPOINT;
			-- dirty pages so the next CHECKPOINT walks o_chkp_pkey
			UPDATE o_chkp SET v = v + 1;
		""")

		con_chkp = node.connect(autocommit=True)
		con_set = node.connect()

		# Park the next checkpointer inside o_chkp_pkey at first descent.
		con_set.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
		                "'$.action == \"walkDownwards\" && "
		                "$.treeName == \"o_chkp_pkey\"');")

		t_chkp = ThreadQueryExecutor(con_chkp, "CHECKPOINT;")
		t_chkp.start()
		wait_checkpointer_stopevent(node)

		# Capture stderr from psql so any "Tree is under checkpoint now"
		# NOTICE would be visible if the wait failed to kick in.
		verify_proc = subprocess.Popen([
		    'psql', '-p',
		    str(node.port), '-h', node.host, '-d', 'postgres', '-At', '-c',
		    "SELECT msg FROM verify_heapam('o_chkp'::regclass);"
		],
		                               stdout=subprocess.PIPE,
		                               stderr=subprocess.PIPE,
		                               text=True)

		# verify_heapam must block on the gate while the stopevent holds.
		time.sleep(0.5)
		self.assertIsNone(
		    verify_proc.poll(),
		    "verify_heapam returned before the checkpoint released")

		con_set.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		t_chkp.join()
		stdout, stderr = verify_proc.communicate(timeout=10)

		self.assertNotIn(
		    "Tree is under checkpoint now", stderr,
		    f"verify_heapam emitted the spurious gate notice: {stderr!r}")

		con_chkp.close()
		con_set.close()

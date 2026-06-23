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

	def test_verify_orioledb(self):
		"""verify_orioledb returns no rows on a healthy relation"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			CREATE INDEX o_t_v_idx ON o_t (v);
			INSERT INTO o_t SELECT i, 'v' || i FROM generate_series(1, 100) i;
			CHECKPOINT;
		""")

		rows = node.execute('postgres',
		                    "SELECT * FROM verify_orioledb('o_t'::regclass);")
		self.assertEqual(rows, [])

		# Thorough variant (force_file_check) also clean.
		rows = node.execute(
		    'postgres',
		    "SELECT * FROM verify_orioledb('o_t'::regclass, true);")
		self.assertEqual(rows, [])

	def test_verify_orioledb_no_false_positive_with_broken_split(self):
		"""Pre-fix, a rightlink pointing to a BROKEN_SPLIT page made
		verify_orioledb emit 'BTree has a broken split.' and return a
		spurious ('o_t_pkey', 'check failed') row, even though a broken
		split is a benign in-progress state finished by the next inserter
		or checkpoint. This test forces a split failure via the split_fail
		stopevent and asserts the default mode is clean both before and
		after the fixup checkpoint."""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t (id text PRIMARY KEY) USING orioledb;
			INSERT INTO o_t SELECT repeat('x', 400) || id
				FROM generate_series(1, 20) id;
			CHECKPOINT;
		""")

		con_set = node.connect()
		con_set.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		# An INSERT that triggers a split now errors out; the error
		# cleanup hook marks the in-progress split as broken.
		con_ins = node.connect()
		con_ins.execute("SET orioledb.enable_stopevents = true;")
		with self.assertRaises(Exception):
			con_ins.execute("INSERT INTO o_t SELECT repeat('x', 400) || id "
			                "FROM generate_series(131, 139) id;")
		con_ins.close()
		con_set.execute("SELECT pg_stopevent_reset('split_fail');")
		con_set.close()

		# Pre-fix, the broken-split walk emits a NOTICE and the function
		# returns a 'check failed' row.
		rows = node.execute("SELECT * FROM verify_orioledb('o_t'::regclass);")
		self.assertEqual(rows, [], f"verify returned false positive: {rows!r}")

		# A CHECKPOINT completes the pending split. Both modes clean now.
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertEqual(
		    node.execute("SELECT * FROM verify_orioledb('o_t'::regclass);"),
		    [])
		self.assertEqual(
		    node.execute(
		        "SELECT * FROM verify_orioledb('o_t'::regclass, true);"), [])
		node.stop()

	def test_verify_heapam_rejects_orioledb(self):
		"""verify_heapam() must error on non-heap relations"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_t (k int PRIMARY KEY) USING orioledb;
		""")
		with self.assertRaises(Exception) as cm:
			node.execute('postgres',
			             "SELECT * FROM verify_heapam('o_t'::regclass);")
		self.assertIn("only heap AM is supported", str(cm.exception))

	def test_pg_amcheck(self):
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

	def test_verify_orioledb_corruption(self):
		"""
		Corrupting the on-disk CheckpointFileHeader.datafileLength of an
		index's .map file makes check_btree see a mismatch between the
		in-memory busy/free extent counts and the loaded data_file_len.
		The function then emits one row per failed index.
		"""
		import struct

		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
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
		    "SELECT * FROM verify_orioledb('o_corrupt'::regclass);")
		self.assertTrue(
		    rows, "verify_orioledb should have reported at least one "
		    "corrupt index")
		self.assertTrue(any('check failed' in r[1] for r in rows),
		                f"unexpected verify_orioledb output: {rows!r}")

	def test_verify_orioledb_during_checkpoint(self):
		"""
		With the checkpointer parked inside our pkey tree via a stopevent,
		verify_orioledb must block on the gate (rather than spuriously
		emit "Tree is under checkpoint now") and only complete after the
		checkpoint releases the boundary.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
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
		    "SELECT * FROM verify_orioledb('o_chkp'::regclass);"
		],
		                               stdout=subprocess.PIPE,
		                               stderr=subprocess.PIPE,
		                               text=True)

		# Must block on the gate while the stopevent holds.
		time.sleep(0.5)
		self.assertIsNone(
		    verify_proc.poll(),
		    "verify_orioledb returned before checkpoint released")

		con_set.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		t_chkp.join()
		stdout, stderr = verify_proc.communicate(timeout=10)

		self.assertNotIn(
		    "Tree is under checkpoint now", stderr,
		    f"verify_orioledb emitted spurious gate notice: {stderr!r}")

		con_chkp.close()
		con_set.close()

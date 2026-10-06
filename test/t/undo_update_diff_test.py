#!/usr/bin/env python3
# coding: utf-8
"""
Row-level undo records of updates holding a difference from the newer version.

An update record may keep only the bytes the update changed, and the previous
version is rebuilt from the version above it.  Every reader of the undo chain
has to carry the right version down: rollback in the page, a reader with an
old snapshot, recovery rolling back a transaction that was in progress at a
checkpoint.
"""

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent

ROWS = 1000

SETUP = """
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_diff (
		id int PRIMARY KEY,
		a int NOT NULL,
		b text,
		c int8,
		filler char(200) DEFAULT ''
	) USING orioledb;
	CREATE INDEX o_diff_a ON o_diff (a);
	CREATE INDEX o_diff_b ON o_diff (b);
	INSERT INTO o_diff (id, a, b, c)
		SELECT i, i, 'b' || i, i * 10 FROM generate_series(1, %d) i;
""" % ROWS

# Updates of fixed and variable length fields, changing the tuple length both
# ways, and to and from NULL.
UPDATES = [
    "UPDATE o_diff SET a = a + 1;",
    "UPDATE o_diff SET b = b || repeat('y', mod(id, 30));",
    "UPDATE o_diff SET c = NULL WHERE mod(id, 3) = 0;",
    "UPDATE o_diff SET b = 'short', a = a + 1 WHERE mod(id, 5) = 0;",
    "UPDATE o_diff SET filler = 'f' || id, c = 7 WHERE mod(id, 7) = 0;",
    "UPDATE o_diff SET a = a + 1000 WHERE mod(id, 2) = 0;",
]

CHECK = "SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM o_diff t;"


class UndoUpdateDiffTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.start()
		self.node.safe_psql('postgres', SETUP)

	def row_undo_location(self, con):
		return con.execute("""
			SELECT lastUsedLocation FROM orioledb_get_undo_meta()
				WHERE undo_type = 'row';""")[0][0]

	def check_indexes(self, con, expected):
		"""Both secondary indexes agree with the table."""
		con.execute("SET enable_seqscan = off;")
		con.execute("SET enable_bitmapscan = off;")
		for column in ['a', 'b']:
			got = con.execute("""
				SELECT md5(string_agg(t::text, ',' ORDER BY id))
					FROM (SELECT * FROM o_diff
							WHERE %s IS NOT NULL ORDER BY %s) t;""" % (column, column))[0][0]
			self.assertEqual(got, expected)
		con.execute("RESET enable_seqscan;")
		con.execute("RESET enable_bitmapscan;")

	def test_undo_size(self):
		with self.node.connect() as con:
			con.begin()
			start = self.row_undo_location(con)
			# No index covers c, so the update makes one record per row
			con.execute("UPDATE o_diff SET c = c + 1;")
			used = self.row_undo_location(con) - start
			con.rollback()

		# A full record holds the whole old tuple, over 200 bytes of filler.
		self.assertLess(used / ROWS, 150)

	def test_rollback(self):
		node = self.node
		original = node.execute(CHECK)[0][0]

		with node.connect() as con:
			con.begin()
			for update in UPDATES:
				con.execute(update)
			con.rollback()
			self.assertEqual(con.execute(CHECK)[0][0], original)
			self.check_indexes(con, original)

	def test_rollback_to_savepoint(self):
		node = self.node

		with node.connect() as con:
			con.begin()
			for update in UPDATES[:3]:
				con.execute(update)
			middle = con.execute(CHECK)[0][0]
			con.execute("SAVEPOINT s;")
			for update in UPDATES[3:]:
				con.execute(update)
			con.execute("ROLLBACK TO SAVEPOINT s;")
			self.assertEqual(con.execute(CHECK)[0][0], middle)
			con.commit()
			self.assertEqual(con.execute(CHECK)[0][0], middle)
			self.check_indexes(con, middle)

	def test_old_snapshot(self):
		node = self.node
		original = node.execute(CHECK)[0][0]

		reader = node.connect()
		reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(reader.execute(CHECK)[0][0], original)

		# Committed versions, then an in-progress chain on top of them.
		for update in UPDATES[:3]:
			node.safe_psql('postgres', update)
		writer = node.connect()
		writer.begin()
		for update in UPDATES[3:]:
			writer.execute(update)

		self.assertEqual(reader.execute(CHECK)[0][0], original)
		self.check_indexes(reader, original)
		writer.rollback()
		self.assertEqual(reader.execute(CHECK)[0][0], original)
		reader.commit()
		writer.close()
		reader.close()

	def test_structure_print(self):
		node = self.node

		with node.connect() as con:
			con.begin()
			for update in UPDATES:
				con.execute(update)
			# Prints every version of the row along its undo chain.
			structure = con.execute("""
				SELECT orioledb_tbl_structure('o_diff'::regclass, 'nue');
			""")[0][0]
			con.rollback()
		self.assertIn("'b1'", structure)

	def test_crash_recovery(self):
		node = self.node

		node.safe_psql('postgres', UPDATES[0])
		original = node.execute(CHECK)[0][0]

		# The checkpoint keeps the undo of the transaction in progress, and
		# recovery rolls it back from there.
		writer = node.connect()
		writer.begin()
		for update in UPDATES[1:]:
			writer.execute(update)
		node.safe_psql('postgres', "CHECKPOINT;")
		node.stop(['-m', 'immediate'])
		writer.close()

		node.start()
		with node.connect() as con:
			self.assertEqual(con.execute(CHECK)[0][0], original)
			self.check_indexes(con, original)
		node.stop()

	def test_sk_fixup(self):
		"""
		A checkpoint taken between the update of the primary key tree and the
		update of a secondary index makes recovery fix the index up from the
		undo record of the row.  The record holds a difference.
		"""
		node = self.node
		node.stop()
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()

		con_ctl = node.connect()
		con = node.connect()
		con.execute("SET application_name = 's_upd';")
		con.commit()
		pid = con.execute("SELECT pg_backend_pid();")[0][0]
		con.commit()

		con_ctl.execute("SELECT pg_stopevent_set('sk_modify_pending', "
		                "'$applicationName == \"s_upd\"');")
		t = ThreadQueryExecutor(
		    con, "BEGIN; UPDATE o_diff SET a = -a WHERE id = 10; COMMIT;")
		t.start()
		wait_stopevent(node, pid)
		con_ctl.execute("CHECKPOINT;")
		con_ctl.execute("SELECT pg_stopevent_reset('sk_modify_pending');")
		t.join()
		con_ctl.close()
		con.close()

		expected = node.execute(CHECK)[0][0]
		self.crash_with_os_buffer_loss()
		node.start()
		with node.connect() as con:
			self.assertEqual(con.execute(CHECK)[0][0], expected)
			self.check_indexes(con, expected)
			self.assertEqual(
			    con.execute("SELECT id FROM o_diff WHERE a = -10;"), [(10, )])
		node.stop()

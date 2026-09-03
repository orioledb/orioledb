#!/usr/bin/env python3
# coding: utf-8
"""
Regression test for add_pending_truncate() (src/btree/undo.c).

When a DROP TABLE commits while a backup is in progress, OrioleDB defers
the file cleanup of the dropped table's trees: it records the tree list
into a "pending truncates" file, to be replayed by check_pending_truncates()
once the backup ends.  add_pending_truncate() wrote the tree count and the
tree list at a hardcoded file offset of 0 instead of the running offset (and
wrote the address of the local "trees" pointer instead of the array it
points to).  That corrupts the file: check_pending_truncates() reads back a
garbage tree count for the record, which both skips the real cleanup and
can make it read far past the end of the (tiny) file, raising a FATAL error
that kills the backend that triggered it.
"""

import os

from .base_test import BaseTest


class PendingTruncatesTest(BaseTest):

	def test_drop_table_during_backup_is_cleaned_up(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_test (\n"
		    "	id integer NOT NULL PRIMARY KEY,\n"
		    "	val text\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_test\n"
		    "	(SELECT id, id || 'val' FROM generate_series(1, 100, 1) id);\n")

		con = node.connect(autocommit=True)
		datoid = con.execute(
		    "SELECT oid FROM pg_database WHERE datname = current_database();"
		)[0][0]
		# o_test's own on-disk storage is its primary key's tree, not the
		# relnode recorded against pg_class for the table itself.
		relnode = con.execute(
		    "SELECT relfilenode FROM pg_class WHERE relname = 'o_test_pkey';"
		)[0][0]
		fname = f"{node.data_dir}/orioledb_data/{datoid}/{relnode}"
		self.assertTrue(os.path.exists(fname))

		# pg_backup_start/pg_backup_stop must run on the same connection:
		# the backup is tied to the session that started it, and gets
		# aborted if that session disconnects.
		con.execute("SELECT pg_backup_start('pending_truncates_test', true);")
		con.execute("DROP TABLE o_test;")
		con.execute("SELECT pg_backup_stop(wait_for_archive => false);")

		# Replay the pending-truncates file deterministically, instead of
		# waiting for the (disabled) bgwriter's next cycle.  With the bug,
		# this reads a garbage tree count off the corrupted file and raises
		# a FATAL error that kills this connection.
		con.execute("SELECT orioledb_check_pending_truncates();")
		con.close()

		node.stop()

		self.assertFalse(
		    os.path.exists(fname),
		    "o_test_pkey's file should have been cleaned up by "
		    "check_pending_truncates() once the backup ended")

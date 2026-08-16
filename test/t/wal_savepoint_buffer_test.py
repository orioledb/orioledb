#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest


class WalSavepointBufferTest(BaseTest):

	def test_rollback_to_savepoint_at_buffer_end(self):
		"""
		A ROLLBACK TO SAVEPOINT whose record lands in the last bytes of the
		local WAL buffer must not overrun it.

		The record is written after add_xid_wal_record_if_needed(), so the
		buffer has to have room for both.  Only the last sizeof(WALRecXid)
		bytes of the 8192-byte buffer expose a miscount, and the rollback
		record itself flushes the buffer, so the offset it sees is exactly
		what has been written since the previous rollback.  Sweeping the
		number and size of the intervening updates walks that total across
		the end of the buffer; an assert build aborts on the offending
		iteration.
		"""
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(
		    'postgres', """
			CREATE TABLE wal_sp (id int PRIMARY KEY, v text) USING orioledb;
			INSERT INTO wal_sp SELECT g, 'x' FROM generate_series(1, 500) g;
		""")
		node.safe_psql(
		    'postgres', """
			DO $$
			DECLARE
				n int;
				pad int;
				k int;
			BEGIN
				FOR n IN 405..450 LOOP
					FOR pad IN 1..70 LOOP
						BEGIN
							FOR k IN 1..n LOOP
								UPDATE wal_sp SET v = 'a' WHERE id = k;
							END LOOP;
							UPDATE wal_sp SET v = repeat('b', pad)
								WHERE id = 1;
							RAISE EXCEPTION 'rollback to savepoint';
						EXCEPTION WHEN OTHERS THEN
							NULL;
						END;
					END LOOP;
				END LOOP;
			END $$;
		""")
		self.assertEqual(
		    500,
		    int(node.execute('postgres', "SELECT count(*) FROM wal_sp")[0][0]))
		node.stop()

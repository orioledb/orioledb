#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class RecoveryRowLockTest(BaseTest):

	def test_row_lock_in_checkpoint_image(self):
		"""
		A row lock taken by an in-progress transaction reaches the checkpoint
		image, and the same transaction then updates the locked row.  On
		restart the leaf page comes back from disk still carrying the
		lock-only header, and WAL replay re-applies the UPDATE under the very
		same oxid.  Recovery must not mistake that header for its own written
		version: doing so rolls back the committed version underneath it and
		then reads a row-lock undo record as a modify one.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE o_test_row_lock (
				id int PRIMARY KEY,
				v text
			) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO o_test_row_lock
				SELECT i, 'initial' FROM generate_series(1, 50) i;
		""")
		node.safe_psql("CHECKPOINT;")

		with node.connect() as con:
			with node.connect() as con2:
				con.execute("""
					SELECT v FROM o_test_row_lock WHERE id = 7 FOR UPDATE;
				""")
				# push the lock-only tuple header into the checkpoint image
				con2.execute("CHECKPOINT;")
				con2.commit()
				con.execute("""
					UPDATE o_test_row_lock SET v = 'by-locker' WHERE id = 7;
				""")
				con.commit()

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual([(7, 'by-locker')],
		                 node.execute("""
							SELECT id, v FROM o_test_row_lock WHERE id = 7;
						 """))
		self.assertEqual([(50, )],
		                 node.execute("SELECT count(*) FROM o_test_row_lock;"))
		node.stop()

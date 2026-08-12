#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class RecoveryItemRollbackTest(BaseTest):

	def test_item_rollback_locator_reuse(self):
		"""
		Replay re-applies records the page already carries, which is the only
		path that reaches the item rollback: a CHECKPOINT taken while the
		transaction is still open writes its in-progress versions into the
		image, and after an immediate stop those same UPDATE records are
		replayed on top of them.  The callback then sees a version at least as
		new as the incoming one, rolls the item back -- which resizes it --
		and retries.  The retry must not reuse the locator the rollback made
		stale, or the next resize shifts the wrong range of the page.

		The updates change the item's size in both directions on purpose: a
		rollback that does not change the size cannot expose the stale
		locator.
		"""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE o_test_item_rollback (
				id int PRIMARY KEY,
				v text
			) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO o_test_item_rollback
				SELECT i, repeat('x', 40) FROM generate_series(1, 60) i;
		""")
		node.safe_psql("CHECKPOINT;")

		with node.connect() as con:
			with node.connect() as con2:
				con.execute("""
					UPDATE o_test_item_rollback SET v = repeat('L', 900)
						WHERE id = 7;
				""")
				con.execute("""
					UPDATE o_test_item_rollback SET v = 'short' WHERE id = 7;
				""")
				con.execute("""
					UPDATE o_test_item_rollback SET v = repeat('M', 300)
						WHERE id = 7;
				""")
				# the image now holds the in-progress versions, while replay
				# still starts before the records that made them
				con2.execute("CHECKPOINT;")
				con2.commit()
				con.commit()

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(
		    [(60, )],
		    node.execute("SELECT count(*) FROM o_test_item_rollback;"))
		self.assertEqual([(7, 300)],
		                 node.execute("""
							SELECT id, length(v) FROM o_test_item_rollback
								WHERE length(v) <> 40 ORDER BY id;
						 """))
		self.assertEqual([],
		                 node.execute("""
			SELECT g.id FROM generate_series(1, 60) g(id)
				WHERE NOT EXISTS (SELECT 1 FROM o_test_item_rollback t
									WHERE t.id = g.id);
		"""))
		node.stop()

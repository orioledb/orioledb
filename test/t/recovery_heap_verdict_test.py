#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class RecoveryHeapVerdictTest(BaseTest):

	def test_oxid_born_after_pre_commit(self):
		"""
		A temp table with ON COMMIT DELETE ROWS is truncated from
		PreCommit_on_commit_actions(), which runs after
		XACT_EVENT_PRE_COMMIT.  A statement that needed no oxid of its own --
		a plain read -- therefore acquires one there, too late for
		wal_joint_commit(), and puts records on the wire under it while
		nothing will ever write a finish record for it.

		Replay has to take such a transaction's verdict from the heap
		transaction it rode on.  Rolling it back instead would undo changes
		PostgreSQL considers committed.

		The checkpoint before the temp work keeps those records inside the
		replayed range, and the transactions after it push runXmin -- and so
		recovery_xmin -- past the oxid, which is the case recovery_finish()
		used to handle by aborting.
		"""
		node = self.node
		# A timed checkpoint anywhere after the temp work would move the redo
		# point past those records and replay would never face the oxid.  The
		# run is far slower than checkpoint_timeout under valgrind.
		node.append_conf('postgresql.conf', "checkpoint_timeout = 1d\n")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE o_test_keep (
				id int PRIMARY KEY,
				val text
			) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO o_test_keep
				SELECT i, 'initial' FROM generate_series(1, 50) i;
		""")
		node.safe_psql("CHECKPOINT;")

		with node.connect() as con:
			con.execute("""
				CREATE TEMP TABLE o_test_on_commit (
					val int2 PRIMARY KEY,
					val2 int2 UNIQUE
				) USING orioledb ON COMMIT DELETE ROWS;
			""")
			con.commit()
			con.execute("TRUNCATE o_test_on_commit;")
			con.commit()
			con.execute("TABLE o_test_on_commit;")
			con.commit()

		# no checkpoint here: the records above must stay in the replayed
		# range while the horizon moves past their oxid.  One connection, one
		# transaction per statement -- what matters is the number of oxids
		# consumed, and spawning 200 psql processes costs minutes under
		# valgrind.
		with node.connect() as con:
			for i in range(1, 201):
				con.execute(
				    "UPDATE o_test_keep SET val = 'r%d' WHERE id = %d;" %
				    (i, i % 50 + 1))
				con.commit()

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual([(50, )],
		                 node.execute("SELECT count(*) FROM o_test_keep;"))
		with open(node.pg_log_file) as f:
			self.assertIn('committed oxid', f.read())
		node.stop()

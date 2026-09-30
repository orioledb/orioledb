#!/usr/bin/env python3
# coding: utf-8
"""
Deleting a type marks its entries in the system caches deleted, and the
checkpoint after that removes the marked entries from the trees.  The mark is
set by the transaction that drops the type: until that transaction commits it
has to be possible to take the mark back, and the checkpoint must not remove
the entries it is on.  Otherwise a rollback leaves the entries deleted, and the
next crash recovery, which reads the types from these caches only, cannot
replay the rows that use them.
"""

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

# Sys trees holding the entries a DROP TYPE marks deleted
CACHE_TREES = {
    'ENUM_CACHE': 5,
    'ENUMOID_CACHE': 6,
    'RANGE_CACHE': 7,
    'CLASS_CACHE': 8,
    'TYPE_CACHE': 12,
}

SETUP = """
	CREATE TYPE o_enum AS ENUM ('x', 'y', 'z');
	CREATE TYPE o_comp AS (a int, b text, c float, e o_enum);
	CREATE TYPE o_range AS RANGE (subtype = float8);
	CREATE TABLE o_tbl (
		key o_comp NOT NULL,
		rng o_range NOT NULL,
		val int NOT NULL,
		PRIMARY KEY(key)
	) USING orioledb;
	CREATE INDEX o_tbl_rng ON o_tbl (rng);
	INSERT INTO o_tbl
		SELECT (id, 'row ' || id, id * 1.5, 'x')::o_comp,
			   o_range(id, id + 1), id
		FROM generate_series(1, 5) id;
"""

DROP = "DROP TABLE o_tbl; DROP TYPE o_comp; DROP TYPE o_range; DROP TYPE o_enum;"


class SysCacheDeleteTest(BaseTest):

	def cache_state(self, node):
		"""Per cache: (entries not removed from the tree, of them marked)."""
		state = {}
		for name, num in CACHE_TREES.items():
			alive, marked = node.execute("""
				SELECT count(*) FILTER (WHERE NOT (k->'tupHdr'->>'deleted')::bool),
					   count(*) FILTER (WHERE NOT (k->'tupHdr'->>'deleted')::bool
										  AND (k->'key'->>'deleted')::bool)
				FROM orioledb_sys_tree_rows(%d) k;
			""" % num)[0]
			state[name] = (alive, marked)
		return state

	def setup_types(self, node):
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(SETUP)
		self.live = self.cache_state(node)
		for name, (alive, marked) in self.live.items():
			self.assertGreater(alive, 0, "no %s entries" % name)
			self.assertEqual(marked, 0, "%s entries marked already" % name)
		self.marked = None

	def assertMarked(self, node):
		"""The entries are in place, the drop's ones marked deleted."""
		state = self.cache_state(node)
		if self.marked is None:
			# The first look after the drop tells which entries it marks
			for name, (alive, marked) in state.items():
				self.assertEqual(alive, self.live[name][0],
				                 "%s entries removed" % name)
				self.assertGreater(marked, 0, "no %s entries marked" % name)
			self.marked = state
		self.assertEqual(state, self.marked)

	def assertLive(self, node):
		"""The entries are in place and not marked."""
		self.assertEqual(self.cache_state(node), self.live)

	def assertRemoved(self, node):
		"""A checkpoint has removed the marked entries."""
		self.assertIsNotNone(self.marked)
		self.assertEqual(
		    self.cache_state(node), {
		        name: (alive - marked, 0)
		        for name, (alive, marked) in self.marked.items()
		    })

	def assertUsable(self, node, restart=True):
		"""The types still work, also for a crash recovery replaying rows."""
		self.assertEqual(
		    node.execute("SELECT count(*), sum(val) FROM o_tbl "
		                 "WHERE (key).e = 'x' AND rng @> 3.0::float8;")[0],
		    (1, 3))
		node.safe_psql("""
			INSERT INTO o_tbl
				SELECT (id, 'row ' || id, id * 1.5,
						(ARRAY['x', 'y', 'z'])[id % 3 + 1]::o_enum)::o_comp,
					   o_range(id, id + 1), id
				FROM generate_series(6, 1000) id;
		""")
		if restart:
			node.stop(['-m', 'immediate'])
			node.start()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_tbl;")[0][0], 1000)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_tbl "
		                 "WHERE (key).e = 'y';")[0][0], 332)
		self.assertEqual(
		    node.execute("SELECT val FROM o_tbl ORDER BY key LIMIT 1;")[0][0],
		    1)

	def test_committed_drop_removed_by_checkpoint(self):
		node = self.node
		node.start()
		self.setup_types(node)

		node.safe_psql(DROP)
		self.assertMarked(node)
		node.safe_psql("CHECKPOINT;")
		self.assertRemoved(node)

		node.stop(['-m', 'immediate'])
		node.start()
		self.assertRemoved(node)
		node.stop()

	def test_rollback_after_checkpoint(self):
		node = self.node
		node.start()
		self.setup_types(node)

		con = node.connect()
		con.begin()
		con.execute(DROP)
		self.assertMarked(node)

		# The mark belongs to a transaction that is still running
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)

		con.rollback()
		con.close()
		self.assertLive(node)
		node.safe_psql("CHECKPOINT;")
		self.assertLive(node)
		self.assertUsable(node)
		node.stop()

	def test_crash_during_drop_after_checkpoint(self):
		node = self.node
		node.start()
		self.setup_types(node)

		con = node.connect()
		con.begin()
		con.execute(DROP)
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		# Make whatever the checkpoint wrote after itself reach the disk
		node.safe_psql("SELECT pg_switch_wal();")
		node.stop(['-m', 'immediate'])
		con.close()

		node.start()
		self.assertLive(node)
		node.safe_psql("CHECKPOINT;")
		self.assertLive(node)
		self.assertUsable(node)
		node.stop()

	def test_commit_after_checkpoint(self):
		node = self.node
		node.start()
		self.setup_types(node)

		con = node.connect()
		con.begin()
		con.execute(DROP)
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		con.commit()
		con.close()

		self.assertMarked(node)
		node.safe_psql("CHECKPOINT;")
		self.assertRemoved(node)
		node.stop()

	def test_crash_after_committed_drop(self):
		node = self.node
		node.start()
		self.setup_types(node)

		node.safe_psql(DROP)
		self.assertMarked(node)
		node.stop(['-m', 'immediate'])

		# The end-of-recovery checkpoint removes what the drop marked
		node.start()
		self.assertRemoved(node)
		node.safe_psql("CHECKPOINT;")
		self.assertRemoved(node)
		node.stop()

	def test_rollback_to_savepoint(self):
		node = self.node
		node.start()
		self.setup_types(node)

		con = node.connect()
		con.begin()
		con.execute("SAVEPOINT s1;")
		con.execute(DROP)
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		con.execute("ROLLBACK TO SAVEPOINT s1;")
		self.assertLive(node)
		con.commit()
		con.close()

		node.safe_psql("CHECKPOINT;")
		self.assertLive(node)
		self.assertUsable(node)
		node.stop()

	def test_released_savepoint_then_rollback(self):
		node = self.node
		node.start()
		self.setup_types(node)

		con = node.connect()
		con.begin()
		con.execute("SAVEPOINT s1;")
		con.execute(DROP)
		con.execute("RELEASE SAVEPOINT s1;")
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		con.rollback()
		con.close()

		self.assertLive(node)
		node.safe_psql("CHECKPOINT;")
		self.assertLive(node)
		self.assertUsable(node)
		node.stop()

	def test_drop_again_after_rollback_to_savepoint(self):
		node = self.node
		node.start()
		self.setup_types(node)

		con = node.connect()
		con.begin()
		con.execute("SAVEPOINT s1;")
		con.execute(DROP)
		con.execute("ROLLBACK TO SAVEPOINT s1;")
		node.safe_psql("CHECKPOINT;")
		self.assertLive(node)
		con.execute(DROP)
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		con.commit()
		con.close()

		node.safe_psql("CHECKPOINT;")
		self.assertRemoved(node)
		node.stop()

	def test_rollback_in_plpgsql_exception_block(self):
		node = self.node
		node.start()
		self.setup_types(node)

		# The exception block rolls the drop back as a subtransaction, and the
		# checkpoint comes while the drop is still there to roll back.
		node.safe_psql("""
			CREATE FUNCTION o_drop_and_fail() RETURNS void LANGUAGE plpgsql
			AS $$
			BEGIN
				BEGIN
					DROP TABLE o_tbl;
					DROP TYPE o_comp;
					DROP TYPE o_range;
					DROP TYPE o_enum;
					-- Hold on here, with all the drops done, until the test
					-- has looked and let go of the lock
					PERFORM pg_catalog.pg_advisory_xact_lock(1);
					RAISE EXCEPTION 'undo the drop';
				EXCEPTION WHEN raise_exception THEN
					NULL;
				END;
			END $$;
		""")
		holder = node.connect()
		holder.begin()
		holder.execute("SELECT pg_advisory_xact_lock(1);")
		con = node.connect()
		con.begin()
		t = ThreadQueryExecutor(con, "SELECT o_drop_and_fail();")
		t.start()
		node.poll_query_until(
		    "SELECT count(*) > 0 FROM pg_stat_activity "
		    "WHERE wait_event_type = 'Lock' AND wait_event = 'advisory';")
		self.assertMarked(node)
		node.safe_psql("CHECKPOINT;")
		self.assertMarked(node)
		holder.rollback()
		holder.close()
		t.join()
		con.commit()
		con.close()

		self.assertLive(node)
		node.safe_psql("CHECKPOINT;")
		self.assertLive(node)
		self.assertUsable(node)
		node.stop()

	def test_rollback_on_replica(self):
		node = self.node
		node.start()
		self.setup_types(node)

		with self.getReplica().start() as replica:
			con = node.connect()
			con.begin()
			con.execute(DROP)
			node.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)
			self.assertMarked(replica)
			con.rollback()
			con.close()

			node.safe_psql("CHECKPOINT;")
			self.assertUsable(node, restart=False)
			self.catchup_orioledb(replica)
			self.assertLive(replica)
			self.assertEqual(
			    replica.execute("SELECT count(*) FROM o_tbl "
			                    "WHERE (key).e = 'y';")[0][0], 332)

			# The replica replays the rows through its caches too
			replica.stop(['-m', 'immediate'])
			replica.start()
			self.catchup_orioledb(replica)
			self.assertEqual(
			    replica.execute("SELECT count(*) FROM o_tbl;")[0][0], 1000)

			# A committed drop reaches the replica as well
			node.safe_psql(DROP)
			node.safe_psql("CHECKPOINT;")
			self.assertRemoved(node)
			self.catchup_orioledb(replica)
			self.assertEqual(
			    replica.execute("SELECT count(*) FROM pg_type "
			                    "WHERE typname = 'o_comp';")[0][0], 0)
		node.stop()

#!/usr/bin/env python3
# coding: utf-8
"""
Stress variants of the test_replication_non_root_eviction scenario.

These tests load a primary heavily under settings that force aggressive
page eviction during recovery on the standby (small main_buffers,
many recovery workers, big payload that splits often), and check the
standby catches up.  They are deliberately repetitive — multiple
truncate/refill loops, multiple table shapes, mixed write workloads —
so any race in the recovery + eviction + split paths has many
independent chances to manifest.
"""

from .base_test import BaseTest

# Stress configuration shared by every test in this file.  Tweaked to
# put the recovery workers under tight buffer pressure: 64MB main
# buffers vs. ~500MB of dirty data forces walk_page eviction during
# replay, which is exactly the scenario where the hung-recovery /
# shmem-corruption pattern appeared.
_REPLICA_STRESS_CONF = ("log_min_messages = notice\n"
                        "max_worker_processes = 64\n"
                        "orioledb.main_buffers = 64MB\n"
                        "orioledb.undo_buffers = 256MB\n"
                        "orioledb.recovery_pool_size = 50\n"
                        "checkpoint_timeout = 9000\n"
                        "max_wal_size = 5GB\n")


class ReplicationStressTest(BaseTest):

	def _stress_init(self, master):
		master.append_conf('postgresql.conf', _REPLICA_STRESS_CONF)
		master.start()

	def _bulk_insert(self, master, n, val_size):
		"""Single-transaction INSERT of n rows with a val2 of given size."""
		con = master.connect()
		con.begin()
		con.execute("INSERT INTO o_test (SELECT id, %s - id, repeat('x', %s) "
		            "FROM generate_series(1, %s, 1) id);" % (n, val_size, n))
		con.commit()
		con.close()

	def _check_replica(self, replica, expected_count):
		replica.poll_query_until("SELECT orioledb_has_retained_undo();",
		                         expected=False)
		self.assertEqual(
		    expected_count,
		    replica.execute(
		        "SELECT count(*) FROM (SELECT * FROM o_test ORDER BY key) x;")
		    [0][0])

	# ---------------------------------------------------------------
	# 1. Original scenario, repeated — same INSERT, TRUNCATE, INSERT,
	#    TRUNCATE, ...  Each iteration replays ~500MB of WAL on the
	#    standby under full eviction pressure.  Five iterations gives
	#    five independent chances to trip the bug per run.
	# ---------------------------------------------------------------
	def test_truncate_refill_loop(self):
		ITERS = 5
		N = 200000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				for _ in range(ITERS):
					self._bulk_insert(master, N, 1000)
					self.catchup_orioledb(replica)
					self._check_replica(replica, N)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 2. Same loop but with a secondary index — every row insert
	#    drives a second btree's split path concurrently.
	# ---------------------------------------------------------------
	def test_truncate_refill_loop_with_secondary(self):
		ITERS = 5
		N = 200000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n"
				    "CREATE INDEX o_test_val_idx ON o_test (val);\n")

				for _ in range(ITERS):
					self._bulk_insert(master, N, 800)
					self.catchup_orioledb(replica)
					self._check_replica(replica, N)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 3. Loop, but with random-order keys.  Random keys hit the whole
	#    btree at once (no append-friendly fast path), so split /
	#    merge_waited_tuples paths fire for almost every row.
	# ---------------------------------------------------------------
	def test_random_key_refill_loop(self):
		ITERS = 5
		N = 150000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				for it in range(ITERS):
					master.safe_psql(
					    'postgres', f"INSERT INTO o_test "
					    f"SELECT (hashint4(g) % {N * 4}), g, repeat('x', 700) "
					    f"FROM generate_series(1, {N}) g "
					    f"ON CONFLICT (key) DO NOTHING;")
					self.catchup_orioledb(replica)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 4. Insert-then-update loop.  The UPDATE path replays as
	#    update-in-place-or-split; combined with eviction, exercises
	#    the same modify_record dispatch the original test caught.
	# ---------------------------------------------------------------
	def test_insert_then_update_loop(self):
		ITERS = 4
		N = 150000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				for _ in range(ITERS):
					self._bulk_insert(master, N, 700)
					self.catchup_orioledb(replica)
					master.safe_psql(
					    'postgres',
					    "UPDATE o_test SET val2 = repeat('y', 1100);")
					self.catchup_orioledb(replica)
					self._check_replica(replica, N)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 5. Bulk INSERT followed by DELETE of every other row, repeated.
	#    DELETE replay drives merge / vacated-bytes paths.
	# ---------------------------------------------------------------
	def test_insert_delete_alternating_loop(self):
		ITERS = 4
		N = 150000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				for _ in range(ITERS):
					self._bulk_insert(master, N, 800)
					self.catchup_orioledb(replica)
					master.safe_psql('postgres',
					                 "DELETE FROM o_test WHERE key % 2 = 0;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, N // 2)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 6. Bigger payload — pushes more pressure on TOAST + main btree.
	# ---------------------------------------------------------------
	def test_truncate_refill_loop_big_payload(self):
		ITERS = 3
		N = 80000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				for _ in range(ITERS):
					self._bulk_insert(master, N, 4000)
					self.catchup_orioledb(replica)
					self._check_replica(replica, N)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 7. Composite primary key (text + int).  Composite keys reorder
	#    hash distribution across recovery workers and produce a
	#    different leaf-level shape than a plain int PK.
	# ---------------------------------------------------------------
	def test_truncate_refill_loop_composite_pk(self):
		ITERS = 4
		N = 120000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	tag text NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (tag, key)\n"
				    ") USING orioledb;\n")

				for _ in range(ITERS):
					master.safe_psql(
					    'postgres', f"INSERT INTO o_test "
					    f"SELECT g, 't' || (g % 17), g, repeat('x', 800) "
					    f"FROM generate_series(1, {N}) g;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, N)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

	# ---------------------------------------------------------------
	# 8. Multiple bulk INSERTs concatenated into a single transaction.
	#    A long single-xact replay holds undo / xid records longer
	#    and amplifies oxid bookkeeping coverage.
	# ---------------------------------------------------------------
	def test_long_single_xact_loop(self):
		ITERS = 3
		BATCHES = 5
		N = 80000
		with self.node as master:
			self._stress_init(master)
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				for it in range(ITERS):
					con = master.connect()
					con.begin()
					for b in range(BATCHES):
						lo = b * N + 1
						hi = (b + 1) * N
						con.execute("INSERT INTO o_test "
						            "SELECT id, %s - id, repeat('x', 800) "
						            "FROM generate_series(%s, %s, 1) id;" %
						            (hi, lo, hi))
					con.commit()
					con.close()
					self.catchup_orioledb(replica)
					self._check_replica(replica, BATCHES * N)
					master.safe_psql('postgres', "TRUNCATE o_test;")
					self.catchup_orioledb(replica)
					self._check_replica(replica, 0)

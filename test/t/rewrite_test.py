from .base_test import BaseTest
import unittest


class RewriteTest(BaseTest):
	#
	# Plan B, Step 6 -- replication of the native adoption (FIXED).
	#
	# The native tableam rewrite (ALTER COLUMN TYPE on a non-bridged table,
	# and CREATE/REFRESH MATERIALIZED VIEW) adopts the filled primary tree by
	# reusing relnode Rnew across an OIndex delete+insert within one
	# transaction (o_drop_table_ext(carry_primary=true) +
	# recreate_o_table_ext).  On the PRIMARY, carry_primary excludes Rnew
	# from the master's undo drop queue, so the filled tree survives.  The
	# carry is not signalled in the WAL, so the STANDBY would otherwise wipe
	# the carried tree at commit (recovery_apply_systree_modify unconditionally
	# queues a drop undo for every deleted OIndex row).  The fix is a
	# standby-side pre-pass over the transaction's on-commit undo chain
	# (btree_relnode_recovery_prescan_carried) that records each relnode seen
	# in both a drop and a create, then btree_relnode_undo_callback skips the
	# data-destroying cleanup for those carried relnodes.  See .kilo/plan_b.md
	# "Step 6".

	def test_replication_alter_type_pk(self):
		"""
		ALTER COLUMN TYPE on a primary-key orioledb table drives the native
		tableam rewrite path (begin_heap_rewrite_body builds the primary on
		the transient heap, finish_heap_swap_body adopts the filled tree).
		The OTable drop/add in adoption must emit WAL that the replica replays
		to the same end state: same data, primary and secondaries readable.
		"""
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_rw_1(
							val_1 int PRIMARY KEY,
							val_2 int,
							val_3 text
						) USING orioledb;

						CREATE UNIQUE INDEX o_rw_1_val2_idx ON o_rw_1(val_2);
						CREATE INDEX o_rw_1_val3_idx ON o_rw_1(val_3);
						INSERT INTO o_rw_1
							SELECT x, 2 * x, 'test_data' || x
							FROM generate_series(1, 1000) x;
					""")
					con1.commit()

				# ALTER COLUMN TYPE triggers make_new_heap -> native fill ->
				# adoption on the master; the replica must replay to the same
				# state.
				master.execute("ALTER TABLE o_rw_1 ALTER val_2 TYPE bigint;")

				self.catchup_orioledb(replica)

				set_scan = ("set enable_seqscan = {}; "
				            "set enable_indexscan = {}; "
				            "set enable_bitmapscan = {};")

				# Primary-key lookup must find the row on the replica.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')} "
				        "SELECT val_1, val_2 FROM o_rw_1 WHERE val_1 = 500;"
				    ), [(500, 1000)])

				# Secondary (val_2) lookup: data type is now bigint.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')} "
				        "SELECT val_1 FROM o_rw_1 WHERE val_2 = 1000;"
				    ), [(500,)])

				# Secondary (val_3) lookup unchanged.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')} "
				        "SELECT val_1, val_2 FROM o_rw_1 "
				        "WHERE val_3 = 'test_data500';"
				    ), [(500, 1000)])

				# Row counts must match between master and replica.
				self.assertEqual(
				    master.execute("SELECT count(*) FROM o_rw_1;")[0][0],
				    replica.execute("SELECT count(*) FROM o_rw_1;")[0][0])

	def test_replication_alter_type_bridged(self):
		"""
		ALTER COLUMN TYPE on a BRIDGED orioledb table (Step 7 Path B).  The
		native rewrite now carries the bridge tree (Btrans) maintained inline
		during the fill across the adoption, instead of falling back to the
		old OAT rewrite_table path.  The replica must replay the adoption to
		the same state: data, primary, and the stock-PG bridged index (read
		through the carried bridge tree) all correct.  Covers both the
		primary-key and the no-primary-key (ctid-primary) bridged cases.
		"""
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_rw_bridge(
							pk int PRIMARY KEY,
							j int,
							val_3 text
						) USING orioledb;

						-- A stock-PG btree with orioledb_index=off is
						-- auto-bridged: it points at bridge_ctids resolved
						-- through the orioledb bridge tree.
						CREATE INDEX o_rw_bridge_j_idx ON o_rw_bridge
							USING btree (j) WITH (orioledb_index = off);
						INSERT INTO o_rw_bridge
							SELECT x, 2 * x, 'test_data' || x
							FROM generate_series(1, 1000) x;
					""")
					con1.commit()

				# ALTER COLUMN TYPE drives make_new_heap -> native fill (which
				# maintains the bridge tree inline and stamps fresh
				# bridge_ctids) -> adoption carrying Btrans on the master.
				master.execute("ALTER TABLE o_rw_bridge ALTER j TYPE bigint;")

				self.catchup_orioledb(replica)

				set_scan = ("set enable_seqscan = {}; "
				            "set enable_indexscan = {}; "
				            "set enable_bitmapscan = {};")

				# Primary-key lookup must find the row on the replica.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')} "
				        "SELECT pk, j FROM o_rw_bridge WHERE pk = 500;"
				    ), [(500, 1000)])

				# The bridged stock-PG btree index must read correctly on the
				# replica through the carried bridge tree.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'off')} "
				        "SELECT pk FROM o_rw_bridge WHERE j = 1000;"
				    ), [(500,)])

				# Row counts must match between master and replica.
				self.assertEqual(
				    master.execute("SELECT count(*) FROM o_rw_bridge;")[0][0],
				    replica.execute("SELECT count(*) FROM o_rw_bridge;")[0][0])

				# A bridged index scan must return every row, ordered, on the
				# replica (exercises the bridge tree end-to-end).
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'off')} "
				        "SELECT count(*) FROM o_rw_bridge WHERE j > 0;"
				    ), [(1000,)])

	def test_replication_alter_type_bridged_no_pk(self):
		"""
		The no-primary-key bridged case (mirrors index_bridging.sql's
		ALTER COLUMN TYPE after dropping the PK): the ctid primary is used,
		the bridge tree is still carried across the native adoption.  The
		bridged index must read correctly on the replica after catchup.
		"""
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_rw_bridge_nopk(
							pk int,
							j int
						) USING orioledb;

						CREATE INDEX o_rw_bridge_nopk_j_idx ON o_rw_bridge_nopk
							USING btree (j) WITH (orioledb_index = off);
						INSERT INTO o_rw_bridge_nopk
							SELECT x, 2 * x FROM generate_series(1, 500) x;
						ALTER TABLE o_rw_bridge_nopk
							ADD PRIMARY KEY (pk);
						ALTER TABLE o_rw_bridge_nopk
							DROP CONSTRAINT o_rw_bridge_nopk_pkey;
					""")
					con1.commit()

				# ALTER COLUMN TYPE on a no-PK bridged table drives the native
				# rewrite via the ctid primary, carrying Btrans.
				master.execute(
				    "ALTER TABLE o_rw_bridge_nopk ALTER j TYPE bigint;")

				self.catchup_orioledb(replica)

				set_scan = ("set enable_seqscan = {}; "
				            "set enable_indexscan = {}; "
				            "set enable_bitmapscan = {};")

				# The bridged index must read correctly on the replica.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'off')} "
				        "SELECT pk FROM o_rw_bridge_nopk WHERE j = 1000;"
				    ), [(500,)])

				self.assertEqual(
				    master.execute(
				        "SELECT count(*) FROM o_rw_bridge_nopk;")[0][0],
				    replica.execute(
				        "SELECT count(*) FROM o_rw_bridge_nopk;")[0][0])

	def test_replication_refresh_matview(self):
		"""
		REFRESH MATERIALIZED VIEW on an orioledb matview drives the native
		fill path (make_new_heap -> fill -> adopt). The replica must replay the
		adoption WAL to the same state: refreshed rows are readable on the
		replica via the primary.
		"""
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_mv_src(k int, v int) USING orioledb;
						INSERT INTO o_mv_src
							SELECT g, g * g FROM generate_series(1, 100) g;

						CREATE MATERIALIZED VIEW o_mv_1 AS
							SELECT k, v FROM o_mv_src
							WITH DATA;
					""")
					con1.commit()

				# Add rows that should appear after refresh, then REFRESH drives
				# make_new_heap -> native fill -> adoption on the master.
				master.execute("INSERT INTO o_mv_src "
				               "SELECT g, g * g FROM generate_series(101, 200) g;")
				master.execute("REFRESH MATERIALIZED VIEW o_mv_1 WITH DATA;")

				self.catchup_orioledb(replica)

				set_scan = ("set enable_seqscan = {}; "
				            "set enable_indexscan = {}; "
				            "set enable_bitmapscan = {};")

				# The refreshed matview on the replica must reflect the new rows.
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')} "
				        "SELECT count(*) FROM o_mv_1;")[0][0], 200)
				self.assertEqual(
				    replica.execute(
				        f"{set_scan.format('off', 'on', 'on')} "
				        "SELECT v FROM o_mv_1 WHERE k = 125;"), [(125 * 125,)])

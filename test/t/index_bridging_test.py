#!/usr/bin/env python3
# coding: utf-8

import re
import unittest

from .base_test import BaseTest


class IndexBridgingTest(BaseTest):

	def test_create_finish_bridge_and_toast(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")

		def bridge_count(table):
			indices = node.execute(
			    "SELECT orioledb_tbl_indices('%s'::regclass, true, false);" %
			    table)[0][0]
			return indices.count("Index index_bridge")

		# OrioleDB needs its TOAST metadata even for a fixed-width schema.
		node.safe_psql("""
			CREATE TABLE o_fixed (id integer) USING orioledb;
		""")
		self.assertEqual(
		    node.execute("""
				SELECT reltoastrelid <> 0
				FROM pg_class WHERE oid = 'o_fixed'::regclass;
			"""), [(True, )])
		self.assertIn(
		    "Index toast",
		    node.execute("""
				SELECT orioledb_tbl_indices(
					'o_fixed'::regclass, true, false);
			""")[0][0])

		# LIKE-generated indexes are later statements in the transformed CREATE
		# list.  Both bridged indexes must share one bridge initialized before
		# the first user tuple is inserted.
		node.safe_psql("""
			CREATE TABLE h_source (id integer PRIMARY KEY, value integer,
			                       tags integer[]);
			CREATE INDEX h_source_hash_idx ON h_source USING hash (value);
			CREATE INDEX h_source_gin_idx ON h_source USING gin (tags);
			CREATE TABLE o_like (LIKE h_source INCLUDING ALL) USING orioledb;
			INSERT INTO o_like VALUES (1, 10, ARRAY[1, 2]),
			                          (2, 20, ARRAY[2, 3]);
		""")
		self.assertEqual(bridge_count("o_like"), 1)
		self.assertEqual(
		    node.execute("SELECT id FROM o_like WHERE tags @> ARRAY[3];"),
		    [(2, )])

		# A partition index cloned inside DefineRelation is finalized through the
		# same once-per-logical-CREATE callback.
		node.safe_psql("""
			CREATE TABLE o_parent (id integer, tags integer[])
				PARTITION BY RANGE (id) USING orioledb;
			CREATE INDEX o_parent_tags_idx ON o_parent USING gin (tags);
			CREATE TABLE o_child PARTITION OF o_parent
				FOR VALUES FROM (0) TO (10);
			INSERT INTO o_parent VALUES (1, ARRAY[4, 5]);
		""")
		self.assertEqual(bridge_count("o_child"), 1)
		self.assertEqual(
		    node.execute("SELECT id FROM o_parent WHERE tags @> ARRAY[5];"),
		    [(1, )])

		# A later bridged index on a populated table keeps the rebuild path.
		node.safe_psql("""
			CREATE TABLE o_later (id integer PRIMARY KEY, value integer)
				USING orioledb;
			INSERT INTO o_later SELECT i, i FROM generate_series(1, 20) i;
			CREATE INDEX o_later_hash_idx ON o_later USING hash (value);
		""")
		self.assertEqual(bridge_count("o_later"), 1)
		self.assertEqual(
		    node.execute("SELECT id FROM o_later WHERE value = 17;"), [(17, )])

		# CTAS and materialized views are finalized before their first data fill.
		node.safe_psql("""
			CREATE TABLE o_ctas USING orioledb AS
				SELECT 1 AS id, repeat('x', 10000) AS value;
			CREATE MATERIALIZED VIEW o_matview USING orioledb AS
				SELECT 1 AS id, repeat('y', 10000) AS value;
		""")
		self.assertEqual(node.execute("SELECT length(value) FROM o_ctas;"),
		                 [(10000, )])
		self.assertEqual(node.execute("SELECT length(value) FROM o_matview;"),
		                 [(10000, )])

		node.safe_psql("""
			BEGIN;
			CREATE TABLE o_rolled_back (LIKE h_source INCLUDING ALL)
				USING orioledb;
			ROLLBACK;
		""")
		self.assertEqual(
		    node.execute("SELECT to_regclass('o_rolled_back') IS NULL;"),
		    [(True, )])

		node.safe_psql("""
			CREATE TABLE o_later_rollback (id integer, value integer)
				USING orioledb;
			INSERT INTO o_later_rollback VALUES (1, 1);
		""")
		node.safe_psql("""
			BEGIN;
			CREATE INDEX o_later_rollback_hash_idx
				ON o_later_rollback USING hash (value);
			ROLLBACK;
		""")
		self.assertEqual(bridge_count("o_later_rollback"), 0)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_later_rollback;"), [(1, )])

		node.stop()
		node.start()
		self.assertEqual(bridge_count("o_like"), 1)
		self.assertEqual(bridge_count("o_child"), 1)
		self.assertEqual(node.execute("SELECT count(*) FROM o_like;"), [(2, )])

	@unittest.skipIf(not BaseTest.extension_installed("pageinspect"),
	                 "'pageinspect' is not installed")
	def test_ctid_overflow(self):
		node = self.node
		node.append_conf("orioledb.debug_max_bridge_ctid_blkno=1")
		node.start()

		def check(expected_ctids):
			self.assertEqual(
			    node.execute("""
					SELECT ctid FROM  generate_series(1,
													(SELECT relpages - 1 FROM pg_class
														WHERE oid = 'o_test_ix1'::regclass)) p,
						LATERAL bt_page_items('o_test_ix1', p)
						WHERE htid IS NOT NULL
						ORDER BY ctid;
				"""), expected_ctids)

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE EXTENSION pageinspect;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int
			) USING orioledb;

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (orioledb_index=off);
			CREATE INDEX o_test_ix2 on o_test using btree (j);
		""")

		nrows = 291  # MaxHeapTuplesPerPage
		node.safe_psql("""
			INSERT INTO o_test SELECT v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % nrows)

		expected_ctids = [(f'(0,{x})', ) for x in range(1, nrows + 1)]
		check(expected_ctids)

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 4) = 0;
		""")
		check(expected_ctids)

		_, _, err = node.psql("""
			VACUUM VERBOSE;
		""")
		vacuumed = err.decode("utf-8").split("INFO:  vacuuming")
		bridged = next(
		    filter(
		        lambda x: x.split('\n')[0] ==
		        ' bridged indexes "postgres.public.o_test"', vacuumed))
		dead = re.search(r"had (\d+) dead", bridged)[1]

		orig_len = len(expected_ctids)
		del expected_ctids[3::4]  # removed every 4th
		check(expected_ctids)
		self.assertTrue(dead, orig_len - len(expected_ctids))

		self.assertEqual(
		    len(expected_ctids),
		    node.execute("""
							SELECT reltuples FROM pg_class WHERE oid = 'o_test_ix1'::regclass
						 """)[0][0])

		nrows = 10
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 100, v * 200 FROM generate_series(1, %d) v;
		""" % nrows)
		expected_ctids.extend([(f'(0,{x*4})', ) for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

	def test_ctid_reuse_over_deleted_tuple(self):
		"""A reused bridge ctid must not unbalance the vacated counter.

		Tuples of a bridge tree are kept after a delete, for VACUUM to clean
		the bridged indexes with, so their space is never counted as vacated
		(o_btree_modify_delete()).  When the ctid counter comes back around to
		such a tuple, the insert replaces it -- and the replace used to take
		that space back out of the counter, which no one had put in:

		    TRAP: failed Assert("((BTreePageHeader *)(p))->field2 >= ..."),
		          File: "src/btree/insert.c"

		Without assertions the counter is a LocationIndex and wraps instead,
		which leaves the page looking almost entirely reclaimable to page
		compaction and to the merge heuristic.

		debug_max_bridge_ctid_blkno caps the counter at one block, so the
		wraparound arrives after MaxHeapTuplesPerPage rows rather than after
		2^32 blocks of them.
		"""
		node = self.node
		node.append_conf("orioledb.debug_max_bridge_ctid_blkno=1")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int,
				PRIMARY KEY (i)
			) USING orioledb;

			CREATE INDEX o_test_ix ON o_test USING btree (j)
				WITH (orioledb_index = off);
		""")

		nrows = 291  # MaxHeapTuplesPerPage, so the block is full afterwards
		node.safe_psql("INSERT INTO o_test SELECT v, v"
		               " FROM generate_series(1, %d) v;" % nrows)
		# Their ctids are the ones the counter comes back to; the tuples stay
		# in the bridge tree, deleted.
		node.safe_psql("DELETE FROM o_test WHERE i <= 10;")

		node.safe_psql("INSERT INTO o_test SELECT v, v"
		               " FROM generate_series(1000, 1005) v;")
		# Rolling one back walks the same accounting backwards.
		with node.connect() as con:
			con.begin()
			con.execute("INSERT INTO o_test SELECT v, v"
			            " FROM generate_series(2000, 2002) v;")
			con.rollback()
		node.safe_psql("""
			DELETE FROM o_test WHERE i BETWEEN 20 AND 30;
			INSERT INTO o_test SELECT v, v FROM generate_series(3000, 3010) v;
			UPDATE o_test SET j = j + 1 WHERE i BETWEEN 100 AND 150;
		""")

		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test;")[0][0],
		    nrows - 10 + 6 - 11 + 11)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_test'::regclass);")[0]
		    [0])
		# The bridged index has to answer like the table itself.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test"
		                 " WHERE j BETWEEN 100 AND 200;")[0][0],
		    node.execute("SELECT count(*) FROM (SELECT j FROM o_test) x"
		                 " WHERE j BETWEEN 100 AND 200;")[0][0])

	@unittest.skipIf(not BaseTest.extension_installed("pageinspect"),
	                 "'pageinspect' is not installed")
	def test_ctid_overflow_two_times(self):
		node = self.node
		node.append_conf("orioledb.debug_max_bridge_ctid_blkno=1")
		node.start()

		def check(expected_ctids):
			self.assertEqual(
			    node.execute("""
					SELECT ctid FROM  generate_series(1,
													(SELECT relpages - 1 FROM pg_class
														WHERE oid = 'o_test_ix1'::regclass)) p,
						LATERAL bt_page_items('o_test_ix1', p)
						WHERE htid IS NOT NULL
						ORDER BY ctid;
				"""), expected_ctids)

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE EXTENSION pageinspect;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int,
				k int
			) USING orioledb;

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (orioledb_index=off);
			CREATE INDEX o_test_ix2 on o_test using btree (k);
		""")

		all_rows = 0
		nrows = 291  # MaxHeapTuplesPerPage
		node.safe_psql("""
			INSERT INTO o_test SELECT v, 10000 + v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % nrows)
		all_rows += nrows

		expected_ctids = [(f'(0,{x})', ) for x in range(1, nrows + 1)]
		check(expected_ctids)

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 4) = 0;
		""")
		check(expected_ctids)

		node.safe_psql("""
			VACUUM;
		""")

		del expected_ctids[3::4]  # removed every 4th
		check(expected_ctids)

		self.assertEqual(
		    len(expected_ctids),
		    node.execute("""
							SELECT reltuples FROM pg_class WHERE oid = 'o_test_ix1'::regclass
						 """)[0][0])

		nrows = 10
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 4, %d + v, v FROM generate_series(1, %d) v;
		""" % (10000 + all_rows, nrows))
		all_rows += nrows
		expected_ctids.extend([(f'(0,{x*4})', ) for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

		nrows = 291 - len(expected_ctids)
		node.safe_psql("""
			INSERT INTO o_test SELECT v * 4, %d + v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % (10000 + all_rows, nrows))
		all_rows += nrows
		expected_ctids.extend([(f'(0,{(x+10)*4})', )
		                       for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 8) = 0;
		""")
		check(expected_ctids)

		node.safe_psql("""
			VACUUM;
		""")

		del expected_ctids[7::8]  # removed every 8th
		check(expected_ctids)

		nrows = 10
		node.safe_psql("""
			INSERT INTO o_test SELECT %d + v, %d + v, v FROM generate_series(1, %d) v;
			ANALYZE o_test;
		""" % (all_rows, 10000 + all_rows, nrows))
		all_rows += nrows
		expected_ctids.extend([(f'(0,{x*8})', ) for x in range(1, nrows + 1)])
		expected_ctids = sorted(
		    expected_ctids, key=lambda ctid: int(ctid[0][1:-1].split(',')[1]))
		check(expected_ctids)

	def test_bridge_gin_dead_tids_on_earlier_page(self):
		"""When the bridge_ctid counter crosses
		MaxHeapTuplesPerPage (291), bridge index holds dead TIDs on block 0 and the
		live TID on block 1.  Bridge bitmap scan must advance past the
		all-dead page rather than terminating after it."""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id  int NOT NULL,
				arr bigint[],
				PRIMARY KEY (id)
			) USING orioledb;
			ALTER TABLE o_test SET (autovacuum_enabled = off);
			CREATE INDEX ON o_test USING GIN (arr);
			INSERT INTO o_test VALUES (1, ARRAY[1]::bigint[]);
			DO $$
			BEGIN
				FOR k IN 2..294 LOOP
					UPDATE o_test SET arr = ARRAY[k]::bigint[] WHERE id = 1;
				END LOOP;
				UPDATE o_test SET arr = ARRAY[1]::bigint[] WHERE id = 1;
			END $$;
		""")
		self.assertEqual(
		    node.execute("""
				SET enable_seqscan = off;
				SET enable_indexscan = off;
				SELECT count(*) FROM o_test WHERE arr @> ARRAY[1]::bigint[];
			""")[0][0], 1)

	def test_bridge_recovery(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION orioledb;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int,
				k int
			) USING orioledb;

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (orioledb_index=off);
			CREATE INDEX o_test_ix2 on o_test using btree (k);
		""")

		node.safe_psql("""
			INSERT INTO o_test SELECT v, 10000 + v, v FROM generate_series(1, 2000) v;
			ANALYZE o_test;
		""")

		node.safe_psql("""
			DELETE FROM o_test WHERE mod(i, 2) = 0;
		""")

		con1 = node.connect(autocommit=True)
		con1.execute("""
			VACUUM;
		""")

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY j;
		""")[0][0][0]["Plan"]
		self.assertEqual('Index Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		tuples = node.execute("SELECT * FROM o_test ORDER BY j;")

		node.stop(['-m', 'immediate'])
		node.start()

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY j;
		""")[0][0][0]["Plan"]
		self.assertEqual('Index Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		self.assertEqual(
		    tuples,
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test ORDER BY j;
						 """))

	def bridge_ctids(self, node, primary_is_ctid, table='o_test'):
		"""
		The bridge ctid of every row still in the primary tree.

		orioledb_tbl_structure() prints a section per tree, and in the primary
		one a bridged row's first field is the bridge ctid it holds -- the
		second, when the table has no primary key and the first field is the
		surrogate ctid.
		"""
		struct = node.execute("SELECT orioledb_tbl_structure('%s'::regclass,"
		                      " 'nue');" % table)[0][0]
		primary = [
		    part for part in struct.split('\nIndex ')
		    if 'ix_type = primary' in part
		]
		self.assertEqual(len(primary), 1,
		                 "no single primary tree in:\n%s" % struct)
		if primary_is_ctid:
			return re.findall(r"tuple = \('\(\d+,\d+\)', '(\(\d+,\d+\))'",
			                  primary[0])
		return re.findall(r"tuple = \('(\(\d+,\d+\))'", primary[0])

	def check_bridge_ctids_are_sane(self,
	                                node,
	                                total,
	                                live,
	                                gone,
	                                primary_is_ctid=False):
		"""
		What a rewound bridge ctid counter breaks.

		`total` is how many rows the table holds, `live` the ids left of the
		ones inserted before the counter rewound, `gone` ids whose rows were
		deleted before the counter rewound -- their bridge tuples are kept
		for VACUUM, so an insert that lands on one replaces it instead of
		failing, and the bridged index entries still pointing at that ctid
		start resolving to whichever row took it over.
		"""
		ctids = self.bridge_ctids(node, primary_is_ctid)
		self.assertEqual(len(ctids), len(set(ctids)),
		                 "two rows hold one bridge ctid: %s" % sorted(ctids))
		# Deleted rows linger in the primary tree until VACUUM, so the dump
		# holds at least one tuple per live row -- and the tuple of a deleted
		# row whose ctid was handed out again is right there next to the row
		# that took it, which is what the distinctness check above sees.
		self.assertGreaterEqual(len(ctids), total)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test;")[0][0], total)

		plan = node.execute("""
			SET enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT i FROM o_test WHERE doc ? 'k';
		""")[0][0][0]["Plan"]
		inner = plan["Plans"][0]
		self.assertEqual('Bitmap Index Scan', inner["Node Type"],
		                 "not reading through the bridged index: %s" % plan)
		self.assertEqual('o_test_gin', inner["Index Name"])

		# The bridged index has to answer like the table itself.  A row that
		# took over a deleted row's ctid shows up here: the deleted row's GIN
		# entries have not been vacuumed away, and they now lead to it.
		self.assertEqual(
		    node.execute("""
				SET enable_seqscan = off;
				SELECT array_agg(i ORDER BY i) FROM o_test WHERE doc ? 'k';
			""")[0][0], live)
		self.assertEqual(
		    node.execute("""
				SET enable_seqscan = on;
				SET enable_bitmapscan = off;
				SET enable_indexscan = off;
				SELECT array_agg(i ORDER BY i) FROM o_test WHERE doc ? 'k';
			""")[0][0], live)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test WHERE i = ANY(%s);" %
		                 ('ARRAY%s' % gone))[0][0], 0)

	def create_bridged_table(self, node, primary_is_ctid=False):
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (
				i int NOT NULL,
				doc jsonb%s
			) USING orioledb;
			CREATE INDEX o_test_gin ON o_test USING gin (doc);
		""" % ('' if primary_is_ctid else ',\n\t\t\t\tPRIMARY KEY (i)'))
		# Everything the counter hands out after this point reaches disk only
		# through the WAL, which is what recovery has to read it back from.
		node.safe_psql("CHECKPOINT;")

	def fill_bridged_table(self, node):
		node.safe_psql("""
			INSERT INTO o_test SELECT v, jsonb_build_object('k', v)
				FROM generate_series(1, 50) v;
			DELETE FROM o_test WHERE i <= 5;
		""")
		# An update of a bridged column gives the row a new bridge ctid, so
		# the highest ctids in use are these and not the inserts': recovery
		# has to come back past the update records too.
		node.safe_psql("""
			UPDATE o_test SET doc = jsonb_build_object('k', i, 'u', 1)
				WHERE i BETWEEN 10 AND 20;
		""")

	def test_bridge_ctid_survives_crash_recovery(self):
		"""
		The bridge ctid counter must come back from the WAL, not from zero.

		It lives in the primary tree's meta page, so a crash leaves it at
		whatever the last checkpoint saved.  Replay used to pass every ctid it
		saw to btree_ctid_update_if_needed() -- the surrogate primary ctid's
		counter, not this one -- so the bridge counter stayed where the
		checkpoint had left it and started handing out ctids that live rows
		already held:

		    ERROR:  duplicate key value violates unique constraint
		            "index_bridge"
		    DETAIL:  Key (index_bridging_ctid)=((0,6)) already exists.

		The rows it reaches before that one are worse off than the error: they
		land on deleted rows' bridge tuples, which are kept until VACUUM, and
		take over their ctid.
		"""
		node = self.node
		node.start()
		self.create_bridged_table(node)
		self.fill_bridged_table(node)

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("""
			INSERT INTO o_test SELECT v, jsonb_build_object('n', v)
				FROM generate_series(101, 120) v;
		""")

		self.check_bridge_ctids_are_sane(node,
		                                 total=65,
		                                 live=list(range(6, 51)),
		                                 gone=list(range(1, 6)))

	def test_bridge_ctid_survives_promote(self):
		"""
		The same counter, recovered by a standby rather than by crash
		recovery: a promoted replica that replayed the inserts has to know
		which ctids they used.

		The rows have to be inserted after the base backup, or the replica
		starts from a copy of the primary's meta page and never has to
		recover the counter at all.
		"""
		node = self.node
		node.start()
		self.create_bridged_table(node)

		with self.getReplica().start() as replica:
			self.fill_bridged_table(node)
			self.catchup_orioledb(replica)
			node.stop(['-m', 'immediate'])
			replica.promote()
			replica.poll_query_until("SELECT NOT pg_is_in_recovery();",
			                         expected=True)

			replica.safe_psql("""
				INSERT INTO o_test SELECT v, jsonb_build_object('n', v)
					FROM generate_series(101, 120) v;
			""")

			self.check_bridge_ctids_are_sane(replica,
			                                 total=65,
			                                 live=list(range(6, 51)),
			                                 gone=list(range(1, 6)))

	def test_bridge_ctid_survives_crash_recovery_without_pkey(self):
		"""
		The same, on a table whose primary key is the surrogate ctid.

		Its rows carry two ctids, the row's own and the bridge one, and only
		the second is this counter's.  They are also counted differently --
		2048 rows to a block against MaxHeapTuplesPerPage -- so reading the
		wrong one puts the counter in the wrong place rather than merely
		behind.
		"""
		node = self.node
		node.start()
		self.create_bridged_table(node, primary_is_ctid=True)
		self.fill_bridged_table(node)

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("""
			INSERT INTO o_test SELECT v, jsonb_build_object('n', v)
				FROM generate_series(101, 120) v;
		""")

		self.check_bridge_ctids_are_sane(node,
		                                 total=65,
		                                 live=list(range(6, 51)),
		                                 gone=list(range(1, 6)),
		                                 primary_is_ctid=True)

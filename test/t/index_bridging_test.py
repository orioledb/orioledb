#!/usr/bin/env python3
# coding: utf-8

import re

from .base_test import BaseTest


class IndexBridgingTest(BaseTest):

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
			) USING orioledb WITH (index_bridging);

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (index_bridging);
			CREATE INDEX o_test_ix2 on o_test using btree (j);
		""")

		nrows = 2047  # max offset of ctid to overflow
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
			) USING orioledb WITH (index_bridging);

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (index_bridging);
			CREATE INDEX o_test_ix2 on o_test using btree (k);
		""")

		all_rows = 0
		nrows = 2047  # max offset of ctid to overflow
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

		nrows = 2047 - len(expected_ctids)
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

	def test_bridge_recovery(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE EXTENSION pageinspect;
		""")

		node.safe_psql("""
			CREATE TABLE o_test (
				i int NOT NULL,
				j int,
				k int
			) USING orioledb WITH (index_bridging);

			CREATE INDEX o_test_ix1 on o_test using btree (j) WITH (index_bridging);
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
			SET log_error_verbosity = 'terse';
		""")
		con1.execute("""
			VACUUM;
		""")
		con1.execute("""
			RESET log_error_verbosity;
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

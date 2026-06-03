#!/usr/bin/env python3
# coding: utf-8
# Perf A/B: COPY FROM with and without orioledb.bulk_insert.
# Skipped unless RUN_PERF_TESTS=1 so it stays out of the Valgrind run.

import os
import time
import unittest

from .base_test import BaseTest


@unittest.skipUnless(os.environ.get("RUN_PERF_TESTS") == "1",
                     "set RUN_PERF_TESTS=1 to run perf tests")
class PerfCopyTest(BaseTest):

	# Knobs (override via env for local runs):
	NROWS = int(os.environ.get("PERF_COPY_NROWS", "1000000"))
	MIN_SPEEDUP = float(os.environ.get("PERF_COPY_MIN_SPEEDUP", "2.0"))

	def _run_leg(self, bulk_on):
		node = self.node
		setting = "on" if bulk_on else "off"
		node.safe_psql(f"""
			SET orioledb.bulk_insert = {setting};
			DROP TABLE IF EXISTS perf_copy;
			CREATE TABLE perf_copy (id int PRIMARY KEY, v text, w int)
				USING orioledb;
			CREATE INDEX ON perf_copy(v);
			CREATE INDEX ON perf_copy(w);
		""")
		copy_sql = f"""
			SET orioledb.bulk_insert = {setting};
			COPY perf_copy FROM PROGRAM
			    'seq 1 {self.NROWS} | awk ''{{printf("%d\\trow_%d\\t%d\\n", $1, $1, $1 * 7)}}''';
		"""
		t0 = time.monotonic()
		node.safe_psql(copy_sql)
		elapsed = time.monotonic() - t0
		row, page = node.execute("""
			SELECT
			    (SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'row'),
			    (SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'page');
		""")[0]
		size = node.execute(
			"SELECT pg_total_relation_size('perf_copy');")[0][0]
		undo_per_byte = (int(row) + int(page)) / max(int(size), 1)
		return elapsed, int(row) + int(page), undo_per_byte

	def test_copy_speedup(self):
		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		t_off, undo_off, ratio_off = self._run_leg(bulk_on=False)
		t_on, undo_on, ratio_on = self._run_leg(bulk_on=True)

		# Emit numbers so CI history tracks the trend over time.
		print(f"\nPERF_COPY off  : {t_off:.3f}s  undo={undo_off:>12d}B  "
		      f"undo/data={ratio_off:.3f}")
		print(f"PERF_COPY on   : {t_on:.3f}s  undo={undo_on:>12d}B  "
		      f"undo/data={ratio_on:.3f}")
		print(f"PERF_COPY speedup={t_off / max(t_on, 1e-6):.2f}x  "
		      f"undo-reduction={(1 - undo_on / max(undo_off, 1)) * 100:.1f}%")

		self.assertLess(undo_on, undo_off,
		                "bulk path must reduce undo")
		self.assertLessEqual(t_on * self.MIN_SPEEDUP, t_off + 1.0,
		                     f"bulk path slower than {self.MIN_SPEEDUP}x "
		                     f"target: off={t_off:.3f}s on={t_on:.3f}s")
		node.stop()

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
	# Measured ~1.37x on a primary + two secondary index schema; pin a
	# conservative 1.2x so distribution drift doesn't flake the test.
	MIN_SPEEDUP = float(os.environ.get("PERF_COPY_MIN_SPEEDUP", "1.2"))

	UNDO_META_SQL = """
		SELECT
		    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
		    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page'),
		    (SELECT cleanedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
		    (SELECT cleanedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page');
	"""

	def _undo_sample(self, node):
		row_u, page_u, row_c, page_c = node.execute(self.UNDO_META_SQL)[0]
		return int(row_u), int(page_u), int(row_c), int(page_c)

	def _run_leg(self, table, bulk_on):
		node = self.node
		setting = "on" if bulk_on else "off"
		node.safe_psql(f"""
			SET orioledb.bulk_insert = {setting};
			CREATE TABLE {table} (id int PRIMARY KEY, v text, w int)
				USING orioledb;
			CREATE INDEX ON {table}(v);
			CREATE INDEX ON {table}(w);
		""")
		# Three samples around the COPY.  Undo bytes written =
		# lastUsedLocation delta (real per-COPY write count;
		# orioledb_undo_size includes speculative reserves).
		# Undo bytes still pinned = lastUsedLocation - cleanedLocation
		# at the "after-cleanup" sample.  The COPY itself auto-commits
		# but cleanup of its undo runs lazily, triggered by the next
		# statement -- so the second sample shows the COPY's whole undo
		# as still pinned, and the third sample (taken after a no-op
		# query that nudges the cleanup pass) shows what's left.
		ru0, pu0, _, _ = self._undo_sample(node)
		# `seq -f %.0f` keeps the count as integers; macOS BSD seq
		# switches to scientific notation at 1e6 and breaks awk's %d.
		copy_sql = f"""
			SET orioledb.bulk_insert = {setting};
			COPY {table} FROM PROGRAM
			    'seq -f %.0f 1 {self.NROWS} | awk ''{{printf("%d\\trow_%d\\t%d\\n", $1, $1, $1 * 7)}}''';
		"""
		t0 = time.monotonic()
		node.safe_psql(copy_sql)
		elapsed = time.monotonic() - t0
		ru1, pu1, rc1, pc1 = self._undo_sample(node)
		# Poll for lazy cleanup: each node.execute opens a fresh psql
		# connection, so the bgwriter / undo cleanup may need a few
		# nudges before pinned drops.
		ru2, pu2, rc2, pc2 = ru1, pu1, rc1, pc1
		for _ in range(20):
			node.execute(f"SELECT count(*) FROM {table};")
			ru2, pu2, rc2, pc2 = self._undo_sample(node)
			if (ru2 - rc2) + (pu2 - pc2) < 64 * 1024:
				break
			time.sleep(0.1)
		size = node.execute(
			f"SELECT pg_total_relation_size('{table}');")[0][0]
		written = (ru1 - ru0) + (pu1 - pu0)
		# Pinned: still-needed undo that the system has not reclaimed.
		# Two snapshots: right after COPY commits (cleanup hasn't run
		# yet) and after the next query.
		pinned_post_commit = (ru1 - rc1) + (pu1 - pc1)
		pinned_post_cleanup = (ru2 - rc2) + (pu2 - pc2)
		return {
			"elapsed": elapsed,
			"undo_written": written,
			"undo_per_byte": written / max(int(size), 1),
			"undo_pinned_post_commit": pinned_post_commit,
			"undo_pinned_post_cleanup": pinned_post_cleanup,
		}

	def test_copy_speedup(self):
		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		off = self._run_leg("t_off", bulk_on=False)
		on_ = self._run_leg("t_on", bulk_on=True)

		# Emit numbers so CI history tracks the trend over time.
		def fmt(label, m):
			print(f"PERF_COPY {label:<4}: {m['elapsed']:.3f}s  "
			      f"written={m['undo_written']:>12d}B  "
			      f"undo/data={m['undo_per_byte']:.3f}  "
			      f"pinned_post_commit={m['undo_pinned_post_commit']:>12d}B  "
			      f"pinned_post_cleanup={m['undo_pinned_post_cleanup']:>10d}B")
		print()
		fmt("off", off)
		fmt("on", on_)
		print(f"PERF_COPY speedup={off['elapsed'] / max(on_['elapsed'], 1e-6):.2f}x  "
		      f"undo-reduction={(1 - on_['undo_written'] / max(off['undo_written'], 1)) * 100:.1f}%")

		self.assertLess(on_["undo_written"], off["undo_written"],
		                "bulk path must reduce undo written")
		self.assertLessEqual(on_["elapsed"] * self.MIN_SPEEDUP,
		                     off["elapsed"] + 1.0,
		                     f"bulk path slower than {self.MIN_SPEEDUP}x "
		                     f"target: off={off['elapsed']:.3f}s "
		                     f"on={on_['elapsed']:.3f}s")
		# After the next query nudges the cleanup pass, the COPY's
		# undo should be fully reclaimed in both legs.  Allow a small
		# slack for whatever the meta query itself has written.
		SLACK = 64 * 1024
		self.assertLess(off["undo_pinned_post_cleanup"], SLACK,
		                f"off leg leaked undo after cleanup: "
		                f"{off['undo_pinned_post_cleanup']}B still pinned")
		self.assertLess(on_["undo_pinned_post_cleanup"], SLACK,
		                f"on leg leaked undo after cleanup: "
		                f"{on_['undo_pinned_post_cleanup']}B still pinned")
		node.stop()

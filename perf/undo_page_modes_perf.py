#!/usr/bin/env python3
"""
Measure time of SELECT count(*) against a btree mid-INSERT across the
three page-level undo strategies (SKIP / DIFF / FULL).

Each mode is forced by concurrent backends around the timed writer:
  SKIP -- no concurrent backends (fresh leaves have undoLocation =
          Invalid).
  DIFF -- one concurrent backend holding a REPEATABLE READ snapshot
          pinned before touching t, plus a brief warmup during which
          another concurrent backend keeps a seq scan running so
          warmup splits write FULL images.  The seq scan ends before
          the timed writer runs; the pinned snapshot keeps SKIP
          disabled, and with no seq scan running DIFF stays enabled.
  FULL -- one concurrent backend holding the snapshot and another
          keeping a seq scan running throughout the timed phase
          (numSeqScans > 0 disables both SKIP and DIFF).

Runs each mode against every requested undo-buffer size (--undo-buffers)
and PK insertion order (--orders).  Reports table_grew / row_undo /
page_undo sizes and writer / SELECT ms per cell.  Observed page_undo
(0 / small / ~table-sized) is itself the evidence of which mode fired.
"""
import argparse
import os
import subprocess
import sys
import time
from threading import Thread

# Make the testgres helpers importable when invoked from the repo root.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_REPO, "test"))
import testgres  # noqa: E402


def detect_commit():
	try:
		return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
		                               cwd=_REPO,
		                               text=True).strip()
	except Exception:
		return "<unknown>"


def _undo_used(conn, kind):
	rows = conn.execute(
	    "SELECT undo_type, lastUsedLocation FROM orioledb_get_undo_meta();")
	for r in rows:
		if r[0] == kind:
			return int(r[1])
	return 0


def page_undo_used(conn):
	return _undo_used(conn, "page")


def row_undo_used(conn):
	return _undo_used(conn, "row")


def table_size_bytes(conn):
	return int(conn.execute("SELECT pg_total_relation_size('t');")[0][0])


def fmt_mib(n_bytes):
	# MiB, labelled MB in the results table.
	return f"{n_bytes/1024/1024:.1f}"


def make_node(undo_buffer_blocks):
	node = testgres.get_new_node(name="undo_page_modes_perf")
	node.init(["--no-locale", "--encoding=UTF8"])
	node.append_conf(
	    "postgresql.conf",
	    "shared_preload_libraries = 'orioledb'\n"
	    "log_min_messages = warning\n"
	    "shared_buffers = '512MB'\n"
	    "fsync = off\n"
	    "synchronous_commit = off\n"
	    "checkpoint_timeout = '1h'\n"
	    "max_connections = 32\n"
	    "max_worker_processes = 4\n"
	    "max_wal_senders = 0\n"
	    "autovacuum_max_workers = 1\n"
	    "max_replication_slots = 0\n"
	    "max_parallel_workers = 0\n"
	    "max_parallel_workers_per_gather = 0\n"
	    "orioledb.debug_checkpoint_timeout = 3600\n"
	    "orioledb.main_buffers = '512MB'\n"
	    f"orioledb.undo_buffers = {undo_buffer_blocks}\n")
	node.start()
	requested_mib = undo_buffer_blocks * 8 / 1024
	effective_kb = int(
	    node.execute("SELECT setting::bigint * 8 FROM pg_settings "
	                 "WHERE name = 'orioledb.undo_buffers';")[0][0])
	print(f"# undo_buffers requested={requested_mib:.1f} MiB, "
	      f"effective={effective_kb/1024:.1f} MiB",
	      flush=True)
	node.safe_psql(
	    "postgres", "CREATE EXTENSION orioledb;\n"
	    "CREATE TABLE t (id bigint NOT NULL, pad text NOT NULL, "
	    "PRIMARY KEY (id)) USING orioledb;\n")
	return node


def _open_seq_scan(node):
	"""
	Start a concurrent seq scan and leave it paused mid-stream so
	numSeqScans stays > 0 for the tree until _close_seq_scan.
	"""
	# Seed row committed on a throwaway connection so it lives outside
	# every other snapshot the benchmark holds.
	seed = node.connect()
	seed.execute("INSERT INTO t (id, pad) VALUES (0, 's');")
	seed.close()
	c = node.connect()
	c.begin()
	c.execute("SET enable_indexscan = off; SET enable_bitmapscan = off;")
	c.execute("DECLARE seq_c CURSOR FOR SELECT id FROM t;")
	c.execute("FETCH 1 FROM seq_c;")
	return c


def _close_seq_scan(c):
	try:
		c.execute("CLOSE seq_c;")
	except Exception:
		pass
	try:
		c.rollback()
	except Exception:
		pass
	c.close()


def run_one(node, mode, n_rows, padlen, order, warmup_rows):
	"""
	Returns a dict with select_s, count, {page,row,table}_delta, writer_s.
	"""
	node.safe_psql("postgres", "TRUNCATE t;")

	# Reader / holder: snapshot pinned before anything happens.
	reader = node.connect()
	reader.begin()
	reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
	reader.execute("SELECT count(*) FROM t;")

	probe = node.connect()
	seq_conn = None

	if mode in ("diff", "full"):
		# Start a concurrent seq scan (also commits the seed row).
		seq_conn = _open_seq_scan(node)
		# Warmup: insert warmup_rows while the seq scan is running.
		# These splits produce FULL images (numSeqScans > 0), leaving
		# the warmed pages with valid undoLocations > holder.retainLoc.
		wu = node.connect()
		wu.execute(
		    f"INSERT INTO t (id, pad) "
		    f"SELECT i, repeat('x', {padlen}) "
		    f"FROM generate_series(1, {warmup_rows}) i;")
		wu.close()
		if mode == "diff":
			# End the seq scan; SKIP is disabled by the snapshot's
			# retain, and DIFF is now allowed (numSeqScans == 0).
			_close_seq_scan(seq_conn)
			seq_conn = None
		# For "full", keep the seq scan running through the timed phase.

	# Capture undo / table size AFTER any warmup so the deltas reflect
	# only the timed writer's contribution.
	page_before = page_undo_used(probe)
	row_before = row_undo_used(probe)
	table_before = table_size_bytes(probe)

	# Timed writer.
	writer = node.connect()
	writer.begin()
	id_lo = warmup_rows + 1
	id_hi = warmup_rows + n_rows
	if order == "ordered":
		insert_sql = (
		    f"INSERT INTO t (id, pad) "
		    f"SELECT i, repeat('x', {padlen}) "
		    f"FROM generate_series({id_lo}, {id_hi}) i;")
	elif order == "random":
		insert_sql = (
		    f"INSERT INTO t (id, pad) "
		    f"SELECT ((i * 982451653::bigint) %% {n_rows}) + {id_lo}, "
		    f"repeat('x', {padlen}) "
		    f"FROM generate_series(0, {n_rows} - 1) i;")
	else:
		raise ValueError(f"unknown order: {order!r}")

	writer_seconds = [0.0]

	def writer_target():
		t0 = time.perf_counter()
		try:
			writer.execute(insert_sql)
		except Exception as exc:
			print(f"# writer error: {exc}", file=sys.stderr)
		writer_seconds[0] = time.perf_counter() - t0

	t_writer = Thread(target=writer_target)
	t_writer.start()
	t_writer.join()

	# Timed SELECT through the held snapshot.
	t0 = time.perf_counter()
	cnt = reader.execute("SELECT count(*) FROM t;")
	elapsed = time.perf_counter() - t0
	count = int(cnt[0][0])

	page_after = page_undo_used(probe)
	row_after = row_undo_used(probe)
	table_after = table_size_bytes(probe)

	# Cleanup.
	writer.rollback()
	writer.close()
	if seq_conn is not None:
		_close_seq_scan(seq_conn)
	reader.rollback()
	reader.close()
	probe.close()

	return {
	    "mode": mode,
	    "order": order,
	    "select_s": elapsed,
	    "count": count,
	    "page_delta": page_after - page_before,
	    "row_delta": row_after - row_before,
	    "table_delta": table_after - table_before,
	    "writer_s": writer_seconds[0],
	}


def main():
	ap = argparse.ArgumentParser(
	    description="Measure time of SELECT count(*) mid-INSERT across "
	    "page-undo modes.")
	ap.add_argument("--rows", type=int, default=300_000,
	                help="rows the timed writer inserts (default 300000)")
	ap.add_argument("--padlen", type=int, default=200,
	                help="text pad length per row in bytes (default 200)")
	ap.add_argument("--undo-buffers", default="2048,1",
	                help="comma-separated list of orioledb.undo_buffers "
	                "requests in MiB (default: 2048,1).  Each value is "
	                "run once per mode/order; sizes below orioledb's "
	                "16*max_procs floor are clamped up and the "
	                "effective size is printed on start.")
	ap.add_argument("--warmup", type=int, default=500,
	                help="warm-up rows for DIFF/FULL (default 500)")
	ap.add_argument("--modes", default="skip,diff,full",
	                help="comma-separated subset of skip,diff,full "
	                "(default: all three)")
	ap.add_argument("--orders", default="ordered,random",
	                help="comma-separated subset of ordered,random "
	                "(default: both)")
	args = ap.parse_args()

	commit = detect_commit()
	modes = [m.strip() for m in args.modes.split(",") if m.strip()]
	orders = [o.strip() for o in args.orders.split(",") if o.strip()]
	undo_mbs = [int(m.strip()) for m in args.undo_buffers.split(",")
	            if m.strip()]

	print(f"# commit = {commit}")
	print(f"# rows   = {args.rows}, pad = {args.padlen} bytes/row, "
	      f"warmup = {args.warmup} rows")
	print(f"# modes  = {modes}, orders = {orders}, "
	      f"undo_buffers = {undo_mbs} MiB")
	print()
	hdr1 = (f"{'mode':>4} {'order':>8} {'undo_buf':>9} "
	        f"{'table':>7} {'row_undo':>9} {'page_undo':>10} "
	        f"{'writer':>8} {'select':>10}")
	hdr2 = (f"{'':>4} {'':>8} {'MB':>9} "
	        f"{'MB':>7} {'MB':>9} {'MB':>10} "
	        f"{'ms':>8} {'ms':>10}")
	print(hdr1)
	print(hdr2)

	for mode in modes:
		for order in orders:
			for mb in undo_mbs:
				blocks = mb * 128
				node = make_node(blocks)
				try:
					r = run_one(node,
					            mode,
					            args.rows,
					            args.padlen,
					            order,
					            warmup_rows=args.warmup)
				finally:
					node.stop()
					node.cleanup()
				print(f"{mode:>4} {order:>8} {mb:>9d} "
				      f"{fmt_mib(r['table_delta']):>7} "
				      f"{fmt_mib(r['row_delta']):>9} "
				      f"{fmt_mib(r['page_delta']):>10} "
				      f"{int(r['writer_s']*1000):>8} "
				      f"{int(r['select_s']*1000):>10}",
				      flush=True)


if __name__ == "__main__":
	main()

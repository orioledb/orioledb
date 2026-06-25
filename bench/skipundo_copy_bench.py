#!/usr/bin/env python3
"""
Benchmark: SELECT count(*) latency against a btree that is mid-INSERT.

Two backends:

  reader  -- opens a REPEATABLE READ txn BEFORE the writer starts and
             materialises its snapshot (SELECT 1).  Later, while the
             writer is still inside its uncommitted INSERT, runs the
             timed SELECT count(*) FROM t through this same connection.
             Because its snapshot.csn is below every split csn the
             writer will produce, the SELECT either walks the page-level
             undo chain back to the pre-insert state (DIFF / FULL modes)
             or reads the live frozen pages and filters via per-tuple
             MVCC (SKIP mode).  Returns 0 -- the writer's tuples are
             all in-progress.

  writer  -- separate backend.  In a background thread, opens a txn and
             runs one large INSERT ... SELECT generate_series(...) that
             causes many leaf splits.  Stays inside the uncommitted txn
             until the SELECT has finished, then ROLLBACK.

orioledb.debug_checkpoint_timeout is set high enough that the
checkpointer cannot push undo to disk on its own.  The undo-buffer
size is the only knob that decides whether undo stays in memory or
spills.

Three modes:

  SKIP -- on page_level_diff_undo_records-add2 (the branch with the
          skip-undo gate in o_btree_insert_split).  noRetainedReader is
          true on every fresh leaf (php->undoLocation = Invalid), so
          the writer writes no page-level image at all; the SELECT
          falls through to reading the (frozen) live pages and applies
          per-tuple MVCC to skip the writer's in-progress tuples.

  DIFF -- on a pre-skip-undo branch (e.g. 1f13e053 or 2a2efd44, which
          add diff images but NOT skip-undo).  Writer writes the
          differential split image at every leaf split.

  FULL -- on main (pre-diff-undo).  Writer writes a full ORIOLEDB_BLCKSZ
          page image at every split.

Run the script on each of those three branches.  --undo-buffer-mb
controls the in-memory undo buffer; run with --undo-buffer-mb 512 for
the "big" variant and --undo-buffer-mb 4 for the "small" variant.
"""
import argparse
import os
import subprocess
import sys
import time
from threading import Thread

# Make the testgres helpers importable when invoked from the repo root.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "test"))
import testgres  # noqa: E402

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def detect_branch():
	try:
		out = subprocess.check_output(["git", "branch", "--show-current"],
		                              cwd=os.path.dirname(
		                                  os.path.dirname(os.path.abspath(__file__))),
		                              text=True).strip()
		return out
	except Exception:
		return "<unknown>"


def _undo_used(conn, kind):
	"""
	lastUsedLocation for the named undo log slot.  Subtracting two readings
	gives the exact number of bytes written between them (including bytes
	still in the in-memory buffer, unlike orioledb_undo_size() which only
	sees on-disk files).
	"""
	rows = conn.execute(
	    "SELECT undo_type, lastUsedLocation FROM orioledb_get_undo_meta();")
	for r in rows:
		if r[0] == kind:
			return int(r[1])
	return 0


def page_undo_used(conn):
	# 'page' is UndoLogRegularPageLevel.
	return _undo_used(conn, "page")


def row_undo_used(conn):
	# 'row' is UndoLogRegular (per-tuple version chain).
	return _undo_used(conn, "row")


def table_size_bytes(conn):
	return int(
	    conn.execute("SELECT pg_total_relation_size('t');")[0][0])


def classify_mode(delta_bytes, n_inserted):
	"""
	Classify the observed page-level undo growth.  At ~3-5 small-padded
	rows per 8 KiB leaf the writer trips roughly n_inserted/4 splits;
	a diff image is on the order of 100-200 bytes, a full image is
	~ORIOLEDB_BLCKSZ.
	"""
	if delta_bytes < 4 * 1024:
		return "SKIP"
	approx_splits = max(1, n_inserted // 4)
	per_split = delta_bytes / approx_splits
	if per_split < 1024:
		return "DIFF"
	return "FULL"


def fmt_bytes(n):
	for unit in ("B", "KiB", "MiB", "GiB"):
		if n < 1024 or unit == "GiB":
			return f"{n:.1f} {unit}"
		n /= 1024


# ----------------------------------------------------------------------------
# Node setup
# ----------------------------------------------------------------------------


def make_node(undo_buffer_blocks):
	"""
	Spin up a fresh PostgresNode.  max_connections is set as low as the
	bench plausibly needs (3 user backends plus background workers) so
	orioledb's `16 * max_procs` floor on undo_buffers stays out of the
	way of the small-buffer variant.  The effective undo_buffers value
	is printed when the node starts (it can be clamped above the
	requested value if max_procs is still too large).
	"""
	node = testgres.get_new_node(name="bench_skipundo")
	node.init(["--no-locale", "--encoding=UTF8"])
	node.append_conf(
	    "postgresql.conf",
	    "shared_preload_libraries = 'orioledb'\n"
	    "log_min_messages = warning\n"
	    "shared_buffers = '512MB'\n"
	    "fsync = off\n"
	    "synchronous_commit = off\n"
	    "checkpoint_timeout = '1h'\n"
	    "max_connections = 16\n"
	    "max_worker_processes = 4\n"
	    "max_wal_senders = 0\n"
	    "autovacuum_max_workers = 1\n"
	    "max_replication_slots = 0\n"
	    # Single-worker plan so SELECT timing reflects one backend's
	    # undo-walk cost; parallelism would mask the small-buffer spill.
	    "max_parallel_workers = 0\n"
	    "max_parallel_workers_per_gather = 0\n"
	    "orioledb.debug_checkpoint_timeout = 3600\n"
	    "orioledb.main_buffers = '512MB'\n"
	    f"orioledb.undo_buffers = {undo_buffer_blocks}\n")
	node.start()
	requested_mib = undo_buffer_blocks * 8 / 1024
	effective_kb = int(
	    node.execute("SELECT setting::bigint * 8 "
	                 "FROM pg_settings "
	                 "WHERE name = 'orioledb.undo_buffers';")[0][0])
	print(f"# undo_buffers requested={requested_mib:.1f} MiB, "
	      f"effective={effective_kb/1024:.1f} MiB")
	node.safe_psql(
	    "postgres", "CREATE EXTENSION orioledb;\n"
	    "CREATE TABLE t (\n"
	    "    id bigint NOT NULL,\n"
	    "    pad text NOT NULL,\n"
	    "    PRIMARY KEY (id)\n"
	    ") USING orioledb;\n")
	return node


# ----------------------------------------------------------------------------
# One measurement
# ----------------------------------------------------------------------------


def run_one(node, n_rows, padlen, order="ordered"):
	"""
	Returns (select_seconds, count, undo_delta_bytes, writer_seconds).

	The reader holds a snapshot taken BEFORE the writer touches the
	table; that is what (on diff-undo / full-undo branches) makes the
	writer's splits emit a page-level image and what makes the reader
	walk that image during its later count(*).

	Sequence:
	  1. Reader BEGIN + SELECT 1 -> snapshot pinned at csn_R.
	  2. Writer BEGIN + INSERT N rows.  Statement runs in a background
	     thread and returns only after every row is in the btree.  Every
	     split done by the INSERT has csn > csn_R, so all the page-level
	     images the writer wrote (DIFF / FULL) are above the snapshot.
	  3. The writer's INSERT completes (large amount inserted) but its
	     txn is still uncommitted -- per-tuple xact_info is in-progress.
	  4. Reader, still on the same csn_R snapshot, runs the timed
	     SELECT count(*).  In DIFF / FULL it walks the page-level undo
	     chain back to csn_R (empty table).  In SKIP the chain doesn't
	     exist; the live frozen pages are read and per-tuple MVCC skips
	     the in-progress tuples.  Result: 0 rows in every mode.
	  5. Writer ROLLBACK, reader ROLLBACK.
	"""
	node.safe_psql("postgres", "TRUNCATE t;")

	# Reader: open and materialise the snapshot first.
	reader = node.connect()
	reader.begin()
	reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
	reader.execute("SELECT count(*) FROM t;")

	# Probe channel for orioledb_undo_size measurements (separate
	# connection so it doesn't interfere with reader's snapshot).
	probe = node.connect()
	page_before = page_undo_used(probe)
	row_before = row_undo_used(probe)
	table_before = table_size_bytes(probe)

	# Writer in a background thread.
	writer = node.connect()
	writer.begin()
	# The id column is plain bigint (not bigserial) so we choose the
	# insertion order explicitly.  ORDERED inserts go to the right edge
	# of the btree on every row -- splits cascade rightward.  RANDOM
	# inserts use a modular-multiplicative permutation of [1..N] so
	# every id is unique but the insertion order is pseudo-random;
	# splits then happen across the whole btree.  The two patterns
	# stress page-level undo very differently.
	if order == "ordered":
		insert_sql = (
		    "INSERT INTO t (id, pad) "
		    f"SELECT i, repeat('x', {padlen}) "
		    f"FROM generate_series(1, {n_rows}) i;")
	elif order == "random":
		# 982451653 is a 30-bit prime coprime to any n_rows < 2**30.
		# Using it as a multiplicative step in mod-N arithmetic gives a
		# deterministic permutation of [1..N] in pseudo-random order.
		# Note: %% escapes the SQL modulo operator -- psycopg2 reads %
		# as a parameter placeholder otherwise.
		insert_sql = (
		    "INSERT INTO t (id, pad) "
		    f"SELECT ((i * 982451653::bigint) %% {n_rows}) + 1, "
		    f"repeat('x', {padlen}) "
		    f"FROM generate_series(0, {n_rows} - 1) i;")
	else:
		raise ValueError(f"unknown order: {order!r}")

	writer_seconds = [0.0]

	def writer_target():
		t_start = time.perf_counter()
		try:
			writer.execute(insert_sql)
		except Exception as exc:
			print(f"# writer thread error: {exc}", file=sys.stderr)
		writer_seconds[0] = time.perf_counter() - t_start

	t_writer = Thread(target=writer_target)
	t_writer.start()
	# Block until the INSERT has finished and every row is in the btree.
	# Writer's txn is still uncommitted at this point, which is the
	# state we want to time.
	t_writer.join()

	# All the writer's structural work is done.  Now time the SELECT
	# in the reader's pre-writer snapshot.  WHERE id > 0 keeps the
	# planner from using any aggregate fastpath -- we want a real seq
	# scan that has to visit every leaf so the page-level undo cost is
	# observable.
	t0 = time.perf_counter()
	count_rows = reader.execute("SELECT count(*) FROM t;")
	elapsed = time.perf_counter() - t0
	count = int(count_rows[0][0])

	page_after = page_undo_used(probe)
	row_after = row_undo_used(probe)
	table_after = table_size_bytes(probe)

	# Tear down
	writer.rollback()
	writer.close()
	reader.rollback()
	reader.close()
	probe.close()

	return {
	    "select_s": elapsed,
	    "count": count,
	    "page_delta": page_after - page_before,
	    "row_delta": row_after - row_before,
	    "table_delta": table_after - table_before,
	    "writer_s": writer_seconds[0],
	}


# ----------------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------------


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("--rows", type=int, default=200_000,
	                help="rows to insert in the writer (default: 200000)")
	ap.add_argument("--padlen", type=int, default=400,
	                help="text pad length per row (default: 400 bytes)")
	ap.add_argument("--big-mb", type=int, default=512,
	                help="undo_buffers size in MiB for the BIG variant")
	ap.add_argument("--small-mb", type=int, default=4,
	                help="undo_buffers size in MiB for the SMALL variant")
	ap.add_argument("--label", default="",
	                help="optional human label for the run "
	                "(default: branch name)")
	ap.add_argument("--orders", default="ordered,random",
	                help="insert orderings to test "
	                "(comma list of 'ordered' / 'random'; default: both)")
	args = ap.parse_args()

	branch = detect_branch()
	label = args.label or branch

	orders = [o.strip() for o in args.orders.split(",") if o.strip()]
	for o in orders:
		if o not in ("ordered", "random"):
			ap.error(f"unknown order {o!r}; expected 'ordered' or 'random'")

	print(f"# branch = {branch}")
	print(f"# rows   = {args.rows}, pad = {args.padlen} bytes/row, "
	      f"orders = {orders}")
	print()
	hdr = (f"{'label':>24} {'order':>8} {'buf':>5} {'count':>8} "
	       f"{'writer':>10} {'select':>10} "
	       f"{'table':>10} {'row_undo':>10} {'page_undo':>10}  {'mode'}")
	print(hdr)

	results = []
	for order in orders:
		for buf_label, mb in (("big", args.big_mb), ("small", args.small_mb)):
			blocks = mb * 128
			node = make_node(blocks)
			try:
				r = run_one(node, args.rows, args.padlen, order=order)
			finally:
				node.stop()
				node.cleanup()
			mode = classify_mode(r["page_delta"], args.rows)
			r["order"] = order
			r["buf_label"] = buf_label
			r["buf_mb"] = mb
			r["mode"] = mode
			print(f"{label[:24]:>24} {order:>8} {buf_label:>5} "
			      f"{r['count']:>8d} "
			      f"{r['writer_s']:>9.3f}s {r['select_s']:>9.3f}s "
			      f"{fmt_bytes(r['table_delta']):>10} "
			      f"{fmt_bytes(r['row_delta']):>10} "
			      f"{fmt_bytes(r['page_delta']):>10}  {mode}")
			results.append(r)

	print()
	for r in results:
		print(f"# {r['order']:>7} {r['buf_label']:>5} "
		      f"undo_buffer={r['buf_mb']}MiB: "
		      f"writer={r['writer_s']*1000:.1f}ms "
		      f"SELECT={r['select_s']*1000:.1f}ms "
		      f"table_grew={fmt_bytes(r['table_delta'])} "
		      f"row_undo={fmt_bytes(r['row_delta'])} "
		      f"page_undo={fmt_bytes(r['page_delta'])} "
		      f"mode={r['mode']}")


if __name__ == "__main__":
	main()

#!/usr/bin/env python3
"""
Compare COPY throughput across three configurations of the orioledb
multi-insert path, for three row shapes.

Only the executor's MultiInsert path (COPY FROM, INSERT ... SELECT into
partitioned tables, etc.) routes through orioledb_multi_insert; plain
INSERT ... SELECT into a non-partitioned table goes row-by-row and
never exercises o_tbl_multi_insert.  This benchmark uses COPY FROM.

  off          orioledb.debug_disable_multi_insert = 'all'
               per-row o_tbl_insert; per-row TOAST inside o_tbl_insert.
  multi        orioledb.debug_disable_multi_insert = 'toast'
               o_tbl_multi_insert Phases 1-3; Phase 4 falls back to per-row
               o_toast_insert_values.
  multi+toast  orioledb.debug_disable_multi_insert = 'none' (default)
               o_tbl_multi_insert Phases 1-3 plus Phase 4 batched TOAST insert.

Each iteration COPYs the same in-memory TSV payload once per config, and
iterations are interleaved across configs so all three modes see identical
cache warmness / steady state -- running all iterations of one mode first
biases the next mode with a fully warm node.

TOAST payloads are hex-encoded random bytes rotated per row, so pglz can't
shrink them below the inline threshold and the value actually goes into the
TOAST index (a low-entropy pattern compresses well and would never TOAST).

Exits non-zero if any multi-insert config's throughput ratio vs off falls
below --min-ratio on any shape (default 0.85 -- comfortable at --iterations
= 5 on this host).
"""
import argparse
import io
import os
import statistics
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_REPO, "test"))
import testgres  # noqa: E402

CONFIGS = [
    ("off", "all"),
    ("multi", "toast"),
    ("multi+toast", "none"),
]

# Non-compressible seeds; each row rotates the seed by (i % 10) chars so
# rows don't share content and pglz has nothing to grab onto.
_SEED_SMALL = os.urandom(1_500).hex()  # 3000 hex chars
_SEED_MED = os.urandom(6_000).hex()  # 12000 hex chars


def _rot(seed, i):
	return seed[i % 10:] + seed[:i % 10]


SHAPES = {
    # No TOAST at all -- exercises only Phase 1-3 primary batching.
    "short": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "c varchar(40) NOT NULL",
        "row": lambda i: f"{i}\t{i}\t{'a' * 40}\n",
    },
    # ~3 KB non-compressible -> 1-2 TOAST chunks per row.
    "toast_small": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "big text NOT NULL",
        "row": lambda i: f"{i}\t{i}\t{_rot(_SEED_SMALL, i)}\n",
    },
    # ~12 KB non-compressible -> ~6 TOAST chunks per row.
    "toast": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "big text NOT NULL",
        "row": lambda i: f"{i}\t{i}\t{_rot(_SEED_MED, i)}\n",
    },
}


def detect_commit():
	try:
		return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
		                               cwd=_REPO,
		                               text=True).strip()
	except Exception:
		return "<unknown>"


def make_node():
	node = testgres.get_new_node(name="multi_insert_perf")
	node.init(["--no-locale", "--encoding=UTF8"])
	node.append_conf(
	    "postgresql.conf", "shared_preload_libraries = 'orioledb'\n"
	    "shared_buffers = '1GB'\n"
	    "orioledb.main_buffers = '1GB'\n"
	    "fsync = off\n"
	    "synchronous_commit = off\n"
	    "checkpoint_timeout = '30min'\n"
	    "max_wal_size = '4GB'\n")
	node.start()
	node.safe_psql("postgres", "CREATE EXTENSION orioledb;")
	return node


def build_payload(shape, n_rows):
	row = SHAPES[shape]["row"]
	buf = io.StringIO()
	for i in range(1, n_rows + 1):
		buf.write(row(i))
	return buf.getvalue()


def run_one(node, shape, mode, payload):
	spec = SHAPES[shape]
	conn = node.connect()
	try:
		c = conn.cursor
		c.execute("DROP TABLE IF EXISTS t")
		c.execute("CREATE TABLE t (%s) USING orioledb" % spec["columns"])
		c.execute("SET orioledb.debug_disable_multi_insert = '%s'" % mode)
		t0 = time.perf_counter()
		c.copy_expert("COPY t FROM STDIN", io.StringIO(payload))
		conn.connection.commit()
		elapsed = time.perf_counter() - t0
	finally:
		conn.close()
	return elapsed


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("--rows",
	                type=int,
	                default=30_000,
	                help="rows per iteration (default: 30000, sized so a full "
	                "perfcheck run of all shapes completes in ~2-3 min)")
	ap.add_argument("--iterations",
	                type=int,
	                default=5,
	                help="repetitions per (config, shape) cell "
	                "(default: 5, median reported; iterations interleaved "
	                "across configs to keep cache warmness identical)")
	ap.add_argument("--shapes",
	                default="short,toast_small,toast",
	                help="comma-separated subset of: short,toast_small,toast "
	                "(default: all)")
	ap.add_argument("--min-ratio",
	                type=float,
	                default=0.85,
	                help="regression floor: every multi-insert config's "
	                "throughput ratio vs off must be >= this; "
	                "otherwise exit non-zero (default: 0.85)")
	args = ap.parse_args()

	shapes = [s.strip() for s in args.shapes.split(",") if s.strip()]
	for s in shapes:
		if s not in SHAPES:
			sys.exit("Unknown shape: %s (choices: %s)" % (s, ",".join(SHAPES)))

	print(f"# commit     = {detect_commit()}")
	print(f"# rows       = {args.rows}")
	print(f"# iterations = {args.iterations} (interleaved)")
	print()
	baseline = CONFIGS[0][0]  # "off"
	print(f"{'shape':<12} {'config':<12} "
	      f"{'median_s':>10} {'stdev_s':>10} {'rows/s':>12} "
	      f"{'vs_' + baseline:>10}")

	regressions = []
	node = make_node()
	try:
		for shape in shapes:
			payload = build_payload(shape, args.rows)
			# Warmup one iteration per config so the first measurement round
			# already runs against a warm node.
			for _, mode in CONFIGS:
				run_one(node, shape, mode, payload)
			# Interleaved measurements: rotate through configs per iteration
			# so all modes see the same steady state.
			runs = {name: [] for name, _ in CONFIGS}
			for _ in range(args.iterations):
				for name, mode in CONFIGS:
					runs[name].append(run_one(node, shape, mode, payload))
			baseline_median = statistics.median(runs[baseline])
			for name, _ in CONFIGS:
				vs = runs[name]
				med = statistics.median(vs)
				std = statistics.stdev(vs) if len(vs) > 1 else 0.0
				rps = args.rows / med
				if name == baseline:
					speedup = ""
				else:
					ratio = baseline_median / med
					speedup = f"{ratio:>9.2f}x"
					if ratio < args.min_ratio:
						regressions.append((shape, name, ratio))
				print(
				    f"{shape:<12} {name:<12} "
				    f"{med:>10.3f} {std:>10.3f} {rps:>12.0f} "
				    f"{speedup:>10}",
				    flush=True)
			print()
	finally:
		node.stop()
		node.cleanup()

	if regressions:
		print(f"REGRESSION: optimization must not slow inserts below "
		      f"{args.min_ratio:.2f}x {baseline}:")
		for shape, name, ratio in regressions:
			print(f"  {shape:<12} {name:<12} ratio={ratio:.2f}x")
		sys.exit(1)


if __name__ == "__main__":
	main()

#!/usr/bin/env python3
"""
Compare COPY throughput across all eight configurations of the orioledb
multi-insert GUCs, for five row shapes.

Three independent boolean GUCs control the batching:
  debug_disable_multi_insert          -- PK batching
  debug_disable_toast_multi_insert    -- TOAST batching
  debug_disable_sk_multi_insert       -- secondary-index batching

Only the executor's MultiInsert path (COPY FROM, INSERT ... SELECT into
partitioned tables, etc.) routes through orioledb_multi_insert; plain
INSERT ... SELECT into a non-partitioned table goes row-by-row and
never exercises o_tbl_multi_insert.  This benchmark uses COPY FROM.

Each iteration COPYs the same in-memory TSV payload once per config.
By default iterations are interleaved across configs so all modes see
identical cache warmness; --random fully shuffles the schedule instead.

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
import random
import statistics
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_REPO, "test"))
import testgres  # noqa: E402

CONFIGS = [
    ("off", True, True, True),
    ("pk", False, True, True),
    ("toast", True, False, True),
    ("sk", True, True, False),
    ("pk+toast", False, False, True),
    ("pk+sk", False, True, False),
    ("toast+sk", True, False, False),
    ("all", False, False, False),
]

# Non-compressible seeds; each row rotates the seed by (i % 10) chars so
# rows don't share content and pglz has nothing to grab onto.
_SEED_TINY = os.urandom(150).hex()  # 300 hex chars
_SEED_SMALL = os.urandom(1_400).hex()  # 2800 hex chars, ~1 TOAST chunk
_SEED_MED = os.urandom(3_000).hex()  # 6000 hex chars, ~3 TOAST chunks


def _rot(seed, i):
	return seed[i % 10:] + seed[:i % 10]


SHAPES = {
    "short": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "c varchar(40) NOT NULL",
        "rows": 1_000_000,
        "row": lambda i, k: f"{i}\t{k}\t{'a' * 40}\n",
    },
    "short_sk": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "c varchar(40) NOT NULL",
        "rows": 1_000_000,
        "indexes": ["CREATE INDEX ON t (k)", "CREATE INDEX ON t (c)"],
        "row": lambda i, k: f"{i}\t{k}\t{'a' * 40}\n",
    },
    "toast_small": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "big text NOT NULL",
        "rows": 30_000,
        "row": lambda i, k: f"{i}\t{k}\t{_rot(_SEED_SMALL, i)}\n",
    },
    "toast": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "big text NOT NULL",
        "rows": 10_000,
        "row": lambda i, k: f"{i}\t{k}\t{_rot(_SEED_MED, i)}\n",
    },
    "toast_sk": {
        "columns": "id int PRIMARY KEY, k int NOT NULL, "
        "big text NOT NULL",
        "rows": 10_000,
        "indexes": ["CREATE INDEX ON t (k)"],
        "row": lambda i, k: f"{i}\t{k}\t{_rot(_SEED_MED, i)}\n",
    },
    "toast_20col": {
        "columns":
        "id int PRIMARY KEY, " + ", ".join(f"c{i} text NOT NULL"
                                           for i in range(20)),
        "rows":
        10_000,
        "row":
        lambda i, k: (f"{i}\t" + "\t".join(
            _rot(_SEED_TINY, (i + j) % 10) for j in range(20)) + "\n"),
    },
    "toast_sk_20col": {
        "columns":
        "id int PRIMARY KEY, k int NOT NULL, " +
        ", ".join(f"c{i} text NOT NULL" for i in range(19)),
        "rows":
        10_000,
        "indexes": ["CREATE INDEX ON t (k)"],
        "row":
        lambda i, k: (f"{i}\t{k}\t" + "\t".join(
            _rot(_SEED_TINY, (i + j) % 10) for j in range(19)) + "\n"),
    },
}


def detect_commit():
	try:
		return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
		                               cwd=_REPO,
		                               text=True).strip()
	except Exception:
		return "<unknown>"


def make_node(main_buffers="1GB"):
	node = testgres.get_new_node(name="multi_insert_perf")
	node.init(["--no-locale", "--encoding=UTF8"])
	node.append_conf(
	    "postgresql.conf", "shared_preload_libraries = 'orioledb'\n"
	    "shared_buffers = '1GB'\n"
	    f"orioledb.main_buffers = '{main_buffers}'\n"
	    "fsync = off\n"
	    "synchronous_commit = off\n"
	    "checkpoint_timeout = '30min'\n"
	    "max_wal_size = '4GB'\n")
	node.start()
	node.safe_psql("postgres", "CREATE EXTENSION orioledb;")
	return node


def build_payload(shape, n_rows, shuffle_sk=False):
	row = SHAPES[shape]["row"]
	if shuffle_sk:
		ks = list(range(1, n_rows + 1))
		random.shuffle(ks)
	buf = io.StringIO()
	for i in range(1, n_rows + 1):
		k = ks[i - 1] if shuffle_sk else i
		buf.write(row(i, k))
	return buf.getvalue()


def run_one(node, shape, disable_pk, disable_toast, disable_sk, payload):
	spec = SHAPES[shape]
	conn = node.connect()
	try:
		c = conn.cursor
		c.execute("DROP TABLE IF EXISTS t")
		c.execute("CREATE TABLE t (%s) USING orioledb" % spec["columns"])
		for idx in spec.get("indexes", []):
			c.execute(idx)
		c.execute("SET orioledb.debug_disable_multi_insert = %s",
		          (disable_pk, ))
		c.execute("SET orioledb.debug_disable_toast_multi_insert = %s",
		          (disable_toast, ))
		c.execute("SET orioledb.debug_disable_sk_multi_insert = %s",
		          (disable_sk, ))
		t0 = time.perf_counter()
		c.copy_expert("COPY t FROM STDIN", io.StringIO(payload))
		conn.connection.commit()
		elapsed = time.perf_counter() - t0
	finally:
		conn.close()
	return elapsed


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument(
	    "--rows",
	    type=int,
	    default=0,
	    help="override per-shape row count (0 = use shape default)")
	ap.add_argument("--iterations",
	                type=int,
	                default=5,
	                help="repetitions per (config, shape) cell "
	                "(default: 5, median reported)")
	ap.add_argument("--random",
	                action="store_true",
	                help="fully randomize the measurement order instead "
	                "of interleaving by config")
	ap.add_argument("--shapes",
	                default="short,short_sk,toast_small,toast,toast_sk,"
	                "toast_20col,toast_sk_20col",
	                help="comma-separated subset of shapes (default: all)")
	ap.add_argument("--main-buffers",
	                default="1GB",
	                help="orioledb.main_buffers setting (default: 1GB)")
	ap.add_argument("--shuffle-sk",
	                action="store_true",
	                help="randomize the SK column (k) values so "
	                "secondary-index inserts are unsorted")
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

	order = "random" if args.random else "interleaved"
	print(f"# commit       = {detect_commit()}")
	print(f"# iterations   = {args.iterations} ({order})")
	print(f"# shuffle_sk   = {args.shuffle_sk}")
	print(f"# main_buffers = {args.main_buffers}")
	print()
	baseline = CONFIGS[0][0]  # "off"
	print(f"{'shape':<12} {'rows':>8} {'config':<12} "
	      f"{'median_s':>10} {'stdev_s':>10} {'rows/s':>12} "
	      f"{'vs_' + baseline:>10}")

	regressions = []
	node = make_node(args.main_buffers)
	try:
		for shape in shapes:
			n_rows = args.rows if args.rows else SHAPES[shape]["rows"]
			payload = build_payload(shape, n_rows, args.shuffle_sk)
			# Warmup one iteration per config so the first measurement round
			# already runs against a warm node.
			for _, dpk, dt, dsk in CONFIGS:
				run_one(node, shape, dpk, dt, dsk, payload)
			runs = {name: [] for name, _, _, _ in CONFIGS}
			if args.random:
				schedule = [(name, dpk, dt, dsk)
				            for _ in range(args.iterations)
				            for name, dpk, dt, dsk in CONFIGS]
				random.shuffle(schedule)
				for name, dpk, dt, dsk in schedule:
					runs[name].append(
					    run_one(node, shape, dpk, dt, dsk, payload))
			else:
				for _ in range(args.iterations):
					for name, dpk, dt, dsk in CONFIGS:
						runs[name].append(
						    run_one(node, shape, dpk, dt, dsk, payload))
			baseline_median = statistics.median(runs[baseline])
			for name, _, _, _ in CONFIGS:
				vs = runs[name]
				med = statistics.median(vs)
				std = statistics.stdev(vs) if len(vs) > 1 else 0.0
				rps = n_rows / med
				if name == baseline:
					speedup = ""
				else:
					ratio = baseline_median / med
					speedup = f"{ratio:>9.2f}x"
					if ratio < args.min_ratio:
						regressions.append((shape, name, ratio))
				print(
				    f"{shape:<12} {n_rows:>8} {name:<12} "
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

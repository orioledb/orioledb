#!/usr/bin/env python3
"""
Stand-in for the jepsen "append" workload's *internal* checks (ORI-229).

Jepsen's append workload treats the database as a map of key -> list of
integers and the only mutation is appending a globally unique integer to a
key's list.  The anomalies reported in ORI-229 are all detectable from within a
single transaction, without any cycle detection:

  read-own-writes  -- append v to k, then a later read of k in the same
                      transaction does not end with v (or returns nothing);
  non-repeatable   -- two reads of the same untouched k in one transaction
                      disagree (REPEATABLE READ forbids this);
  too-many         -- a lookup for one key returns more than one row.

This mirrors the real workload's SQL closely, because the shape of the
statements is what decides which OrioleDB code paths run:

  * predicates are drawn 50/50 from the primary key (`id`, index scan) and the
    unindexed secondary column (`sk`, sequential scan) -- jepsen's default
    --key-types is primary,secondary;
  * appends use either INSERT ... ON CONFLICT DO UPDATE or the
    update / insert / update fallback chain, 50/50 -- jepsen's default
    --upsert-types minus copy-on-write, which needs indirection tables;
  * the fallback chain wraps its INSERT in a SAVEPOINT, so subtransactions are
    exercised -- jepsen defaults --savepoints to true;
  * keys are drawn from a small rotating pool (--key-count 10) and retired
    after --max-writes-per-key appends, which is what keeps the rows narrow
    and the contention on one or two leaf pages.

Antithesis runs it with --concurrency 50 --rate 500 --max-writes-per-key 4.

Usage:
  rr_append_hunt.py --port 5432 --setup [--workers 50] [--seconds 300]
                    [--isolation repeatable-read|read-committed]
"""

import argparse
import os
import random
import sys
import threading
import time

import psycopg2
import psycopg2.extensions

# Jepsen's default table count; a key lives in table (hash(k) mod 6).
NTABLES = 6

stop = threading.Event()
anomalies = []
anomalies_lock = threading.Lock()
counters = {'txn': 0, 'ok': 0, 'conflict': 0, 'uiu_failed': 0, 'err': 0}
counters_lock = threading.Lock()


def report(kind, detail):
	with anomalies_lock:
		anomalies.append((kind, detail))
	print('ANOMALY %s: %s' % (kind, detail), flush=True)


def bump(name, n=1):
	with counters_lock:
		counters[name] += n


def table_of(key):
	return 'txn%d' % (key % NTABLES, )


class KeyPool:
	"""
	Jepsen keeps a pool of active keys and retires a key once it has taken
	max_writes appends, replacing it with a fresh one.  With the default
	--max-writes-per-key 4 that means the workload is mostly *fresh inserts*,
	not updates of long-lived rows.
	"""

	def __init__(self, size, max_writes):
		self.lock = threading.Lock()
		self.max_writes = max_writes
		self.next_key = size
		self.active = {k: 0 for k in range(size)}

	def pick(self, rnd, write):
		with self.lock:
			keys = sorted(self.active)
			# jepsen's default --key-dist is exponential: earlier (older) keys
			# in the pool are picked far more often than fresh ones.
			i = min(int(rnd.expovariate(1.0)), len(keys) - 1)
			k = keys[i]
			if write:
				self.active[k] += 1
				if self.active[k] >= self.max_writes:
					del self.active[k]
					self.active[self.next_key] = 0
					self.next_key += 1
			return k


def setup(dsn):
	con = psycopg2.connect(dsn)
	con.autocommit = True
	cur = con.cursor()
	cur.execute("CREATE EXTENSION IF NOT EXISTS orioledb;")
	for i in range(NTABLES):
		cur.execute("DROP TABLE IF EXISTS txn%d;" % (i, ))
		cur.execute("""
			CREATE TABLE txn%d (
				id int NOT NULL PRIMARY KEY,
				sk int NOT NULL,
				val text
			) USING orioledb;
		""" % (i, ))
	con.close()


def parse_list(val):
	if val is None:
		return None
	return [int(x) for x in val.split(',')] if val != '' else []


class Aborted(Exception):
	"""A legitimate serialization failure / deadlock; retry the transaction."""


class Anomaly(Exception):
	"""An invariant violation; stop the run."""


class Txn:
	"""One jepsen transaction, plus the bookkeeping its checks need."""

	def __init__(self, cur, rnd, isolation, mop_delay):
		self.cur = cur
		self.rnd = rnd
		self.isolation = isolation
		self.mop_delay = mop_delay
		self.seen = {}
		self.appended = {}

	def mop_sleep(self):
		"""
		jepsen sleeps a zipfian 0..--mop-delay ms between micro-operations
		specifically to overlap transaction steps with each other.  Mostly
		sub-millisecond, occasionally the full delay.
		"""
		if self.mop_delay <= 0:
			return
		ms = self.mop_delay ** self.rnd.random() - 1.0
		if ms > 0:
			time.sleep(ms / 1000.0)

	def col(self):
		return 'id' if self.rnd.random() < 0.5 else 'sk'

	def read(self, k):
		tbl = table_of(k)
		self.cur.execute("SELECT (val) FROM %s WHERE %s = %%s;" %
		                 (tbl, self.col()), (k, ))
		rows = self.cur.fetchall()
		if len(rows) > 1:
			report('too-many', 'key=%d rows=%r' % (k, rows))
			raise Anomaly()
		got = parse_list(rows[0][0]) if rows else None

		# read-own-writes: what this transaction appended to k must be present,
		# in order, at the end of the list.
		tail = self.appended.get(k)
		if tail is not None and (got is None or got[-len(tail):] != tail):
			report('read-own-writes',
			       'key=%d appended=%r read=%r' % (k, tail, got))
			raise Anomaly()

		# Under REPEATABLE READ an unmodified key must read the same twice.
		if (self.isolation == 'repeatable-read' and k in self.seen
		        and self.seen[k] != got):
			report('non-repeatable',
			       'key=%d first=%r then=%r' % (k, self.seen[k], got))
			raise Anomaly()
		self.seen[k] = got
		return got

	def note_append(self, k, e):
		self.appended.setdefault(k, []).append(e)
		if k in self.seen:
			self.seen[k] = (self.seen[k] or []) + [e]

	def append_on_conflict(self, k, e):
		tbl = table_of(k)
		self.cur.execute(
		    "INSERT INTO %s AS t (id, sk, val) VALUES (%%s, %%s, %%s) "
		    "ON CONFLICT (id) DO UPDATE SET val = CONCAT(t.val, ',', %%s) "
		    "WHERE t.%s = %%s;" % (tbl, self.col()),
		    (k, k, str(e), str(e), k))

	def update(self, k, e):
		tbl = table_of(k)
		self.cur.execute(
		    "UPDATE %s SET val = CONCAT(val, ',', %%s) WHERE %s = %%s;" %
		    (tbl, self.col()), (str(e), k))
		return self.cur.rowcount > 0

	def insert_with_savepoint(self, k, e):
		tbl = table_of(k)
		self.cur.execute("SAVEPOINT upsert;")
		try:
			self.cur.execute(
			    "INSERT INTO %s (id, sk, val) VALUES (%%s, %%s, %%s);" %
			    (tbl, ), (k, k, str(e)))
		except psycopg2.errors.UniqueViolation:
			self.cur.execute("ROLLBACK TO SAVEPOINT upsert;")
			return False
		self.cur.execute("RELEASE SAVEPOINT upsert;")
		return True

	def append(self, k, e):
		if self.rnd.random() < 0.5:
			self.append_on_conflict(k, e)
		else:
			# update / insert / update, exactly as jepsen falls back.
			if not self.update(k, e):
				if not self.insert_with_savepoint(k, e):
					if not self.update(k, e):
						bump('uiu_failed')
						raise Aborted()
		self.note_append(k, e)


def worker(idx, dsn, pool, isolation, max_ops, rate, mop_delay, seed,
           conn_isolation=None):
	rnd = random.Random(seed)
	level = (psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
	         if (conn_isolation or isolation) == 'repeatable-read' else
	         psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
	con = psycopg2.connect(dsn)
	con.set_isolation_level(level)
	cur = con.cursor()

	# Elements are globally unique, as in jepsen -- that is what makes a wrong
	# read identifiable.
	next_elem = idx * 100000000 + 1
	delay = 1.0 / rate if rate > 0 else 0.0

	while not stop.is_set():
		ops = []
		for _ in range(rnd.randint(1, max_ops)):
			write = rnd.random() < 0.5
			k = pool.pick(rnd, write)
			if write:
				ops.append(('append', k, next_elem))
				next_elem += 1
			else:
				ops.append(('r', k, None))

		txn = Txn(cur, rnd, isolation, mop_delay)
		bump('txn')
		try:
			for (f, k, v) in ops:
				txn.mop_sleep()
				if f == 'append':
					txn.append(k, v)
				else:
					txn.read(k)
			con.commit()
			bump('ok')
		except Anomaly:
			print('  ops were: %r' % (ops, ), flush=True)
			stop.set()
			break
		except Aborted:
			try:
				con.rollback()
			except psycopg2.Error:
				pass
		except psycopg2.Error as e:
			if e.pgcode in ('40001', '40P01', '23505'):
				bump('conflict')
			else:
				bump('err')
				print('SQL ERROR %s: %s' % (e.pgcode, e), flush=True)
			try:
				con.rollback()
			except psycopg2.Error:
				con = psycopg2.connect(dsn)
				con.set_isolation_level(level)
				cur = con.cursor()
		if delay:
			time.sleep(delay * rnd.random() * 2)
	try:
		con.close()
	except psycopg2.Error:
		pass


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument('--port', type=int, default=5432)
	ap.add_argument('--host', default='127.0.0.1')
	ap.add_argument('--dbname', default='postgres')
	ap.add_argument('--user', default=os.environ.get('USER', 'postgres'))
	ap.add_argument('--workers', type=int, default=50)
	ap.add_argument('--keys', type=int, default=10)
	ap.add_argument('--max-writes-per-key', type=int, default=4)
	ap.add_argument('--seconds', type=float, default=300.0)
	ap.add_argument('--max-ops', type=int, default=4)
	ap.add_argument('--rate',
	                type=float,
	                default=0.0,
	                help='per-worker statements/s; 0 means as fast as possible')
	ap.add_argument('--mop-delay',
	                type=float,
	                default=100.0,
	                help='max zipfian delay between micro-ops, ms '
	                '(jepsen default 100)')
	ap.add_argument('--isolation',
	                default='repeatable-read',
	                choices=['repeatable-read', 'read-committed'])
	ap.add_argument('--conn-isolation',
	                default=None,
	                choices=['repeatable-read', 'read-committed'],
	                help='isolation to actually SET on the connection; '
	                'differs from --isolation only for self-testing the '
	                'checker (RC connection + RR checks must report '
	                'non-repeatable reads)')
	ap.add_argument('--setup', action='store_true')
	ap.add_argument('--seed', type=int, default=1)
	args = ap.parse_args()

	dsn = "host=%s port=%d dbname=%s user=%s" % (args.host, args.port,
	                                             args.dbname, args.user)
	if args.setup:
		setup(dsn)

	pool = KeyPool(args.keys, args.max_writes_per_key)
	threads = []
	for i in range(args.workers):
		t = threading.Thread(target=worker,
		                     args=(i, dsn, pool, args.isolation, args.max_ops,
		                           args.rate, args.mop_delay,
		                           args.seed * 1000 + i, args.conn_isolation))
		t.daemon = True
		t.start()
		threads.append(t)

	deadline = time.time() + args.seconds
	while time.time() < deadline and not stop.is_set():
		time.sleep(0.25)
	stop.set()
	for t in threads:
		t.join(timeout=30)

	print('txn=%d ok=%d conflict=%d uiu_failed=%d err=%d anomalies=%d' %
	      (counters['txn'], counters['ok'], counters['conflict'],
	       counters['uiu_failed'], counters['err'], len(anomalies)),
	      flush=True)
	return 1 if anomalies else 0


if __name__ == '__main__':
	sys.exit(main())

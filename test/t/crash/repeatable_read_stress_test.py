#!/usr/bin/env python3
# coding: utf-8

# REPEATABLE READ correctness stress test
#
# Model: N accounts with a fixed total balance and unique tokens.
# Writer transactions transfer money between random accounts and
# swap tokens through each writer's private token so the set of
# tokens on the table stays unique at commit. Readers verify the
# balance sum and token uniqueness invariants under REPEATABLE
# READ, the strongest isolation level OrioleDB currently supports
# (see o_check_isolation_level in include/transam/oxid.h).
#
# Reference values used by invariants (expected total, account
# count) are Python constants computed once at setup; readers
# never recompute them from live state. Writer/reader bodies are
# inline SQL over per-thread RR connections, matching the
# threading pattern used by other tests in test/t/ (see
# checkpoint_concurrent_test.py).

import random
import threading
import time

from ..base_test import BaseTest

SETUP_SQL = """
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_bank_account (
		id int NOT NULL,
		balance bigint NOT NULL,
		token bigint NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT o_bank_account_token_uniq UNIQUE (token)
			DEFERRABLE INITIALLY DEFERRED
	) USING orioledb;
"""


class RepeatableReadStressTest(BaseTest):

	def test_bank_account_invariant(self):
		n_accounts = 100
		initial_balance = 1000
		n_writers = 8
		n_readers_pk = 3
		n_readers_sk = 3
		n_readers_mixed = 3
		duration = 5.0
		checkpoint_interval = 0.5

		# Pre-computed references, frozen for the whole run.
		expected_total = n_accounts * initial_balance

		node = self.node
		node.start()
		node.safe_psql(SETUP_SQL)
		node.safe_psql(f"""
			INSERT INTO o_bank_account(id, balance, token)
				SELECT i, {initial_balance}, i
				FROM generate_series(1, {n_accounts}) AS i;
		""")

		stop = threading.Event()
		errors = []
		errors_lock = threading.Lock()
		counters_lock = threading.Lock()
		write_count = [0]
		read_count = [0]
		conflict_count = [0]

		def record_error(msg):
			with errors_lock:
				errors.append(msg)

		def make_rr_conn():
			con = node.connect()
			con.execute(
			    "SET default_transaction_isolation = 'repeatable read'")
			con.commit()
			return con

		def writer_loop(writer_id):
			con = make_rr_conn()
			# Each writer's private token starts outside {1..N}, so
			# the temporary state (after the first UPDATE) still has
			# unique tokens even if the constraint were IMMEDIATE.
			my_token = n_accounts + writer_id
			local_w = 0
			local_c = 0
			try:
				while not stop.is_set():
					v_from = random.randint(1, n_accounts)
					v_to = random.randint(1, n_accounts)
					if v_from == v_to:
						continue
					amount = random.randint(1, 10)
					try:
						con.begin()
						from_bal, from_token = con.execute(
						    "SELECT balance, token "
						    "FROM o_bank_account "
						    f"WHERE id = {v_from}")[0]
						to_bal, to_token = con.execute(
						    "SELECT balance, token "
						    "FROM o_bank_account "
						    f"WHERE id = {v_to}")[0]
						con.execute(
						    "UPDATE o_bank_account "
						    f"SET balance = {from_bal - amount}, "
						    f"    token = {my_token} "
						    f"WHERE id = {v_from}")
						con.execute(
						    "UPDATE o_bank_account "
						    f"SET balance = {to_bal + amount}, "
						    f"    token = {from_token} "
						    f"WHERE id = {v_to}")
						con.commit()
						# Only advance my_token after a successful
						# commit; on failure the old value is still
						# outside the table's token set.
						my_token = to_token
						local_w += 1
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
						local_c += 1
			finally:
				try:
					con.close()
				except Exception:
					pass
				with counters_lock:
					write_count[0] += local_w
					conflict_count[0] += local_c

		def reader_pk_loop():
			con = make_rr_conn()
			local_r = 0
			try:
				while not stop.is_set():
					try:
						con.begin()
						total, uniq, rows = con.execute(
						    "SELECT sum(balance)::bigint, "
						    "       count(DISTINCT token)::int, "
						    "       count(*)::int "
						    "FROM o_bank_account")[0]
						con.commit()
						if total != expected_total:
							record_error(
							    f'PK: total {total} != {expected_total}')
						if uniq != n_accounts:
							record_error(
							    f'PK: unique tokens {uniq} != {n_accounts}')
						if rows != n_accounts:
							record_error(
							    f'PK: rows {rows} != {n_accounts}')
						local_r += 1
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
			finally:
				try:
					con.close()
				except Exception:
					pass
				with counters_lock:
					read_count[0] += local_r

		def reader_sk_loop():
			con = make_rr_conn()
			local_r = 0
			try:
				while not stop.is_set():
					try:
						con.begin()
						# Scan ordered by token (secondary index)
						# then a full PK scan inside the same RR
						# snapshot: both must see the same per-row
						# balances and the same totals.
						sk_rows = con.execute(
						    "SELECT id, balance, token "
						    "FROM o_bank_account "
						    "ORDER BY token")
						pk_rows = con.execute(
						    "SELECT id, balance "
						    "FROM o_bank_account "
						    "ORDER BY id")
						con.commit()
						pk_bal = {r[0]: r[1] for r in pk_rows}
						sk_total = 0
						tokens = set()
						for rid, bal, tok in sk_rows:
							sk_total += bal
							tokens.add(tok)
							if pk_bal.get(rid) != bal:
								record_error(
								    f'SK xref: id={rid} sk={bal} '
								    f'pk={pk_bal.get(rid)}')
						pk_total = sum(pk_bal.values())
						if sk_total != expected_total:
							record_error(
							    f'SK: total {sk_total} != {expected_total}')
						if pk_total != expected_total:
							record_error(
							    f'SK xref: pk total {pk_total} != '
							    f'{expected_total}')
						if len(sk_rows) != n_accounts:
							record_error(
							    f'SK: rows {len(sk_rows)} != {n_accounts}')
						if len(tokens) != n_accounts:
							record_error(
							    f'SK: unique tokens {len(tokens)} != '
							    f'{n_accounts}')
						local_r += 1
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
			finally:
				try:
					con.close()
				except Exception:
					pass
				with counters_lock:
					read_count[0] += local_r

		def reader_mixed_loop():
			con = make_rr_conn()
			local_r = 0
			try:
				while not stop.is_set():
					start = random.randint(1, n_accounts)
					try:
						con.begin()
						ge_sum, ge_cnt = con.execute(
						    "SELECT coalesce(sum(balance), 0)::bigint, "
						    "       count(*)::int "
						    "FROM o_bank_account "
						    f"WHERE id >= {start}")[0]
						lt_sum, lt_cnt = con.execute(
						    "SELECT coalesce(sum(balance), 0)::bigint, "
						    "       count(*)::int "
						    "FROM o_bank_account "
						    f"WHERE id < {start}")[0]
						v_min, v_max = con.execute(
						    "SELECT min(id), max(id) "
						    "FROM o_bank_account")[0]
						con.commit()
						if ge_sum + lt_sum != expected_total:
							record_error(
							    f'mixed: GE+LT {ge_sum + lt_sum} != '
							    f'{expected_total}')
						if ge_cnt + lt_cnt != n_accounts:
							record_error(
							    f'mixed: GE+LT count '
							    f'{ge_cnt + lt_cnt} != {n_accounts}')
						if v_min is None or v_max is None:
							record_error('mixed: min/max NULL')
						local_r += 1
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
			finally:
				try:
					con.close()
				except Exception:
					pass
				with counters_lock:
					read_count[0] += local_r

		def checkpointer_loop():
			con = node.connect()
			try:
				while not stop.is_set():
					try:
						con.execute("CHECKPOINT")
						con.commit()
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
					if stop.wait(checkpoint_interval):
						break
			finally:
				try:
					con.close()
				except Exception:
					pass

		threads = []
		for wid in range(1, n_writers + 1):
			threads.append(
			    threading.Thread(target=writer_loop, args=(wid,)))
		for _ in range(n_readers_pk):
			threads.append(threading.Thread(target=reader_pk_loop))
		for _ in range(n_readers_sk):
			threads.append(threading.Thread(target=reader_sk_loop))
		for _ in range(n_readers_mixed):
			threads.append(threading.Thread(target=reader_mixed_loop))
		threads.append(threading.Thread(target=checkpointer_loop))

		for t in threads:
			t.start()

		time.sleep(duration)
		stop.set()
		for t in threads:
			t.join()

		final_total = node.execute(
		    "SELECT sum(balance)::bigint FROM o_bank_account")[0][0]
		self.assertEqual(
		    final_total, expected_total,
		    f'final total {final_total} != expected {expected_total} '
		    f'(mismatch = lost update)')

		unique_tokens = node.execute(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account")[0][0]
		self.assertEqual(
		    unique_tokens, n_accounts,
		    f'expected {n_accounts} unique tokens, got {unique_tokens}')

		rows = node.execute(
		    "SELECT count(*)::int FROM o_bank_account")[0][0]
		self.assertEqual(
		    rows, n_accounts,
		    f'expected {n_accounts} rows, got {rows}')

		with errors_lock:
			seen = list(errors)
		self.assertEqual(
		    seen[:20], [],
		    f'snapshot reads saw inconsistency (torn scan): {seen[:20]}')

		self.assertGreater(write_count[0], 0, 'writers must have committed')
		self.assertGreater(read_count[0], 0, 'readers must have completed')

		retained = node.execute(
		    "SELECT orioledb_has_retained_undo()")[0][0]
		self.assertFalse(
		    retained,
		    'orioledb retained undo after stop (possible snapshot leak)')

		self.assertTrue(
		    node.execute(
		        "SELECT orioledb_tbl_check('o_bank_account'::regclass)")
		    [0][0], 'orioledb_tbl_check(o_bank_account) failed')

		node.stop()

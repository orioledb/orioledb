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

from testgres.enums import IsolationLevel

from ..base_test import BaseTest

SETUP_SQL = """
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE EXTENSION IF NOT EXISTS injection_points;

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
		n_writers = 20
		n_readers_pk = 6
		n_readers_sk = 6
		n_readers_mixed = 6
		duration = 10.0
		checkpoint_interval = 0.5
		ddl_nemesis_interval = 0.2
		# wal_chaos: briefly poison WAL writes, then clear. The
		# "on" window is implicitly the SQL round-trip between
		# enable/disable (~hundreds of μs), matching Tarantool's
		# fiber.yield()-tick injection pattern. Idle gap controls
		# the chaos frequency.
		wal_chaos_idle = 0.1

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
		print_lock = threading.Lock()
		write_count = [0]
		read_count = [0]
		conflict_count = [0]

		def record_error(msg):
			with errors_lock:
				errors.append(msg)

		def dprint(msg):
			with print_lock:
				print(msg, flush=True)

		def writer_loop(writer_id):
			con = node.connect()
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
						con.begin(IsolationLevel.RepeatableRead)
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
				dprint(
				    f'[writer #{writer_id}] commits={local_w} '
				    f'conflicts={local_c}')

		def reader_pk_loop(reader_id):
			con = node.connect()
			local_r = 0
			try:
				while not stop.is_set():
					try:
						con.begin(IsolationLevel.RepeatableRead)
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
				dprint(f'[reader-pk #{reader_id}] commits={local_r}')

		def reader_sk_loop(reader_id):
			con = node.connect()
			local_r = 0
			try:
				while not stop.is_set():
					try:
						con.begin(IsolationLevel.RepeatableRead)
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
				dprint(f'[reader-sk #{reader_id}] commits={local_r}')

		def reader_mixed_loop(reader_id):
			con = node.connect()
			local_r = 0
			try:
				while not stop.is_set():
					start = random.randint(1, n_accounts)
					try:
						con.begin(IsolationLevel.RepeatableRead)
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
				dprint(f'[reader-mixed #{reader_id}] commits={local_r}')

		def checkpointer_loop():
			con = node.connect()
			local_cp = 0
			try:
				while not stop.is_set():
					try:
						con.execute("CHECKPOINT")
						con.commit()
						local_cp += 1
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
				dprint(f'[checkpointer] checkpoints={local_cp}')

		def ddl_nemesis_loop():
			# Concurrent DDL nemesis: repeatedly add and drop a
			# nullable column on o_bank_account. ALTER TABLE takes
			# a strong lock and must not tear readers/writers'
			# RR snapshots or the bank-account invariants.
			con = node.connect()
			local_add = 0
			local_drop = 0
			try:
				while not stop.is_set():
					try:
						con.execute(
						    "ALTER TABLE o_bank_account "
						    "ADD COLUMN IF NOT EXISTS nemesis_col int")
						con.commit()
						local_add += 1
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
					if stop.wait(ddl_nemesis_interval):
						break
					try:
						con.execute(
						    "ALTER TABLE o_bank_account "
						    "DROP COLUMN IF EXISTS nemesis_col")
						con.commit()
						local_drop += 1
					except Exception:
						try:
							con.rollback()
						except Exception:
							pass
					if stop.wait(ddl_nemesis_interval):
						break
			finally:
				try:
					con.execute(
					    "ALTER TABLE o_bank_account "
					    "DROP COLUMN IF EXISTS nemesis_col")
					con.commit()
				except Exception:
					try:
						con.rollback()
					except Exception:
						pass
				try:
					con.close()
				except Exception:
					pass
				dprint(
				    f'[ddl-nemesis] adds={local_add} drops={local_drop}')

		def wal_chaos_loop():
			# WAL chaos nemesis: attach/detach the
			# orioledb-wal-modify injection point. While attached
			# with the `error` action, every call to
			# add_modify_wal_record_extended raises ERROR and drives
			# the aborting transaction through Postgres xact
			# machinery (wal_rollback + undo apply + CSN=ABORTED).
			# Backed by the contrib `injection_points` extension;
			# requires Postgres built --enable-injection-points.
			# Analog of Tarantool's start_wal_chaos fiber.
			con = node.connect()
			local_on = 0
			logged_error = [False]

			def log_once(context, exc):
				if not logged_error[0]:
					logged_error[0] = True
					dprint(
					    f'[wal-chaos] {context} failed: {exc!r} '
					    f'(subsequent errors suppressed)')

			try:
				while not stop.is_set():
					if stop.wait(wal_chaos_idle):
						break
					try:
						con.execute(
						    "SELECT injection_points_attach("
						    "'orioledb-wal-modify', 'error')")
						con.commit()
						local_on += 1
					except Exception as e:
						try:
							con.rollback()
						except Exception:
							pass
						log_once('attach', e)
					# No explicit on-window: the SQL round-trip
					# between attach and detach is itself the
					# yield-tick. A few writers in flight at this
					# moment hit the injection; the rest don't.
					try:
						con.execute(
						    "SELECT injection_points_detach("
						    "'orioledb-wal-modify')")
						con.commit()
					except Exception as e:
						try:
							con.rollback()
						except Exception:
							pass
						log_once('detach', e)
			finally:
				# Always leave the point detached so post-run
				# teardown (final aggregates, orioledb_tbl_check,
				# node.stop) does not hit the injection. Detach can
				# fail if not currently attached -- ignore.
				try:
					con.execute(
					    "SELECT injection_points_detach("
					    "'orioledb-wal-modify')")
					con.commit()
				except Exception:
					try:
						con.rollback()
					except Exception:
						pass
				try:
					con.close()
				except Exception:
					pass
				dprint(f'[wal-chaos] windows={local_on}')
		
		dprint("")

		threads = []
		for wid in range(1, n_writers + 1):
			threads.append(
			    threading.Thread(target=writer_loop, args=(wid,)))
		for rid in range(1, n_readers_pk + 1):
			threads.append(
			    threading.Thread(target=reader_pk_loop, args=(rid,)))
		for rid in range(1, n_readers_sk + 1):
			threads.append(
			    threading.Thread(target=reader_sk_loop, args=(rid,)))
		for rid in range(1, n_readers_mixed + 1):
			threads.append(
			    threading.Thread(target=reader_mixed_loop, args=(rid,)))
		threads.append(threading.Thread(target=checkpointer_loop))
		threads.append(threading.Thread(target=ddl_nemesis_loop))
		threads.append(threading.Thread(target=wal_chaos_loop))

		for t in threads:
			t.start()

		time.sleep(duration)
		stop.set()
		for t in threads:
			t.join()

		dprint(
		    f'[totals] writes={write_count[0]} '
		    f'reads={read_count[0]} '
		    f'conflicts={conflict_count[0]}')

		final_total = node.execute(
		    "SELECT sum(balance)::bigint FROM o_bank_account")[0][0]
		unique_tokens = node.execute(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account")[0][0]
		rows = node.execute(
		    "SELECT count(*)::int FROM o_bank_account")[0][0]
		retained = node.execute(
		    "SELECT orioledb_has_retained_undo()")[0][0]
		tbl_check_ok = node.execute(
		    "SELECT orioledb_tbl_check('o_bank_account'::regclass)")[0][0]

		with errors_lock:
			seen = list(errors)
		print(
		    f'snapshot reads saw inconsistency (torn scan): {seen}')

		self.assertEqual(
		    final_total, expected_total,
		    f'final total {final_total} != expected {expected_total} '
		    f'(mismatch = lost update)')
		self.assertEqual(
		    unique_tokens, n_accounts,
		    f'expected {n_accounts} unique tokens, got {unique_tokens}')
		self.assertEqual(
		    rows, n_accounts,
		    f'expected {n_accounts} rows, got {rows}')
		self.assertEqual(
		    seen, [], 'snapshot reads saw inconsistency (torn scan)')
		self.assertGreater(
		    write_count[0], 0, 'writers must have committed')
		self.assertGreater(
		    read_count[0], 0, 'readers must have completed')
		self.assertFalse(
		    retained,
		    'orioledb retained undo after stop (possible snapshot leak)')
		self.assertTrue(
		    tbl_check_ok, 'orioledb_tbl_check(o_bank_account) failed')

		node.stop()

#!/usr/bin/env python3
# coding: utf-8

# REPEATABLE READ correctness stress test (development copy).
#
# Standalone bank-account invariant stress test forked from
# repeatable_read_stress_test.py. Adds env-var configuration,
# crash watchdog, injection-point trace, post-test backend reaping.
# Use this file for iteration / experiments without touching the
# original test.

import datetime
import inspect
import os
import random
import shutil
import subprocess
import threading
import time

from testgres.enums import IsolationLevel

from ..base_test import BaseTest


def _env_int(name, default):
	v = os.getenv(name)
	return int(v) if v else default


def _env_float(name, default):
	v = os.getenv(name)
	return float(v) if v else default


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


class RrStressTest(BaseTest):

	def setUp(self):
		_inst = os.getenv('RR_INSTANCE')
		if _inst:
			self._myName = (
			    'rr_stress-bank_account_invariant_' + _inst)
		super().setUp()

	def tearDown(self):
		try:
			super().tearDown()
		finally:
			# Reap this test's own postgres backends (matched by its
			# unique data dir) so dangling backends from a crashed
			# cluster don't outlive the test. The data-dir match keeps
			# parallel test instances unaffected.
			data_dir = os.path.join(self.node.base_dir, 'data')
			subprocess.run(
			    f"ps -Af | grep -F '{data_dir}' | grep -v grep "
			    f"| awk '{{print $2}}' | xargs kill -9 2>/dev/null",
			    shell=True, check=False)

	def test_bank_account_invariant(self):
		n_accounts = _env_int('RR_ACCOUNTS', 100)
		initial_balance = 1000
		n_writers = _env_int('RR_WRITERS', 20)
		n_readers_pk = _env_int('RR_READERS_PK', 6)
		n_readers_sk = _env_int('RR_READERS_SK', 6)
		n_readers_mixed = _env_int('RR_READERS_MIXED', 6)
		duration = _env_int('RR_DURATION', 3 * 60)
		# When 1: PANIC + cluster recovery is treated as a test
		# failure. When 0: PANIC is tolerated, cluster restart is
		# allowed, and the test only fails on data-invariant
		# violations (the original intent).
		panic_fatal = _env_int('RR_PANIC_FATAL', 1)
		checkpoint_interval = 0.5
		ddl_nemesis_interval = 0.2
		wal_chaos_idle = _env_float('RR_WAL_CHAOS_IDLE', 0.1)
		stopevent_chaos_idle = 0.5
		stopevent_chaos_window = 0.1
		stopevent_chaos_events = (
		    'page_read',
		    'modify_start',
		    'step_down',
		    'apply_undo',
		    'checkpoint_step',
		)

		expected_total = n_accounts * initial_balance

		node = self.node
		# When PANIC is fatal, disable in-place crash recovery so a
		# PANIC takes the whole postmaster down -- crash detection
		# becomes unambiguous (connect-refused == cluster gone).
		# When PANIC is tolerated, keep the base default (= 'on') so
		# the cluster recovers and the test can still validate data
		# invariants after the crash.
		if panic_fatal:
			node.append_conf(
			    'postgresql.conf', 'restart_after_crash = off\n')
		else:
			node.append_conf(
			    'postgresql.conf', 'restart_after_crash = on\n')
		node.start()
		node.safe_psql(SETUP_SQL)

		node.safe_psql(
		    "ALTER SYSTEM SET orioledb.enable_stopevents = on"
		)
		node.safe_psql("SELECT pg_reload_conf()")
		node.safe_psql(f"""
			INSERT INTO o_bank_account(id, balance, token)
				SELECT i, {initial_balance}, i
				FROM generate_series(1, {n_accounts}) AS i;
		""")

		test_start = time.time()
		first_error_time = [None]
		first_crash_time = [None]
		current_injection = [None]
		crash_injection = [None]
		injection_history = []
		injection_history_lock = threading.Lock()
		# Per-point counter: how many times each injection point was
		# attached (i.e. won the rating tournament) by `wal_chaos_loop`.
		# Reported at the end of the test.
		attach_counts = {}
		attach_counts_lock = threading.Lock()
		stop = threading.Event()
		errors = []
		errors_lock = threading.Lock()

		def note_crash():
			with errors_lock:
				if first_crash_time[0] is None:
					first_crash_time[0] = time.time() - test_start
					crash_injection[0] = current_injection[0]
			stop.set()

		counters_lock = threading.Lock()
		print_lock = threading.Lock()
		write_count = [0]
		read_count = [0]
		conflict_count = [0]   # server-side ERRORs while con is alive
		disconnect_count = [0]  # connection-level failures (cluster down etc.)

		def record_error(msg):
			with errors_lock:
				if first_error_time[0] is None:
					first_error_time[0] = time.time() - test_start
				errors.append(msg)

		def dprint(msg):
			with print_lock:
				print(msg, flush=True)

		def writer_loop(writer_id):
			con = node.connect()
			my_token = n_accounts + writer_id
			local_w = 0
			local_c = 0
			local_disc = 0
			try:
				while not stop.is_set():
					if con is None:
						try:
							con = node.connect()
						except Exception:
							local_disc += 1
							if stop.wait(0.5):
								break
							continue
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
						if random.randint(0, 1) == 0:
							time.sleep(0)
						con.execute(
						    "UPDATE o_bank_account "
						    f"SET balance = {to_bal + amount}, "
						    f"    token = {from_token} "
						    f"WHERE id = {v_to}")
						con.commit()
						my_token = to_token
						local_w += 1
					except Exception:
						# Probe connection health via rollback.  If even
						# rollback fails, the backend is gone (cluster
						# crashed, FATAL'd, or network reset) -- count as
						# a disconnect and force reconnect on next round.
						con_dead = False
						try:
							con.rollback()
						except Exception:
							con_dead = True
						if con_dead:
							try:
								con.close()
							except Exception:
								pass
							con = None
							local_disc += 1
						else:
							local_c += 1
			finally:
				if con is not None:
					try:
						con.close()
					except Exception:
						pass
				with counters_lock:
					write_count[0] += local_w
					conflict_count[0] += local_c
					disconnect_count[0] += local_disc

				disconnect_count[0] += local_disc

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

		def reader_sk_loop(reader_id):
			con = node.connect()
			local_r = 0
			try:
				while not stop.is_set():
					try:
						con.begin(IsolationLevel.RepeatableRead)
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

		def ddl_nemesis_loop():
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

		wal_chaos_points = [
			'before-tx-commit',
			# 'postgres-precommit-on-commit-actions', -> recursion during injection detach
			# detaching tx wanna commit and fall into the precommit injection
			'orioledb-set-csn-guarded',
			'orioledb-set-xlog-ptr-guarded',
			'orioledb-add-finish-wal-guarded',
			'orioledb-wal-flush-guarded',
			'orioledb-csn-incremented',
			'orioledb-pk-mutated-pre-wal',
			'orioledb-update-pk-done-pre-sk',
			'orioledb-sk-mid-update',
		]
		# `orioledb-commit-assert` and `orioledb-before-pre-commit-wal-finish`
		# are NOT in `wal_chaos_points` because attaching either always
		# causes a PANIC (both sit inside a START_CRIT_SECTION).  They
		# share a dedicated worker (`assert_chaos_loop` below) that arms
		# one of them on a slow, duration-relative cadence so we get a
		# controlled number of crash+recovery cycles per run instead of
		# one per ~0.1s.
		_env_pts = os.getenv('RR_INJECTION_POINTS')
		if _env_pts and _env_pts != 'ALL':
			_subset = [p.strip() for p in _env_pts.split(',') if p.strip()]
			wal_chaos_points = [p for p in wal_chaos_points if p in _subset]

		def wal_chaos_loop():
			con = node.connect()
			local_on = 0
			logged_error = [False]

			def log_once(context, exc):
				if not logged_error[0]:
					logged_error[0] = True

			try:
				while not stop.is_set():
					if stop.wait(wal_chaos_idle):
						break
					try:
						rating = []
						for point in wal_chaos_points:
							rating.append(
							    (point,
							     random.randint(1, len(wal_chaos_points))))
						rating.sort(key=lambda x: x[1])

						con.execute(
							"SELECT injection_points_attach("
							f"'{rating[0][0]}', 'error')")
						con.commit()
						current_injection[0] = rating[0][0]
						with attach_counts_lock:
							attach_counts[rating[0][0]] = (
							    attach_counts.get(rating[0][0], 0) + 1)
						with injection_history_lock:
							injection_history.append(
							    (time.time() - test_start, rating[0][0]))
						local_on += 1
					except Exception as e:
						try:
							con.rollback()
						except Exception:
							pass
						log_once('attach', e)
					time.sleep(0)
					try:
						for point in wal_chaos_points:
							con.execute(
							    "SELECT injection_points_detach("
							    f"'{point}')")
						con.commit()
						current_injection[0] = None
					except Exception as e:
						try:
							con.rollback()
						except Exception:
							pass
						log_once('detach', e)
			finally:
				try:
					for point in wal_chaos_points:
						con.execute(
						    "SELECT injection_points_detach("
						    f"'{point}')")
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

		# produce the state corruption without nay injection enabled
		assert_chaos_points = [
			'orioledb-commit-assert',
			'orioledb-before-pre-commit-wal-finish',
		]
		_env_assert_pts = os.getenv('RR_ASSERT_POINTS')
		if _env_assert_pts and _env_assert_pts != 'ALL':
			_assert_subset = [
			    p.strip() for p in _env_assert_pts.split(',') if p.strip()]
			assert_chaos_points = [
			    p for p in assert_chaos_points if p in _assert_subset]

		def assert_chaos_loop():
			# Periodically arm one of `assert_chaos_points`.  Each point
			# sits inside a START_CRIT_SECTION, so an attached `error`
			# action turns into a PANIC on the next commit -- which
			# takes the cluster down.  Same pattern as `wal_chaos_loop`:
			# pick one via rating tournament, attach -> `time.sleep(0.5)`
			# (release GIL for 500 ms so a writer has a realistic
			# chance to slip into commit at low writer counts) ->
			# detach every point in the list.  The attach window is
			# wide enough that at higher writer counts several
			# backends may hit the injection before the worker pulls
			# it back; postmaster's quickdie fan-out kills every
			# other backend, so the in-process abort pileup is still
			# bounded.
			target = max(_env_int('RR_ASSERT_FIRINGS', 0), 1)
			interval = max(duration / (target + 1), 1.0)
			con = None
			local_arms = 0
			next_arm_at = test_start + interval

			try:
				while not stop.is_set():
					if stop.wait(0.5):
						break
					if con is None:
						try:
							con = node.connect()
						except Exception:
							continue
					if time.time() < next_arm_at:
						continue
					try:
						rating = []
						for point in assert_chaos_points:
							rating.append(
							    (point,
							     random.randint(
							         1, len(assert_chaos_points))))
						rating.sort(key=lambda x: x[1])
						chosen = rating[0][0]
						con.execute(
						    "SELECT injection_points_attach("
						    f"'{chosen}', 'error')")
						con.commit()
						local_arms += 1
						next_arm_at = time.time() + interval
						with attach_counts_lock:
							attach_counts[chosen] = (
							    attach_counts.get(chosen, 0) + 1)
						with injection_history_lock:
							injection_history.append(
							    (time.time() - test_start,
							     f'{chosen} (assert-armed)'))
					except Exception:
						try:
							con.close()
						except Exception:
							pass
						con = None
						continue
					time.sleep(0)
					try:
						for point in assert_chaos_points:
							con.execute(
							    "SELECT injection_points_detach("
							    f"'{point}')")
						con.commit()
					except Exception:
						# Detach failed -- cluster almost certainly
						# PANICked while the injection was armed.
						# Drop the now-broken connection; we'll
						# reconnect on the next iteration.
						try:
							con.close()
						except Exception:
							pass
						con = None
			finally:
				if con is not None:
					try:
						for point in assert_chaos_points:
							con.execute(
							    "SELECT injection_points_detach("
							    f"'{point}')")
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

		def stopevent_chaos_loop():
			con = node.connect()
			local_cycles = 0
			armed = [None]
			logged_error = [False]

			def log_once(ctx, exc):
				if not logged_error[0]:
					logged_error[0] = True

			def reset_armed():
				if armed[0] is None:
					return
				try:
					con.execute(
					    f"SELECT pg_stopevent_reset("
					    f"'{armed[0]}')")
					con.commit()
				except Exception as e:
					try:
						con.rollback()
					except Exception:
						pass
					log_once('reset', e)
				armed[0] = None

			try:
				while not stop.is_set():
					if stop.wait(stopevent_chaos_idle):
						break
					event = random.choice(stopevent_chaos_events)
					try:
						con.execute(
						    f"SELECT pg_stopevent_set("
						    f"'{event}', 'true')")
						con.commit()
						armed[0] = event
						local_cycles += 1
					except Exception as e:
						try:
							con.rollback()
						except Exception:
							pass
						log_once('set', e)
						continue
					stop.wait(stopevent_chaos_window)
					reset_armed()
			finally:
				reset_armed()
				try:
					con.close()
				except Exception:
					pass

		def crash_watchdog():
			# Poll postgresql.log for PANIC. Authoritative signal —
			# unlike connection probing, it isn't fooled by load.
			log_path = os.path.join(node.logs_dir, 'postgresql.log')
			pos = 0
			try:
				if os.path.exists(log_path):
					pos = os.path.getsize(log_path)
			except OSError:
				pos = 0
			while not stop.is_set():
				if stop.wait(0.5):
					break
				try:
					with open(log_path, 'r', errors='replace') as f:
						f.seek(pos)
						chunk = f.read()
						pos = f.tell()
				except Exception:
					continue
				# Match `PANIC` (ereport) and `TRAP:` (Assert/abort).
				# Both indicate the cluster went down.  When
				# `panic_fatal` is on, treat any crash as fatal and
				# stop the test.  When tolerating PANIC (recovery
				# mode), only record the first crash time and keep the
				# watchdog running so subsequent crashes also get
				# logged but the workload continues.
				if 'PANIC' in chunk or 'TRAP:' in chunk:
					for line in chunk.splitlines():
						if 'PANIC' in line or 'TRAP:' in line:
							print(f'[watchdog] {line}', flush=True)
					if panic_fatal:
						note_crash()
						break
					else:
						# Record the first crash time without setting
						# stop, so the test continues exercising the
						# crash-recovery path.
						with errors_lock:
							if first_crash_time[0] is None:
								first_crash_time[0] = (
								    time.time() - test_start)
								crash_injection[0] = current_injection[0]

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
		if wal_chaos_points:
			threads.append(threading.Thread(target=wal_chaos_loop))
		if _env_int('RR_ASSERT_FIRINGS', 0) > 0:
			threads.append(threading.Thread(target=assert_chaos_loop))
		threads.append(threading.Thread(target=crash_watchdog))

		print(
		    f'\n[config] duration={duration} writers={n_writers} '
		    f'readers_pk={n_readers_pk} readers_sk={n_readers_sk} '
		    f'readers_mixed={n_readers_mixed} '
		    f'injection_points={len(wal_chaos_points)} '
		    f'list={wal_chaos_points}', flush=True)

		for t in threads:
			t.start()

		stop.wait(duration)
		stop.set()
		for t in threads:
			t.join()

		# Final authoritative check against the PG log.  Match both
		# `PANIC` (ereport-driven) and `TRAP:` (Assert-driven crashes
		# such as the orioledb-commit-assert injection).
		log_path = os.path.join(node.logs_dir, 'postgresql.log')
		panic_lines = []
		try:
			with open(log_path, 'r', errors='replace') as f:
				for line in f:
					if 'PANIC' in line or 'TRAP:' in line:
						panic_lines.append(line.rstrip())
		except Exception:
			pass
		if panic_lines and first_crash_time[0] is None:
			first_crash_time[0] = time.time() - test_start
			crash_injection[0] = current_injection[0]

		log_saved_path = [None]

		def _save_log(reason):
			results_dir = os.path.join(
			    os.path.dirname(inspect.getfile(self.__class__)),
			    'results')
			os.makedirs(results_dir, exist_ok=True)
			tag = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
			pts = (os.getenv('RR_INJECTION_POINTS') or 'ALL'
			       ).replace(',', '+')[:60]
			inst = os.getenv('RR_INSTANCE') or 'solo'
			dst = os.path.join(
			    results_dir,
			    f'{tag}_{inst}_{pts}_{reason}.log')
			try:
				shutil.copy(log_path, dst)
				print(f'[saved-log] {dst}', flush=True)
				log_saved_path[0] = dst
			except Exception as _ce:
				print(f'[save-log failed] {_ce!r}', flush=True)

		if panic_lines:
			_save_log('panic')

		_fet = first_error_time[0]
		_fct = first_crash_time[0]
		_fet_s = f'{_fet:.2f}s' if _fet is not None else 'none'
		_fct_s = f'{_fct:.2f}s' if _fct is not None else 'none'
		print(
		    f'\n[totals] writes={write_count[0]} '
		    f'reads={read_count[0]} '
		    f'conflicts={conflict_count[0]} '
		    f'disconnects={disconnect_count[0]} '
		    f'first_error_at={_fet_s} '
		    f'first_crash_at={_fct_s} '
		    f'crash_injection={crash_injection[0]!r} '
		    f'panic_lines={len(panic_lines)}')
		with attach_counts_lock:
			_counts = sorted(
			    attach_counts.items(), key=lambda kv: -kv[1])
		_total_attaches = sum(c for _, c in _counts)
		print(f'[wal-chaos attach counts] total={_total_attaches}')
		for _pt, _c in _counts:
			print(f'  {_c:5d}  {_pt}')
		for ln in panic_lines[:5]:
			print(f'[PANIC] {ln}')
		if first_crash_time[0] is not None:
			_window = [
			    (t, p)
			    for t, p in injection_history
			    if t >= first_crash_time[0] - 5.0
			    and t <= first_crash_time[0] + 1.0
			]
			print(f'[wal-chaos history near crash] {_window}')

		if panic_fatal and first_crash_time[0] is not None:
			self.fail(
			    f'cluster crashed at {first_crash_time[0]:.2f}s '
			    f'(writes={write_count[0]} reads={read_count[0]} '
			    f'crash_injection={crash_injection[0]!r})')
		# When tolerating PANIC, wait briefly for recovery before
		# running invariant queries; node.execute will keep getting
		# 57P03 ("in recovery mode") until WAL replay completes.
		if not panic_fatal and first_crash_time[0] is not None:
			deadline = time.time() + 30.0
			while time.time() < deadline:
				try:
					node.execute("SELECT 1")
					break
				except Exception:
					time.sleep(0.5)
		# Assume the state may be corrupted -- run a PK-authoritative
		# diagnostic before the regular invariant queries so we can tell
		# PK-side corruption from SK-side corruption.  `count(*)` etc
		# can be planned via the SK token unique index; forcing seq scan
		# routes through the PK heap (orioledb is index-organized on
		# PK).  If `pk_*_seq` disagrees with the default-plan numbers,
		# the SK is the only thing that's off; if `pk_*_seq` itself is
		# wrong, the PK is corrupt too.
		def _safe_exec(sql):
			try:
				return node.execute(sql)
			except Exception as _e:
				return f'ERROR: {_e!r}'

		final_total = node.execute(
		    "SELECT sum(balance)::bigint FROM o_bank_account")[0][0]

		unique_tokens = node.execute(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account")[0][0]
		pk_oor_tokens = _safe_exec(
		    "BEGIN; "
		    "SET LOCAL enable_indexonlyscan = off; "
		    "SET LOCAL enable_indexscan = off; "
		    "SET LOCAL enable_bitmapscan = off; "
		    "SELECT token FROM o_bank_account "
		    f"WHERE token < 1 OR token > {n_accounts} "
		    "ORDER BY token; "
		    "COMMIT")

		rows = node.execute(
		    "SELECT count(*)::int FROM o_bank_account")[0][0]
		retained = node.execute(
		    "SELECT orioledb_has_retained_undo()")[0][0]
		tbl_check_ok = node.execute(
		    "SELECT orioledb_tbl_check('o_bank_account'::regclass)")[0][0]
		# PK-authoritative: force seq scan over the PK heap.  Use a
		# single statement so the SET LOCAL stays in scope.
		pk_seq = _safe_exec(
		    "BEGIN; "
		    "SET LOCAL enable_indexonlyscan = off; "
		    "SET LOCAL enable_indexscan = off; "
		    "SET LOCAL enable_bitmapscan = off; "
		    "SELECT count(*)::int, count(DISTINCT id)::int, "
		    "count(DISTINCT token)::int, sum(balance)::bigint "
		    "FROM o_bank_account; "
		    "COMMIT")
		# Per-row PK contents (only if it would be small)
		pk_dump = _safe_exec(
		    "BEGIN; "
		    "SET LOCAL enable_indexonlyscan = off; "
		    "SET LOCAL enable_indexscan = off; "
		    "SET LOCAL enable_bitmapscan = off; "
		    "SELECT id, balance, token FROM o_bank_account ORDER BY id; "
		    "COMMIT")
		# Duplicate tokens visible from PK -- non-empty => PK has dups
		pk_token_dups = _safe_exec(
		    "BEGIN; "
		    "SET LOCAL enable_indexonlyscan = off; "
		    "SET LOCAL enable_indexscan = off; "
		    "SET LOCAL enable_bitmapscan = off; "
		    "SELECT token, count(*) FROM o_bank_account "
		    "GROUP BY token HAVING count(*) > 1; "
		    "COMMIT")
		# SK-side default-planned count (likely picks the unique idx)
		sk_default = _safe_exec(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account")

		with errors_lock:
			seen = list(errors)
		print(
		    f'snapshot reads saw inconsistency (torn scan): {seen}')
		print(f'[diag] pk_seq (count, dist_id, dist_token, sum_bal) = '
		      f'{pk_seq!r}')
		print(f'[diag] sk_default count(DISTINCT token) = {sk_default!r}')
		print(f'[diag] pk_token_dups = {pk_token_dups!r}')
		print(f'[diag] pk_oor_tokens (outside [1,{n_accounts}]) = '
		      f'{pk_oor_tokens!r}')
		print(f'[diag] tbl_check_ok = {tbl_check_ok!r} '
		      f'retained_undo = {retained!r}')
		if isinstance(pk_seq, list) and pk_seq and len(pk_seq[0]) >= 3:
			_pk_rows, _pk_ids, _pk_toks, _pk_sum = pk_seq[0]
			_pk_corrupt = (
			    _pk_rows != n_accounts or
			    _pk_ids != n_accounts or
			    _pk_toks != n_accounts or
			    _pk_sum != expected_total)
			if _pk_corrupt:
				print('[diag] PK CORRUPTION DETECTED')
				print(f'[diag] pk_full_dump:')
				if isinstance(pk_dump, list):
					for _r in pk_dump:
						print(f'  {_r}')
				else:
					print(f'  {pk_dump}')

		# Detect dangling backends: stop the node and then count any
		# process that still references our data dir. After a clean
		# shutdown the count must be zero. Anything else indicates a
		# stuck backend / orphan worker.
		try:
			node.stop()
		except Exception as _se:
			print(f'[node.stop failed] {_se!r}', flush=True)
		data_dir = os.path.join(node.base_dir, 'data')
		_ps = subprocess.run(
		    f"ps -Af | grep -F '{data_dir}' | grep -v grep | wc -l",
		    shell=True, capture_output=True, text=True, check=False)
		n_dangling = int((_ps.stdout or '0').strip() or 0)
		print(f'[dangling-check] surviving backends: {n_dangling}',
		      flush=True)

		# Collect every invariant violation so a single failure does not
		# hide the others.  All checks are evaluated first, then a
		# single assertEqual against an empty list reports all failures
		# together.
		violations = []
		if final_total != expected_total:
			violations.append(
			    f'final total {final_total} != expected '
			    f'{expected_total} (mismatch = lost update)')
		if unique_tokens != n_accounts:
			_msg = f'unique_tokens {unique_tokens} != {n_accounts}'
			if isinstance(pk_oor_tokens, list) and pk_oor_tokens:
				_msg += (
				    f' [out-of-range tokens (outside [1,{n_accounts}]): '
				    f'{[r[0] for r in pk_oor_tokens]}]')
			if isinstance(pk_token_dups, list) and pk_token_dups:
				_msg += f' [duplicate tokens: {pk_token_dups}]'
			violations.append(_msg)
		if rows != n_accounts:
			violations.append(f'rows {rows} != {n_accounts}')
		if seen:
			violations.append(
			    f'snapshot reads saw inconsistency: {seen}')
		if not tbl_check_ok:
			violations.append('orioledb_tbl_check returned false')
		if retained:
			violations.append('orioledb retained undo after stop')
		# PK-authoritative invariants (PK heap forced seq scan).
		if isinstance(pk_seq, list) and pk_seq and len(pk_seq[0]) >= 4:
			_pk_rows, _pk_ids, _pk_toks, _pk_sum = pk_seq[0]
			if _pk_rows != n_accounts:
				violations.append(f'pk_rows {_pk_rows} != {n_accounts}')
			if _pk_ids != n_accounts:
				violations.append(
				    f'pk_distinct_id {_pk_ids} != {n_accounts}')
			if _pk_toks != n_accounts:
				violations.append(
				    f'pk_distinct_token {_pk_toks} != {n_accounts}')
			if _pk_sum != expected_total:
				violations.append(
				    f'pk_sum_balance {_pk_sum} != {expected_total}')
		if isinstance(pk_token_dups, list) and pk_token_dups:
			violations.append(
			    f'pk_token_dups (PK has duplicate tokens!) = '
			    f'{pk_token_dups}')
		if violations and log_saved_path[0] is None:
			_save_log('invariant')
		# Opt-in: save the log even on a clean pass so a batch can
		# compare buggy vs normal runs side-by-side.
		if (_env_int('RR_SAVE_ALL_LOGS', 0)
		        and log_saved_path[0] is None):
			_save_log('clean')
		self.assertEqual(violations, [],
		                 f'invariant violations: {violations}')
		if n_writers > 0:
			self.assertGreater(
			    write_count[0], 0, 'writers must have committed')
		if (n_readers_pk + n_readers_sk + n_readers_mixed) > 0:
			self.assertGreater(
			    read_count[0], 0, 'readers must have completed')
		self.assertFalse(
		    retained,
		    'orioledb retained undo after stop (possible snapshot leak)')
		self.assertTrue(
		    tbl_check_ok, 'orioledb_tbl_check(o_bank_account) failed')
		self.assertEqual(
		    n_dangling, 0,
		    f'{n_dangling} postgres backends survived node.stop()')

		node.stop()

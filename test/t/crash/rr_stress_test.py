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
import signal
import subprocess
import threading
import time

from testgres.enums import IsolationLevel

from ..base_test import BaseTest
from ..logical_test import wait_ready


def _env_int(name, default):
	v = os.getenv(name)
	return int(v) if v else default


def _env_float(name, default):
	v = os.getenv(name)
	return float(v) if v else default


# Storage-engine selector: 'orioledb' (default) or 'heap'.  The
# table-creation USING clause and the post-trial structural check
# both branch on this value.  Heap mode swaps orioledb_tbl_check for
# amcheck's verify_heapam + bt_index_check, so the two engines run
# the same workload under an apples-to-apples structural validator.
STORAGE_ENGINE = os.getenv('RR_STORAGE_ENGINE', 'orioledb').lower()
assert STORAGE_ENGINE in ('orioledb', 'heap'), (
    f'unknown RR_STORAGE_ENGINE={STORAGE_ENGINE!r}; '
    'expected orioledb or heap')

_USING_CLAUSE = 'USING orioledb' if STORAGE_ENGINE == 'orioledb' else ''

SETUP_SQL = f"""
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE EXTENSION IF NOT EXISTS injection_points;
	CREATE EXTENSION IF NOT EXISTS amcheck;

	CREATE TABLE o_bank_account (
		id int NOT NULL,
		balance bigint NOT NULL,
		token bigint NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT o_bank_account_token_uniq UNIQUE (token)
			DEFERRABLE INITIALLY DEFERRED
	) {_USING_CLAUSE};
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
		n_rollbackers = _env_int('RR_ROLLBACKERS', 0)
		duration = _env_int('RR_DURATION', 3 * 60)
		# When 1: PANIC + cluster recovery is treated as a test
		# failure. When 0: PANIC is tolerated, cluster restart is
		# allowed, and the test only fails on data-invariant
		# violations (the original intent).
		panic_fatal = _env_int('RR_PANIC_FATAL', 1)
		# Postmaster SIGKILL chaos: when on, a dedicated worker
		# periodically `kill -9`s the postmaster and restarts the
		# cluster, exercising the unclean-shutdown recovery path.
		# Interval defaults to duration/3 so a trial sees exactly
		# two fires (at ~1/3 and ~2/3 of the trial) plus restart
		# slack.
		postmaster_kill_enabled = _env_int('RR_KILL_POSTMASTER', 0)
		postmaster_kill_interval = _env_float(
		    'RR_KILL_POSTMASTER_INTERVAL', duration / 3.0)
		# Replica mode.  When non-'none', a second PG node is spun
		# up alongside the primary and verified at end-of-test
		# against the same invariants as the primary.  No reader/
		# writer threads run on the replica -- it's a pure passive
		# subscriber.
		#   'none'      -- no replica.
		#   'logical'   -- second node subscribes to o_bank_account
		#                  via testgres.publish() / .subscribe()
		#                  (logical decoding apply path).  Primary
		#                  gets wal_level=logical.
		#   'streaming' -- second node is a hot-standby spawned via
		#                  pg_basebackup + spawn_replica (binary
		#                  WAL replay).  Primary gets wal_level=
		#                  replica + hot_standby + wal_keep_size.
		# Backwards compat: RR_LOGICAL_REPLICA=1 still works and
		# resolves to 'logical' when RR_REPLICA_MODE is unset.
		replica_mode = os.getenv('RR_REPLICA_MODE', '').lower()
		if not replica_mode:
			replica_mode = ('logical'
			                if _env_int('RR_LOGICAL_REPLICA', 0)
			                else 'none')
		assert replica_mode in ('none', 'logical', 'streaming'), (
		    f'unknown RR_REPLICA_MODE={replica_mode!r}; '
		    'expected none, logical or streaming')
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
		if postmaster_kill_enabled:
			# SIGKILL'd postmaster -> unclean shutdown.  Recovery
			# from this path is only safe under explicit durability
			# guarantees: fsync so WAL really hits the platter,
			# full_page_writes so torn-page tears are correctable
			# from the FPI.  Pin both regardless of the base
			# testgres conf.
			node.append_conf(
			    'postgresql.conf',
			    'fsync = on\n'
			    'full_page_writes = on\n')
		if replica_mode != 'none':
			# Primary must run at a higher wal_level so the
			# replica's slot/walsender can extract enough info.
			# 'logical' implies 'replica'.  Raise the slot/sender
			# counts so reconnect after each SIGKILL has headroom.
			# wal_keep_size keeps recent WAL around so a streaming
			# replica can resume after primary restarts.
			# log_connections / log_disconnections produce the
			# walsender attach/detach LOG lines we need to confirm
			# replica connectivity post-mortem.
			_wal_level = (
			    'logical' if replica_mode == 'logical' else 'replica')
			node.append_conf(
			    'postgresql.conf',
			    f'wal_level = {_wal_level}\n'
			    'max_wal_senders = 10\n'
			    'max_replication_slots = 10\n'
			    'hot_standby = on\n'
			    'wal_keep_size = 64\n'
			    'log_connections = on\n'
			    'log_disconnections = on\n')
		node.start()
		node.safe_psql(SETUP_SQL)

		node.safe_psql(
		    "ALTER SYSTEM SET orioledb.enable_stopevents = on"
		)
		# Bump server-side log level so check_btree's NOTICE
		# messages (`BTree has a broken split.`, `Not used file
		# blocks ...`, `Excess file blocks ...`, etc.) end up in
		# postgresql.log -- otherwise orioledb_tbl_check returns
		# False without explaining what tripped it.
		node.safe_psql(
		    "ALTER SYSTEM SET log_min_messages = notice")
		node.safe_psql("SELECT pg_reload_conf()")
		node.safe_psql(f"""
			INSERT INTO o_bank_account(id, balance, token)
				SELECT i, {initial_balance}, i
				FROM generate_series(1, {n_accounts}) AS i;
		""")

		# Replica setup.  Spawned only when replica_mode != 'none'.
		# BaseTest.tearDown handles subscriber/replica stop + cleanup,
		# so no explicit teardown is needed here.
		#   `replica`  -- the node we'll query at end-of-test
		#                 (subscriber for logical, standby for
		#                 streaming).
		#   `sub_obj`  -- testgres Subscription handle (logical only;
		#                 used for sub_obj.catchup()).
		replica = None
		sub_obj = None
		if replica_mode == 'logical':
			replica = self.getSubsriber()
			replica.start()
			# Same DDL on the subscriber; rows come via the
			# subscription's initial table sync (copy_data=true
			# by default).
			replica.safe_psql(SETUP_SQL)
			pub = node.publish('rr_pub', tables=['o_bank_account'])
			sub_obj = replica.subscribe(pub, 'rr_sub')
			wait_ready(replica)
		elif replica_mode == 'streaming':
			# pg_basebackup the primary at its current state, then
			# spawn the binary copy as a hot-standby.  Replica
			# inherits the schema and the just-loaded 100 rows
			# from the backup -- no separate DDL run.
			replica = self.getReplica()
			replica.start()

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

		# Drain log: each writer pushes its final my_token here when it
		# exits, so the harness can check the token-universe invariant
		# (PK rows ∪ SK entries ∪ writer pockets == [1, n_accounts+n_writers]).
		drained_tokens = []
		drained_lock = threading.Lock()

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
		kill_count = [0]       # successful postmaster SIGKILLs by postmaster_kill_loop
		rollback_count = [0]   # deliberate ROLLBACKs by rollbacker_loop

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
						# Pocket-free 2-row swap: tokens rotate directly
						# between v_from and v_to with no writer-side
						# pocket state. The old 3-participant variant
						# (writer pocket + v_from + v_to) drifted under
						# cascade conditions because a writer SIGQUIT'd
						# between XLogFlush(commit) and the Python
						# `my_token = to_token` assignment ended up with
						# a stale pocket while the DB had the post-commit
						# state -- producing thousands of spurious
						# `duplicate key value violates unique constraint
						# o_bank_account_token_uniq` errors that throttled
						# the workload by ~15x without surfacing any real
						# SK bug. With the direct swap, no inter-tx
						# writer state exists, so no drift is possible.
						con.execute(
						    "UPDATE o_bank_account "
						    f"SET balance = {from_bal - amount}, "
						    f"    token = {to_token} "
						    f"WHERE id = {v_from}")
						if random.randint(0, 1) == 0:
							time.sleep(0)
						con.execute(
						    "UPDATE o_bank_account "
						    f"SET balance = {to_bal + amount}, "
						    f"    token = {from_token} "
						    f"WHERE id = {v_to}")
						con.commit()
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
				# Drain: writer's final pocketed token. my_token is set
				# only after a successful commit (line ~213), so this is
				# the token that left the database for the writer's pocket
				# in the writer's last completed tx. On a rolled-back tx,
				# my_token retains its pre-tx value, which is still the
				# token NOT on any row.
				with drained_lock:
					drained_tokens.append((writer_id, my_token))
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

		def rollbacker_loop(rollbacker_id):
			# Abort-path traffic generator.  Every iteration opens a
			# REPEATABLE READ tx and ALWAYS ends it with rollback(), never
			# commit() -- so it never alters committed state and never
			# touches the token universe (no pocket, no drain).  Its purpose
			# is to feed the abort path that produces deferred WAL_REC_ROLLBACK
			# records on crash recovery (the streaming-standby livelock /
			# update_run_xmin-assert trigger).  A coin flip picks between two
			# abort shapes each round:
			#   2.1  empty tx: open RR, sleep, rollback with no DML.  Intended
			#        to exercise wal_rollback's no-material-changes fast path.
			#        NB: orioledb assigns an oxid lazily (on first modify), so
			#        a tx that does zero writes may never acquire an oxid and
			#        thus never reach wal_rollback at all -- it can be a pure
			#        no-op.  Kept as specified; the oxid-bearing empty aborts
			#        come mostly from txns SIGKILL'd mid-write.
			#   2.2  dirty tx: do the same 2-row token swap as writer_loop,
			#        then rollback instead of commit -- a has-material-changes
			#        abort (durable rollback record / undo to unwind).
			con = node.connect()
			local_rb = 0
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
					try:
						con.begin(IsolationLevel.RepeatableRead)
						if random.randint(0, 1) == 0:
							# 2.1 empty abort: sleep, then rollback.
							time.sleep(random.randint(0, 10) * 0.1)
						else:
							# 2.2 dirty abort: writer-style swap, then rollback.
							v_from = random.randint(1, n_accounts)
							v_to = random.randint(1, n_accounts)
							if v_from != v_to:
								amount = random.randint(1, 10)
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
								    f"    token = {to_token} "
								    f"WHERE id = {v_from}")
								con.execute(
								    "UPDATE o_bank_account "
								    f"SET balance = {to_bal + amount}, "
								    f"    token = {from_token} "
								    f"WHERE id = {v_to}")
						con.rollback()
						local_rb += 1
					except Exception:
						# Same connection-health probe as writer_loop: if even
						# rollback fails the backend is gone -> reconnect.
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
			finally:
				if con is not None:
					try:
						con.close()
					except Exception:
						pass
				with counters_lock:
					rollback_count[0] += local_rb
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
			'orioledb-after-flush-local-wal',
			'orioledb-after-local_wal_has_material_changes-true',
			'oriole-before-on-commit-undo-stack',
			'oriole-before-curOxid-clear', # leads to PK/SK desync, currently does not look as a real problem. Nedd further investigation
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
						# sleep between iterations
						time.sleep(0.5 * random.random())

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
			'orioledb-before-xlog-insert',
			'orioledb-after-page-io',
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

		def postmaster_kill_loop():
			# Periodically SIGKILL the postmaster.  Unlike a backend
			# PANIC, SIGKILL of the postmaster bypasses
			# restart_after_crash (that GUC controls how the postmaster
			# reacts to a CHILD's death, not its own), so this loop
			# both kills and restarts on every cycle, exercising the
			# unclean-shutdown recovery path each iteration.  Existing
			# writer/reader loops reconnect via their own except
			# handlers; their disconnect_count will inflate by roughly
			# kill_count * (n_writers+n_readers_*) -- see the
			# expected-baseline figure in the [totals] line.
			#
			# Uses testgres's `node.kill()` rather than a raw
			# `os.kill(pid, SIGKILL)` because testgres caches an
			# `is_started` flag internally; a raw SIGKILL leaves
			# that flag at True, so the next `node.start()` short-
			# circuits and never invokes pg_ctl.  `node.kill()`
			# SIGKILLs the postmaster AND flips `is_started=False`.
			READY_TIMEOUT = 30.0
			local_kills = 0
			while not stop.is_set():
				if stop.wait(postmaster_kill_interval):
					break
				try:
					pm_pid = node.pid
				except Exception:
					pm_pid = 0
				if node.is_started and pm_pid:
					dprint(f'[killer] SIGKILL postmaster pid={pm_pid}')
					try:
						node.kill()
						local_kills += 1
					except Exception as exc:
						dprint(f'[killer] node.kill() failed: {exc!r}')
					# Brief grace for children to detect postmaster
					# death and exit before pg_ctl start sees a
					# clean data directory.
					time.sleep(0.5)
				else:
					dprint(
					    '[killer] cluster already down -- '
					    'attempting restart')
					node.is_started = False
				# Restart + wait until the cluster is fully ready
				# to accept connections.  pg_ctl start can fail
				# transiently right after a SIGKILL: lingering
				# postmaster children, postmaster.pid not yet
				# stale-cleaned, the port still in TIME_WAIT.
				# Retry with backoff up to RESTART_TIMEOUT.
				# Failure past that is fatal: the test cannot
				# validate invariants against a dead cluster.
				RESTART_TIMEOUT = 30.0
				RESTART_BACKOFF = 1.0
				restart_deadline = time.time() + RESTART_TIMEOUT
				restart_attempts = 0
				last_exc = None
				# NB: do NOT gate this restart on `stop`.  A SIGKILL above may
				# have just killed the postmaster; if `stop` was set meanwhile
				# (duration elapsed) and we skipped the restart, the cluster
				# would be left dead and teardown checks would fail spuriously.
				# Always bring it back at least once.
				while (time.time() < restart_deadline):
					restart_attempts += 1
					try:
						node.start()
						last_exc = None
						break
					except Exception as exc:
						last_exc = exc
						dprint(
						    f'[killer] node.start attempt '
						    f'#{restart_attempts} failed: '
						    f'{exc!r}; retrying in '
						    f'{RESTART_BACKOFF}s')
						# Make sure is_started stays False so
						# the next pg_ctl call isn't short-
						# circuited by the cached flag.
						node.is_started = False
						time.sleep(RESTART_BACKOFF)
				if last_exc is not None:
					record_error(
					    f'[killer] node.start failed after '
					    f'{restart_attempts} attempts in '
					    f'{RESTART_TIMEOUT}s: {last_exc!r}')
					stop.set()
					break
				ready_deadline = time.time() + READY_TIMEOUT
				ready = False
				while (time.time() < ready_deadline
				       and not stop.is_set()):
					try:
						with node.connect(autocommit=True) as con:
							con.execute("SELECT 1")
						ready = True
						break
					except Exception:
						time.sleep(0.1)
				if not ready:
					if not stop.is_set():
						record_error(
						    f'[killer] cluster did not accept '
						    f'connections within {READY_TIMEOUT}s '
						    f'after restart')
						stop.set()
					break
				dprint(
				    f'[killer] cluster ready after kill '
				    f'#{local_kills}')
			with counters_lock:
				kill_count[0] += local_kills

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
		for rbid in range(1, n_rollbackers + 1):
			threads.append(
			    threading.Thread(target=rollbacker_loop, args=(rbid,)))
		threads.append(threading.Thread(target=checkpointer_loop))
		if wal_chaos_points:
			threads.append(threading.Thread(target=wal_chaos_loop))
		if _env_int('RR_ASSERT_FIRINGS', 0) > 0:
			threads.append(threading.Thread(target=assert_chaos_loop))
		if postmaster_kill_enabled:
			threads.append(
			    threading.Thread(target=postmaster_kill_loop))
		threads.append(threading.Thread(target=crash_watchdog))

		_kill_cfg = (
		    f' postmaster_kill=on(every {postmaster_kill_interval:.1f}s)'
		    if postmaster_kill_enabled else '')
		print(
		    f'\n[config] storage={STORAGE_ENGINE} '
		    f'duration={duration} writers={n_writers} '
		    f'readers_pk={n_readers_pk} readers_sk={n_readers_sk} '
		    f'readers_mixed={n_readers_mixed} rollbackers={n_rollbackers} '
		    f'injection_points={len(wal_chaos_points)} '
		    f'list={wal_chaos_points}'
		    f'{_kill_cfg}', flush=True)

		for t in threads:
			t.start()

		stop.wait(duration)
		stop.set()
		for t in threads:
			t.join()

		# A SIGKILL can land right as `duration` elapses, leaving the primary
		# mid-restart; the teardown checks below would then hit a refused
		# connection (a harness race, not a product bug).  Wait up to 10s for
		# the primary to accept connections again.  If it is STILL down after
		# 10s that is itself a real bug, so we stop waiting and let the checks
		# surface it rather than masking it.
		_ready_deadline = time.monotonic() + 10.0
		_node_ready = False
		while time.monotonic() < _ready_deadline:
			try:
				node.execute("SELECT 1")
				_node_ready = True
				break
			except Exception:
				time.sleep(0.3)
		if not _node_ready:
			print('[teardown] primary did not accept connections within 10s '
			      'after the final kill -- proceeding (possible real restart bug)')

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
			# Also archive the replica's postgresql.log if a
			# replica was spawned this trial.  testgres tears
			# down its tmp dir on teardown, so the replica log
			# is otherwise unrecoverable post-mortem.
			if replica is not None:
				try:
					replica_log = os.path.join(
					    replica.logs_dir, 'postgresql.log')
					rdst = os.path.join(
					    results_dir,
					    f'{tag}_{inst}_{pts}_{reason}_replica.log')
					shutil.copy(replica_log, rdst)
					print(f'[saved-log] {rdst}', flush=True)
				except Exception as _ce:
					print(f'[save-log replica failed] '
					      f'{_ce!r}', flush=True)

		if panic_lines:
			_save_log('panic')

		_fet = first_error_time[0]
		_fct = first_crash_time[0]
		_fet_s = f'{_fet:.2f}s' if _fet is not None else 'none'
		_fct_s = f'{_fct:.2f}s' if _fct is not None else 'none'
		# Baseline disconnects we expect just from kill-loop restarts:
		# every active worker that touches disconnect_count
		# (writers + the three reader pools) sees one disconnect per
		# kill.  Anything above this baseline is "real" connection
		# loss (cluster instability between kills).
		_expected_kill_disc = (
		    kill_count[0] * (
		        n_writers + n_readers_pk
		        + n_readers_sk + n_readers_mixed))
		_disc_breakdown = (
		    f'disconnects={disconnect_count[0]}'
		    + (f' (expected_from_kills~{_expected_kill_disc})'
		       if postmaster_kill_enabled else ''))
		_kill_field = (
		    f' kills={kill_count[0]}'
		    if postmaster_kill_enabled else '')
		print(
		    f'\n[totals] writes={write_count[0]} '
		    f'reads={read_count[0]} '
		    f'conflicts={conflict_count[0]} '
		    f'rollbacks={rollback_count[0]} '
		    f'{_disc_breakdown}'
		    f'{_kill_field} '
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

		# Each helper logs the EXPLAIN plan under a label so post-trial
		# inspection can verify which scan actually answered the query.
		# The planner picks Seq Scan over SK index-only at this row count,
		# so without forcing GUCs the "SK" queries silently read from PK.
		def _explain_lines(con, sql):
			return [r[0] for r in con.execute('EXPLAIN (COSTS OFF) ' + sql)]

		# PK-authoritative diagnostic: force seq scan so the query is
		# answered from the PK heap, not the SK index. The previous
		# `BEGIN; SET LOCAL ...; SELECT; COMMIT` wrap silently returned
		# None because testgres exposes only the LAST statement's result
		# (COMMIT, no rows). We instead open one connection, set the
		# planner GUCs session-level, run the SELECT, return its rows.
		def _pk_force_seq(sql, label=None):
			try:
				with node.connect(autocommit=True) as con:
					con.execute("SET enable_indexonlyscan = off")
					con.execute("SET enable_indexscan = off")
					con.execute("SET enable_bitmapscan = off")
					if label:
						for _ln in _explain_lines(con, sql):
							print(f'[explain pk-forced {label}] {_ln}')
					return con.execute(sql)
			except Exception as _e:
				return f'ERROR: {_e!r}'

		# SK-authoritative: disable seqscan/bitmapscan so the only viable
		# plan for `SELECT token ...` is an index-(only-)scan on the
		# unique(token) SK. Lets us enumerate what the SK actually
		# contains and diff against the PK row set.
		def _sk_force_idx(sql, label=None):
			try:
				with node.connect(autocommit=True) as con:
					con.execute("SET enable_seqscan = off")
					con.execute("SET enable_bitmapscan = off")
					if label:
						for _ln in _explain_lines(con, sql):
							print(f'[explain sk-forced {label}] {_ln}')
					return con.execute(sql)
			except Exception as _e:
				return f'ERROR: {_e!r}'

		# Default-plan execution with EXPLAIN logging, so we can see what
		# the planner picked for queries that have no override.
		def _default_exec(sql, label=None):
			try:
				with node.connect(autocommit=True) as con:
					if label:
						for _ln in _explain_lines(con, sql):
							print(f'[explain default {label}] {_ln}')
					return con.execute(sql)
			except Exception as _e:
				return f'ERROR: {_e!r}'

		def _fmt_dups(rows):
			return ', '.join(
			    f'token {t} appears {c} times' for t, c in rows)

		final_total = _default_exec(
		    "SELECT sum(balance)::bigint FROM o_bank_account",
		    label='sum(balance)')[0][0]

		# SK-forced count: this is the authoritative "what does the SK
		# index say?" answer. Without -idx forcing, the planner may pick
		# Seq Scan and silently reads from PK heap instead.
		sk_distinct_tokens = _sk_force_idx(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account",
		    label='count(DISTINCT token)')[0][0]
		# PK-forced mirror of the same count -- if the two disagree the
		# indices are out of sync.
		pk_distinct_tokens = _pk_force_seq(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account",
		    label='count(DISTINCT token)')[0][0]
		pk_oor_tokens = _pk_force_seq(
		    "SELECT token FROM o_bank_account "
		    f"WHERE token < 1 OR token > {n_accounts + n_writers} "
		    "ORDER BY token",
		    label='token OOR predicate')

		# Row count via PK seq scan and via SK index-only scan. PK is
		# the row source of truth (orioledb is index-organized on PK);
		# SK row count must match because every PK row should have an
		# SK entry.
		pk_row_count = _pk_force_seq(
		    "SELECT count(*)::int FROM o_bank_account",
		    label='count(*)')[0][0]
		sk_row_count = _sk_force_idx(
		    "SELECT count(*)::int FROM o_bank_account",
		    label='count(*)')[0][0]
		rows = pk_row_count
		# Structural-consistency check: branches on STORAGE_ENGINE.
		# OrioleDB: orioledb_tbl_check returns True/False.
		# Heap: amcheck's verify_heapam returns one row per
		# corruption (count(*)=0 means clean), and bt_index_check
		# raises on damage; wrap so failure -> tbl_check_ok=False
		# rather than tearing the test down with an uncaught
		# exception.
		if STORAGE_ENGINE == 'orioledb':
			retained = node.execute(
			    "SELECT orioledb_has_retained_undo()")[0][0]
			tbl_check_ok = node.execute(
			    "SELECT orioledb_tbl_check"
			    "('o_bank_account'::regclass)")[0][0]
		else:
			retained = False
			_heap_bad = node.execute(
			    "SELECT count(*)::int "
			    "FROM verify_heapam('o_bank_account')")[0][0]
			try:
				node.execute(
				    "SELECT bt_index_check"
				    "('o_bank_account_pkey'::regclass)")
				node.execute(
				    "SELECT bt_index_check"
				    "('o_bank_account_token_uniq'::regclass)")
				_idx_ok = True
			except Exception as _e:
				dprint(f'[diag] bt_index_check raised: {_e!r}')
				_idx_ok = False
			tbl_check_ok = (_heap_bad == 0) and _idx_ok
			if not tbl_check_ok:
				dprint(
				    f'[diag] amcheck: heap_bad={_heap_bad} '
				    f'idx_ok={_idx_ok}')
		# PK-authoritative aggregate (count, dist_id, dist_token, sum_bal).
		pk_seq = _pk_force_seq(
		    "SELECT count(*)::int, count(DISTINCT id)::int, "
		    "count(DISTINCT token)::int, sum(balance)::bigint "
		    "FROM o_bank_account",
		    label='aggregate')
		# Per-row PK contents. Printed unconditionally below so a
		# post-processor can correlate each (id, token) pair with the
		# SK token list for per-row PK<->SK matching.
		pk_dump = _pk_force_seq(
		    "SELECT id, balance, token FROM o_bank_account ORDER BY id",
		    label='per-row dump')
		# Duplicate tokens visible from PK -- non-empty => PK has dups
		pk_token_dups = _pk_force_seq(
		    "SELECT token, count(*) FROM o_bank_account "
		    "GROUP BY token HAVING count(*) > 1",
		    label='duplicate tokens')
		# Default-plan canary -- at 100 rows the planner picks Seq Scan
		# so this actually answers from PK heap, NOT SK. Kept for the
		# diagnostic log; not used as an invariant signal.
		sk_default = _default_exec(
		    "SELECT count(DISTINCT token)::int FROM o_bank_account",
		    label='count(DISTINCT token)')
		# Enumerate SK contents directly (forced index path on unique
		# token SK). The query returns (token, id): in orioledb, SK
		# entries implicitly include the PK columns, so an Index Only
		# Scan on the token-unique SK delivers both without touching
		# the PK heap. Used below for token-set diffs AND for per-row
		# ghost mapping (see ghost-row analysis).
		sk_dump = _sk_force_idx(
		    "SELECT token, id FROM o_bank_account ORDER BY token",
		    label='token+id enumeration')
		sk_set = set()
		sk_pairs = set()
		pk_set = set()
		pk_by_id = {}
		if isinstance(sk_dump, list):
			sk_set = {r[0] for r in sk_dump}
			sk_pairs = {(r[0], r[1]) for r in sk_dump}
		if isinstance(pk_dump, list):
			pk_set = {r[2] for r in pk_dump}
			pk_by_id = {r[0]: r[2] for r in pk_dump}
		sk_extra = sorted(sk_set - pk_set)
		sk_missing = sorted(pk_set - sk_set)
		# Ghost-row mapping: SK entries (token, id) where the PK row
		# with that id carries a *different* token. Confirms that the
		# leak is the DELETE-side of an UPDATE not landing on the SK --
		# the row's current token is on PK and (possibly) on SK as a
		# second entry, while the old token still has its (token, id)
		# SK entry pointing at the row that has since moved on.
		# Format: (sk_token, id, pk_token_now). pk_token_now is None
		# if the id is missing from PK entirely (orphan SK entry).
		sk_ghost_rows = sorted(
		    (tok, _id, pk_by_id.get(_id))
		    for tok, _id in sk_pairs
		    if pk_by_id.get(_id) != tok)
		# PK rows whose (token, id) is NOT present in SK -- the
		# INSERT-side of an UPDATE was lost (or the row's SK entry
		# was missed at first insertion).
		sk_missing_rows = sorted(
		    (tok, _id)
		    for _id, tok in pk_by_id.items()
		    if (tok, _id) not in sk_pairs)
		# Duplicate SK entries: (token, id) appearing more than once
		# would not show up in sk_pairs (it dedupes), so detect via
		# the raw dump length vs distinct pair count.
		sk_duplicate_pairs = []
		if isinstance(sk_dump, list) and len(sk_dump) != len(sk_pairs):
			from collections import Counter
			_c = Counter((r[0], r[1]) for r in sk_dump)
			sk_duplicate_pairs = sorted(
			    (tok, _id, n)
			    for (tok, _id), n in _c.items() if n > 1)

		# Drain summary: the writer threads' final pocketed tokens.
		# Kept as a forensic diagnostic only — writers SIGQUIT'd in the
		# PG commit-window (after XLogFlush of COMMIT, before the Python
		# `my_token = to_token` assignment runs) leave `drained_set` with
		# stale pre-tx pockets while the DB reflects the post-commit
		# state, so any invariant that joins `drained_set` against
		# `pk_set`/`sk_set` fires spuriously on every cascade trial.
		# See git history (`drained ∩` checks removed 2026-05-15) and
		# project_v4_investigation_state.md for the full race analysis.
		with drained_lock:
			drained_raw = list(drained_tokens)
		drained_set = {tok for _, tok in drained_raw}
		expected_universe = set(range(1, n_accounts + n_writers + 1))
		universe_union = pk_set | sk_set | drained_set
		universe_extra = sorted(universe_union - expected_universe)

		with errors_lock:
			seen = list(errors)
		print(
		    f'snapshot reads saw inconsistency (torn scan): {seen}')
		print(f'[diag] pk_seq (count, dist_id, dist_token, sum_bal) = '
		      f'{pk_seq!r}')
		print(f'[diag] pk_distinct_tokens (forced) = {pk_distinct_tokens}')
		print(f'[diag] sk_distinct_tokens (forced) = {sk_distinct_tokens}')
		print(f'[diag] sk_default count(DISTINCT token) = {sk_default!r}')
		print(f'[diag] pk_row_count (forced) = {pk_row_count} '
		      f'sk_row_count (forced) = {sk_row_count}')
		print(f'[diag] pk_token_dups = {pk_token_dups!r}')
		print(f'[diag] pk_oor_tokens (outside [1,{n_accounts + n_writers}]) = '
		      f'{pk_oor_tokens!r}')
		print(f'[diag] sk_set\\pk_set (in SK, not PK) = {sk_extra}')
		print(f'[diag] pk_set\\sk_set (in PK, not SK) = {sk_missing}')
		# Per-row ghost mapping (the load-bearing diagnostic for the
		# DELETE-side leak hypothesis): every SK entry (token, id)
		# whose id's PK row carries a *different* token. Each row here
		# is direct evidence that the SK did not delete the old entry
		# when the PK row's token was UPDATEd. The third tuple element
		# is the PK row's current token (None if the id is missing
		# from PK entirely -- orphan SK entry).
		print(f'[diag] sk_ghost_rows (sk_token, id, pk_token_now) = '
		      f'{sk_ghost_rows}')
		# Per-row missing mapping (the INSERT-side leak): every PK row
		# (token, id) for which the SK has no matching entry. Format:
		# (pk_token, id) so it can be diffed against sk_ghost_rows by
		# id directly.
		print(f'[diag] sk_missing_rows (pk_token, id) = '
		      f'{sk_missing_rows}')
		print(f'[diag] sk_duplicate_pairs (token, id, count) = '
		      f'{sk_duplicate_pairs}')
		print(f'[diag] drained_tokens (writer_id, my_token) = {sorted(drained_raw)}')
		print(f'[diag] drained_set (unique) = {sorted(drained_set)}')
		print(f'[diag] universe coverage: '
		      f'{len(universe_union & expected_universe)}/{len(expected_universe)} '
		      f'(pk={len(pk_set)} sk_distinct={len(sk_set)} drained_distinct={len(drained_set)})')
		print(f'[diag] universe_extra (in PK/SK/drained but outside '
		      f'[1,{n_accounts + n_writers}]) = {universe_extra}')
		# Full per-row PK dump and SK token list -- unconditional, so a
		# post-processor can do per-row PK<->SK matching across trials.
		print(f'[diag] pk_dump (id, balance, token) = {pk_dump!r}')
		print(f'[diag] sk_dump (token, id, ordered by token) = {sk_dump!r}')
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

		# Replica invariants.  Run *before* node.stop() so catchup
		# can still reach primary's current LSN and the dump-vs-
		# primary comparison can query both nodes.  Results are
		# stashed in `_replica_violations` and merged into the main
		# `violations` list below.
		_replica_violations = []
		if replica_mode != 'none' and replica is not None:
			# Wait for replica to catch up to primary's current
			# LSN.  Mechanism differs per mode:
			#   logical:   sub_obj.catchup() (LSN of confirmed-
			#              flush vs primary's current_wal_insert).
			#   streaming: replica.catchup() (apply LSN vs
			#              primary's current_wal_insert).
			try:
				if replica_mode == 'logical':
					sub_obj.catchup()
				else:  # streaming
					replica.catchup()
			except Exception as _e:
				_replica_violations.append(
				    f'replica: catchup() failed: {_e!r}')
			# Core scalar invariants on replica.
			try:
				_rc, _rs, _rdt = replica.execute(
				    "SELECT count(*)::int, sum(balance)::bigint, "
				    "count(DISTINCT token)::int "
				    "FROM o_bank_account")[0]
				print(
				    f'[replica:{replica_mode}] rows={_rc} '
				    f'sum={_rs} distinct_tokens={_rdt}')
				if _rc != n_accounts:
					_replica_violations.append(
					    f'replica rows {_rc} != {n_accounts}')
				if _rs != expected_total:
					_replica_violations.append(
					    f'replica sum_balance {_rs} != '
					    f'{expected_total}')
				if _rdt != n_accounts:
					_replica_violations.append(
					    f'replica distinct_tokens {_rdt} != '
					    f'{n_accounts}')
			except Exception as _e:
				_replica_violations.append(
				    f'replica: scalar check failed: {_e!r}')
			# Structural check on replica.  Only meaningful for
			# logical replication, where the replica's data file
			# layout is independent of primary's.  Streaming
			# replicas are byte-identical to primary at catch-up
			# (and AccessExclusiveLock would fail on a hot
			# standby anyway), so we skip the structural check
			# in that mode.
			if replica_mode == 'logical':
				try:
					if STORAGE_ENGINE == 'orioledb':
						_rtbl = replica.execute(
						    "SELECT orioledb_tbl_check"
						    "('o_bank_account'::regclass)")[0][0]
						if not _rtbl:
							_replica_violations.append(
							    'replica: orioledb_tbl_check '
							    'returned false')
					else:
						_rheap_bad = replica.execute(
						    "SELECT count(*)::int FROM "
						    "verify_heapam('o_bank_account')"
						    )[0][0]
						if _rheap_bad != 0:
							_replica_violations.append(
							    f'replica: verify_heapam found '
							    f'{_rheap_bad} corruption rows')
						replica.execute(
						    "SELECT bt_index_check"
						    "('o_bank_account_pkey'::regclass)")
						replica.execute(
						    "SELECT bt_index_check"
						    "('o_bank_account_token_uniq'"
						    "::regclass)")
				except Exception as _e:
					_replica_violations.append(
					    f'replica: structural check raised: '
					    f'{_e!r}')
			# Row-by-row dump comparison: every (id, balance,
			# token) triple must match between primary and
			# replica.
			try:
				_primary_dump = node.execute(
				    "SELECT id, balance, token FROM o_bank_account "
				    "ORDER BY id")
				_replica_dump = replica.execute(
				    "SELECT id, balance, token FROM o_bank_account "
				    "ORDER BY id")
				if _primary_dump != _replica_dump:
					_diffs = [
					    (p, r)
					    for p, r in zip(_primary_dump, _replica_dump)
					    if p != r
					][:5]
					_replica_violations.append(
					    f'replica: PK dump diverges from primary '
					    f'(first {len(_diffs)} diffs): {_diffs}')
			except Exception as _e:
				_replica_violations.append(
				    f'replica: dump comparison failed: {_e!r}')

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
		if sk_distinct_tokens != n_accounts:
			_msg = f'sk_distinct_tokens {sk_distinct_tokens} != {n_accounts}'
			if sk_extra:
				_msg += f' [SK extra (in SK, not PK): {sk_extra}]'
			if sk_missing:
				_msg += f' [SK missing (in PK, not SK): {sk_missing}]'
			if isinstance(pk_oor_tokens, list) and pk_oor_tokens:
				_msg += (
				    f' [out-of-range tokens (outside [1,{n_accounts + n_writers}]): '
				    f'{[r[0] for r in pk_oor_tokens]}]')
			if isinstance(pk_token_dups, list) and pk_token_dups:
				_msg += f' [duplicate tokens: {_fmt_dups(pk_token_dups)}]'
			violations.append(_msg)
		# Direct PK<->SK count comparisons. These fire even if both sides
		# are individually off by the same amount from n_accounts, so they
		# catch a class of bugs the per-side count checks miss.
		if pk_distinct_tokens != sk_distinct_tokens:
			violations.append(
			    f'pk_distinct_tokens ({pk_distinct_tokens}) != '
			    f'sk_distinct_tokens ({sk_distinct_tokens}) '
			    f'-- PK heap and SK index disagree on token set')
		if pk_row_count != sk_row_count:
			violations.append(
			    f'pk_row_count ({pk_row_count}) != '
			    f'sk_row_count ({sk_row_count}) '
			    f'-- PK heap and SK index disagree on row count')
		if universe_extra:
			violations.append(
			    f'token universe extra: {len(universe_extra)} '
			    f'token(s) outside [1,{n_accounts + n_writers}]: '
			    f'{universe_extra}')
		if rows != n_accounts:
			violations.append(f'rows {rows} != {n_accounts}')
		if seen:
			violations.append(
			    f'snapshot reads saw inconsistency: {seen}')
		if not tbl_check_ok:
			violations.append(
			    f'{STORAGE_ENGINE} structural check returned false')
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
			    f'{_fmt_dups(pk_token_dups)}')
		# Replica-side violations were collected before node.stop()
		# while both nodes were still alive; merge them in now.
		violations.extend(_replica_violations)
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
		    tbl_check_ok,
		    f'{STORAGE_ENGINE} structural check on o_bank_account failed')
		self.assertEqual(
		    n_dangling, 0,
		    f'{n_dangling} postgres backends survived node.stop()')

		node.stop()

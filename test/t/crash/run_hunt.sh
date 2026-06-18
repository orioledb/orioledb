#!/usr/bin/env bash
# Run the bank-account stress test N times under the standard
# chaos config and summarize per-trial outcomes.
#
# Usage:
#   ./test/t/crash/run_hunt.sh [TRIALS] [INSTANCE]
#
# Env overrides (all optional; defaults match the production-style hunt):
#   RR_WRITERS=20  RR_ACCOUNTS=100  RR_DURATION=15  RR_ROLLBACKERS=0
#   RR_ASSERT_FIRINGS=3  RR_ASSERT_POINTS=commit_assert
#   RR_REPLICA_MODE=none|logical|streaming  RR_REPLICAS=1
#   RR_PANIC_FATAL=0  RR_SAVE_ALL_LOGS=0  TIMEOUT=90
#
# Stop-event names (RR_ASSERT_POINTS / RR_INJECTION_POINTS) use the
# underscore form registered in stopevents.txt, e.g. commit_assert,
# before_pre_commit_wal_finish, set_csn_guarded, wal_flush.
#
# Output: /tmp/hunt_<instance>.log
#   - One header per trial: === trial N (Ts s, writes=W) ===
#   - If clean: just "clean"
#   - If buggy: every [diag], [explain ...], and `invariant violations:`
#     line from the trial's stdout. The [explain ...] lines prove which
#     scan answered each diagnostic query (sk-forced => SK index-only,
#     pk-forced => PK seq scan).
#   - Per-trial plan-check: `[plan-check trial=N PASS]` if every
#     `[explain sk-forced ...]` block used `o_bank_account_token_uniq`
#     AND every `[explain pk-forced ...]` block used `Seq Scan on
#     o_bank_account` (and did NOT leak into the SK index path).
#     Otherwise one `[plan-check trial=N FAIL] <label> <reason>` line
#     per broken expectation. Catches planner regressions where a
#     "forced" GUC combination silently lets the planner fall back to
#     the wrong tree, which would invalidate the PK-vs-SK diagnostic
#     conclusions downstream.
#   - End of file: `[plan-check summary] P/N trials passed`.

set -u

TRIALS=${1:-30}
INSTANCE=${2:-hunt}

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
VENV_ACTIVATE=${VENV_ACTIVATE:-/home/user/work/venv/bin/activate}

if [ ! -f "$VENV_ACTIVATE" ]; then
	echo "venv activate not found at $VENV_ACTIVATE" >&2
	exit 1
fi
# shellcheck disable=SC1090
source "$VENV_ACTIVATE"

# Trial parameters (env-overridable)
: "${RR_WRITERS:=20}"
: "${RR_ACCOUNTS:=100}"
: "${RR_DURATION:=15}"
: "${RR_ROLLBACKERS:=0}"
: "${RR_INJECTION_POINTS:=NONE}"
: "${RR_ASSERT_POINTS:=commit_assert}"
: "${RR_ASSERT_FIRINGS:=3}"
: "${RR_REPLICA_MODE:=none}"
: "${RR_REPLICAS:=1}"
: "${RR_PANIC_FATAL:=0}"
: "${RR_SAVE_ALL_LOGS:=0}"
: "${TIMEOUT:=90}"

export RR_WRITERS RR_ACCOUNTS RR_DURATION RR_ROLLBACKERS \
	RR_INJECTION_POINTS RR_ASSERT_POINTS RR_ASSERT_FIRINGS \
	RR_REPLICA_MODE RR_REPLICAS \
	RR_PANIC_FATAL RR_SAVE_ALL_LOGS

LOG=/tmp/hunt_${INSTANCE}.log
rm -f "$LOG"

cd "$REPO_ROOT"

CONFIG_LINE="[config] trials=$TRIALS instance=$INSTANCE writers=$RR_WRITERS \
accounts=$RR_ACCOUNTS duration=$RR_DURATION rollbackers=$RR_ROLLBACKERS \
injection_points=$RR_INJECTION_POINTS \
assert_firings=$RR_ASSERT_FIRINGS assert_points=$RR_ASSERT_POINTS \
replica_mode=$RR_REPLICA_MODE replicas=$RR_REPLICAS \
panic_fatal=$RR_PANIC_FATAL timeout=${TIMEOUT}s log=$LOG"
echo "$CONFIG_LINE"
# Record the config (incl. injection_points) at the top of the log file too.
echo "$CONFIG_LINE" >> "$LOG"

# Per-trial plan-check: assert that every sk-forced query actually used
# the token unique SK index and every pk-forced query was answered by
# Seq Scan on the PK heap. Emits PASS or one FAIL line per broken
# expectation. The verifier operates on the trial's captured stdout
# regardless of clean/buggy outcome.
verify_explain_plans() {
	local out=$1
	local trial=$2
	local fails
	fails=$(awk -v trial="$trial" '
		match($0, /^\[explain (sk-forced|pk-forced) [^]]+\] /) {
			header = substr($0, RSTART, RLENGTH - 1)
			plans[header] = plans[header] substr($0, RSTART + RLENGTH) "\n"
		}
		END {
			for (h in plans) {
				p = plans[h]
				kind = (index(h, "sk-forced") ? "sk" : "pk")
				idx_used = (index(p, "using o_bank_account_token_uniq") > 0)
				seq_used = (index(p, "Seq Scan on o_bank_account") > 0)
				if (kind == "sk" && !idx_used) {
					print "[plan-check trial=" trial " FAIL] " h \
					      " did NOT use o_bank_account_token_uniq"
				}
				if (kind == "pk") {
					if (!seq_used)
						print "[plan-check trial=" trial " FAIL] " h \
						      " missing Seq Scan on o_bank_account"
					if (idx_used)
						print "[plan-check trial=" trial " FAIL] " h \
						      " leaked into SK index path"
				}
			}
		}
	' <<< "$out")
	if [ -n "$fails" ]; then
		echo "$fails" >> "$LOG"
		return 1
	fi
	echo "[plan-check trial=$trial PASS]" >> "$LOG"
	return 0
}

plan_pass=0
plan_fail=0
n_clean=0
n_bug=0
n_timeout=0
n_error=0

for trial in $(seq 1 "$TRIALS"); do
	t0=$(date +%s)
	out=$(timeout "$TIMEOUT" env RR_INSTANCE="$INSTANCE" \
		python3 -m unittest \
		test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant \
		2>&1)
	rc=$?
	dt=$(( $(date +%s) - t0 ))
	w=$(echo "$out" | grep -oE 'writes=[0-9]+' | head -1 | cut -d= -f2)
	v=$(echo "$out" | grep -E 'invariant violations: \[' | head -1)

	# Classify trial outcome based on the test process exit code AND
	# whether an invariant-violation line was emitted:
	#   rc=124            -> TIMEOUT (`timeout` killed the process;
	#                       almost always means a hang in test setup
	#                       or in replica.catchup() / sub.catchup())
	#   rc=0              -> clean   (unittest passed every assertion,
	#                       including replica invariants -- they all
	#                       feed into the same violations list)
	#   rc!=0 AND $v set  -> BUG     (test asserted on a non-empty
	#                       violations list)
	#   rc!=0 AND $v empty -> ERROR  (test framework reported a failure
	#                       but emitted no recognized violation -- the
	#                       trial died before reaching the diagnostic
	#                       phase, e.g. primary failed to start because
	#                       a previous trial left an orphan on the port)
	if [ "$rc" -eq 124 ]; then
		status=TIMEOUT
		n_timeout=$((n_timeout + 1))
	elif [ "$rc" -eq 0 ]; then
		status=clean
		n_clean=$((n_clean + 1))
	elif [ -n "$v" ]; then
		status=BUG
		n_bug=$((n_bug + 1))
	else
		status=ERROR
		n_error=$((n_error + 1))
	fi

	echo "=== trial $trial (${dt}s, writes=${w:-?}, exit=$rc, status=$status) ===" >> "$LOG"
	# Dump the resolved stop-event set the test actually armed this trial
	# (RR_INJECTION_POINTS=ALL expands to the full wal_chaos list inside the
	# test; this records the exact names so the log is self-describing).
	echo "$out" | grep -oE "stopevents=[0-9]+ list=\[[^]]*\]" | head -1 >> "$LOG"
	case "$status" in
		clean)
			echo "clean" >> "$LOG"
			;;
		BUG)
			echo "$out" \
				| grep -E "\[diag\]|\[explain |invariant violations" \
				>> "$LOG"
			;;
		TIMEOUT)
			echo "TIMEOUT after ${dt}s (TIMEOUT=${TIMEOUT}s) -- last 30 lines of stdout:" >> "$LOG"
			echo "$out" | tail -30 >> "$LOG"
			;;
		ERROR)
			echo "ERROR exit=$rc (no invariant violation; setup/runtime failure) -- last 30 lines of stdout:" >> "$LOG"
			echo "$out" | tail -30 >> "$LOG"
			;;
	esac

	if verify_explain_plans "$out" "$trial"; then
		plan_pass=$((plan_pass + 1))
		pc=plan-OK
	else
		plan_fail=$((plan_fail + 1))
		pc=plan-FAIL
	fi
	printf 't=%02d %2ss W=%s %-7s %s\n' "$trial" "${dt}" "${w:-?}" \
		"$status" "$pc"
done

echo "HUNT-DONE" >> "$LOG"
echo "[summary] clean=${n_clean} BUG=${n_bug} TIMEOUT=${n_timeout} ERROR=${n_error}" >> "$LOG"
echo "[plan-check summary] ${plan_pass}/${TRIALS} trials passed" >> "$LOG"
echo "[done] $LOG  trials: clean=${n_clean} BUG=${n_bug} TIMEOUT=${n_timeout} ERROR=${n_error}  plan-check: ${plan_pass}/${TRIALS} pass"

#!/usr/bin/env bash
# Run the bank-account stress test N times under the standard
# chaos config and summarize per-trial outcomes.
#
# Usage:
#   ./test/t/crash/run_hunt.sh [TRIALS] [INSTANCE]
#
# Env overrides (all optional; defaults match the production-style hunt):
#   RR_WRITERS=20  RR_ACCOUNTS=100  RR_DURATION=15  RR_ROLLBACKERS=0
#   RR_ASSERT_FIRINGS=3  RR_ASSERT_POINTS=orioledb-commit-assert
#   RR_PANIC_FATAL=0  RR_SAVE_ALL_LOGS=0  TIMEOUT=90
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
: "${RR_ASSERT_POINTS:=orioledb-commit-assert}"
: "${RR_ASSERT_FIRINGS:=3}"
: "${RR_PANIC_FATAL:=0}"
: "${RR_SAVE_ALL_LOGS:=0}"
: "${TIMEOUT:=90}"

export RR_WRITERS RR_ACCOUNTS RR_DURATION RR_ROLLBACKERS \
	RR_INJECTION_POINTS RR_ASSERT_POINTS RR_ASSERT_FIRINGS \
	RR_PANIC_FATAL RR_SAVE_ALL_LOGS

LOG=/tmp/hunt_${INSTANCE}.log
rm -f "$LOG"

cd "$REPO_ROOT"

echo "[config] trials=$TRIALS instance=$INSTANCE writers=$RR_WRITERS \
accounts=$RR_ACCOUNTS duration=$RR_DURATION rollbackers=$RR_ROLLBACKERS \
assert_firings=$RR_ASSERT_FIRINGS panic_fatal=$RR_PANIC_FATAL \
timeout=${TIMEOUT}s log=$LOG"

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

for trial in $(seq 1 "$TRIALS"); do
	t0=$(date +%s)
	out=$(timeout "$TIMEOUT" env RR_INSTANCE="$INSTANCE" \
		python3 -m unittest \
		test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant \
		2>&1)
	dt=$(( $(date +%s) - t0 ))
	w=$(echo "$out" | grep -oE 'writes=[0-9]+' | head -1 | cut -d= -f2)
	v=$(echo "$out" | grep -E 'invariant violations: \[' | head -1)
	echo "=== trial $trial (${dt} s, writes=${w:-?}) ===" >> "$LOG"
	if [ -n "$v" ]; then
		echo "$out" \
			| grep -E "\[diag\]|\[explain |invariant violations" \
			>> "$LOG"
	else
		echo "clean" >> "$LOG"
	fi
	if verify_explain_plans "$out" "$trial"; then
		plan_pass=$((plan_pass + 1))
		pc=plan-OK
	else
		plan_fail=$((plan_fail + 1))
		pc=plan-FAIL
	fi
	printf 't=%02d %2ss W=%s %s %s\n' "$trial" "${dt}" "${w:-?}" \
		"$([ -n "$v" ] && echo BUG || echo clean)" "$pc"
done

echo "HUNT-DONE" >> "$LOG"
echo "[plan-check summary] ${plan_pass}/${TRIALS} trials passed" >> "$LOG"
echo "[done] $LOG (plan-check: ${plan_pass}/${TRIALS} pass, ${plan_fail} fail)"

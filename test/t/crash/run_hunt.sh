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
	printf 't=%02d %2ss W=%s %s\n' "$trial" "${dt}" "${w:-?}" \
		"$([ -n "$v" ] && echo BUG || echo clean)"
done

echo "HUNT-DONE" >> "$LOG"
echo "[done] $LOG"

#!/bin/bash
#
# Generic hang watchdog for jobs without a replication-specific detector
# (e.g. the pg_upgrade job).  It dumps backtraces of every postgres backend
# WHILE a hang is still live -- before a step `timeout-minutes` tears the
# cluster down and the runner reaps the orphaned backends with no trace.
#
# Unlike ci/hang_watchdog.sh (which keys on the standby replay-vs-receive gap
# of 027_stream_regress), this one is signature-agnostic and fires when either:
#
#   - a postgres backend SPINS: it stays busy (>= BUSY_PCT of the sampling
#     interval on-CPU, read straight from /proc/<pid>/stat) for STUCK_CONSEC
#     consecutive samples.  This catches the uninterruptible btree-modify spin
#     class (o_tbl_insert / o_tbl_delete looping with no CHECK_FOR_INTERRUPTS),
#     which is exactly what the pg_upgrade regression run hit; or
#
#   - a FALLBACK timer elapses (FALLBACK_SECONDS) while postgres backends are
#     still alive -- so a deadlock / idle-wait hang, which shows no CPU, still
#     gets a backtrace shortly before a 30-minute step timeout would kill it.
#
# Detection reads only ps titles and /proc, so it never connects to (or
# perturbs) a healthy cluster.  gdb (which briefly stops the target) is
# attached only once a process is already judged frozen.

set -u

export PATH="${GITHUB_WORKSPACE:-}/pgsql/bin:$PATH"
here="$(dirname "$0")"

INTERVAL="${WATCHDOG_INTERVAL:-15}"                 # seconds between samples
STUCK_CONSEC="${WATCHDOG_STUCK_CONSEC:-4}"          # busy samples -> spinning
BUSY_PCT="${WATCHDOG_BUSY_PCT:-50}"                 # >= this %CPU over interval = busy
FALLBACK_SECONDS="${WATCHDOG_FALLBACK_SECONDS:-1500}"  # ~25min: dump before a 30min step timeout
SNAP_ROUNDS="${WATCHDOG_SNAP_ROUNDS:-3}"            # backtrace snapshots per dump
SNAP_INTERVAL="${WATCHDOG_SNAP_INTERVAL:-4}"
MAX_DUMPS="${WATCHDOG_MAX_DUMPS:-3}"                # stop after this many dumps

CLK=$(getconf CLK_TCK 2>/dev/null || echo 100)
declare -A prev_cpu                                 # pid -> last (utime+stime) ticks
declare -A busy_streak                              # pid -> consecutive busy samples

# utime+stime of a backend, in clock ticks (empty if the pid is gone).
cpu_ticks() {
    awk '{print $14 + $15}' "/proc/$1/stat" 2>/dev/null
}

dump_one() {
    local p="$1" psout
    psout=$(ps -o pid,command "$p" 2>/dev/null | tail -n +2)
    [ -z "$psout" ] && return
    echo "--- pid $psout ---"
    # Signal mask / pending -- cheap, non-perturbing, straight from /proc.
    grep -E "SigBlk|SigPnd|SigCgt" "/proc/$p/status" 2>/dev/null
    sudo gdb --batch --quiet \
        -ex "thread apply all bt full" \
        -ex 'eval "p *((LWLockHandle (*) [%u]) held_lwlocks)", num_held_lwlocks' \
        -ex 'eval "p *((MyLockedPage (*) [%u]) myLockedPages)", numberOfMyLockedPages' \
        -ex "source $here/dump_stuck_pages.py" \
        -ex "print InterruptPending" \
        -ex "print InterruptHoldoffCount" \
        -ex "print CritSectionCount" \
        -ex "quit" \
        -p "$p" 2>/dev/null
}

dump_all() {
    local tag="$1" r p
    for r in $(seq 1 "$SNAP_ROUNDS"); do
        echo "::group::WATCHDOG $tag snapshot $r/$SNAP_ROUNDS at $(date -u +%H:%M:%S)"
        pgrep postgres | xargs -r ps -o pid,command
        for p in $(pgrep postgres); do
            dump_one "$p"
        done
        echo "::endgroup::"
        [ "$r" -lt "$SNAP_ROUNDS" ] && sleep "$SNAP_INTERVAL"
    done
}

echo "hang_watchdog_cpu: started (interval=${INTERVAL}s, busy>=${BUSY_PCT}% x${STUCK_CONSEC} => spin; fallback ${FALLBACK_SECONDS}s)"

start=$SECONDS
dumps=0

while [ "$dumps" -lt "$MAX_DUMPS" ]; do
    sleep "$INTERVAL"

    spun=""
    for p in $(pgrep postgres 2>/dev/null); do
        cur=$(cpu_ticks "$p")
        [ -z "$cur" ] && continue
        if [ -n "${prev_cpu[$p]:-}" ]; then
            delta=$(( cur - prev_cpu[$p] ))
            # busy% over the interval = 100 * delta_ticks / (CLK * INTERVAL);
            # compare without floats: 100*delta >= BUSY_PCT * CLK * INTERVAL.
            if [ "$(( 100 * delta ))" -ge "$(( BUSY_PCT * CLK * INTERVAL ))" ]; then
                busy_streak[$p]=$(( ${busy_streak[$p]:-0} + 1 ))
                [ "${busy_streak[$p]}" -ge "$STUCK_CONSEC" ] && spun="$p"
            else
                busy_streak[$p]=0
            fi
        fi
        prev_cpu[$p]=$cur
    done

    if [ -n "$spun" ]; then
        echo "hang_watchdog_cpu: backend $spun spinning (>=${BUSY_PCT}% CPU for ${STUCK_CONSEC} samples) -- dumping backtraces"
        pgrep -a postgres 2>/dev/null || true
        dump_all "cpu-spin"
        dumps=$((dumps + 1))
        busy_streak=()          # require a fresh spin streak before dumping again
        continue
    fi

    if [ "$(( SECONDS - start ))" -ge "$FALLBACK_SECONDS" ] && pgrep postgres >/dev/null 2>&1; then
        echo "hang_watchdog_cpu: fallback timer ($((SECONDS - start))s) with live backends -- dumping backtraces"
        dump_all "time-fallback"
        dumps=$((dumps + 1))
        start=$SECONDS          # re-arm the fallback window
    fi
done

echo "hang_watchdog_cpu: reached MAX_DUMPS=${MAX_DUMPS}, exiting"

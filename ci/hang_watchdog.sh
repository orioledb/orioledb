#!/bin/bash
#
# Background watchdog for the flaky streaming-regress replay hang.
#
# The 027_stream_regress TAP test (and the manual replication-regress run)
# spin up their own primary+standby.  When the standby's replay freezes, the
# test's poll_query_until eventually times out and the TAP harness tears the
# nodes down -- so the `always()` list_stuck.sh step at the end finds nothing
# left to backtrace.  This watchdog runs *during* the Check step and dumps
# backtraces WHILE the hang is still live.
#
# Detection uses only `ps` titles, so it never connects to (or perturbs) a
# healthy cluster:
#   - the startup process advertises "startup recovering <WAL segment>", which
#     advances as replay proceeds;
#   - a "walreceiver streaming <LSN>" is present while streaming.
# If the startup segment stays unchanged for several consecutive samples while
# a walreceiver is up, replay is stuck -> dump.  gdb (which briefly stops the
# target) is attached only once we've decided the processes are already frozen.
#
# Output goes to stdout; the launcher redirects it to a file that
# list_stuck.sh surfaces at the end of the job.

set -u

INTERVAL="${WATCHDOG_INTERVAL:-15}"      # seconds between samples
STUCK_CONSEC="${WATCHDOG_STUCK_CONSEC:-3}"  # unchanged samples -> "stuck"
SNAP_ROUNDS="${WATCHDOG_SNAP_ROUNDS:-3}"    # backtrace snapshots per dump
SNAP_INTERVAL="${WATCHDOG_SNAP_INTERVAL:-4}"
MAX_DUMPS="${WATCHDOG_MAX_DUMPS:-3}"        # stop after this many dumps

here="$(dirname "$0")"
prev_sig=""
same=0
dumps=0

dump_all() {
    local tag="$1"
    local r p psout
    for r in $(seq 1 "$SNAP_ROUNDS"); do
        echo "::group::WATCHDOG $tag snapshot $r/$SNAP_ROUNDS at $(date -u +%H:%M:%S)"
        pgrep postgres | xargs -r ps -o pid,command
        for p in $(pgrep postgres); do
            psout=$(ps -o pid,command "$p" 2>/dev/null | tail -n +2)
            [ -z "$psout" ] && continue
            echo "--- pid $psout ---"
            sudo gdb --batch --quiet \
                -ex "thread apply all bt full" \
                -ex 'eval "p *((LWLockHandle (*) [%u]) held_lwlocks)", num_held_lwlocks' \
                -ex 'eval "p *((MyLockedPage (*) [%u]) myLockedPages)", numberOfMyLockedPages' \
                -ex "source $here/dump_stuck_pages.py" \
                -ex "quit" \
                -p "$p" 2>/dev/null
        done
        echo "::endgroup::"
        [ "$r" -lt "$SNAP_ROUNDS" ] && sleep "$SNAP_INTERVAL"
    done
}

echo "hang_watchdog: started (interval=${INTERVAL}s, stuck after ${STUCK_CONSEC} unchanged samples)"

while [ "$dumps" -lt "$MAX_DUMPS" ]; do
    sleep "$INTERVAL"

    # Coarse replay position(s) and whether a walreceiver is streaming.
    sig=$(pgrep -a postgres 2>/dev/null \
          | grep -oE "startup recovering [0-9A-F]+" | sort -u | tr '\n' '|')
    wr=$(pgrep -a postgres 2>/dev/null | grep -c "walreceiver")

    if [ -z "$sig" ] || [ "$wr" -eq 0 ]; then
        # No standby in recovery right now.
        prev_sig=""; same=0
        continue
    fi

    if [ "$sig" = "$prev_sig" ]; then
        same=$((same + 1))
    else
        same=1
        prev_sig="$sig"
    fi

    if [ "$same" -ge "$STUCK_CONSEC" ]; then
        echo "hang_watchdog: standby replay STUCK for $((same * INTERVAL))s" \
             "(recovering '${sig}', walreceiver up) -- dumping backtraces"
        # Show the receive-vs-replay gap straight from ps titles.
        pgrep -a postgres 2>/dev/null | grep -E "startup recovering|walreceiver|walsender" || true
        dump_all "stuck-replay"
        dumps=$((dumps + 1))
        same=0   # require a fresh stuck streak before dumping again
    fi
done

echo "hang_watchdog: reached MAX_DUMPS=${MAX_DUMPS}, exiting"

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
STUCK_CONSEC="${WATCHDOG_STUCK_CONSEC:-4}"  # unchanged samples -> candidate
GAP_MIN_SEG="${WATCHDOG_GAP_MIN_SEG:-3}"    # receive must lead replay by >= this many 16MB segments
SNAP_ROUNDS="${WATCHDOG_SNAP_ROUNDS:-3}"    # backtrace snapshots per dump
SNAP_INTERVAL="${WATCHDOG_SNAP_INTERVAL:-4}"
MAX_DUMPS="${WATCHDOG_MAX_DUMPS:-3}"        # stop after this many dumps

here="$(dirname "$0")"
prev_replay=""
same=0
dumps=0

# Absolute 16MB-segment number of the most-behind standby's replay position,
# parsed from "startup recovering <24-hex WAL filename>" (logid*256 + seg).
replay_seg() {
    pgrep -a postgres 2>/dev/null \
        | grep -oE "recovering [0-9A-F]{24}" | awk '{print $2}' \
        | while read -r w; do echo $(( 16#${w:8:8} * 256 + 16#${w:16:8} )); done \
        | sort -n | head -1
}

# Absolute 16MB-segment number of the furthest-ahead streamed position, parsed
# from "streaming <hi>/<lo>" of any walsender/walreceiver (hi*256 + lo/16MB).
recv_seg() {
    pgrep -a postgres 2>/dev/null \
        | grep -oE "streaming [0-9A-F]+/[0-9A-F]+" | awk '{print $2}' \
        | while read -r l; do echo $(( 16#${l%/*} * 256 + 16#${l#*/} / 16777216 )); done \
        | sort -n | tail -1
}

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

    rseg=$(replay_seg)
    wr=$(pgrep -a postgres 2>/dev/null | grep -c "walreceiver")

    if [ -z "$rseg" ] || [ "$wr" -eq 0 ]; then
        # No standby in recovery right now.
        prev_replay=""; same=0
        continue
    fi

    # Count how long replay has sat at the same segment.
    if [ "$rseg" = "$prev_replay" ]; then
        same=$((same + 1))
    else
        same=1
        prev_replay="$rseg"
    fi

    [ "$same" -lt "$STUCK_CONSEC" ] && continue

    # Frozen long enough -- but only a hang if the receive side is far ahead of
    # the frozen replay (the real signature: flush hundreds of MB past replay).
    # A benign quiet spell has receive ~= replay (small gap) -> skip.
    vseg=$(recv_seg)
    [ -z "$vseg" ] && continue
    gap=$(( vseg - rseg ))
    if [ "$gap" -lt "$GAP_MIN_SEG" ]; then
        continue
    fi

    echo "hang_watchdog: standby replay STUCK for $((same * INTERVAL))s at" \
         "segment $rseg while receive reached $vseg (gap ${gap} x16MB) -- dumping backtraces"
    pgrep -a postgres 2>/dev/null | grep -E "startup recovering|walreceiver|walsender" || true
    dump_all "stuck-replay"
    dumps=$((dumps + 1))
    same=0   # require a fresh stuck streak before dumping again
done

echo "hang_watchdog: reached MAX_DUMPS=${MAX_DUMPS}, exiting"

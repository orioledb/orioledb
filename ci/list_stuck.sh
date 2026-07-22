# Take several snapshots a few seconds apart.  A process that is genuinely
# stuck shows an identical stack (and the same held locks / locked pages)
# across rounds, while one merely making slow progress moves between rounds --
# and a wait-for cycle can be confirmed to persist rather than being a
# one-frame coincidence.  Tunable via env.
SNAP_ROUNDS="${STUCK_SNAP_ROUNDS:-3}"
SNAP_INTERVAL="${STUCK_SNAP_INTERVAL:-5}"

pgrep memcheck | xargs -r ps
pgrep python | xargs -r ps

for round in $(seq 1 "$SNAP_ROUNDS"); do
    echo "=== stuck-process snapshot round $round/$SNAP_ROUNDS at $(date -u +%H:%M:%S) ==="
    pgrep postgres | xargs -r ps
    for process in $(pgrep postgres); do
        psout=$(ps -o pid,command $process)
        status=$?
        if [ $status -eq 0 ]; then
            psout=$(echo -ne "$psout" | tail +2)
            echo "::group::Backtrace [round $round/$SNAP_ROUNDS] $psout"
            echo -e $psout
            sudo gdb --batch --quiet \
                -ex "thread apply all bt full" \
                -ex 'eval "p *((LWLockHandle (*) [%u]) held_lwlocks)", num_held_lwlocks' \
                -ex 'eval "p *((MyLockedPage (*) [%u]) myLockedPages)", numberOfMyLockedPages' \
                -ex "source $(dirname "$0")/dump_stuck_pages.py" \
                -ex "quit" \
                -p $process
            echo ::endgroup::
            echo $psout
        fi
    done
    if [ "$round" -lt "$SNAP_ROUNDS" ]; then
        sleep "$SNAP_INTERVAL"
    fi
done

# Tail each live server's log once, after the snapshots.
for process in $(pgrep postgres); do
    psout=$(ps -o pid,command $process 2>/dev/null | tail +2)
    if [[ "$psout" =~ ^.*\ -D\ /tmp/([a-z0-9_]+)/.*$ ]]; then
        logfile="/tmp/${BASH_REMATCH[1]}/logs/postgresql.log"
        echo ::group::tail -n 100 $logfile
        tail -n 100 $logfile
        echo ::endgroup::
    fi
done

for process in $(pgrep memcheck); do
    psout=$(ps -o pid,command $process)
    status=$?
    if [ $status -eq 0 ]; then
        psout=$(echo -ne "$psout" | tail +2)
        psout=$(echo $psout | sed 's/\([0-9]\+\).*initdb /\1 /')
        if [[ $psout == *"/postgres"* ]]; then
            echo $psout >command_$process.log
            mkfifo vgdb-$process-input
            tail -f vgdb-$process-input | gdb --quiet \
                -ex "target remote | vgdb --pid=$process" \
                -ex "thread apply all bt full" \
                -ex 'eval "p *((LWLockHandle (*) [%u]) held_lwlocks)", num_held_lwlocks' \
                -ex 'eval "p *((MyLockedPage (*) [%u]) myLockedPages)", numberOfMyLockedPages' \
                -ex "source $(dirname "$0")/dump_stuck_pages.py" \
                -ex "handle all nostop pass" \
                -ex "c" \
                $(which postgres) >vgdb_$process.log 2>&1 &
        fi
    fi
done

for process in $(pgrep memcheck); do
    psout=$(ps -o pid,command $process)
    status=$?
    if [ $status -eq 0 ]; then
        psout=$(echo -ne "$psout" | tail +2)
        psout=$(echo $psout | sed 's/\([0-9]\+\).*initdb /\1 /')
        if [[ $psout == *"/postgres"* ]]; then
            echo "quit" > vgdb-$process-input
        fi
    fi
done

pkill -KILL tail
pkill -KILL gdb
pkill -KILL postgres
pkill -KILL memcheck
rm vgdb-*-input

for vgdb_file in vgdb_*.log; do
    if [ -e "${vgdb_file}" ]; then
        echo $vgdb_file
        pid=$(echo $vgdb_file | sed 's/vgdb_\(.*\)\.log/\1/')
        echo ::group::{Backtrace VALGRIND $pid}
        echo $vgdb_file
        cat $vgdb_file | awk '/\(No debugging symbols.*|SIG.*|Reading symbols from|EXC_/ {} !/\(No debugging symbols.*|SIG.*|Reading symbols from|EXC_/ { print }'
        # rm $vgdb_file
        echo ::endgroup::
        command=$(cat command_$pid.log)
        # rm command_$process.log
        if [[ "$command" =~ ^.*\ -D\ /tmp/([a-z0-9_]+)/.*$ ]]; then
            logfile="/tmp/${BASH_REMATCH[1]}/logs/postgresql.log"
            echo ::group::tail -n 100 $logfile
            tail -n 100 $logfile
            echo ::endgroup::
        fi
    fi
done
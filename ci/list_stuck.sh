#!/bin/bash
#
# Dump the server log belonging to a postgres process, given its command line.
#
# The data directory tells us where to look, and there are two layouts to
# cope with: the regress/isolation one, /tmp/<name>/data with the log beside
# it, and testgres', which puts the cluster under test/tmp_check_t/<name>/data
# and the log in a sibling logs/ directory.  Matching only the first is why
# the hang dumps for every testgres test used to arrive with no log at all.
#
dump_server_log() {
    local datadir="$1"
    local candidate

    for candidate in \
        "$(dirname "$datadir")/logs/postgresql.log" \
        "$datadir/../logs/postgresql.log" \
        "$datadir/logs/postgresql.log" \
        "$datadir"/log/postgresql*.log \
        "$datadir"/log/*.log
    do
        if [ -f "$candidate" ]; then
            echo ::group::tail -n 100 "$candidate"
            tail -n 100 "$candidate"
            echo ::endgroup::
            return
        fi
    done
    echo "no server log found for -D $datadir"
}

pgrep postgres | xargs -r ps
pgrep memcheck | xargs -r ps
pgrep python | xargs -r ps

for process in $(pgrep postgres); do
    psout=$(ps -o pid,command $process)
    status=$?
    if [ $status -eq 0 ]; then
        psout=$(echo -ne "$psout" | tail +2)
        echo ::group::Backtrace $psout
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
        if [[ "$psout" =~ \ -D\ ([^[:space:]]+) ]]; then
            dump_server_log "${BASH_REMATCH[1]}"
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
            echo $psout >command_$process.log
            mkfifo vgdb-$process-input
            tail -f vgdb-$process-input | gdb --quiet \
                -ex "target remote | vgdb --pid=$process" \
                -ex "thread apply all bt full" \
                -ex "frame function shm_mq_send_bytes" \
                -ex "p *mqh" \
                -ex "p *mqh->mqh_queue" \
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

#
# Let the vgdb captures finish writing.  Killing them the moment the "quit"
# is queued races the capture, and a dump that loses that race carries only
# gdb's connection preamble -- which is how issue #1197 cost a full round of
# guessing.  Wait, but not for ever: a gdb that is itself stuck must not hold
# up the rest of the teardown.
#
for _ in $(seq 60); do
    jobs -rp | grep -q . || break
    sleep 1
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
        if [[ "$command" =~ \ -D\ ([^[:space:]]+) ]]; then
            dump_server_log "${BASH_REMATCH[1]}"
        fi
    fi
done
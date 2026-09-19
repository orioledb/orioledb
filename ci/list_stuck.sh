#!/bin/bash

# Dump the PostgreSQL log belonging to a stuck process.
#
# The data directory comes from the process's own -D, and the log lives
# beside it: testgres keeps <base>/logs/postgresql.log next to <base>/data,
# while the pg_ctl-driven scripts keep it where they were told to.  Only the
# /tmp/ form used to be handled, so a hang in any testgres test arrived with
# no server log at all -- which is how issue #1197 cost a round with nobody
# able to read what the node had been doing.
dump_server_log() {
	local command="$1"
	local datadir logfile

	[[ "$command" =~ -D\ ([^ ]+) ]] || return 0
	datadir="${BASH_REMATCH[1]}"

	for logfile in "$(dirname "$datadir")/logs/postgresql.log" \
	               "$(dirname "$datadir")/pg.log" \
	               "$datadir/log/postgresql.log"; do
		if [ -f "$logfile" ]; then
			echo "::group::tail -n 200 $logfile"
			tail -n 200 "$logfile"
			echo ::endgroup::
			return 0
		fi
	done
	echo "no server log found next to $datadir"
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
            -ex "frame function shm_mq_send_bytes" \
            -ex "p *mqh->mqh_queue" \
            -ex "frame function worker_queue_flush" \
            -ex "p *state" \
            -ex "source $(dirname "$0")/dump_stuck_pages.py" \
            -ex "quit" \
            -p $process
        echo ::endgroup::
        echo $psout
        dump_server_log "$psout"
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
                -ex "frame function shm_mq_send_bytes" \
                -ex "p *mqh->mqh_queue" \
                -ex "frame function worker_queue_flush" \
                -ex "p *state" \
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

# Let the gdb instances finish writing.  They are fed "quit" just above,
# but killing them the instant afterwards races the dump: one run of issue
# #1197 kept nothing but gdb's connection preamble, and that hang had to be
# guessed at for a round instead of read.
for _ in $(seq 1 60); do
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
        dump_server_log "$command"
    fi
done
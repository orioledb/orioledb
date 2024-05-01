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
        sudo gdb --batch --quiet -ex "thread apply all bt full" -ex "quit" -p $process
        echo ::endgroup::
        echo $psout
        if [[ "$psout" =~ ^.*\ -D\ /tmp/([a-z0-9_]+)/.*$ ]]; then
            logfile="/tmp/${BASH_REMATCH[1]}/logs/postgresql.log"
            echo ::group::tail -n 100 $logfile
            tail -n 100 $logfile
            echo ::endgroup::
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
            tail -f vgdb-$process-input | gdb --quiet -ex "target remote | vgdb --pid=$process" -ex "thread apply all bt full" -ex "handle all nostop pass" -ex "c" $(which postgres) >vgdb_$process.log 2>&1 &
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
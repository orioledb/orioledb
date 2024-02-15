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
    fi
done

for process in $(pgrep memcheck); do
    psout=$(ps -o pid,command $process)
    status=$?
    if [ $status -eq 0 ]; then
        psout=$(echo -ne "$psout" | tail +2)
        psout=$(echo $psout | sed 's/\([0-9]\+\).*initdb /\1 /')
        if [[ $psout == *"/postgres"* ]]; then
            echo ::group::{Backtrace VALGRIND $psout}
            echo -e $psout
            gdb --batch --quiet -ex "target remote | vgdb --pid=$process" -ex "thread apply all bt full" -ex "quit" $(which postgres)
            echo ::endgroup::
        fi
    fi
done
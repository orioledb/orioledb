#!/bin/bash

# Runs the jepsen-append internal-consistency hunt (ORI-229) against a
# freshly initialised cluster.  Meant to be driven by .github/workflows/
# rr-hunt.yml after ci/build.sh, but works locally too as long as pgsql/ is
# an installed patched PostgreSQL with orioledb built against it.

set -eux

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$GITHUB_WORKSPACE/python3-venv/bin:$PATH"

DATA="$GITHUB_WORKSPACE/rrdata"
PGLOG="$GITHUB_WORKSPACE/rr_pg.log"
PORT="${RR_PORT:-5678}"
WORKERS="${RR_WORKERS:-50}"
SECONDS_TO_RUN="${RR_SECONDS:-900}"
ISOLATION="${RR_ISOLATION:-repeatable-read}"
SEED="${RR_SEED:-1}"

ulimit -c unlimited -S
mkdir -p /tmp/rrcores
sudo sh -c 'echo "/tmp/rrcores/%t_%p.core" > /proc/sys/kernel/core_pattern'

rm -rf "$DATA"
initdb -D "$DATA" --no-locale -E UTF8 -U "$(id -un)"

cat >> "$DATA/postgresql.conf" <<EOF
port = $PORT
listen_addresses = '127.0.0.1'
shared_preload_libraries = 'orioledb'
max_connections = 300
orioledb.main_buffers = 256MB
orioledb.undo_buffers = 64MB
default_transaction_isolation = '$(echo "$ISOLATION" | tr '-' ' ')'
log_min_messages = warning
log_line_prefix = '%m [%p] '
EOF

pg_ctl -D "$DATA" -l "$PGLOG" -w start

status=0
python3 orioledb/ci/rr_append_hunt.py \
	--port "$PORT" --setup \
	--user "$(id -un)" \
	--workers "$WORKERS" \
	--seconds "$SECONDS_TO_RUN" \
	--isolation "$ISOLATION" \
	--seed "$SEED" || status=$?

pg_ctl -D "$DATA" -m immediate stop || true

echo "=== server log ==="
tail -n 200 "$PGLOG" || true

# A crash, a PANIC or an assertion is just as interesting as an anomaly.
if grep -Eq 'PANIC|TRAP:|server process .* was terminated' "$PGLOG"; then
	echo "server log contains a crash/PANIC"
	status=1
fi

exit $status

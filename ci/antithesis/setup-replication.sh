#!/bin/bash

if [ "x$REPLICATE_FROM" == "x" ]; then

cat >> ${PGDATA}/postgresql.conf <<EOF
listen_addresses='*'
wal_level = hot_standby
max_wal_senders = $PG_MAX_WAL_SENDERS
wal_keep_size = $PG_WAL_KEEP_SIZE
hot_standby = on
EOF

else

cat > ${PGDATA}/postgresql.conf <<EOF
listen_addresses='*'
wal_level = hot_standby
max_wal_senders = $PG_MAX_WAL_SENDERS
wal_keep_size = $PG_WAL_KEEP_SIZE
hot_standby = on
primary_conninfo = 'host=${REPLICATE_FROM} port=5432 user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}'
EOF
touch ${PGDATA}/standby.signal

fi

initdb pgdata --locale=C
echo -e "wal_level logical\nmax_replication_slots 1\nshared_preload_libraries = 'orioledb.so'" > ./pgdata/postgresql.auto.conf

pg-ctl -D pgdata -l logfile start

psql -dpostgres -c "CREATE EXTENSION IF NOT EXISTS orioledb;"
psql -dpostgres -c "CREATE TABLE IF NOT EXISTS t(id integer PRIMARY KEY, v1 text, v2 text, v3 text) using orioledb;"
psql -dpostgres -c "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);"

for i in {1..5}
do
	psql -dpostgres -c "INSERT INTO t VALUES ('$i', repeat('Pg', 2500), repeat('ab', 2500), repeat('xy', 2500));"
	psql -dpostgres -c "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
done


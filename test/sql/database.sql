CREATE DATABASE heapdb_template;
\c heapdb_template

CREATE TABLE heap_table (i int) USING heap;

\c postgres

-- This CREATE DATABASE should succeed
CREATE DATABASE heapdb TEMPLATE heapdb_template;

CREATE DATABASE orioledb_template;
\c orioledb_template

CREATE EXTENSION orioledb;

CREATE TABLE oriole_table (i int) USING orioledb;

\c postgres

-- This CREATE DATABASE should fail
CREATE DATABASE orioledb TEMPLATE orioledb_template;

DROP DATABASE orioledb_template;
DROP DATABASE heapdb;
DROP DATABASE heapdb_template;

-- Check pg_database_size()
CREATE EXTENSION orioledb;
CREATE DATABASE oriole_database;
\c oriole_database

-- generate pseudo-random string function in deterministic way
CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
        AS $$
                SELECT substr(string_agg(
                                                substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
                                ''), 1, length)
                FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

CHECKPOINT;
select round(pg_database_size('oriole_database'), -6);

CREATE EXTENSION orioledb;
CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
INSERT INTO oriole_table(t) select generate_string(i, 270) FROM  generate_series(1, 10000) as i;
CHECKPOINT;
select round(pg_database_size('oriole_database'), -6);

SET allow_in_place_tablespaces = true;
CREATE TABLESPACE dbsize_tblspace LOCATION '';
CREATE TABLE oriole_table_tblspc (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb TABLESPACE dbsize_tblspace;
INSERT INTO oriole_table_tblspc(t) select generate_string(i, 270) FROM  generate_series(1, 10000) as i;
CHECKPOINT;
select round(pg_database_size('oriole_database'), -6);

\d+ oriole_table
\d+ oriole_table_tblspc

\c postgres
DROP DATABASE oriole_database;
SELECT orioledb_rewind_sync();
DROP TABLESPACE dbsize_tblspace;
DROP EXTENSION orioledb CASCADE;

CREATE EXTENSION orioledb;
CREATE DATABASE mixed_database;
\c mixed_database

-- generate pseudo-random string function in deterministic way
CREATE FUNCTION generate_string(seed integer, length integer) RETURNS text
        AS $$
                SELECT substr(string_agg(
                                                substr(encode(sha256(seed::text::bytea || '_' || i::text::bytea), 'hex'), 1, 21),
                                ''), 1, length)
                FROM generate_series(1, (length + 20) / 21) i; $$
LANGUAGE SQL;

CREATE EXTENSION orioledb;
CHECKPOINT;
select round(pg_database_size('mixed_database'), -6);

CREATE TABLE heap_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING heap;
INSERT INTO heap_table(t) select generate_string(i, 270) FROM  generate_series(1, 10000) as i;
CHECKPOINT;
select round(pg_database_size('mixed_database'), -6);

CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
INSERT INTO oriole_table(t) select generate_string(i, 270) FROM  generate_series(1, 10000) as i;
CHECKPOINT;
select round(pg_database_size('mixed_database'), -6);

SET allow_in_place_tablespaces = true;
CREATE TABLESPACE dbsize_tblspace LOCATION '';

CREATE TABLE oriole_table_tblspc (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb TABLESPACE dbsize_tblspace;
INSERT INTO oriole_table_tblspc(t) select generate_string(i, 270) FROM  generate_series(1, 10000) as i;
CHECKPOINT;
select round(pg_database_size('mixed_database'), -6);

\d+ oriole_table
\d+ heap_table
\d+ oriole_table_tblspc

\c postgres

DROP DATABASE mixed_database;
SELECT orioledb_rewind_sync();
DROP TABLESPACE dbsize_tblspace;
DROP EXTENSION orioledb CASCADE;

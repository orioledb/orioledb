-----
-- ALTER COLUMN SET STORAGE tests for OrioleDB
-- Storage types: 'p' = PLAIN, 'm' = MAIN, 'e' = EXTERNAL, 'x' = EXTENDED
-----
CREATE SCHEMA alter_storage;
SET SESSION search_path = 'alter_storage';
CREATE EXTENSION orioledb;

----
-- Test 1: Basic storage attribute changes
----

-- Create a test table with different column types
CREATE TABLE o_storage_test (
    id integer NOT NULL,
    text_col text,
    bytea_col bytea,
    json_col jsonb,
    PRIMARY KEY(id)
) USING orioledb;

-- Check initial storage settings (should be default EXTENDED for varlena types)
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attnum > 0 
    AND NOT attisdropped
ORDER BY attnum;

-- Change text column to EXTERNAL (prefer out-of-line, no compression)
ALTER TABLE o_storage_test ALTER COLUMN text_col SET STORAGE EXTERNAL;

-- Verify the change
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attname = 'text_col';

-- Change bytea column to MAIN (prefer compression, avoid out-of-line)
ALTER TABLE o_storage_test ALTER COLUMN bytea_col SET STORAGE MAIN;

-- Verify the change
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attname = 'bytea_col';

-- Try to change non-existent column (should fail)
\set ON_ERROR_STOP 0
ALTER TABLE o_storage_test ALTER COLUMN nonexistent_col SET STORAGE MAIN;
\set ON_ERROR_STOP 1

-- Try to change integer column to non-PLAIN (should fail)
\set ON_ERROR_STOP 0
ALTER TABLE o_storage_test ALTER COLUMN id SET STORAGE EXTENDED;
\set ON_ERROR_STOP 1

----
-- Test 2: PLAIN storage limitations
----

-- Change bytea column to PLAIN to test limitations
ALTER TABLE o_storage_test ALTER COLUMN bytea_col SET STORAGE PLAIN;

-- Verify the change
SELECT attname, attstorage 
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attname = 'bytea_col';

-- Try to insert data with large bytea (should fail or be limited with PLAIN storage)
\set ON_ERROR_STOP 0
INSERT INTO o_storage_test VALUES (
    10,
    'test plain storage',
    decode(repeat('FF', 5000), 'hex'),
    '{"plain": "test"}'::jsonb
);
\set ON_ERROR_STOP 1

-- Reset bytea column back to MAIN for further tests
ALTER TABLE o_storage_test ALTER COLUMN bytea_col SET STORAGE MAIN;

----
-- Test 3: Data insertion and TOAST behavior
----

-- Insert small data first
INSERT INTO o_storage_test VALUES (
    1, 
    'small text', 
    E'\\x010203',
    '{"test": "data"}'::jsonb
);

-- Insert large data to trigger TOAST behavior
INSERT INTO o_storage_test VALUES (
    2,
    repeat('This is a long text string that should trigger TOAST behavior. ', 100),
    decode(repeat('0102030405060708090A', 500), 'hex'),
    ('{"large": "' || repeat('data for testing TOAST behavior ', 150) || '"}')::jsonb
);

-- Verify data can be retrieved correctly
SELECT 
    id, 
    length(text_col) as text_len,
    length(bytea_col) as bytea_len,
    length(json_col::text) as json_len
FROM o_storage_test 
ORDER BY id;

----
-- Test 4: Storage changes after data exists
----

-- Change storage after data exists (should only affect new data)
ALTER TABLE o_storage_test ALTER COLUMN text_col SET STORAGE MAIN;

-- Insert more data with new storage policy
INSERT INTO o_storage_test VALUES (
    3,
    repeat('New text with MAIN storage policy. ', 100),
    decode(repeat('0A0B0C0D', 300), 'hex'),
    ('{"new": "' || repeat('data with MAIN policy ', 150) || '"}')::jsonb
);

-- Verify all data is still accessible
SELECT 
    id, 
    length(text_col) as text_len,
    length(bytea_col) as bytea_len,
    length(json_col::text) as json_len
FROM o_storage_test 
ORDER BY id;

-- Verify final storage settings
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attnum > 0 
    AND NOT attisdropped
ORDER BY attnum;

----
-- Test 5: Multiple storage changes on same column
----

-- Change text column multiple times
ALTER TABLE o_storage_test ALTER COLUMN text_col SET STORAGE EXTERNAL;
ALTER TABLE o_storage_test ALTER COLUMN text_col SET STORAGE EXTENDED;
ALTER TABLE o_storage_test ALTER COLUMN text_col SET STORAGE MAIN;

-- Verify final state
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attname = 'text_col';

----
-- Test 6: Transaction rollback test
----

BEGIN;
ALTER TABLE o_storage_test ALTER COLUMN json_col SET STORAGE EXTERNAL;

-- Verify change within transaction
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attname = 'json_col';

ROLLBACK;

-- Verify rollback worked
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_test'::regclass 
    AND attname = 'json_col';

----
-- Test 7: Storage with indexes
----

CREATE TABLE o_storage_index_test (
    id integer NOT NULL,
    indexed_text text,
    non_indexed_text text,
    PRIMARY KEY(id)
) USING orioledb;

CREATE INDEX idx_text ON o_storage_index_test(indexed_text);

-- Change storage on indexed column
ALTER TABLE o_storage_index_test ALTER COLUMN indexed_text SET STORAGE EXTERNAL;

-- Change storage on non-indexed column
ALTER TABLE o_storage_index_test ALTER COLUMN non_indexed_text SET STORAGE MAIN;

-- Insert test data
INSERT INTO o_storage_index_test VALUES (
    1,
    repeat('indexed long text ', 50),
    repeat('non-indexed long text ', 50)
);

-- Verify index still works
SELECT count(*) FROM o_storage_index_test WHERE indexed_text LIKE 'indexed%';

-- Verify storage settings
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_index_test'::regclass 
    AND attnum > 0 
    AND NOT attisdropped
ORDER BY attnum;

----
-- Test 8: DEFAULT storage
----

CREATE TABLE o_storage_default_test (
    id integer NOT NULL,
    text_col text,
    PRIMARY KEY(id)
) USING orioledb;

-- Change to non-default, then back to DEFAULT
ALTER TABLE o_storage_default_test ALTER COLUMN text_col SET STORAGE MAIN;
ALTER TABLE o_storage_default_test ALTER COLUMN text_col SET STORAGE DEFAULT;

-- Verify DEFAULT resolves to type's default (EXTENDED for text)
SELECT 
    attname, 
    attstorage
FROM pg_attribute 
WHERE attrelid = 'o_storage_default_test'::regclass 
    AND attname = 'text_col';

----
-- Test 9: Plain varlena
----

CREATE TABLE o_plain_varlena (
    b bytea NOT NULL,
    c numeric(5,0) NOT NULL,
    d bytea NOT NULL
) using orioledb;
ALTER TABLE ONLY o_plain_varlena ALTER COLUMN b SET STORAGE PLAIN;
ALTER TABLE ONLY o_plain_varlena ALTER COLUMN d SET STORAGE PLAIN;

CREATE UNIQUE INDEX plain_varlena_idx ON o_plain_varlena USING btree (b, c, d);

INSERT INTO o_plain_varlena VALUES (
    decode('DEADBEEF', 'hex'),
    1,
    decode('00112233445566778899AABBCCDDEEFF', 'hex')
);

SELECT * from o_plain_varlena;

-- Cleanup
DROP TABLE o_storage_default_test;
DROP TABLE o_storage_index_test;
DROP TABLE o_storage_test;
DROP TABLE o_plain_varlena;

DROP SCHEMA alter_storage CASCADE;
RESET search_path;

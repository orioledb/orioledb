CREATE SCHEMA tuple_chunk;
SET SESSION search_path = 'tuple_chunk';
CREATE EXTENSION orioledb;

CREATE TABLE tuple_chunk_test
(
    id int PRIMARY KEY,
    value text
) USING orioledb;

SELECT test_tuple_chunk_initialize('tuple_chunk_test'::regclass, 2::smallint);

-- Estimate INSERT operation
SELECT test_tuple_chunk_estimate_change(0::smallint, 0::smallint, row(1, 'value 1')::tuple_chunk_test);
-- Perform INSERT operation
SELECT test_tuple_chunk_perform_change(0::smallint, 0::smallint, row(1, 'value 1')::tuple_chunk_test);
SELECT test_tuple_chunk_perform_change(1::smallint, 0::smallint, row(2, 'value 2')::tuple_chunk_test);

SELECT * FROM test_tuple_chunk_read_tuple(0::smallint) AS (id int, value text);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(0, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(1, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(2, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_search(row(0, 'value 0')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_search(row(1, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_search(row(2, 'value 2')::tuple_chunk_test);

-- Estimate UPDATE operation
SELECT test_tuple_chunk_estimate_change(0::smallint, 1::smallint, row(1, 'value 2')::tuple_chunk_test);
-- Estimate DELETE operation
SELECT test_tuple_chunk_estimate_change(0::smallint, 2::smallint, NULL);

-- Perform UPDATE operation
SELECT test_tuple_chunk_perform_change(0::smallint, 1::smallint, row(1, 'value 2')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_read_tuple(0::smallint) AS (id int, value text);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(0, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(1, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(2, 'value 1')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_search(row(1, 'value 1')::tuple_chunk_test);

-- Perform DELETE operation
SELECT test_tuple_chunk_perform_change(0::smallint, 2::smallint, NULL);
SELECT * FROM test_tuple_chunk_read_tuple(0::smallint) AS (id int, value text);
SELECT * FROM test_tuple_chunk_cmp(0::smallint, row(2, 'value 2')::tuple_chunk_test);
SELECT * FROM test_tuple_chunk_search(row(1, 'value 1')::tuple_chunk_test);

SELECT test_tuple_chunk_free();

DROP SCHEMA tuple_chunk CASCADE;
RESET search_path;

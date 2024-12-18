CREATE SCHEMA tuple_chunk;
SET SESSION search_path = 'tuple_chunk';
CREATE EXTENSION orioledb;

CREATE TABLE tuple_chunk_test
(
    id int PRIMARY KEY,
    value text
) USING orioledb;

SELECT test_btree_leaf_tuple_chunk('tuple_chunk_test'::regclass);

SELECT test_btree_leaf_tuple_chunk_builder('tuple_chunk_test'::regclass);

SELECT test_btree_hikey_chunk('tuple_chunk_test'::regclass);

SELECT test_btree_hikey_chunk_builder('tuple_chunk_test'::regclass);

DROP SCHEMA tuple_chunk CASCADE;
RESET search_path;

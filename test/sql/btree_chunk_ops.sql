CREATE SCHEMA chunk_ops;
SET SESSION search_path = 'chunk_ops';
CREATE EXTENSION orioledb;

CREATE TABLE chunk_ops_test
(
    id int PRIMARY KEY,
    value text
) USING orioledb;

SELECT test_btree_leaf_tuple_chunk('chunk_ops_test'::regclass);

SELECT test_btree_leaf_tuple_chunk_builder('chunk_ops_test'::regclass);

SELECT test_btree_hikey_chunk('chunk_ops_test'::regclass);

SELECT test_btree_hikey_chunk_builder('chunk_ops_test'::regclass);

DROP SCHEMA chunk_ops CASCADE;
RESET search_path;

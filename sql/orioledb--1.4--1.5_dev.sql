
-- tuple_chunk implementation testing functions

CREATE FUNCTION test_btree_leaf_tuple_chunk(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_btree_leaf_tuple_chunk_builder(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_btree_hikey_chunk(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_btree_hikey_chunk_builder(relation regclass)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

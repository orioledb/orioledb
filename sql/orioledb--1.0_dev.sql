/* contrib/orioledb/sql/orioledb--1.0.sql */

CREATE FUNCTION orioledb_parallel_debug_start()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION orioledb_parallel_debug_stop()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION s3_get(objectname text)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION s3_put(objectname text, filename text)
RETURNS text
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

-- tuple_chunk implementation testing functions

CREATE FUNCTION test_tuple_chunk_initialize(relation regclass, chunk_type smallint)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_tuple_chunk_estimate_change(item_offset smallint,
                                                 operation smallint,
                                                 tuple record)
RETURNS int
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_tuple_chunk_perform_change(item_offset smallint,
                                                operation smallint,
                                                tuple record)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

CREATE FUNCTION test_tuple_chunk_cmp(item_offset smallint, key record)
RETURNS int
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C STRICT;

CREATE FUNCTION test_tuple_chunk_search(key record)
RETURNS smallint
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C STRICT;

CREATE FUNCTION test_tuple_chunk_read_tuple(item_offset smallint)
RETURNS record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C STRICT;

CREATE FUNCTION test_tuple_chunk_free()
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;

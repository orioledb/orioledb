/* contrib/orioledb/orioledb--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.3'" to load this file. \quit

-------------------------------------
-- Index AM interface functions
-------------------------------------
CREATE FUNCTION orioledb_indexam_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE ACCESS METHOD orioledb_btree TYPE INDEX
HANDLER orioledb_indexam_handler;

CREATE OPERATOR FAMILY orioledb_integer_ops USING orioledb_btree;

CREATE OPERATOR CLASS orioledb_int2_ops DEFAULT
	FOR TYPE int2 USING orioledb_btree FAMILY orioledb_integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint2cmp(int2, int2),
	FUNCTION 2  btint2sortsupport(internal),
	FUNCTION 3  in_range(int2, int2, int2, boolean, boolean),
	FUNCTION 4  btequalimage(oid);

CREATE OPERATOR CLASS orioledb_int4_ops DEFAULT
	FOR TYPE int4 USING orioledb_btree FAMILY orioledb_integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint4cmp(int4, int4),
	FUNCTION 2  btint4sortsupport(internal),
	FUNCTION 3  in_range(int4, int4, int4, boolean, boolean),
	FUNCTION 4  btequalimage(oid);

CREATE OPERATOR CLASS orioledb_int8_ops DEFAULT
	FOR TYPE int8 USING orioledb_btree FAMILY orioledb_integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint8cmp(int8, int8),
	FUNCTION 2  btint8sortsupport(internal),
	FUNCTION 3  in_range(int8, int8, int8, boolean, boolean),
	FUNCTION 4  btequalimage(oid);


ALTER OPERATOR FAMILY orioledb_integer_ops USING orioledb_btree ADD
	-- cross-type comparisons int8 vs int2
	OPERATOR 1 < (int8, int2),
	OPERATOR 2 <= (int8, int2),
	OPERATOR 3 = (int8, int2),
	OPERATOR 4 >= (int8, int2),
	OPERATOR 5 > (int8, int2),
	FUNCTION 1 (int8, int2) btint82cmp(int8, int2),

	-- cross-type comparisons int8 vs int4
	OPERATOR 1 < (int8, int4),
	OPERATOR 2 <= (int8, int4),
	OPERATOR 3 = (int8, int4),
	OPERATOR 4 >= (int8, int4),
	OPERATOR 5 > (int8, int4),
	FUNCTION 1 (int8, int4) btint84cmp(int8, int4),

	-- cross-type comparisons int4 vs int2
	OPERATOR 1 < (int4, int2),
	OPERATOR 2 <= (int4, int2),
	OPERATOR 3 = (int4, int2),
	OPERATOR 4 >= (int4, int2),
	OPERATOR 5 > (int4, int2),
	FUNCTION 1 (int4, int2) btint42cmp(int4, int2),

	-- cross-type comparisons int4 vs int8
	OPERATOR 1 < (int4, int8),
	OPERATOR 2 <= (int4, int8),
	OPERATOR 3 = (int4, int8),
	OPERATOR 4 >= (int4, int8),
	OPERATOR 5 > (int4, int8),
	FUNCTION 1 (int4, int8) btint48cmp(int4, int8),

	-- cross-type comparisons int2 vs int8
	OPERATOR 1 < (int2, int8),
	OPERATOR 2 <= (int2, int8),
	OPERATOR 3 = (int2, int8),
	OPERATOR 4 >= (int2, int8),
	OPERATOR 5 > (int2, int8),
	FUNCTION 1 (int2, int8) btint28cmp(int2, int8),

	-- cross-type comparisons int2 vs int4
	OPERATOR 1 < (int2, int4),
	OPERATOR 2 <= (int2, int4),
	OPERATOR 3 = (int2, int4),
	OPERATOR 4 >= (int2, int4),
	OPERATOR 5 > (int2, int4),
	FUNCTION 1 (int2, int4) btint24cmp(int2, int4),

	-- cross-type in_range functions
	FUNCTION 3 (int4, int8) in_range(int4, int4, int8, boolean, boolean),
	FUNCTION 3 (int4, int2) in_range(int4, int4, int2, boolean, boolean),
	FUNCTION 3 (int2, int8) in_range(int2, int2, int8, boolean, boolean),
	FUNCTION 3 (int2, int4) in_range(int2, int2, int4, boolean, boolean);

CREATE OPERATOR FAMILY orioledb_text_ops_fam USING orioledb_btree;

CREATE OPERATOR CLASS orioledb_text_ops DEFAULT
	FOR TYPE text USING orioledb_btree FAMILY orioledb_text_ops_fam AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  bttextcmp(text, text),
	FUNCTION 2  bttextsortsupport(internal),
	FUNCTION 4  btvarstrequalimage(oid);

CREATE OPERATOR FAMILY orioledb_bool_ops_fam USING orioledb_btree;

CREATE OPERATOR CLASS orioledb_bool_ops DEFAULT
	FOR TYPE bool USING orioledb_btree FAMILY orioledb_bool_ops_fam AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       btboolcmp(bool,bool),
    FUNCTION        4       btequalimage(oid);
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

CREATE ACCESS METHOD orioledb_btree TYPE INDEX FOR orioledb
HANDLER orioledb_indexam_handler;

CREATE OPERATOR FAMILY integer_ops USING orioledb_btree;

CREATE OPERATOR CLASS int2_ops DEFAULT
	FOR TYPE int2 USING orioledb_btree FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint2cmp(int2, int2),
	FUNCTION 2  btint2sortsupport(internal),
	FUNCTION 3  in_range(int2, int2, int2, boolean, boolean),
	FUNCTION 4  btequalimage(oid),

	-- cross-type comparisons int2 vs int4
	OPERATOR 1 < (int2, int4),
	OPERATOR 2 <= (int2, int4),
	OPERATOR 3 = (int2, int4),
	OPERATOR 4 >= (int2, int4),
	OPERATOR 5 > (int2, int4),
	FUNCTION 1 (int2, int4) btint24cmp(int2, int4),
	FUNCTION 3 (int2, int4) in_range(int2, int2, int4, boolean, boolean),

	-- cross-type comparisons int2 vs int8
	OPERATOR 1 < (int2, int8),
	OPERATOR 2 <= (int2, int8),
	OPERATOR 3 = (int2, int8),
	OPERATOR 4 >= (int2, int8),
	OPERATOR 5 > (int2, int8),
	FUNCTION 1 (int2, int8) btint28cmp(int2, int8),
	FUNCTION 3 (int2, int8) in_range(int2, int2, int8, boolean, boolean);

CREATE OPERATOR CLASS int4_ops DEFAULT
	FOR TYPE int4 USING orioledb_btree FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint4cmp(int4, int4),
	FUNCTION 2  btint4sortsupport(internal),
	FUNCTION 3  in_range(int4, int4, int4, boolean, boolean),
	FUNCTION 4  btequalimage(oid),

	-- cross-type comparisons int4 vs int2
	OPERATOR 1 < (int4, int2),
	OPERATOR 2 <= (int4, int2),
	OPERATOR 3 = (int4, int2),
	OPERATOR 4 >= (int4, int2),
	OPERATOR 5 > (int4, int2),
	FUNCTION 1 (int4, int2) btint42cmp(int4, int2),
	FUNCTION 3 (int4, int2) in_range(int4, int4, int2, boolean, boolean),

	-- cross-type comparisons int4 vs int8
	OPERATOR 1 < (int4, int8),
	OPERATOR 2 <= (int4, int8),
	OPERATOR 3 = (int4, int8),
	OPERATOR 4 >= (int4, int8),
	OPERATOR 5 > (int4, int8),
	FUNCTION 1 (int4, int8) btint48cmp(int4, int8),
	FUNCTION 3 (int4, int8) in_range(int4, int4, int8, boolean, boolean);

CREATE OPERATOR CLASS int8_ops DEFAULT
	FOR TYPE int8 USING orioledb_btree FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint8cmp(int8, int8),
	FUNCTION 2  btint8sortsupport(internal),
	FUNCTION 3  in_range(int8, int8, int8, boolean, boolean),
	FUNCTION 4  btequalimage(oid),

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
	FUNCTION 1 (int8, int4) btint84cmp(int8, int4);

CREATE OPERATOR FAMILY text_ops USING orioledb_btree;

CREATE OPERATOR CLASS text_ops DEFAULT
	FOR TYPE text USING orioledb_btree FAMILY text_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  bttextcmp(text, text),
	FUNCTION 2  bttextsortsupport(internal),
	FUNCTION 4  btvarstrequalimage(oid);

CREATE OPERATOR FAMILY bool_ops USING orioledb_btree;

CREATE OPERATOR CLASS bool_ops DEFAULT
	FOR TYPE bool USING orioledb_btree FAMILY bool_ops AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       btboolcmp(bool,bool),
    FUNCTION        4       btequalimage(oid);

CREATE OPERATOR FAMILY datetime_ops USING orioledb_btree;

CREATE OPERATOR CLASS date_ops DEFAULT
	FOR TYPE date USING orioledb_btree FAMILY datetime_ops AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 date_cmp(date,date),
    FUNCTION 2 date_sortsupport(internal),
	FUNCTION 3 in_range(date,date,interval,bool,bool),
    FUNCTION 4 btequalimage(oid),

    OPERATOR 1 <(date, timestamp),
    OPERATOR 2 <=(date, timestamp),
    OPERATOR 3 =(date, timestamp),
    OPERATOR 4 >=(date, timestamp),
    OPERATOR 5 >(date, timestamp),
    FUNCTION 1 (date,timestamp) date_cmp_timestamp(date,timestamp),

    OPERATOR 1 <(date, timestamptz),
    OPERATOR 2 <=(date, timestamptz),
    OPERATOR 3 =(date, timestamptz),
    OPERATOR 4 >=(date, timestamptz),
    OPERATOR 5 >(date, timestamptz),
    FUNCTION 1 (date,timestamptz) date_cmp_timestamptz(date,timestamptz);

CREATE OPERATOR CLASS timestamptz_ops DEFAULT
	FOR TYPE timestamptz USING orioledb_btree FAMILY datetime_ops AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 timestamptz_cmp(timestamptz,timestamptz),
    FUNCTION 2 timestamp_sortsupport(internal),
	FUNCTION 3 in_range(timestamptz,timestamptz,interval,bool,bool),
    FUNCTION 4 btequalimage(oid),

    OPERATOR 1 <(timestamptz, date),
    OPERATOR 2 <=(timestamptz, date),
    OPERATOR 3 =(timestamptz, date),
    OPERATOR 4 >=(timestamptz, date),
    OPERATOR 5 >(timestamptz, date),
    FUNCTION 1 (timestamptz,date) timestamptz_cmp_date(timestamptz,date),

    OPERATOR 1 <(timestamptz, timestamp),
    OPERATOR 2 <=(timestamptz, timestamp),
    OPERATOR 3 =(timestamptz, timestamp),
    OPERATOR 4 >=(timestamptz, timestamp),
    OPERATOR 5 >(timestamptz, timestamp),
    FUNCTION 1 (timestamptz,timestamp) timestamptz_cmp_timestamp(timestamptz,timestamp);

CREATE OPERATOR CLASS timestamp_ops DEFAULT
	FOR TYPE timestamp USING orioledb_btree FAMILY datetime_ops AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 timestamp_cmp(timestamp,timestamp),
    FUNCTION 2 timestamp_sortsupport(internal),
	FUNCTION 3 in_range(timestamp,timestamp,interval,bool,bool),
    FUNCTION 4 btequalimage(oid),

    OPERATOR 1 <(timestamp,date),
    OPERATOR 2 <=(timestamp,date),
    OPERATOR 3 =(timestamp,date),
    OPERATOR 4 >=(timestamp,date),
    OPERATOR 5 >(timestamp,date),
    FUNCTION 1 (timestamp,date) timestamp_cmp_date(timestamp,date),

    OPERATOR 1 <(timestamp,timestamptz),
    OPERATOR 2 <=(timestamp,timestamptz),
    OPERATOR 3 =(timestamp,timestamptz),
    OPERATOR 4 >=(timestamp,timestamptz),
    OPERATOR 5 >(timestamp,timestamptz),
    FUNCTION 1 (timestamp,timestamptz) timestamp_cmp_timestamptz(timestamp,timestamptz);